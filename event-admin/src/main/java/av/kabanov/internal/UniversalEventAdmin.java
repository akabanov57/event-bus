package av.kabanov.internal;

import av.kabanov.event.Event;
import av.kabanov.event.EventAdmin;
import av.kabanov.event.EventExceptionHandler;
import av.kabanov.event.Subscriber;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class UniversalEventAdmin implements EventAdmin {

  // Для точных совпадений (O(1))
  private final ConcurrentMap<String, Set<Consumer<Event>>> exactSubscriptions = new ConcurrentHashMap<>();
  // Для подписок с вайлдкардом (например, "app/orders/*")
  private final ConcurrentMap<String, Set<Consumer<Event>>> wildcardSubscriptions = new ConcurrentHashMap<>();

  // Глобальный лимит безопасности (верхний порог для всей JVM, чтобы не упасть по памяти)
  private final Semaphore globalSemaphore = new Semaphore(20_000);

  // Потокобезопасная карта семафоров для каждого корневого модуля/топика
  private final ConcurrentMap<String, Semaphore> rootSemaphores = new ConcurrentHashMap<>();

  // Лимит на один корневой топик (квота)
  private static final int MAX_PER_ROOT_TOPIC = 1_000;

  // Очереди последовательной обработки для каждого уникального routingKey
  private final ConcurrentMap<String, SequentialQueueProcessing> KeyProcessors = new ConcurrentHashMap<>();

  // Пул, выделяющий под каждую задачу легковесный виртуальный поток Java (Project Loom)
  private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
  private final EventExceptionHandler exceptionHandler;

  // Флаг состояния жизненного цикла шины
  private volatile boolean closed = false;

  public UniversalEventAdmin(EventExceptionHandler exceptionHandler) {
    this.exceptionHandler = exceptionHandler != null ? exceptionHandler : this::defaultErrorHandler;
  }

  /**
   * Проверка доступности шины. Запрещает операции после вызова close().
   */
  private void checkNotClosed() {
    if (closed) {
      throw new IllegalStateException("Шина событий закрыта и больше не принимает запросы.");
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <E extends Event> Subscriber subscribe(String topicPattern, Class<E> eventClass,
      Consumer<E> listener) {
    checkNotClosed(); // Защита: нельзя подписываться на закрытую шину

    if (listener == null || topicPattern == null || eventClass == null) {
      return () -> {
      }; // Возвращаем пустой токен в случае некорректных аргументов
    }

    String listenerName = listener.getClass().getName();

    // Создаем уникальную безопасную обертку в памяти.
    Consumer<Event> safeListener = event -> {
      if (eventClass.isInstance(event)) {
        try {
          listener.accept((E) event);
        } catch (Throwable t) {
          exceptionHandler.handle(event, t, listenerName);
        }
      }
    };

    // Разделяем хранение в зависимости от типа маски
    boolean isWildcard = topicPattern.endsWith("*");
    ConcurrentMap<String, Set<Consumer<Event>>> targetMap =
        isWildcard ? wildcardSubscriptions : exactSubscriptions;

    // Добавляем созданную обертку в потокобезопасный Set для данного топика
    targetMap.computeIfAbsent(topicPattern, _ -> new CopyOnWriteArraySet<>()).add(safeListener);

    // Возвращаем лямбда-токен отмены подписки.
    return () -> {
      // Если шина уже закрыта, вызов close() на токене безопасен и ничего не делает,
      // так как вся карта subscriptions уже была очищена глобально.
      if (closed) {
        return;
      }
      Set<Consumer<Event>> listeners = targetMap.get(topicPattern);
      if (listeners != null) {
        listeners.remove(safeListener);
        if (listeners.isEmpty()) {
          targetMap.remove(topicPattern, listeners);
        }
      }
    };
  }

  /**
   * Извлекает корневой сегмент топика.
   * Например: "auth/user/login" -> "auth", "heavy/data/analytics" -> "heavy".
   * Если слэша нет, топик сам является корневым.
   */
  private String getRootTopic(String topic) {
    int slashIndex = topic.indexOf('/');
    if (slashIndex > 0) {
      return topic.substring(0, slashIndex);
    }
    return topic;
  }

  @Override
  public void postEvent(Event event) {
    checkNotClosed();
    if (event == null) return;

    String routingKey = event.routingKey();

    // Если ключ пустой, обрабатываем конкурентно (как раньше), порядок не важен
    if (routingKey == null || routingKey.isEmpty()) {
      submitConcurrent(event);
      return;
    }

    // Если ключ есть, маршрутизируем в последовательную очередь для этого ключа
    // Цикл гарантирует: если мы заберем процессор, который старый поток прямо сейчас
    // удаляет через remove(key, this), мы обнаружим это и зайдем на вторую итерацию,
    // создав абсолютно чистый и актуальный процессор.
    while (true) {
      SequentialQueueProcessing processor = KeyProcessors.computeIfAbsent(
          routingKey,
          SequentialQueueProcessing::new
      );

      processor.enqueue(event);

      // Защитная проверка: если пока мы добавляли событие, старый поток-обработчик
      // УСПЕЛ удалить этот процессор из карты (потому что на момент его проверки queue была пуста)
      if (KeyProcessors.get(routingKey) != processor) {
        // Значит, наше событие застряло в "призрачном" процессоре, который выпал из карты.
        // Извлекаем его обратно и повторяем попытку в цикле для нового процессора.
        if (processor.queue.remove(event)) {
          continue;
        }
      }
      break;
    }
  }

  private void submitConcurrent(Event event) {
    String rootTopic = getRootTopic(event.topic());
    Semaphore rootSemaphore = rootSemaphores.computeIfAbsent(rootTopic, _ -> new Semaphore(MAX_PER_ROOT_TOPIC));

    try {
      // 1. Занимаем локальную квоту топика (Bulkhead)
      rootSemaphore.acquire();

      try {
        // 2. Занимаем общее место в глобальной шине безопасности
        globalSemaphore.acquire();

        // 3. Передаем задачу в пул виртуальных потоков
        try {
          executor.submit(() -> {
            try {
              deliverAsync(event);
            } catch (Throwable t) {
              // Все исключения подписчиков уже изолированы внутри safeListener,
              // но этот catch гарантирует стабильность ядра в случае непредвиденных сбоев
            } finally {
              // Освобождаем оба семафора строго внутри виртуального потока ПОСЛЕ выполнения задачи
              globalSemaphore.release();
              rootSemaphore.release();
            }
          });
        } catch (RejectedExecutionException e) {
          // Если пул закрылся в момент между проверкой closed и submit:
          // 1. Обязательно освобождаем захваченные семафоры!
          globalSemaphore.release();
          rootSemaphore.release();

          // 2. Транслируем в ожидаемое контрактом исключение
          throw new IllegalStateException("Шина событий закрыта и больше не принимает запросы.", e);
        }

      } catch (InterruptedException e) {
        // Сюда попадаем, только если продюсера прервали во время ожидания globalSemaphore.
        // Значит, локальную квоту rootSemaphore мы занять УСПЕЛИ — возвращаем её.
        rootSemaphore.release();
        Thread.currentThread().interrupt();
      }

    } catch (InterruptedException e) {
      // Сюда попадаем, если поток-продюсер прервали на самом первом acquire().
      // Ни один семафор еще не был занят, откатывать ничего не нужно.
      Thread.currentThread().interrupt();
    }
  }

  private void deliverAsync(Event event) {
    if (closed) {
      return;
    }
    List<Consumer<Event>> targets = getMatchingListeners(event.topic());
    for (Consumer<Event> listener : targets) {
      // Для полной асинхронности можем отдавать каждого подписчика в пул
      executor.submit(() -> {
        if (!closed) {
          listener.accept(event);
        }
      });
    }
  }

  // Синхронный последовательный вызов подписчиков (для FIFO режима внутри актора)
  private void deliverSync(Event event) {
    if (closed) return;
    List<Consumer<Event>> targets = getMatchingListeners(event.topic());
    for (Consumer<Event> listener : targets) {
      // Внимание: вызываем .accept() прямо в текущем виртуальном потоке актора!
      // Благодаря этому событие №1 гарантированно не начнется, пока событие №0 не завершит ВСЕХ подписчиков.
      listener.accept(event);
    }
  }

  /**
   * Быстрый сбор всех подходящих подписчиков без полного перебора exact-карты.
   */
  private List<Consumer<Event>> getMatchingListeners(String eventTopic) {
    List<Consumer<Event>> matched = new ArrayList<>();

    // 1. Точное совпадение за O(1)
    Set<Consumer<Event>> exact = exactSubscriptions.get(eventTopic);
    if (exact != null) {
      matched.addAll(exact);
    }

    // 2. Проверка только масок с "*" (их обычно сильно меньше, чем точных топиков)
    if (!wildcardSubscriptions.isEmpty()) {
      for (var entry : wildcardSubscriptions.entrySet()) {
        if (matchesWildcard(eventTopic, entry.getKey())) {
          matched.addAll(entry.getValue());
        }
      }
    }
    return matched;
  }

  /**
   * Проверка маски по правилам OSGi (токен/*)
   */
  private boolean matchesWildcard(String eventTopic, String pattern) {
    // Убираем звездочку (например, "app/orders/*" -> "app/orders/")
    String prefix = pattern.substring(0, pattern.length() - 1);
    return eventTopic.startsWith(prefix);
  }

  private void defaultErrorHandler(Event event, Throwable t, String listenerName) {
    System.err.printf("[EventBus System Error] Listener '%s' failed on topic '%s'. Details: %s%n",
        listenerName, event.topic(), t.getMessage());
  }

  @Override
  public void close() {
    if (closed) {
      return; // Защита от повторного вызова close()
    }

    // Мгновенно блокируем новые операции добавления и отправки
    closed = true;

    // Очищаем карты подписок, разрывая сильные ссылки для Garbage Collector (GC)
    exactSubscriptions.clear();
    wildcardSubscriptions.clear();
    KeyProcessors.clear();
    rootSemaphores.clear();

    // Инициируем вежливое завершение работы пула виртуальных потоков
    executor.shutdown();
    try {
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Внутренний мини-актор для последовательной обработки событий с одинаковым ключом.
   */
  private class SequentialQueueProcessing {
    private final ConcurrentLinkedQueue<Event> queue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final String routingKey;

    public SequentialQueueProcessing(String routingKey) {
      this.routingKey = routingKey;
    }

    public void enqueue(Event event) {
      queue.add(event);
      trySchedule();
    }

    private void trySchedule() {
      // Если в данный момент этот ключ никто не обрабатывает, захватываем право на выполнение
      if (isRunning.compareAndSet(false, true)) {
        executor.submit(this::processQueue);
      }
    }

    private void processQueue() {
      try {
        while (true) {
          Event event = queue.poll();
          if (event == null) {
            break; // Очередь пуста
          }

          // Вызываем доставку. На время обработки ЭТОГО события
          // другие события с ЭТИМ ЖЕ ключом будут просто копиться в queue
          deliverSync(event);
        }
      } finally {
        // 1. Сначала объявляем, что обработка завершена
        isRunning.set(false);

        // 2. Проверяем: не успел ли продюсер положить что-то новое в очередь
        // в промежутке между poll() == null и сбросом флага isRunning?
        if (!queue.isEmpty()) {
          // Если успел, пытаемся запуститься заново
          trySchedule();
        } else {
          // 3. Если очередь пуста, пытаемся удалить ИМЕННО ЭТОТ процессор из глобальной карты.
          // Метод remove(key, value) гарантирует: если продюсер в этот момент затер
          // или перезаписал ключ, удаления не произойдет.
          KeyProcessors.remove(routingKey, this);
        }
      }
    }
  }

}

