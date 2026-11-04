package av.kabanov.internal;

import av.kabanov.event.Event;
import av.kabanov.event.EventAdmin;
import av.kabanov.event.EventAdminFactory;
import av.kabanov.event.Subscriber;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.*;

class UniversalEventAdminTest {

  // Вспомогательный класс события для тестов
  record TestEvent(String topic, String routingKey, int sequenceNumber) implements Event {}

  /**
   * Создается сильное конкурентное давление (100 последовательных событий). Внутрь обработчика
   * закладывается случайная микрозадержка. Если бы события обрабатывались параллельно в разных
   * виртуальных потоках, порядок чисел в финальном списке гарантированно перемешался бы. Тест
   * подтверждает, что наш внутренний актор SequentialQueueProcessing держит строгую очередность.
   *
   * @throws InterruptedException see latch.await(5, TimeUnit.SECONDS);
   */
  @Test
  @DisplayName("1. Тест FIFO: События с одинаковым routingKey должны обрабатываться строго по порядку")
  void testStrictFifoOrderingByRoutingKey() throws InterruptedException {
    try (EventAdmin eventAdmin = EventAdminFactory.create()) {
      int eventsCount = 100;
      String topic = "orders/processed";
      String accountKey = "account-42";

      // Сюда подписчик будет складывать пришедшие номера последовательности
      List<Integer> receivedOrder = new CopyOnWriteArrayList<>();

      // Будем использовать защелку, чтобы поток теста дождался выполнения всех 100 событий
      CountDownLatch latch = new CountDownLatch(eventsCount);

      eventAdmin.subscribe(topic, TestEvent.class, event -> {
        // Имитируем небольшую случайную задержку, чтобы спровоцировать race condition,
        // если бы события обрабатывались конкурентно, а не последовательно
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos((long) (Math.random() * 5)));

        receivedOrder.add(event.sequenceNumber());
        latch.countDown();
      });

      // Активно публикуем события одно за другим
      for (int i = 0; i < eventsCount; i++) {
        eventAdmin.postEvent(new TestEvent(topic, accountKey, i));
      }

      // Ждем завершения обработки всех событий (максимум 5 секунд)
      boolean completed = latch.await(5, TimeUnit.SECONDS);
      assertTrue(completed, "Обработка событий затянулась или зависла");

      // Проверяем, что порядок чисел строго возрастающий (от 0 до 99)
      assertEquals(eventsCount, receivedOrder.size());
      for (int i = 0; i < eventsCount; i++) {
        assertEquals(i, receivedOrder.get(i), "Порядок FIFO нарушен на элементе: " + i);
      }
    }
  }

  /**
   * Эмулирует сценарий «плохого соседа». Мы отправляем событие в тяжелый модуль и полностью
   * блокируем его рабочий поток. Затем отправляем событие в легкий модуль. Тест проверяет, что
   * легкий модуль успевает выполниться за миллисекунды, доказывая, что семафоры квот по топикам
   * успешно изолируют ресурсы.
   *
   * @throws InterruptedException see heavyStartedLatch.await(1, TimeUnit.SECONDS)
   */
  @Test
  @DisplayName("2. Тест Bulkhead: Зависание одного корневого топика не должно блокировать быстрые события в других топиках")
  void testBulkheadIsolationBetweenRootTopics() throws InterruptedException {
    EventAdmin eventAdmin = EventAdminFactory.create();

    String heavyTopic = "heavy/data/analytics";
    String fastTopic = "auth/user/login";

    CountDownLatch heavyStartedLatch = new CountDownLatch(1);
    CountDownLatch fastFinishedLatch = new CountDownLatch(1);
    AtomicInteger fastExecutionCounter = new AtomicInteger(0);

    // Подписчик на "тяжелый" топик, который намертво засыпает при получении события
    eventAdmin.subscribe(heavyTopic, TestEvent.class, _ -> {
      heavyStartedLatch.countDown(); // Сигнализируем, что тяжелый поток пошел в обработку
      LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10)); // Имитируем долгое зависание
    });

    // Подписчик на "быстрый" топик
    eventAdmin.subscribe(fastTopic, TestEvent.class, _ -> {
      fastExecutionCounter.incrementAndGet();
      fastFinishedLatch.countDown();
    });

    // 1. Отправляем событие в тяжелый топик
    eventAdmin.postEvent(new TestEvent(heavyTopic, "", 0));

    // Ждем, пока тяжелый топик действительно начнет обрабатываться и зависнет
    assertTrue(heavyStartedLatch.await(1, TimeUnit.SECONDS));

    // 2. Отправляем событие в быстрый топик, пока тяжелый всё еще висит
    eventAdmin.postEvent(new TestEvent(fastTopic, "", 0));

    // Быстрое событие должно выполниться мгновенно, не дожидаясь отвисания тяжелого
    boolean fastCompleted = fastFinishedLatch.await(500, TimeUnit.MILLISECONDS);

    assertTrue(fastCompleted,
        "Быстрое событие заблокировалось из-за зависшего тяжелого топика (Паттерн Bulkhead сломан!)");
    assertEquals(1, fastExecutionCounter.get());

    eventAdmin.close();
  }

  /**
   * Проверяет, что выброшенный NullPointerException из лямбды подписчика не приводит к поломке пула
   * виртуальных потоков и корректно перенаправляется в кастомный EventExceptionHandler. Также
   * проверяется, что после инцидента шина сохраняет работоспособность и продолжает доставлять
   * последующие события.
   *
   * @throws InterruptedException see exceptionLatch.await(1, TimeUnit.SECONDS)
   */
  @Test
  @DisplayName("3. Тест Исключений: Ошибки бизнес-логики подписчиков не должны ломать шину и должны перехватываться")
  void testExceptionHandlerIsolatesBusinessErrors() throws InterruptedException {
    AtomicReference<Throwable> caughtThrowable = new AtomicReference<>();
    AtomicReference<String> caughtListenerName = new AtomicReference<>();
    CountDownLatch exceptionLatch = new CountDownLatch(1);
    CountDownLatch nextEventLatch = new CountDownLatch(1);

    String topic = "billing/invoice";

    // Создаем шину со специальным хэндлером для верификации ошибок
    EventAdmin eventAdmin = EventAdminFactory.create((_, throwable, listenerName) -> {
      caughtThrowable.set(throwable);
      caughtListenerName.set(listenerName);
      exceptionLatch.countDown(); // Сигнализируем о перехвате ошибки
    });

    // Подписчик, который гарантированно падает
    eventAdmin.subscribe(topic, TestEvent.class, event -> {
      if (event.sequenceNumber() == 1) {
        throw new NullPointerException("Бизнес-ошибка: баланс не найден");
      } else {
        nextEventLatch.countDown(); // Второе событие должно пройти успешно
      }
    });

    // Отправляем первое событие, провоцирующее краш
    eventAdmin.postEvent(new TestEvent(topic, "", 1));
    assertTrue(exceptionLatch.await(1, TimeUnit.SECONDS), "Хэндлер исключений не вызвался");

    // Проверяем, что ошибка дошла до обработчика в неизменном виде
    assertNotNull(caughtThrowable.get());
    assertInstanceOf(NullPointerException.class, caughtThrowable.get());
    assertEquals("Бизнес-ошибка: баланс не найден", caughtThrowable.get().getMessage());
    assertTrue(caughtListenerName.get().contains("Lambda"),
        "Имя слушателя должно указывать на класс подписчика");

    // Проверяем жизнеспособность шины: отправляем второе событие в тот же топик
    eventAdmin.postEvent(new TestEvent(topic, "", 2));
    assertTrue(nextEventLatch.await(1, TimeUnit.SECONDS),
        "Шина упала или перестала доставлять события после исключения");

    eventAdmin.close();
  }

  /**
   * Убеждается в корректности OSGi-like синтаксиса разбора строк, гарантируя, что префиксный поиск
   * по маске типа app/events/* успешно перехватывает глубокие иерархические топики.
   *
   * @throws InterruptedException see latch.await(1, TimeUnit.SECONDS)
   */
  @Test
  @DisplayName("4. Тест масок топиков: Подписка на wildcard должна корректно ловить вложенные структуры")
  void testWildcardTopicMatching() throws InterruptedException {
    try (EventAdmin eventAdmin = EventAdminFactory.create()) {
      String pattern = "app/events/*";
      String concreteTopic = "app/events/user/registered";

      CountDownLatch latch = new CountDownLatch(1);

      // Подписываемся на маску со звездочкой
      eventAdmin.subscribe(pattern, TestEvent.class, _ -> latch.countDown());

      // Публикуем на конкретный топик, подпадающий под префикс
      eventAdmin.postEvent(new TestEvent(concreteTopic, "", 0));

      assertTrue(latch.await(1, TimeUnit.SECONDS), "Событие не дошло по wildcard-выражению");
    }
  }

  /**
   * Проверяет соблюдение строгого контракта жизненного цикла. Если методы вызываются на закрытом
   * объекте, они обязаны немедленно сигнализировать вызывающему коду через IllegalStateException,
   * предотвращая утечку данных «в пустоту».
   */
  @Test
  @DisplayName("5. Граничный случай: subscribe() после close() должен гарантированно выбрасывать IllegalStateException")
  void testSubscribeAfterCloseThrowsException() {
    EventAdmin eventAdmin = EventAdminFactory.create();
    eventAdmin.close(); // Закрываем шину до любых операций

    // Проверяем, что шина жестко блокирует новые подписки
    assertThrows(IllegalStateException.class, () ->
        eventAdmin.subscribe("any/topic", TestEvent.class, _ -> {
        }), "Шина должна запрещать подписки после закрытия");
  }

  /**
   * Проверяет соблюдение строгого контракта жизненного цикла. Если методы вызываются на закрытом
   * объекте, они обязаны немедленно сигнализировать вызывающему коду через IllegalStateException,
   * предотвращая утечку данных «в пустоту».
   */
  @Test
  @DisplayName("6. Граничный случай: postEvent() после close() должен гарантированно выбрасывать IllegalStateException")
  void testPostEventAfterCloseThrowsException() {
    EventAdmin eventAdmin = EventAdminFactory.create();
    eventAdmin.close(); // Закрываем шину

    // Проверяем, что продюсеры не могут отправлять данные в закрытую систему
    assertThrows(IllegalStateException.class, () ->
            eventAdmin.postEvent(new TestEvent("any/topic", "", 1))
        , "Шина должна запрещать публикацию событий после закрытия");
  }

  /**
   * Имитирует жесткий сценарий завершения работы приложения (Graceful Shutdown) под нагрузкой. Один
   * поток бесконечно генерирует события, забивая семафоры и очереди, а основной поток внезапно
   * вызывает eventAdmin.close(). Тест проверяет, что внутренний executor.shutdown() и очистка
   * ресурсов проходят корректно, не приводя к Deadlock (взаимной блокировке потоков) и не
   * выбрасывая неожиданных ошибок в поток, который инициировал закрытие.
   *
   * @throws InterruptedException see producerStarted.await(500, TimeUnit.MILLISECONDS)
   */
  @Test
  @DisplayName("7. Граничный случай: Интенсивная публикация параллельно с close() не должна приводить к зависанию или OOM")
  void testConcurrentPostEventAndClose() throws InterruptedException {
    EventAdmin eventAdmin = EventAdminFactory.create();
    String topic = "concurrent/shutdown";

    // Регистрируем пустой подписчик
    eventAdmin.subscribe(topic, TestEvent.class, _ -> {
    });

    // Флаг для остановки фонового потока-продюсера
    AtomicBoolean stopProducer = new AtomicBoolean(false);
    CountDownLatch producerStarted = new CountDownLatch(1);

    // Запускаем виртуальный поток, который непрерывно спамит событиями
    Thread.ofVirtual().start(() -> {
      producerStarted.countDown();
      int seq = 0;
      while (!stopProducer.get()) {
        try {
          eventAdmin.postEvent(new TestEvent(topic, "", seq++));
        } catch (IllegalStateException e) {
          // Ожидаемый перехват, когда шина закроется посреди цикла
          break;
        }
      }
    });

    // Ждем, чтобы продюсер успел разогнаться
    assertTrue(producerStarted.await(500, TimeUnit.MILLISECONDS));

    // Даем продюсеру поработать 50 миллисекунд вхолостую
    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));

    // Вызываем close() прямо во время агрессивной записи
    assertDoesNotThrow(eventAdmin::close,
        "Вызов close() параллельно с postEvent не должен бросать исключений в вызывающем потоке");

    // Останавливаем продюсера (на случай, если он не вылетел по IllegalStateException)
    stopProducer.set(true);

    eventAdmin.close();
  }

  /**
   * Тестирует идемпотентность токенов подписки. Часто в DI-контейнерах (Spring, Micronaut, OSGi)
   * компоненты уничтожаются в случайном порядке. Если сама шина EventAdmin разрушилась первой,
   * бин-подписчик в своем методе @PreDestroy всё равно попытается вызвать subscriber.close(). Наш
   * лямбда-токен в реализации защищен проверкой if (closed) return;, и этот тест гарантирует, что
   * такое поведение останется стабильным.
   */
  @Test
  @DisplayName("8. Граничный случай: Отмена подписки (Subscriber.close()) после close() шины должна быть идемпотентной и безопасной")
  void testSubscriberCloseAfterAdminClose() {
    EventAdmin eventAdmin = EventAdminFactory.create();

    // Создаем активную подписку
    Subscriber subscriber = eventAdmin.subscribe("lifecycle/test", TestEvent.class, _ -> {
    });

    // Сначала закрываем всю шину (она очищает внутренние карты subscriptions)
    eventAdmin.close();

    // Теперь вызываем close() на токене подписки.
    // Этот вызов не должен приводить к NullPointerException, IllegalStateException или другим ошибкам.
    assertDoesNotThrow(subscriber::close,
        "Отмена конкретной подписки после закрытия всей шины должна быть безопасной");
  }

}
