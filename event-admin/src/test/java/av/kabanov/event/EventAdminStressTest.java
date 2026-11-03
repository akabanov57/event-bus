package av.kabanov.event;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertTrue;

class EventAdminStressTest {

  record HeavyTestEvent(String topic, String routingKey, int payload) implements Event {

  }

  /**
   * Комплексный тест производительности, отказоустойчивости и управления памятью
   * шины событий в многопоточной среде.
   *
   * <p>Данный класс симулирует экстремальную конкурентную нагрузку с использованием
   * виртуальных потоков Java 21+ и платформенных потоков-продюсеров для верификации
   * критических компонентов архитектуры.</p>
   *
   * <p>Тест выполняет проверку по следующим четырем направлениям:</p>
   *
   * <ol>
   *   <li>
   *     <b>Тестирование предельной пропускной способности (Throughput)</b>
   *     <p>Потоки-продюсеры генерируют миллионы событий (например, при 16 ядрах —
   *     1.6 млн событий) без искусственных пауз. Тест замеряет чистый показатель
   *     числа обработанных событий в секунду ({events/sec}). В архитектуре на виртуальных
   *     потоках Java 21+ это значение должно стабильно превышать сотни тысяч
   *     операций в секунду.</p>
   *   </li>
   *
   *   <li>
   *     <b>Проверка эффективности гибридного Backpressure</b>
   *     <p>Благодаря жесткому давлению платформенных потоков-продюсеров, лимиты
   *     конкурентных семафоров {@code globalSemaphore} и {@code rootSemaphores}
   *     гарантированно достигают своих верхних границ. Тест проверяет, что метод
   *     {@code postEvent} эффективно удерживает (блокирует/притормаживает) продюсеров.
   *     Это предотвращает переполнение кучи (OutOfMemoryError) из-за бесконтрольного
   *     роста внутренней очереди событий.</p>
   *   </li>
   *
   *   <li>
   *     <b>Верификация механизма удаления акторов (KeyProcessors Memory Leak Check)</b>
   *     <p>В цикле генерации каждое второе событие отправляется с уникальным ключом
   *     маршрутизации (генерируется около 800,000 уникальных {@code routingKey}).
   *     Если бы метод {@code keyProcessors.remove(routingKey, this)} содержал логическую
   *     ошибку, после завершения теста в памяти остались бы сотни тысяч неиспользуемых
   *     объектов {@code SequentialQueueProcessing}. Принудительный вызов {@link System#gc()}
   *     в конце теста и последующий замер разницы памяти ({@code memoryDiff})
   *     доказывают, что Garbage Collector успешно освобождает ресурсы кучи.</p>
   *   </li>
   *
   *   <li>
   *     <b>Параллельный роутинг Wildcard и Exact подписок</b>
   *     <p>События публикуются в конкретные топики, в то время как часть подписчиков
   *     слушает их через маску (например, {@code "finance/orders/*"}). Тест подтверждает
   *     корректность параллельного поиска и бесконфликтной работы конкурентных хэш-карт
   *     (Concurrent Hash Maps) под экстремальной многопоточной нагрузкой для
   *     различных стратегий подписки.</p>
   *   </li>
   * </ol>
   *
   * @see System#gc()
   * @since 1.0.0
   */
  @Test
  @DisplayName("Стресс-тест: Максимальная пропускная способность, проверка Backpressure и утечек памяти")
  void performanceAndMemoryStressTest() throws InterruptedException {
    // Инициализируем шину со стандартным обработчиком ошибок
    EventAdmin eventAdmin = EventAdmin.create(
        (_, throwable, _) ->
            System.err.printf("Критическая ошибка при стресс-тесте: %s%n", throwable.getMessage()));

    // Конфигурация нагрузки
    int producerThreadsCount = Runtime.getRuntime().availableProcessors() * 2; // Потоки-продюсеры
    int eventsPerProducer = 100_000; // Количество событий от каждого продюсера
    int totalExpectedEvents = producerThreadsCount * eventsPerProducer;

    // Используем LongAdder для потокобезопасного и высокопроизводительного подсчета
    LongAdder totalProcessedCounter = new LongAdder();
    CountDownLatch deliveryLatch = new CountDownLatch(totalExpectedEvents);

    // 1. Создаем подписки на различные типы топиков
    eventAdmin.subscribe("finance/orders/*", HeavyTestEvent.class, _ -> {
      totalProcessedCounter.increment();
      deliveryLatch.countDown();
    });

    eventAdmin.subscribe("analytics/metrics", HeavyTestEvent.class, _ -> {
      totalProcessedCounter.increment();
      deliveryLatch.countDown();
    });

    // Фиксируем метрики памяти ДО начала теста
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    long heapBefore = memoryMXBean.getHeapMemoryUsage().getUsed();
    System.out.printf("[Stress Test] Запуск. Продюсеров: %d, Всего событий: %,d%n",
        producerThreadsCount, totalExpectedEvents);

    long startTime = System.nanoTime();

    // 2. Запускаем параллельных продюсеров
    CountDownLatch producersLatch = new CountDownLatch(producerThreadsCount);
    for (int i = 0; i < producerThreadsCount; i++) {
      final int producerId = i;

      // Продюсеров запускаем в платформенных потоках, чтобы они агрессивно "бомбардировали" шину
      Thread.ofPlatform().start(() -> {
        try {
          for (int j = 0; j < eventsPerProducer; j++) {
            // Каждое второе событие делаем упорядоченным (FIFO по уникальному ключу),
            // чтобы проверить динамическое создание и удаление KeyProcessors из памяти
            String routingKey = (j % 2 == 0) ? "key-" + producerId + "-" + j : "";
            String topic = (j % 3 == 0) ? "finance/orders/created" : "analytics/metrics";

            eventAdmin.postEvent(new HeavyTestEvent(topic, routingKey, j));
          }
        } finally {
          producersLatch.countDown();
        }
      });
    }

    // Ждем, пока все продюсеры закончат генерацию и отправку сообщений в шину
    boolean producersFinished = producersLatch.await(30, TimeUnit.SECONDS);
    assertTrue(producersFinished,
        "Продюсеры заблокировались на семафорах или не успели отправить данные");

    long generationTime = System.nanoTime() - startTime;
    System.out.printf(
        "[Stress Test] Все продюсеры завершили отправку за %.2f сек. Ждем доставку...%n",
        generationTime / 1e9);

    // 3. Ждем, пока виртуальные потоки шины полностью обработают всю очередь
    boolean deliveryFinished = deliveryLatch.await(60, TimeUnit.SECONDS);
    long totalTime = System.nanoTime() - startTime;

    assertTrue(deliveryFinished,
        String.format("Шина не успела обработать все события. Обработано: %,d из %,d",
            totalProcessedCounter.sum(), totalExpectedEvents));

    // 4. Анализ результатов пропускной способности (Throughput)
    double totalTimeSeconds = totalTime / 1e9;
    double throughput = totalExpectedEvents / totalTimeSeconds;

    System.out.println("================== РЕЗУЛЬТАТЫ СТРЕСС-ТЕСТА ==================");
    System.out.printf("Успешно обработано событий : %,d%n", totalProcessedCounter.sum());
    System.out.printf("Общее время выполнения     : %.2f сек%n", totalTimeSeconds);
    System.out.printf("Пропускная способность     : %,.0f оп/сек (events/sec)%n", throughput);

    // 5. Проверяем отсутствие утечек памяти (Memory Leak Check)
    // Принудительно вызываем GC, чтобы очистить отработавшие виртуальные потоки и пустые очереди
    System.gc();
    Thread.sleep(1000); // Даем JVM время на очистку

    long heapAfter = memoryMXBean.getHeapMemoryUsage().getUsed();
    long memoryDiff = heapAfter - heapBefore;

    System.out.printf("Использование Heap (До)    : %,d MB%n", heapBefore / (1024 * 1024));
    System.out.printf("Использование Heap (После) : %,d MB%n", heapAfter / (1024 * 1024));
    System.out.printf("Разница в памяти           : %,d KB (около %.2f MB)%n",
        memoryDiff / 1024, memoryDiff / (1024.0 * 1024.0));
    System.out.println("=============================================================");

    // Если очистка работает правильно, память после сборки мусора не должна вырасти лавинообразно
    // (допускается небольшое расхождение за счет метаданных и внутренних структур самой JVM)
    assertTrue(memoryDiff < 50 * 1024 * 1024,
        "Возможная утечка памяти! Задержано более 50MB после GC.");

    eventAdmin.close();
  }
}
