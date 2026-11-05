package av.kabanov.example.app;

import av.kabanov.event.EventAdmin;
import av.kabanov.event.EventAdminFactory;
import av.kabanov.event.Subscriber;
import av.kabanov.example.api.AppFactory;
import av.kabanov.example.api.CourierAssigned;
import av.kabanov.example.api.DeliveryService;
import av.kabanov.example.api.MapGeoService;
import av.kabanov.example.api.OrderPlaced;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestaurantWorkflowApp {

  private static final Logger log = LoggerFactory.getLogger(RestaurantWorkflowApp.class);

  static void main() {
    // 1. Инициализируем шину событий
    try (EventAdmin eventAdmin = EventAdminFactory.create()) {

      // 2. Создаем наши бизнес-сервисы
      DeliveryService deliveryService = AppFactory.create(eventAdmin);
      MapGeoService mapGeoService = AppFactory.create();

      // 3. Подписываем сервисы на нужные топики с помощью ссылок на методы
      try (
          Subscriber _ = eventAdmin.subscribe(
              "restaurant/orders/placed",
              OrderPlaced.class,
              deliveryService::onOrderPlaced
          );

          Subscriber _ = eventAdmin.subscribe(
              "delivery/couriers/assigned",
              CourierAssigned.class,
              mapGeoService::onCourierAssigned
          )) {

        int totalRestaurants = 5; // Эмулируем 5 одновременно работающих ресторанов
        log.info("[System] Шина запущена. Начинаем одновременную симуляцию {} ресторанов...", totalRestaurants);

        // Пул потоков для эмуляции параллельных продюсеров (ресторанов)
        // Используем FixedThreadPool. Удобней наблюдать виртуальные потоки.
        try (ExecutorService producerExecutor = Executors.newFixedThreadPool(totalRestaurants)) {

          // Барьер для синхронного старта всех ресторанов (эффект "вспышки")
          CountDownLatch startSignal = new CountDownLatch(1);
          // Защелка для ожидания завершения отправки всеми ресторанами
          CountDownLatch finishSignal = new CountDownLatch(totalRestaurants);

          // 4. Запуск параллельных ресторанов
          for (int i = 1; i <= totalRestaurants; i++) {
            final int restaurantId = i;
            final long orderId =
                100_000L + restaurantId; // Уникальный ID заказа для каждого ресторана

            producerExecutor.submit(() -> {
              try {
                // Все потоки встают на барьер и ждут команду "Старт"
                startSignal.await();

                String restaurantName = "Ресторан #" + restaurantId;
                OrderPlaced order = new OrderPlaced(orderId, restaurantName,
                    "Комбо-набор №" + restaurantId);

                log.info("Ресторан {} отправляет заказ {} в шину...", restaurantName, orderId);

                eventAdmin.postEvent(order);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                finishSignal.countDown();
              }
            });
          }

          // Даем команду одновременного старта для всех ресторанов
          long startTime = System.currentTimeMillis();
          startSignal.countDown();

          // Ждем, пока все потоки-продюсеры отправят сообщения
          try {
            // Оборачиваем await в try-catch для обработки InterruptedException
            boolean producersFinished = finishSignal.await(5, TimeUnit.SECONDS);
            if (!producersFinished) {
              log.warn("[Warning] Не все рестораны успели отправить заказы за 5 секунд");
            }
          } catch (InterruptedException e) {
            log.error("[Error] Поток симуляции был прерван");
            Thread.currentThread().interrupt(); // Восстанавливаем статус прерывания
          }
          // Даем виртуальным потокам время полностью завершить всю цепочку реакций
          LockSupport.parkNanos(Duration.ofSeconds(2).toNanos());

          long totalTime = System.currentTimeMillis() - startTime;
          log.info("[System] Симуляция завершена. Все процессы обработаны параллельно за {} мс.", totalTime);
        }

      } // 5. Чистим ресурсы при остановке приложения
    }
    log.info("[System] Приложение успешно остановлено.");
  }
}
