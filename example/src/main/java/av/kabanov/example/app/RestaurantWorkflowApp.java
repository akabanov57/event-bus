package av.kabanov.example.app;

import av.kabanov.event.EventAdmin;
import av.kabanov.event.EventAdminFactory;
import av.kabanov.event.Subscriber;
import av.kabanov.example.api.AppFactory;
import av.kabanov.example.api.CourierAssigned;
import av.kabanov.example.api.DeliveryService;
import av.kabanov.example.api.MapGeoService;
import av.kabanov.example.api.OrderPlaced;
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

        log.info("[System] Шина событий запущена, сервисы подписаны.");

        // 4. Эмулируем создание заказа рестораном (Продюсер)
        long targetOrderId = 98765L;
        OrderPlaced newOrder = new OrderPlaced(targetOrderId, "Пицца Фабрика",
            "Пепперони 35см, Кола");

        log.info("[Restaurant] Клиент оплатил заказ. Отправляем событие в шину...");
        eventAdmin.postEvent(newOrder);

        // Даем виртуальным потокам время полностью завершить всю цепочку реакций
        LockSupport.parkNanos(Duration.ofSeconds(1).toNanos());
      } // 5. Чистим ресурсы при остановке приложения
    }
    log.info("[System] Приложение успешно остановлено.");
  }
}
