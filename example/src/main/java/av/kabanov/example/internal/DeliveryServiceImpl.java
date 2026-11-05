package av.kabanov.example.internal;

import av.kabanov.event.EventAdmin;
import av.kabanov.example.api.CourierAssigned;
import av.kabanov.example.api.DeliveryService;
import av.kabanov.example.api.OrderPlaced;
import java.time.Duration;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeliveryServiceImpl implements DeliveryService {

  private static final Logger log = LoggerFactory.getLogger(DeliveryServiceImpl.class);

  private final EventAdmin eventAdmin;

  public DeliveryServiceImpl(EventAdmin eventAdmin) {
    this.eventAdmin = eventAdmin;
  }

  @Override
  public void onOrderPlaced(OrderPlaced event) {
    log.info("[DeliveryService] Новый заказ {} от '{}'. Ищем курьера...", event.orderId(), event.restaurantName());

    // Имитируем блокирующий поиск водителя (Loom здесь сработает идеально)
    LockSupport.parkNanos(Duration.ofMillis(300).toNanos());

    String courierName = "Иван (Такси #7)";
    log.info("[DeliveryService] Курьер {} найден для заказа {}. Публикуем событие...", courierName, event.orderId());

    // Генерируем новое событие
    eventAdmin.postEvent(new CourierAssigned(event.orderId(), courierName, "Автомобиль"));
  }
}
