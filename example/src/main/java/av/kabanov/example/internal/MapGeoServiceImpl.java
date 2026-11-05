package av.kabanov.example.internal;

import av.kabanov.example.api.CourierAssigned;
import av.kabanov.example.api.MapGeoService;
import java.time.Duration;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapGeoServiceImpl implements MapGeoService {

  private static final Logger log = LoggerFactory.getLogger(MapGeoServiceImpl.class);

  @Override
  public void onCourierAssigned(CourierAssigned event) {
    log.info(
        "[MapGeoService] Курьер {} назначен на заказ {}. Начинаем строить оптимальный маршрут...",
        event.courierName(), event.orderId());

    // Имитируем расчет маршрута
    LockSupport.parkNanos(Duration.ofMillis(150).toNanos());

    log.info("[MapGeoService] Маршрут для заказа {} успешно построен и отправлен клиенту!",
        event.orderId());
  }
}
