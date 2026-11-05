package av.kabanov.example.api;

import av.kabanov.event.EventAdmin;
import av.kabanov.example.internal.DeliveryServiceImpl;
import av.kabanov.example.internal.MapGeoServiceImpl;

public final class AppFactory {

  public static DeliveryService create(EventAdmin eventAdmin) {
    return new DeliveryServiceImpl(eventAdmin);
  }

  public static MapGeoService create() {
    return new MapGeoServiceImpl();
  }
}
