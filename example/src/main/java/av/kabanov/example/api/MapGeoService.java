package av.kabanov.example.api;

/**
 * Картографический сервис.
 */
public interface MapGeoService {

  /**
   * Слушает назначение курьера
   *
   * @param event Назначения курьера
   */
  void onCourierAssigned(CourierAssigned event);
}
