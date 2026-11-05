package av.kabanov.example.api;

/**
 * Служба доставки.
 */
public interface DeliveryService {

  /**
   * Слушает создание заказов
   * @param event Создания заказа
   */
  void onOrderPlaced(OrderPlaced event);
}
