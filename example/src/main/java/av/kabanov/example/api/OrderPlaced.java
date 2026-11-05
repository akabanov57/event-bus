package av.kabanov.example.api;

import av.kabanov.event.Event;

// Шаг 1: Ресторан создал заказ
public record OrderPlaced(
    long orderId,
    String restaurantName,
    String dishNames
) implements Event {

  @Override
  public String topic() {
    return "restaurant/orders/placed";
  }

  @Override
  public String routingKey() {
    return "order-" + orderId;
  } // FIFO по ID заказа
}
