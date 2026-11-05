package av.kabanov.example.api;

import av.kabanov.event.Event;

// Шаг 2: Служба доставки назначила курьера
public record CourierAssigned(
    long orderId,
    String courierName,
    String vehicleType
) implements Event {

  @Override
  public String topic() {
    return "delivery/couriers/assigned";
  }

  @Override
  public String routingKey() {
    return "order-" + orderId;
  } // FIFO по ID заказа
}
