# Event Admin (Project Loom Powered)

A lightweight, high-performance, and thread-safe asynchronous inside-JVM event bus inspired by the OSGi Event Admin specification. The project is designed specifically for modern applications using **Java 21 / 25** and is fully built on top of **virtual threads (Project Loom)**.

## 🚀 Key Features and Architecture

### 1. Virtual Threads Out of the Box (Project Loom)
Unlike traditional event buses that rely on fixed platform thread pools, `EventAdmin` allocates a lightweight virtual thread for each individual task. This enables the system to scale efficiently to hundreds of thousands of concurrent events without blocking operating system threads during I/O operations in subscribers.

### 2. Resource Isolation (Bulkhead Pattern)
The bus is protected from resource starvation issues where a single slow topic could paralyze the entire system. It implements a two-stage quota system powered by semaphores:
* **Global Limit:** A maximum of 20,000 concurrent tasks across the entire bus to prevent Out Of Memory (OOM) errors.
* **Local Topic Quota:** A maximum of 1,000 concurrent tasks per root topic segment (e.g., everything starting with `auth/` or `orders/`). If one module hangs, the others continue running at full speed.

### 3. Guaranteed FIFO Ordering by Keys (Routing Key)
For business logic where the sequence of execution steps is critical (e.g., `OrderCreated` -> `OrderPaid`), the bus provides a built-in ordering mechanism similar to the Actor concept or partitioning in Kafka.
* If an event has a defined `routingKey()`, it enters an isolated sequential queue for that specific key.
* Processing within this queue occurs strictly one after another. Meanwhile, all the benefits of Loom are preserved: if a step gets blocked on I/O, the virtual thread parks without stopping the CPU.

### 4. Reactive Memory Cleanup
Sequential queues based on `routingKey` are created in memory dynamically and lazily. As soon as all events for a specific key are processed and the queue becomes empty, the bus **atomically removes the internal processor from memory** using the thread-safe `ConcurrentHashMap.remove(key, value)` contract. This completely eliminates memory leaks in long-running applications handling millions of unique keys.

### 5. Safe API Without Checked Exceptions
The interfaces are designed to keep client code as concise as possible:
* The base `EventAdmin` interface and the `Subscriber` subscription token both override the `close()` method, removing the annoying `throws Exception` clause.
* The component can be safely used within `try-with-resources` blocks or have `.close()` invoked manually without the need to wrap code in boilerplate `try-catch` blocks.

---

## 🛠️ Interface Structure

### Event (`Event`)
Every event must implement the `Event` marker interface. We recommend using a hierarchical slash-separated (`/`) splitting structure.

```java
public record OrderEvent(long orderId, String status) implements Event {
    @Override
    public String topic() {
        return "app/orders/status-changed";
    }

    @Override
    public String routingKey() {
        // All events for a specific order will be processed strictly one after another (FIFO)
        return "order-" + orderId; 
    }
}
```

### Subscription (`Subscriber`)
The `subscribe` method returns a token to manage the subscription lifecycle.

```java
public interface Subscriber extends AutoCloseable {
    @Override
    void close(); // Does not throw checked exceptions!
}
```

---

## 💻 Usage Examples

### Creating the Event Bus

```java
// Option 1: With standard error output to System.err
EventAdmin eventAdmin = EventAdmin.create();

// Option 2: With a centralized custom error logging strategy for business logic
EventAdmin eventAdmin = EventAdmin.create((event, throwable, listenerName) -> {
    logger.error("Error in subscriber '{}' on topic '{}': {}", 
        listenerName, event.topic(), throwable.getMessage());
});
```

### Subscribing to Events (Exact Match and Wildcard)

```java
// 1. Exact topic match
Subscriber exactSub = eventAdmin.subscribe("app/orders/status-changed", OrderEvent.class, event -> {
    System.out.println("Order status changed: " + event.status());
});

// 2. Using a Wildcard (subscribing to all subtopics under the 'app/orders/' module)
Subscriber wildcardSub = eventAdmin.subscribe("app/orders/*", OrderEvent.class, event -> {
    System.out.println("Logging any event from the orders module: " + event.topic());
});
```

### Publishing Events (Fire-and-Forget)

The `postEvent` method is fully asynchronous. It instantly returns control to the producer, delegating processing to the core virtual threads. If the bus is closed or critically overloaded, the method will throw an `IllegalStateException`.

```java
try {
    eventAdmin.postEvent(new OrderEvent(12345L, "PAID"));
} catch (IllegalStateException e) {
    // Handle the situation when the bus is shutting down
}
```

### Unsubscribing and Closing the Bus

```java
// Cancel a specific subscription
exactSub.close(); 

// Global graceful shutdown of the event bus
// Instantly blocks new publications, clears references for GC, and waits up to 5 seconds for active tasks to finish
eventAdmin.close(); 
```

---

## 🛡️ Lifecycle Safety (Shutdown Handling)
The component is engineered with extreme edge cases in mind:
* If a producer thread calls `postEvent` at the exact microsecond the pool is closing, the bus will intercept the internal system `RejectedExecutionException`, **guaranteed to release all held quotas (semaphores)**, and throw a clear `IllegalStateException`.
* Repeated calls to `close()`, as well as `Subscriber.close()` invocations after the entire bus has been shut down, are completely **idempotent** and safe (they do not produce a `NullPointerException`).
