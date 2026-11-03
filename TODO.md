# TODO: Development Roadmap for `event-admin` Project

This file aggregates ideas, architectural improvements, and potential features for future versions of the event bus, along with the rationale for their practical value.

---

## 🔥 High Priority

### 📈 Metrics Gathering and Monitoring (Micrometer / JMX)
* [ ] **Implement the Decorator Pattern** around `EventAdmin` to isolate metrics code from the bus core.
* [ ] **Add Latency Metrics:** measure the execution time of each subscriber (`listener.accept(event)`) using `System.nanoTime()`.
* [ ] **Add Throughput Metrics:** counters for processed (`processed_events_total`) and failed (`failed_events_total`) events broken down by topics.
* [ ] **Queue and Quota Monitoring:** export the current size of queues in `keyProcessors` and the number of available permits in `globalSemaphore`/`rootSemaphores`.
    * **💡 Practical Value:**
        1. **Incident Prevention:** You can configure alerts (e.g., in Slack/Telegram) before the system goes down. If the queue depth of a specific topic starts growing, you will be the first to know.
        2. **Bottleneck Identification:** The Grafana dashboard will clearly display which specific subscriber is slowing down business chains and which subsystem is heavily exhausting its allocated semaphore quotas.

### 🛡️ Dead Letter Queue (DLQ)
* [ ] **Introduce a Retry Policy:** if a subscriber fails with an error, do not drop the event immediately; instead, retry the invocation up to $N$ times with an *Exponential Backoff*.
* [ ] **Implement DLQ:** if an event completely fails processing after all retry attempts, redirect it to a dedicated system topic `system/dead-letter-queue` or register it via a dedicated `DeadLetterQueueHandler` interface.
    * **💡 Practical Value:**
        1. **Resilience to Transient Failures:** If an external service (e.g., a bank or CRM) "blinks" for 200 milliseconds, the retry attempt will succeed, and the operation won't be lost.
        2. **Data Retention Guarantee:** No business event will vanish without a trace. All "broken" messages will land in the DLQ, allowing administrators to inspect the error, fix the bug in the code, and re-run events without losing customer data.

---

## ⚡ Medium Priority

### 🔍 LDAP-like Dynamic Event Filtering
* [ ] **Integrate Property Filters (Event Filters):** extend the `Event` interface by adding a `Map<String, Object> properties()` method.
* [ ] **Add Subscription Filter Support:** implement an OSGi/LDAP-style filtering syntax (e.g., `(&(org=sales)(price>=100))`) so that a subscriber receives an event from a topic only when it matches specific business criteria.
    * **💡 Practical Value:**
        1. **Offloading Virtual Threads:** The bus discards unnecessary events at the core level. Subscribers don't need to wake up, consume CPU, and waste Loom scheduler resources just to evaluate `if (event.price() < 100) return;` internally.
        2. **Flexible Architecture:** Enables creating dynamic, smart subscriptions without altering the code of the events themselves (e.g., subscribing only to VIP clients or specific regions).

### 🛑 Adaptive and Non-blocking Backpressure (Drop Strategies)
* [ ] **Implement Non-blocking Mode:** add a flag to the bus configuration allowing the `postEvent` method to return `false` or immediately throw an exception during overload instead of waiting on a semaphore (`acquire()`).
* [ ] **Add Drop Strategies:** `DROP_OLDEST` (evict oldest events from queues) or `DROP_NEWEST` (ignore incoming new events).
    * **💡 Practical Value:**
        1. **UI and Reactive Thread Protection:** In critical threads (e.g., socket or UI handling), blocking a producer via a semaphore is unacceptable as it causes user-facing "freezes". Non-blocking mode lets the producer instantly know about the overload and reroute traffic.
        2. **Secondary Data Management:** For metrics, logs, or telemetry, losing a few frames is not critical. The `DROP_OLDEST` strategy will allow the bus to automatically discard stale logs, freeing up space for fresh data and keeping the core system stable.

---

## 🧊 Low Priority & Research

### 🌳 Trie (Prefix Tree) for Wildcard Subscriptions
* [ ] **Optimize Mask Lookups:** replace the linear iteration over the `wildcardSubscriptions.entrySet()` map with a specialized **Trie (Prefix Tree)** data structure.
    * **💡 Practical Value:**
        1. **Stable O(1)/O(L) Lookups as the System Grows:** Currently, when dispatching a message, the bus loops over all wildcard subscriptions. If a large system reaches 5,000 subscriptions, routing performance will degrade. A Trie tree makes lookup speed independent of the total subscription count, guaranteeing that throughput remains at ~2M ops/sec even in massive monoliths.

### 🌐 Distributed Bridge (Network/Cluster Bridge)
* [ ] **Design a Transport Bridge Interface:** allow local `postEvent` calls to be transparently forwarded to external message brokers (Apache Kafka, RabbitMQ, MQTT) for operation within a clustered/microservice architecture.
    * **💡 Practical Value:**
        1. **Scaling to Microservices:** You can start your project as a rapid monolith with a local `EventAdmin` bus. As the business grows, you can transform it into a distributed system with a single configuration line. Local events will seamlessly ship to Kafka instances on other servers, while the business logic of subscribers and producers remains completely untouched.
