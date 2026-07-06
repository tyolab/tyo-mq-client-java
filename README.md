# tyo-mq-client-java

A Java client for **[tyo-mq](https://github.com/tyolab/tyo-mq)** — the
distributed pub/sub messaging service with durable delivery (ACK / retry /
dead-letter queue), MQTT-style topic wildcards, consumer groups, and
multi-tenant auth realms.

Built on [socket.io-client-java](https://github.com/socketio/socket.io-client-java)
(Socket.IO v4). Java 8+.

## Install

```xml
<dependency>
  <groupId>au.com.tyo</groupId>
  <artifactId>tyo-mq-client</artifactId>
  <version>0.1.0</version>
</dependency>
```

(Until the artifact is on Maven Central, build it locally with
`mvn install`.) You'll need a running tyo-mq server:
`npm install tyo-mq && node -e "new (require('tyo-mq').Server)().start()"`,
or Docker — see the [server repo](https://github.com/tyolab/tyo-mq).

## Quick start

```java
import au.com.tyo.mq.*;
import org.json.JSONObject;

Factory mq = new Factory("localhost", 17352);
// with auth enabled on the server:
// Factory mq = new Factory("localhost", 17352, "my-token");

// consume
Subscriber consumer = mq.createConsumer("email-service");
consumer.subscribe("order-service", "order-placed",
        (message, from, ack, raw) ->
                System.out.println("confirmation for " + message));
consumer.connect();

// produce
Producer producer = mq.createProducer("order-service");
producer.connect();
producer.produce("order-placed", new JSONObject().put("orderId", 1001));
```

## Durable delivery, ACK, and retry

```java
consumer.subscribe("order-service", "payment",
        new SubscribeOptions()
                .durable()                       // queue while offline
                .ack()                           // auto-ACK after the handler returns
                .retry(3, "5s", "exponential"),
        (message, from, ack, raw) -> doWork(message));
```

A handler that throws is **not** acknowledged — the server re-delivers on the
retry schedule and dead-letters the message when attempts run out. For full
control use `.manualAck().ackTimeout("30s")` and call the handler's `ack`
argument once the work truly succeeded.

## Topics, groups, broadcast

```java
// MQTT-style wildcards: + is one level, # is the rest
consumer.subscribeTopic("orders/+/status", (m, f, a, r) -> { ... });
consumer.subscribeTopic("factory/#",
        new SubscribeOptions().durable().ack(), (m, f, a, r) -> { ... });

// consumer groups load-balance across workers
consumer.subscribe("dispatcher", "jobs",
        new SubscribeOptions().group("workers"), (m, f, a, r) -> { ... });

// broadcast one copy to every realm member, or every group member
producer.produce("announcement", data, "realm", null);
producer.produce("control", data, "group", "workers");
```

Large messages are chunked automatically in both directions (256 KB frames),
matching the Node.js client.

## Example

`examples/PubSubExample.java` is a complete round trip (durable + auto-ACK):

```bash
mvn package
java -cp "target/classes:$(mvn -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout)" \
     examples/PubSubExample.java localhost 17352
```

## Other clients

Node.js (and browsers) ships with the [server package](https://github.com/tyolab/tyo-mq);
see also [Python](https://github.com/tyolab/tyo-mq-client-python),
[Rust](https://github.com/tyolab/tyo-mq-client-rust),
[C/C++](https://github.com/tyolab/tyo-mq-client-cpp),
[Go](https://github.com/tyolab/tyo-mq-client-go), and
[C#](https://github.com/tyolab/tyo-mq-client-csharp).

## License

Apache-2.0. Built by [TYO Lab](https://tyo.com.au).
