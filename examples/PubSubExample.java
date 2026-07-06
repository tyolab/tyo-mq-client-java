import au.com.tyo.mq.Factory;
import au.com.tyo.mq.Producer;
import au.com.tyo.mq.SubscribeOptions;
import au.com.tyo.mq.Subscriber;
import org.json.JSONObject;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A minimal tyo-mq round trip: produce on one connection, consume (durable,
 * auto-ACK) on another.
 *
 * Start a server first (see https://github.com/tyolab/tyo-mq), then:
 *
 *   java -cp <classpath> PubSubExample [host] [port]
 */
public class PubSubExample {

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 17352;

        Factory mq = new Factory(host, port);
        // with auth enabled on the server:
        // Factory mq = new Factory(host, port, "my-token");

        CountDownLatch received = new CountDownLatch(1);

        Subscriber consumer = mq.createConsumer("java-listener");
        consumer.subscribe("java-example", "order-placed",
                new SubscribeOptions().durable().ack()
                        .retry(3, "5s", "exponential"),
                (message, from, ack, raw) -> {
                    System.out.println("received from " + from + ": " + message
                            + " (msgId: " + raw.optString("msgId") + ")");
                    received.countDown();
                });
        consumer.connect();

        Producer producer = mq.createProducer("java-example");
        producer.connect();

        Thread.sleep(500); // let the subscription register
        producer.produce("order-placed", new JSONObject()
                .put("orderId", 1001).put("total", 129.0));

        boolean ok = received.await(10, TimeUnit.SECONDS);
        producer.disconnect();
        consumer.disconnect();

        System.out.println(ok ? "round trip OK" : "no message received before timeout");
        System.exit(ok ? 0 : 1);
    }
}
