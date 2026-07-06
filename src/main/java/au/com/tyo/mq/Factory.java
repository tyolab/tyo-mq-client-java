package au.com.tyo.mq;

/**
 * Factory for tyo-mq producers and consumers, mirroring the Node.js client.
 *
 * <pre>
 * Factory mq = new Factory("localhost", 17352);
 * // with auth enabled on the server:
 * // Factory mq = new Factory("localhost", 17352, "my-token");
 *
 * Producer producer = mq.createProducer("order-service");
 * producer.connect();
 * producer.produce("order-placed", new JSONObject().put("orderId", 1001));
 *
 * Subscriber consumer = mq.createConsumer("email-service");
 * consumer.subscribe("order-service", "order-placed",
 *         (message, from, ack, raw) -> System.out.println(message));
 * consumer.connect();
 * </pre>
 */
public class Factory {

    private final String host;
    private final int port;
    private final String protocol;
    private final String authToken;

    public Factory() {
        this(null, -1, null, null);
    }

    public Factory(String host, int port) {
        this(host, port, null, null);
    }

    public Factory(String host, int port, String authToken) {
        this(host, port, null, authToken);
    }

    public Factory(String host, int port, String protocol, String authToken) {
        this.host = host;
        this.port = port;
        this.protocol = protocol;
        this.authToken = authToken;
    }

    public Producer createProducer(String name) {
        return createProducer(name, null);
    }

    public Producer createProducer(String name, String eventDefault) {
        return new Producer(name, eventDefault, host, port, protocol, authToken);
    }

    public Subscriber createConsumer(String name) {
        return new Subscriber(name, host, port, protocol, authToken);
    }

    /** Alias of {@link #createConsumer(String)}. */
    public Subscriber createSubscriber(String name) {
        return createConsumer(name);
    }
}
