package au.com.tyo.mq;

import org.json.JSONObject;

import java.util.concurrent.atomic.AtomicBoolean;

/** A tyo-mq consumer. */
public class Subscriber extends MqSocket {

    public Subscriber(String name) {
        this(name, null, -1, null, null);
    }

    public Subscriber(String name, String host, int port, String protocol, String authToken) {
        super(host, port, protocol, authToken);
        this.type = "CONSUMER";
        this.name = name != null ? name : Constants.ANONYMOUS;
    }

    /** Fire-and-forget subscription to one event from one producer. */
    public void subscribe(String producer, String event, ConsumeHandler handler) {
        subscribe(producer, event, new SubscribeOptions(), handler);
    }

    /**
     * Subscribe to {@code event} from {@code producer} with delivery options.
     * The subscription is re-sent automatically after every reconnect.
     */
    public void subscribe(String producer, String event, SubscribeOptions options, ConsumeHandler handler) {
        if ("topic".equals(options.mode) && producer == null)
            producer = Constants.ALL_PRODUCERS;
        final String who = producer;

        JSONObject payload = new JSONObject();
        payload.put("event", event);
        payload.put("producer", who);
        payload.put("consumer", name);
        payload.put("scope", Constants.SCOPE_DEFAULT);
        payload.put("consumer_id", name);
        options.applyTo(payload);

        String consumerEvent = Events.toConsumerEvent(event, who, false);
        String consumeEvent = Events.toConsumeEvent(consumerEvent);
        boolean autoAck = options.ackEnabled() && !options.manualAck;

        on(consumeEvent, obj -> {
            Object message = obj.opt("message");
            String from = obj.optString("from", who);
            String msgId = obj.optString("msgId", obj.optString("msg_id", null));

            AtomicBoolean acked = new AtomicBoolean(false);
            Runnable ack = () -> {
                if (msgId != null && acked.compareAndSet(false, true))
                    emit("ACK", new JSONObject().put("msgId", msgId));
            };

            try {
                handler.onMessage(message, from, ack, obj);
            } catch (Exception e) {
                // No auto-ACK on a failed handler: the server retries on its
                // schedule and dead-letters when attempts are exhausted.
                System.err.println("consume handler failed for " + consumeEvent + ": " + e);
                return;
            }
            if (autoAck)
                ack.run();
        });

        Runnable send = () -> emit("SUBSCRIBE", payload);
        if (connected)
            send.run();
        addOnConnectListener(send);
    }

    /**
     * Subscribe to an MQTT-style topic pattern from any producer:
     * {@code +} matches one level, {@code #} matches any trailing levels.
     */
    public void subscribeTopic(String pattern, SubscribeOptions options, ConsumeHandler handler) {
        subscribe(Constants.ALL_PRODUCERS, pattern, options.topic(), handler);
    }

    public void subscribeTopic(String pattern, ConsumeHandler handler) {
        subscribeTopic(pattern, new SubscribeOptions(), handler);
    }
}
