package au.com.tyo.mq;

import org.json.JSONObject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** A tyo-mq consumer. */
public class Subscriber extends MqSocket {

    /**
     * The on-connect re-send for each live subscription, keyed by its consume event.
     *
     * <p>Kept so a subscription can be undone. Without it the runnable that re-SUBSCRIBEs after
     * every reconnect could never be removed, so {@link #unsubscribe} would last only until the
     * next network blip — after which the client would quietly subscribe again to a topic the
     * caller had finished with.
     */
    private final Map<String, Runnable> resubscribers = new ConcurrentHashMap<>();

    public Subscriber(String name) {
        // Cast disambiguates the String/Supplier token overloads for a bare null.
        this(name, null, -1, null, (String) null);
    }

    public Subscriber(String name, String host, int port, String protocol, String authToken) {
        super(host, port, protocol, authToken);
        this.type = "CONSUMER";
        this.name = name != null ? name : Constants.ANONYMOUS;
    }

    /** Preferred for a long-lived consumer: the supplier is asked for a token on every connect
     *  attempt, so socket.io's automatic reconnects authenticate with a live one rather than
     *  whatever was valid when this object was built. */
    public Subscriber(String name, String host, int port, String protocol,
                      java.util.function.Supplier<String> authTokenSupplier) {
        super(host, port, protocol, authTokenSupplier);
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
        // Replace rather than accumulate: subscribing twice to the same consume event used to add
        // a second re-send runnable each time, so every reconnect re-SUBSCRIBEd N times.
        Runnable previous = resubscribers.put(consumeEvent, send);
        if (previous != null)
            removeOnConnectListener(previous);
        addOnConnectListener(send);
    }

    /** Undo {@link #subscribe(String, String, ConsumeHandler)}. */
    public void unsubscribe(String producer, String event) {
        unsubscribe(producer, event, new SubscribeOptions());
    }

    /**
     * Undo a {@link #subscribe(String, String, SubscribeOptions, ConsumeHandler)} — all of it.
     *
     * <p>{@code subscribe} sets up three things, and leaving any one behind defeats the rest:
     * <ol>
     *   <li>the local handler — removed, so nothing more is dispatched to the caller;</li>
     *   <li>the on-connect re-send — removed, or the next reconnect re-SUBSCRIBEs a topic the
     *       caller has finished with, whatever the broker did;</li>
     *   <li>the broker's subscription — an UNSUBSCRIBE carrying the SAME object SUBSCRIBE sent, so
     *       the broker derives the subscription key exactly as it did when storing it, instead of
     *       every client having to replicate the broker's internal event encoding.</li>
     * </ol>
     *
     * <p>When not connected the UNSUBSCRIBE is deferred to the next connect, as a one-shot
     * listener. That ordering matters: on-connect listeners run after identification, and
     * identification is where the broker RESTORES a named consumer's subscriptions — so an
     * UNSUBSCRIBE sent any earlier would be undone by the restore it preceded.
     *
     * <p>Fire-and-forget: the broker's UNSUBSCRIBE gives no ack today. Note also that as deployed
     * (tyo-mq {@code d7e87be}) it deletes by socket id while SUBSCRIBE stores by consumer name, so
     * for a named consumer the broker half is currently a no-op; raised with the tyo-mq lane. The
     * client half is correct regardless and is what stops the re-send.
     */
    public void unsubscribe(String producer, String event, SubscribeOptions options) {
        if ("topic".equals(options.mode) && producer == null)
            producer = Constants.ALL_PRODUCERS;
        final String who = producer;

        String consumerEvent = Events.toConsumerEvent(event, who, false);
        String consumeEvent = Events.toConsumeEvent(consumerEvent);

        off(consumeEvent);
        Runnable send = resubscribers.remove(consumeEvent);
        if (send != null)
            removeOnConnectListener(send);

        JSONObject payload = new JSONObject();
        payload.put("event", event);
        payload.put("producer", who);
        payload.put("consumer", name);
        payload.put("scope", Constants.SCOPE_DEFAULT);
        payload.put("consumer_id", name);
        if ("topic".equals(options.mode))
            payload.put("mode", "topic");

        if (connected) {
            sendUnsubscribe(event, payload);
            return;
        }
        Runnable[] once = new Runnable[1];
        once[0] = () -> {
            removeOnConnectListener(once[0]);
            sendUnsubscribe(event, payload);
        };
        addOnConnectListener(once[0]);
    }

    /**
     * Send UNSUBSCRIBE and LOG what the broker says about it.
     *
     * <p>Logged because silence is what hid the original bug: for every named consumer the broker
     * used to delete nothing and say nothing, and the client could not tell. Since tyo-mq 1.5.0 it
     * acks {@code {ok:true, removed, event}} or {@code {ok:false, reason}}. A
     * {@code not_subscribed} for a topic this client believed it held is a disagreement between the
     * two sides' views of the world, so it is called out as such. Non-blocking: see
     * {@link #emitWithCallback}.
     */
    private void sendUnsubscribe(String event, JSONObject payload) {
        emitWithCallback("UNSUBSCRIBE", payload, reply -> {
            if (reply == null) {
                System.err.println("UNSUBSCRIBE " + event + ": broker acked with no body");
            } else if (reply.optBoolean("ok", false)) {
                System.err.println("UNSUBSCRIBE " + event + ": removed "
                        + reply.optInt("removed", 0));
            } else {
                System.err.println("UNSUBSCRIBE " + event + ": broker REFUSED ("
                        + reply.optString("reason", "no reason") + ")"
                        + ("not_subscribed".equals(reply.optString("reason"))
                        ? " — it did not hold a subscription this client believed it had" : ""));
            }
        });
    }

    /** Undo {@link #subscribeTopic(String, SubscribeOptions, ConsumeHandler)}. */
    public void unsubscribeTopic(String pattern) {
        unsubscribe(Constants.ALL_PRODUCERS, pattern, new SubscribeOptions().topic());
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
