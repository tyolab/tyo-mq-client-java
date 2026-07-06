package au.com.tyo.mq;

import org.json.JSONObject;

/**
 * Optional delivery semantics for a subscription. The defaults give plain
 * fire-and-forget delivery; durable + ack turn on guaranteed delivery with
 * server-side queuing, retries, and dead-lettering.
 */
public class SubscribeOptions {

    boolean durable;
    boolean ack;
    boolean manualAck;
    String ackTimeout;      // e.g. "30s"
    Integer retryMaxAttempts;
    String retryDelay;      // e.g. "5s"
    String retryBackoff;    // "exponential"
    String mode;            // "topic" for MQTT-style patterns (+, #)
    String group;           // consumer group name

    public SubscribeOptions durable() { this.durable = true; return this; }

    /** Auto-ACK: the client acknowledges after the handler returns normally. */
    public SubscribeOptions ack() { this.ack = true; return this; }

    /** Manual ACK: the handler must call its {@code ack} argument itself. */
    public SubscribeOptions manualAck() { this.manualAck = true; return this; }

    public SubscribeOptions ackTimeout(String timeout) { this.ackTimeout = timeout; return this; }

    public SubscribeOptions retry(int maxAttempts, String delay, String backoff) {
        this.retryMaxAttempts = maxAttempts;
        this.retryDelay = delay;
        this.retryBackoff = backoff;
        return this;
    }

    /** Treat the event as an MQTT-style topic pattern from any producer. */
    public SubscribeOptions topic() { this.mode = "topic"; return this; }

    public SubscribeOptions group(String group) { this.group = group; return this; }

    boolean ackEnabled() { return ack || manualAck; }

    void applyTo(JSONObject payload) {
        if (durable) payload.put("durable", true);
        if (ackEnabled()) payload.put("ack", true);
        if (manualAck) payload.put("manual_ack", true);
        if (ackTimeout != null) payload.put("ack_timeout", ackTimeout);
        if (retryMaxAttempts != null) {
            JSONObject retry = new JSONObject();
            retry.put("max_attempts", (int) retryMaxAttempts);
            if (retryDelay != null) retry.put("delay", retryDelay);
            if (retryBackoff != null) retry.put("backoff", retryBackoff);
            payload.put("retry", retry);
        }
        if (mode != null) payload.put("mode", mode);
        if (group != null) payload.put("group", group);
    }
}
