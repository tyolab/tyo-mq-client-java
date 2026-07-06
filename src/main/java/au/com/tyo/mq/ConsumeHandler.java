package au.com.tyo.mq;

import org.json.JSONObject;

/** Receives one delivered message. */
@FunctionalInterface
public interface ConsumeHandler {

    /**
     * @param message the produced payload (String, JSONObject, Number, …)
     * @param from    the producer's name
     * @param ack     acknowledges this delivery; a no-op unless the
     *                subscription enabled ACK (with auto-ACK it is called for
     *                you after this method returns without throwing)
     * @param raw     the full delivery object (event, msgId, …)
     */
    void onMessage(Object message, String from, Runnable ack, JSONObject raw);
}
