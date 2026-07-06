package au.com.tyo.mq;

import org.json.JSONObject;

import java.util.UUID;

/** A tyo-mq producer. */
public class Producer extends MqSocket {

    private final String eventDefault;

    public Producer(String name) {
        this(name, null, null, -1, null, null);
    }

    public Producer(String name, String eventDefault) {
        this(name, eventDefault, null, -1, null, null);
    }

    public Producer(String name, String eventDefault, String host, int port, String protocol, String authToken) {
        super(host, port, protocol, authToken);
        this.type = "PRODUCER";
        this.name = name != null ? name : Constants.ANONYMOUS;
        this.eventDefault = eventDefault;
    }

    @Override
    protected void sendIdentification() {
        emit(type, new JSONObject().put("name", name));
    }

    /** Publishes to the producer's default event. */
    public void produce(Object data) {
        if (eventDefault == null)
            throw new IllegalStateException("no default event set — use produce(event, data)");
        produce(eventDefault, data);
    }

    /** Publishes one fire-and-forget message. `data` may be a String,
     *  JSONObject, Number, or any org.json-compatible value. */
    public void produce(String event, Object data) {
        produce(event, data, null, null);
    }

    /**
     * Publishes with routing options.
     *
     * @param broadcast null, "realm" (every realm member), or "group"
     * @param group     the consumer group, when broadcast is "group"
     */
    public void produce(String event, Object data, String broadcast, String group) {
        JSONObject message = new JSONObject();
        message.put("event", event);
        message.put("message", data);
        message.put("from", name);
        if (broadcast != null) {
            message.put("method", Constants.METHOD_BROADCAST);
            message.put("broadcast", "group".equals(broadcast) ? "group" : "realm");
            if (group != null)
                message.put("group", group);
        }

        String json = message.toString();
        if (json.length() <= Constants.CHUNK_SIZE) {
            emit("PRODUCE", message);
            return;
        }

        // Large message — split into PRODUCE_CHUNK frames under the limit
        int total = (json.length() + Constants.CHUNK_SIZE - 1) / Constants.CHUNK_SIZE;
        String transferId = UUID.randomUUID().toString().replace("-", "");
        for (int i = 0; i < total; i++) {
            int start = i * Constants.CHUNK_SIZE;
            int end = Math.min(start + Constants.CHUNK_SIZE, json.length());
            JSONObject chunk = new JSONObject();
            chunk.put("transferId", transferId);
            chunk.put("index", i);
            chunk.put("total", total);
            chunk.put("data", json.substring(start, end));
            emit("PRODUCE_CHUNK", chunk);
        }
    }
}
