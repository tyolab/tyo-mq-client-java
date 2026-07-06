package au.com.tyo.mq;

/** Event-name helpers matching the Node.js client's events.js. */
public final class Events {

    public static String toEventString(String event, String prefix, String suffix) {
        return (prefix != null ? prefix + "-" : "") + event + (suffix != null ? "-" + suffix : "");
    }

    /** The socket event on which deliveries for a subscription arrive. */
    public static String toConsumeEvent(String consumerEvent) {
        return toEventString(consumerEvent, "CONSUME", null);
    }

    /**
     * The per-subscription event key: lower-cased "producer-event", or
     * "producer-TM-ALL" (producer lower-cased) when subscribing to all events.
     */
    public static String toConsumerEvent(String event, String producer, boolean isAll) {
        if (isAll)
            return toEventString(event, producer.toLowerCase(), null);
        return toEventString(event, producer, null).toLowerCase();
    }

    public static String toOnSubscribeEvent(String id) {
        return "SUBSCRIBE-TO" + (id != null ? "-" + id : "");
    }

    private Events() {}
}
