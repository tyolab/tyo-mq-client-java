package au.com.tyo.mq;

/** Protocol constants shared with the tyo-mq server and the other clients. */
public final class Constants {

    public static final String ANONYMOUS = "ANONYMOUS";

    public static final String EVENT_DEFAULT = "tyo-mq-mt-default";
    public static final String EVENT_ALL = "TM-ALL";

    public static final String ALL_PRODUCERS = "TYO-MQ-ALL";

    public static final String METHOD_BROADCAST = "broadcast";
    public static final String METHOD_UNICAST = "unicast";

    public static final String SCOPE_ALL = "all";
    public static final String SCOPE_DEFAULT = "default";

    public static final int DEFAULT_PORT = 17352;
    public static final String DEFAULT_PROTOCOL = "http";

    /** Must match Publisher.CHUNK_SIZE in the JS server/client (256 KB). */
    public static final int CHUNK_SIZE = 256 * 1024;

    private Constants() {}
}
