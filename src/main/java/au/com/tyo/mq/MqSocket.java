package au.com.tyo.mq;

import io.socket.client.Ack;
import io.socket.client.IO;
import io.socket.client.Socket;
import org.json.JSONObject;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * The shared connection layer: Socket.IO v4 transport, the AUTHENTICATION
 * handshake, identification, connect listeners (used for re-subscription),
 * and CONSUME_CHUNK reassembly for large messages.
 */
public class MqSocket {

    protected final String host;
    protected final int port;
    protected final String protocol;
    protected final String authToken;

    protected String type = "RAW";
    protected String name = Constants.ANONYMOUS;
    protected final String id = UUID.randomUUID().toString();

    protected Socket socket;
    protected volatile boolean connected = false;
    protected volatile boolean authenticated = false;

    private final List<Runnable> onConnectListeners = new CopyOnWriteArrayList<>();
    private final CountDownLatch readyLatch = new CountDownLatch(1);

    /** Pending inbound CONSUME_CHUNK transfers: transferId → state. */
    private final Map<String, ChunkTransfer> inboundChunks = new ConcurrentHashMap<>();
    /** Direct-dispatch map used by chunk reassembly: consumeEvent → handler. */
    private final Map<String, Consumer<JSONObject>> localHandlers = new ConcurrentHashMap<>();

    private static final class ChunkTransfer {
        final String[] parts;
        final String event;
        int received = 0;
        ChunkTransfer(int total, String event) { this.parts = new String[total]; this.event = event; }
    }

    protected MqSocket(String host, int port, String protocol, String authToken) {
        this.host = host != null ? host : "localhost";
        this.port = port > 0 ? port : Constants.DEFAULT_PORT;
        this.protocol = protocol != null ? protocol : Constants.DEFAULT_PROTOCOL;
        this.authToken = authToken;
    }

    /** Creates the underlying Socket.IO socket without opening it, so that
     *  subscriptions can be registered before connect(). */
    private synchronized Socket ensureSocket() {
        if (socket != null)
            return socket;

        IO.Options options = IO.Options.builder()
                .setTransports(new String[]{"websocket"})
                .setReconnection(true)
                .setReconnectionDelay(1000)
                .setReconnectionDelayMax(30000)
                .build();

        socket = IO.socket(URI.create(url()), options);
        socket.on(Socket.EVENT_CONNECT, args -> onConnect());
        socket.on(Socket.EVENT_DISCONNECT, args -> {
            connected = false;
            authenticated = false;
            inboundChunks.clear();
        });
        socket.on("CONSUME_CHUNK", args -> handleConsumeChunk((JSONObject) args[0]));
        return socket;
    }

    private String url() {
        return protocol + "://" + host + ":" + port;
    }

    /** Connects and returns once the client is registered (and authenticated
     *  when a token was given). */
    public void connect() throws Exception {
        ensureSocket().connect();
        if (!readyLatch.await(10, TimeUnit.SECONDS))
            throw new IllegalStateException("timed out connecting to " + url());
    }

    private void onConnect() {
        connected = true;
        if (authToken != null && !authToken.isEmpty()) {
            socket.once("AUTH_OK", args -> {
                authenticated = true;
                afterConnect();
            });
            socket.once("AUTH_FAIL", args ->
                    System.err.println("tyo-mq authentication failed: "
                            + (args.length > 0 ? args[0] : "")));
            emit("AUTHENTICATION", new JSONObject().put("token", authToken));
        } else {
            afterConnect();
        }
    }

    private void afterConnect() {
        sendIdentification();
        for (Runnable listener : onConnectListeners)
            listener.run();
        readyLatch.countDown();
    }

    protected void sendIdentification() {
        emit(type, new JSONObject().put("name", name).put("id", id).put("consumer_id", name));
    }

    /** Runs on every (re)connect — used by Subscriber to re-subscribe. */
    protected void addOnConnectListener(Runnable listener) {
        onConnectListeners.add(listener);
    }

    protected void on(String event, Consumer<JSONObject> handler) {
        localHandlers.put(event, handler);
        Socket s = ensureSocket();
        s.off(event);
        s.on(event, args -> {
            if (args.length > 0 && args[0] instanceof JSONObject)
                handler.accept((JSONObject) args[0]);
        });
    }

    public void emit(String event, JSONObject payload) {
        ensureSocket().emit(event, payload);
    }

    /**
     * Emit {@code event} with {@code payload} and block until the server's
     * Socket.IO acknowledgement callback fires, returning its first argument as
     * a {@link JSONObject} (the broker replies via {@code callback({...})}).
     *
     * <p>This is the request/response counterpart of {@link #emit} — needed by
     * the E2EE / sealed-sender command set (PREKEY_TAKE, SEALED_CERT_REQUEST,
     * SEALED_DELIVER, ...), whose broker handlers all reply through the ack.
     * The legacy {@link #emit} has no ack support, and the sealed core calls
     * some of these synchronously, so a blocking, timeout-bounded call is
     * required.
     *
     * <p>On timeout this throws (rather than returning {@code null}) so a caller
     * never mistakes "no reply" for a valid one. A {@code null} return means the
     * server acked with no/empty argument.
     *
     * @param timeoutMs how long to wait for the ack before giving up
     * @throws TimeoutException     if no ack arrives within {@code timeoutMs}
     * @throws InterruptedException if the waiting thread is interrupted
     */
    public JSONObject emitWithAck(String event, JSONObject payload, long timeoutMs)
            throws TimeoutException, InterruptedException {
        Socket s = ensureSocket();
        // Capacity-1 queue: the ack fires on Socket.IO's own event thread while
        // the caller blocks here on poll(). A SynchronousQueue would race (offer
        // needs a waiting taker); ArrayBlockingQueue.offer never blocks/loses it.
        final BlockingQueue<Object[]> box = new ArrayBlockingQueue<>(1);
        s.emit(event, new Object[]{payload}, new Ack() {
            @Override
            public void call(Object... args) {
                box.offer(args != null ? args : new Object[0]);
            }
        });

        Object[] res = box.poll(timeoutMs, TimeUnit.MILLISECONDS);
        if (res == null)
            throw new TimeoutException("no ack for '" + event + "' within " + timeoutMs + "ms");
        Object first = res.length > 0 ? res[0] : null;
        if (first == null)
            return null;
        if (first instanceof JSONObject)
            return (JSONObject) first;
        // Defensive: a non-object ack (rare) is wrapped so the caller still gets
        // a JSONObject and can inspect it, rather than a ClassCastException.
        return new JSONObject().put("value", first);
    }

    /**
     * Register a listener for a raw broker-pushed event that carries a single
     * JSON payload — e.g. the sealed-sender {@code SEALED_MESSAGE} push. Public
     * counterpart of the internal {@link #on}: it does NOT enrol the event in
     * the CONSUME_CHUNK reassembly map (these events are not chunked), so it is
     * safe for the E2EE/sealed listeners the secure-chat transport wires up.
     * Registering again for the same event replaces the previous listener.
     */
    public void onEvent(String event, Consumer<JSONObject> handler) {
        onEventWithAck(event, (payload, ack) -> handler.accept(payload));
    }

    /**
     * Like {@link #onEvent}, but also surfaces the SERVER-requested Socket.IO
     * acknowledgement callback when one is present. Socket.IO's Java client
     * delivers a server-requested ack as the LAST element of the listener's
     * args array (an {@link Ack}); the handler answers it with
     * {@code ack.call(responseJson)}.
     *
     * <p>The {@code ack} argument is {@code null} when the server did not
     * request an acknowledgement for this emit (e.g. a live push or a
     * legacy replay) — callers must null-check before answering. Needed by the
     * sealed-sender guaranteed-once drain (`SEALED_SUBSCRIBE {ack:true}`),
     * where the broker deletes a replayed `SEALED_MESSAGE` only after this
     * client confirms {@code {ok:true}} (and dead-letters it on
     * {@code {ok:false}}).
     *
     * <p>Registering again for the same event replaces the previous listener
     * (same replace semantics as {@link #onEvent} — the two share one
     * listener slot per event).
     */
    public void onEventWithAck(String event, BiConsumer<JSONObject, Ack> handler) {
        Socket s = ensureSocket();
        s.off(event);
        s.on(event, args -> {
            if (args.length == 0)
                return;
            // The Socket.IO Java parser usually hands a JSON object back as a
            // JSONObject, but under some server/EIO/transport combinations the
            // first arg arrives as a JSON String (or the ack slot shifts it).
            // Coerce those instead of silently dropping the frame — a dropped
            // SEALED_MESSAGE looks identical to "listener never fired" from the
            // caller's side and is impossible to diagnose downstream.
            JSONObject payload = coercePayload(args[0]);
            if (payload == null) {
                System.err.println("tyo-mq: dropping '" + event
                        + "' — first arg is "
                        + (args[0] == null ? "null" : args[0].getClass().getName())
                        + ", not a JSON object");
                return;
            }
            Ack ack = args[args.length - 1] instanceof Ack
                    ? (Ack) args[args.length - 1] : null;
            handler.accept(payload, ack);
        });
    }

    /** Best-effort: a JSONObject as-is, a JSON String parsed, else null. */
    private static JSONObject coercePayload(Object arg) {
        if (arg instanceof JSONObject)
            return (JSONObject) arg;
        if (arg instanceof String) {
            try { return new JSONObject((String) arg); }
            catch (Exception ignored) { return null; }
        }
        if (arg instanceof Map) {
            try { return new JSONObject((Map<?, ?>) arg); }
            catch (Exception ignored) { return null; }
        }
        return null;
    }

    public void disconnect() {
        if (socket != null)
            socket.disconnect();
    }

    public String getId() { return id; }
    public String getName() { return name; }
    public boolean isConnected() { return connected; }

    private void handleConsumeChunk(JSONObject chunk) {
        try {
            String transferId = chunk.getString("transferId");
            String event = chunk.getString("event");
            int index = chunk.getInt("index");
            int total = chunk.getInt("total");

            ChunkTransfer transfer = inboundChunks.computeIfAbsent(
                    transferId, k -> new ChunkTransfer(total, event));
            synchronized (transfer) {
                transfer.parts[index] = chunk.getString("data");
                transfer.received++;
                if (transfer.received < transfer.parts.length)
                    return;
            }
            inboundChunks.remove(transferId);
            JSONObject assembled = new JSONObject(String.join("", transfer.parts));
            Consumer<JSONObject> handler = localHandlers.get(transfer.event);
            if (handler != null)
                handler.accept(assembled);
        } catch (Exception e) {
            System.err.println("CONSUME_CHUNK: processing error: " + e);
        }
    }
}
