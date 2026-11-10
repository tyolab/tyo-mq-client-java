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
import java.util.function.Supplier;

/**
 * The shared connection layer: Socket.IO v4 transport, the AUTHENTICATION
 * handshake, identification, connect listeners (used for re-subscription),
 * and CONSUME_CHUNK reassembly for large messages.
 */
public class MqSocket {

    protected final String host;
    protected final int port;
    protected final String protocol;
    /**
     * Supplies the auth token for EACH connect attempt.
     *
     * <p>It is a supplier, not a value, because socket.io reconnects on its own and a broker token
     * is short-lived (the issuer mints ~15-minute tokens and validates {@code exp} only at CONNECT).
     * A token captured once at construction is therefore dead for every automatic reconnect after
     * the first few minutes — the socket comes back up, fails AUTHENTICATION, and is never
     * identified or re-subscribed. Asking the owner for a token per attempt is what makes
     * socket.io's own reconnection actually able to succeed.
     */
    protected final Supplier<String> authTokenSupplier;

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

    /** Fixed-token form. Fine for a short-lived socket or a non-expiring token; for anything
     *  long-lived prefer the {@link Supplier} form, so reconnects get a live token. */
    protected MqSocket(String host, int port, String protocol, String authToken) {
        this(host, port, protocol, () -> authToken);
    }

    protected MqSocket(String host, int port, String protocol, Supplier<String> authTokenSupplier) {
        this.host = host != null ? host : "localhost";
        this.port = port > 0 ? port : Constants.DEFAULT_PORT;
        this.protocol = protocol != null ? protocol : Constants.DEFAULT_PROTOCOL;
        this.authTokenSupplier = authTokenSupplier != null ? authTokenSupplier : () -> null;
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
            log("transport disconnected (socket.io will retry)");
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

    /**
     * Socket.IO is connected — for a FIRST connect or any of its automatic reconnects.
     *
     * <p>Note what this deliberately does NOT do: set {@link #connected}. That flag means "session
     * ready", i.e. authenticated AND identified AND re-subscribed, which is what every caller of
     * {@link #isConnected()} actually needs to know. Setting it here — as this used to — made a
     * socket that failed AUTHENTICATION report itself healthy forever: the transport really was up,
     * so {@code isConnected()} stayed true, and every recovery path that early-returns on it
     * (re-dial before send, push-wake drain) became a no-op against a socket the broker had never
     * identified. Messages then queued server-side while the client sat silent until restarted.
     */
    private void onConnect() {
        String token = authTokenSupplier.get();   // fetched per attempt — see authTokenSupplier
        log("transport connected; " + (token != null && !token.isEmpty()
                ? "authenticating" : "no token, identifying directly"));
        if (token != null && !token.isEmpty()) {
            // Clear the previous attempt's handlers first. `once` only unregisters itself when it
            // FIRES, so after N connects that ended in AUTH_OK there were N stale AUTH_FAIL
            // handlers still waiting — which is why a single failure logged itself repeatedly, and
            // why the count grew with every reconnect of a long-lived socket.
            socket.off("AUTH_OK");
            socket.off("AUTH_FAIL");
            socket.once("AUTH_OK", args -> {
                authenticated = true;
                log("AUTH_OK");
                afterConnect();
            });
            socket.once("AUTH_FAIL", args -> {
                // Stay NOT-ready: the transport is up but the broker does not know who we are, so
                // we are deaf. Reporting that honestly is what lets the owner rebuild the socket
                // (with a freshly minted token) instead of trusting a dead session.
                connected = false;
                authenticated = false;
                // NEVER log the token itself.
                log("AUTH_FAIL - not identified, not subscribed, session NOT ready: "
                        + (args.length > 0 ? args[0] : ""));
            });
            emit("AUTHENTICATION", new JSONObject().put("token", token));
        } else {
            afterConnect();
        }
    }

    private void afterConnect() {
        sendIdentification();
        for (Runnable listener : onConnectListeners)
            listener.run();
        // Only now is this socket genuinely usable: identified, and every subscription re-sent.
        connected = true;
        readyLatch.countDown();
        log("session ready (identified as '" + name + "', "
                + onConnectListeners.size() + " on-connect listener(s) run)");
    }

    /** Connection-lifecycle breadcrumb. Deliberately on stderr so this stays platform-neutral
     *  (it surfaces in logcat under the System.err tag on Android). Never logs the auth token. */
    private void log(String msg) {
        // The socket id matters: the owner can build a replacement socket while an older one is
        // still auto-reconnecting underneath, and without this they are indistinguishable in a log.
        System.err.println("tyo-mq[" + name + "/" + id.substring(0, 8) + "]: " + msg);
    }

    protected void sendIdentification() {
        emit(type, new JSONObject().put("name", name).put("id", id).put("consumer_id", name));
    }

    /**
     * Run {@code listener} on every (re)connect, after this socket has re-identified.
     *
     * <p>Used by {@link Subscriber} to re-send its subscriptions, and public because an owner often
     * has its own per-connection registration to repeat. "On connect" must mean every connect: a
     * TRANSPORT-level reconnect (broker restart, network blip, ping timeout) re-identifies the
     * consumer without the owner's connect path running at all, so anything hung only off that path
     * is silently skipped — which is how a secure-chat UAK registration went missing after a broker
     * restart while the consumer looked perfectly re-registered.
     *
     * <p><b>Runs on the socket thread.</b> Do not block in here, and in particular do not call
     * {@link #emitWithAck}: the ack you would wait for arrives on the very thread you are holding.
     * Hand the work to your own executor.
     */
    public void addOnConnectListener(Runnable listener) {
        onConnectListeners.add(listener);
    }

    /** Remove a listener added by {@link #addOnConnectListener}. Safe when it is not present. */
    public void removeOnConnectListener(Runnable listener) {
        onConnectListeners.remove(listener);
    }

    /** How many on-connect listeners are registered. For tests: a subscription that cannot be
     *  undone shows up here as a count that only ever grows. */
    int onConnectListenerCount() {
        return onConnectListeners.size();
    }

    /** Whether a local handler is registered for {@code event}. For tests. */
    boolean hasLocalHandler(String event) {
        return localHandlers.containsKey(event);
    }

    /**
     * Stop dispatching {@code event} locally — the inverse of {@link #on}.
     *
     * <p>Removes it from {@code localHandlers} as well as the live socket, because chunked
     * deliveries are dispatched from that map rather than through the socket's own listeners: a
     * socket-only {@code off} would leave large frames still reaching a handler that was meant to
     * be gone.
     */
    protected void off(String event) {
        localHandlers.remove(event);
        Socket s = socket;
        if (s != null)
            s.off(event);
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
     * Emit with the broker's acknowledgement delivered to {@code onAck} — WITHOUT blocking.
     *
     * <p>The non-blocking sibling of {@link #emitWithAck}. That one parks the caller until the ack
     * or a timeout, which is wrong for anything a user triggers: an Android app that unsubscribes
     * when the user taps a button is on the main thread, and a slow broker would freeze the app.
     * Here the call returns at once and the reply arrives later.
     *
     * <p>{@code onAck} runs on the socket thread, receives the ack's first argument as a
     * {@link JSONObject} (null if the broker acked with nothing, or with a non-object), and must
     * stay trivial — log, set a flag. Never block in it, and never call {@link #emitWithAck} from
     * it: that ack would have to arrive on the very thread you are holding. If the broker never
     * acks (an older broker, or a dropped socket), {@code onAck} is simply never called.
     */
    public void emitWithCallback(String event, JSONObject payload,
                                 java.util.function.Consumer<JSONObject> onAck) {
        ensureSocket().emit(event, new Object[]{payload}, args -> {
            JSONObject reply = (args.length > 0 && args[0] instanceof JSONObject)
                    ? (JSONObject) args[0] : null;
            try {
                onAck.accept(reply);
            } catch (Exception e) {
                System.err.println("ack handler for " + event + " failed: " + e);
            }
        });
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

    /**
     * True only when BOTH our post-identify readiness flag ({@link #connected},
     * set in {@code afterConnect} and flipped false by EVENT_DISCONNECT or a
     * failed AUTHENTICATION) AND Socket.IO's own authoritative
     * {@code socket.connected()} agree.
     *
     * <p>Both halves are load-bearing, and each closes a staleness window that
     * was a real bug:
     *
     * <ul>
     *   <li>Delegating to the library's flag: if a transport drop or OS socket
     *       teardown doesn't surface EVENT_DISCONNECT to us, the shadow boolean
     *       alone could read a stale {@code true}. {@code socket.connected()}
     *       reflects the real engine state.
     *   <li>Setting {@link #connected} only after identify: the transport being
     *       up says nothing about whether the BROKER knows who we are. A socket
     *       that reconnected and then failed AUTHENTICATION is wide open and
     *       completely deaf, and reporting it as connected stranded every
     *       caller that early-returns on this.
     * </ul>
     *
     * <p>A caller may therefore read this as "usable right now": authenticated,
     * identified, and subscriptions re-sent.
     */
    public boolean isConnected() {
        return connected && socket != null && socket.connected();
    }

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
