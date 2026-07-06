package au.com.tyo.mq;

import io.socket.client.IO;
import io.socket.client.Socket;
import org.json.JSONObject;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
