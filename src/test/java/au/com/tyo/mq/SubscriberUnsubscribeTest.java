package au.com.tyo.mq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.json.JSONObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link Subscriber#unsubscribe} has to undo ALL of what {@code subscribe} set up — the local
 * handler, the broker subscription, and the on-connect re-send. These tests run against a socket
 * that never connects, recording emits instead of sending them, so each of the three is checked
 * without a broker.
 */
public class SubscriberUnsubscribeTest {

    private static final int DEAD_PORT = 1;

    /** Records emits instead of sending them, and lets the test choose "connected". */
    private static final class Recording extends Subscriber {
        final List<String> events = new ArrayList<>();
        final List<JSONObject> payloads = new ArrayList<>();

        Recording() {
            super("chat-test", "127.0.0.1", DEAD_PORT, "http", (String) null);
        }

        @Override
        public void emit(String event, JSONObject payload) {
            events.add(event);
            payloads.add(payload);
        }

        void setConnected(boolean c) {
            connected = c;
        }

        /** Stands in for a reconnect completing: run what afterConnect() would run. */
        void simulateReconnect() throws Exception {
            java.lang.reflect.Field f = MqSocket.class.getDeclaredField("onConnectListeners");
            f.setAccessible(true);
            @SuppressWarnings("unchecked")
            List<Runnable> listeners = (List<Runnable>) f.get(this);
            for (Runnable r : listeners) r.run();
        }

        String consumeEvent(String producer, String event) {
            return Events.toConsumeEvent(Events.toConsumerEvent(event, producer, false));
        }
    }

    @Test
    public void unsubscribeRemovesTheHandlerAndTheResend() {
        Recording s = new Recording();
        int baseline = s.onConnectListenerCount();
        s.subscribe(Constants.ALL_PRODUCERS, "hilia-link-x-up", (m, f, a, o) -> { });
        assertTrue(s.hasLocalHandler(s.consumeEvent(Constants.ALL_PRODUCERS, "hilia-link-x-up")));
        assertEquals(baseline + 1, s.onConnectListenerCount());

        s.setConnected(true);
        s.unsubscribe(Constants.ALL_PRODUCERS, "hilia-link-x-up");

        assertFalse("the local handler must be gone",
                s.hasLocalHandler(s.consumeEvent(Constants.ALL_PRODUCERS, "hilia-link-x-up")));
        assertEquals("the re-send must be gone, or the next reconnect re-subscribes",
                baseline, s.onConnectListenerCount());
    }

    /**
     * THE regression this exists for: after an unsubscribe, a reconnect must not quietly
     * SUBSCRIBE again. Before this method existed there was no way to stop it at all.
     */
    @Test
    public void aReconnectAfterUnsubscribeDoesNotResubscribe() throws Exception {
        Recording s = new Recording();
        s.subscribe(Constants.ALL_PRODUCERS, "hilia-link-x-up", (m, f, a, o) -> { });
        s.setConnected(true);
        s.unsubscribe(Constants.ALL_PRODUCERS, "hilia-link-x-up");
        s.events.clear();

        s.simulateReconnect();

        assertFalse("a reconnect re-SUBSCRIBEd a topic the caller had finished with",
                s.events.contains("SUBSCRIBE"));
    }

    /** The UNSUBSCRIBE carries the same object SUBSCRIBE did, so the broker can key it the same. */
    @Test
    public void theUnsubscribePayloadMatchesTheSubscribePayload() {
        Recording s = new Recording();
        s.setConnected(true);
        s.subscribe(Constants.ALL_PRODUCERS, "hilia-link-x-up", (m, f, a, o) -> { });
        s.unsubscribe(Constants.ALL_PRODUCERS, "hilia-link-x-up");

        JSONObject sub = s.payloads.get(s.events.indexOf("SUBSCRIBE"));
        JSONObject unsub = s.payloads.get(s.events.indexOf("UNSUBSCRIBE"));
        for (String k : new String[] {"event", "producer", "consumer", "scope", "consumer_id"}) {
            assertEquals("field " + k, String.valueOf(sub.opt(k)), String.valueOf(unsub.opt(k)));
        }
    }

    /**
     * Offline, the UNSUBSCRIBE waits for the next connect — and is sent exactly once.
     *
     * <p>It has to wait: identification is where the broker restores a named consumer's
     * subscriptions, so an UNSUBSCRIBE sent before it would be undone by that restore.
     */
    @Test
    public void anOfflineUnsubscribeIsSentOnceOnTheNextConnect() throws Exception {
        Recording s = new Recording();
        s.subscribe(Constants.ALL_PRODUCERS, "hilia-link-x-up", (m, f, a, o) -> { });
        s.setConnected(false);
        s.unsubscribe(Constants.ALL_PRODUCERS, "hilia-link-x-up");
        assertFalse("nothing can be sent while disconnected", s.events.contains("UNSUBSCRIBE"));

        s.simulateReconnect();
        assertEquals(1, count(s.events, "UNSUBSCRIBE"));

        s.simulateReconnect();
        assertEquals("a one-shot, not a fixture of every future reconnect",
                1, count(s.events, "UNSUBSCRIBE"));
    }

    /** Subscribing twice to the same event no longer stacks re-sends. */
    @Test
    public void repeatedSubscribeDoesNotAccumulateResends() throws Exception {
        Recording s = new Recording();
        int baseline = s.onConnectListenerCount();
        for (int i = 0; i < 5; i++)
            s.subscribe(Constants.ALL_PRODUCERS, "hilia-link-x-up", (m, f, a, o) -> { });
        assertEquals(baseline + 1, s.onConnectListenerCount());

        s.events.clear();
        s.simulateReconnect();
        assertEquals("one reconnect, one SUBSCRIBE", 1, count(s.events, "SUBSCRIBE"));
    }

    /** Unsubscribing something never subscribed is harmless. */
    @Test
    public void unsubscribingTheUnknownIsHarmless() {
        Recording s = new Recording();
        int baseline = s.onConnectListenerCount();
        s.setConnected(true);
        s.unsubscribe(Constants.ALL_PRODUCERS, "never-subscribed");
        assertEquals(baseline, s.onConnectListenerCount());
    }

    /** Other subscriptions are untouched. */
    @Test
    public void unsubscribingOneLeavesTheOthers() {
        Recording s = new Recording();
        s.subscribe(Constants.ALL_PRODUCERS, "e2ee.inbox", (m, f, a, o) -> { });
        s.subscribe(Constants.ALL_PRODUCERS, "hilia-link-x-up", (m, f, a, o) -> { });
        s.setConnected(true);
        s.unsubscribe(Constants.ALL_PRODUCERS, "hilia-link-x-up");
        assertTrue("the chat inbox must survive a link ending",
                s.hasLocalHandler(s.consumeEvent(Constants.ALL_PRODUCERS, "e2ee.inbox")));
    }

    private static int count(List<String> xs, String x) {
        int n = 0;
        for (String y : xs) if (x.equals(y)) n++;
        return n;
    }
}
