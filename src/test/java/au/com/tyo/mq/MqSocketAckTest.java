package au.com.tyo.mq;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.json.JSONObject;
import org.junit.Test;

import java.util.concurrent.TimeoutException;

/**
 * Unit test for {@link MqSocket#emitWithAck} — added alongside emitWithAck,
 * which the E2EE / sealed-sender command set needs (the legacy emit() has no
 * ack support).
 *
 * <p>The contract that matters most here (cf. tracked issue #40: never silently
 * swallow a missing broker reply) is the timeout branch: when no server acks,
 * emitWithAck must THROW, not return null. We assert that against a socket
 * pointed at a dead port (it never connects, so the ack never fires), with a
 * short timeout so the test stays fast. The happy-path ack (broker replies with
 * a JSONObject) is proven end-to-end by the secure-chat sealed round-trip test,
 * which drives real PREKEY_TAKE / SEALED_* acks through a live broker.
 */
public class MqSocketAckTest {

    /** A dead port on localhost: the socket buffers the emit but never connects,
     *  so the ack callback is never invoked and poll() must time out. */
    private static final int DEAD_PORT = 1;

    @Test
    public void emitWithAckThrowsTimeoutWhenNoAckArrives() throws Exception {
        Subscriber sock = new Subscriber("ack-test", "127.0.0.1", DEAD_PORT, "http", null);
        long start = System.currentTimeMillis();
        try {
            sock.emitWithAck("PREKEY_TAKE", new JSONObject().put("identity", "nobody"), 250);
            fail("expected a TimeoutException when the broker never acks");
        } catch (TimeoutException expected) {
            long elapsed = System.currentTimeMillis() - start;
            // It waited roughly the timeout (proving it blocked for the ack) and
            // then threw instead of returning a bogus value.
            assertTrue("should wait about the timeout before throwing, was " + elapsed + "ms",
                    elapsed >= 200);
        } finally {
            sock.disconnect();
        }
    }
}
