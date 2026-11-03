package io.rowweave.sched.net;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Socket error and timeout close protocol, plus liveness (no starvation), under injected failures.
 *
 * <p><b>The fallback contract</b>: with a large window and many rows in flight, a mid-stream failure takes
 * the deterministic full close (which may surface as RST). The contract is that the receiver **always sees
 * an error** (through FRAME_ERROR or the RST exception fallback) and **never treats it as a normal EOS**,
 * because that would let the job report success and commit partial data. This test pins that the fallback
 * always holds on the current platform (dual-platform CI is infrastructure work and needs a Linux
 * environment configured separately).
 *
 * <p><b>Liveness / deadlock avoidance</b>: with a small byte window and a slow consumer, the sender parks on
 * byte credit while the consumer drains slowly, so the sender must always be unparked and make progress
 * rather than starve on batching or on {@code available()} semantics. This pins that a window smaller than
 * two rows does not deadlock.
 */
@Timeout(30)
class SocketFailPathInjectionTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType),
            new Field("name", DataType.StringType),
            new Field("amt", DataType.BigDecimalType));

    private static DataRow row(int id) {
        DataRow r = new DataRow(3);
        r.set(0, id);
        r.set(1, "row-" + id);
        r.set(2, new BigDecimal(id));
        return r;
    }

/** A large 1M-byte window with many rows in flight, then a failure mid-stream: the receiver must see the error rather than end quietly. */
    @Test
    void largeWindowMidStreamFail_receiverSeesErrorNotSilentEos() throws Exception {
        int window = 1 << 20;  // a 1M-byte window, which lets the sender run far ahead with many rows in flight, so a reset truncates most easily
        SocketChannelSource src = new SocketChannelSource(META, window);
        SocketChannelSink sink = new SocketChannelSink(META, "127.0.0.1", src.port(), window);

        // Send a batch of rows without draining, so all of it stays in flight, then fail mid-stream, as an upstream operator throwing partway would.
        for (int i = 0; i < 500; i++) {
            sink.send(StreamEvent.record(row(i)));
        }
        sink.fail(new RuntimeException("upstream operator boom"));

        // poll blocks until an element appears and returns null only for EOS, that is a quiet normal end. The contract is that after a fail the error comes first, so null must not be returned.
        boolean sawError = false;
        StreamEvent e;
        int polls = 0;
        while ((e = src.poll()) != null && polls++ < 2000) {
            if (e.isError()) { sawError = true; break; }
            // normal RECORD rows continue.
        }
        assertTrue(sawError, "a mid-stream failure always shows the receiver an error, through FRAME_ERROR or the reset fallback, and never looks like a normal EOS");
        src.close();
    }

/** One upstream failure must fail the merged queue of MultiSocketChannelSource rather than silently dropping it. */
    @Test
    void multiSourceOneUpstreamFail_mergedQueueFails() throws Exception {
        int window = 1 << 20;
        MultiSocketChannelSource src = new MultiSocketChannelSource(META, 2, window, 10_000);
        SocketChannelSink s0 = new SocketChannelSink(META, "127.0.0.1", src.port(), window);
        SocketChannelSink s1 = new SocketChannelSink(META, "127.0.0.1", src.port(), window);

        for (int i = 0; i < 200; i++) {
            s0.send(StreamEvent.record(row(i)));
        }
        s1.fail(new RuntimeException("upstream-1 boom"));  // one path fails
        s0.close();  // the other ends normally with an EOS

        boolean sawError = false;
        StreamEvent e;
        int polls = 0;
        while ((e = src.poll()) != null && polls++ < 5000) {
            if (e.isError()) { sawError = true; break; }
        }
        assertTrue(sawError, "fan-in: if any upstream fails the merged queue fails too and never reports success silently");
        src.close();
    }

/** A small byte window with a slow consumer parks the sender on backpressure, and slow draining must still finish rather than starve or deadlock. */
    @Test
    void smallByteWindowSlowConsumer_noStarvation() throws Exception {
        int window = 64;  // below one row in bytes, since a row with row-N plus a serialized BigDecimal exceeds 64B, which exercises the window under twice the maximum row size
        SocketChannelSource src = new SocketChannelSource(META, window);
        SocketChannelSink sink = new SocketChannelSink(META, "127.0.0.1", src.port(), window);

        final int total = 300;
        AtomicReference<Throwable> caught = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            try {
                for (int i = 0; i < total; i++) {
                    sink.send(StreamEvent.record(row(i)));
                }
                sink.close();
            } catch (Throwable t) {
                caught.set(t);
            }
        }, "producer");
        producer.start();

        // A slow consumer that pauses between rows keeps the sender parked on byte credit for a long time.
        // The contract is that LocalChannel.poll() blocks until an element appears and returns null for EOS, with the EOS sentinel left in the queue so every later poll returns null.
        int got = 0;
        StreamEvent e;
        while ((e = src.poll()) != null) {
            if (e.isError()) {
                fail("no error is expected: " + e.cause());
            }
            got++;
            if (got % 50 == 0) Thread.sleep(5);  // periodically slow drains
        }
        // A null from poll means the EOS arrived, which is the normal end.
        producer.join(5000);
        assertNull(caught.get(), "the sender throws nothing, because a small window does not starve it");
        assertFalse(producer.isAlive(), "the sender does not park forever: it makes progress even when the window is smaller than one row");
        assertEquals(total, got, "every row is delivered because backpressure keeps advancing, losing nothing and starving nobody");
        src.close();
    }
}
