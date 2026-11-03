package io.rowweave.sched.net;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.ChannelPoisonedException;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Keepalive frames on the data plane as the stalled-peer fallback.
 * (1) A stalled peer (bound, then sending no data, no keepalive and never closing the socket) is poisoned
 * after SO_TIMEOUT, so the producer is not parked forever;
 * (2) a breaker is not killed by mistake (zero payload bytes while keepalive holds the connection alive, so
 * legitimate slow back-pressure is not mistaken for a timeout and the job completes).
 */
class SocketChannelKeepaliveTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType), new Field("v", DataType.StringType));

    private static DataRow row(int id) {
        DataRow r = new DataRow(2);
        r.set(0, id);
        r.set(1, "x");
        return r;
    }

    /**
     * Stalled-peer fallback: the peer is a bare ServerSocket that accepts and then never
     * reads, never returns credit, never sends a keepalive and never closes. The sink fills the
     * window and parks waiting for credit, so no byte arrives within the socket timeout, the
     * credit-loop read times out, poisons the channel, and the parked send wakes to throw
     * ChannelPoisonedException instead of parking forever. keepaliveMs=80 and soTimeoutMs=300,
     * which are short enough to test.
     */
    @Test
    void zombiePeerTimesOutAndPoisonsSender() throws Exception {
        try (ServerSocket zombie = new ServerSocket(0)) {
            final int window = 2;
            SocketChannelSink sink = new SocketChannelSink(
                    META, "127.0.0.1", zombie.getLocalPort(), window, 80, 300);

            AtomicReference<Throwable> caught = new AtomicReference<>();
            Thread producer = new Thread(() -> {
                try {
                    for (int i = 0; i < 1000; i++) {
                        sink.send(StreamEvent.record(row(i)));  // credit=2 runs out almost at once and parks, since the stalled peer returns no credit
                    }
                } catch (Throwable t) {
                    caught.set(t);
                }
            });
            producer.start();
            // Accept but stay deliberately stalled: no reads, no bytes out, no close.
            Socket accepted = zombie.accept();
            producer.join(5000);
            assertFalse(producer.isAlive(), "a wedged peer does not park the sender forever because SO_TIMEOUT intervenes");
            assertTrue(caught.get() instanceof ChannelPoisonedException
                            || caught.get() instanceof RuntimeException,
                    "after SO_TIMEOUT the channel is poisoned so send throws poison or runtime, measured " + caught.get());
            accepted.close();
        }
    }

    /**
     * A breaker is not killed by mistake, which is the core of result correctness: a real
     * SocketChannelSource (sending credit keepalive) plus a receiver that **deliberately polls slowly**
     * (sleeping far past SO_TIMEOUT before it starts draining, the shape of a downstream breaker that
     * consumes its whole input before emitting). No payload bytes move during that window, but keepalive
     * holds the connection alive, so the sink is never mistaken for timed out; the receiver eventually
     * drains everything, the full set arrives and no poison is raised. soTimeoutMs=300, keepaliveMs=80,
     * slow-poll delay 1200ms (4x the timeout).
     */
    @Test
    void slowBreakerBackpressureNotFalselyKilled() throws Exception {
        final int window = 2;
        SocketChannelSource src = new SocketChannelSource(META, window, 80, 300);
        SocketChannelSink sink = new SocketChannelSink(
                META, "127.0.0.1", src.port(), window, 80, 300);

        final int total = 50;
        AtomicReference<Throwable> producerError = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            try {
                for (int i = 0; i < total; i++) {
                    sink.send(StreamEvent.record(row(i)));
                }
                sink.close();
            } catch (Throwable t) {
                producerError.set(t);
            }
        });
        producer.start();

        // Simulate a breaker: consume the input for a long time without polling, 1200ms against a 300ms socket timeout, so the keepalive must hold the connection live.
        Thread.sleep(1200);

        int got = 0;
        StreamEvent e;
        while ((e = src.poll()) != null) {
            if (e.isRecord()) got++;
            else if (e.isError()) fail("legitimate slow backpressure is not mistaken for an error by the timeout: " + e.cause());
        }
        producer.join(5000);
        assertFalse(producer.isAlive(), "the producer finishes normally");
        assertNull(producerError.get(),
                "keepalive holds the connection so legitimate slow backpressure throws no poison; measured " + producerError.get());
        assertEquals(total, got, "no row is lost while keepalive runs and everything arrives");
        src.close();
    }
}
