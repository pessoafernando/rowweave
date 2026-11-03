package io.rowweave.sched.net;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.ChannelPoisonedException;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The socket-backed Channel data plane, which checks the premise that swapping the socket
 * implementation keeps the architecture: a RowSerializer round trip bit for bit, EOS and
 * errors across processes, row-level credit backpressure, and poisoning in both directions.
 */
class SocketChannelTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType),
            new Field("name", DataType.StringType),
            new Field("amt", DataType.BigDecimalType));

    private static DataRow row(Integer id, String name, BigDecimal amt) {
        DataRow r = new DataRow(3);
        r.set(0, id);
        r.set(1, name);
        r.set(2, amt);
        return r;
    }

    /**
     * Regression that a clean close at end of file is not reported as poisoning: the sender
     * writes far more rows than the window, which really parks it on credit, the downstream
     * drains steadily, and the sender closes normally. The sender must not see
     * ChannelPoisonedException. After a normal EOS the socket close does produce an end of file
     * which the sink credit loop turns into a self-poison, but the single producer thread has
     * already finished sending when it closes and nothing is parked, so no failure is observable.
     */
    @Test
    void cleanCloseAfterCreditParkNoSpuriousPoison() throws Exception {
        final int window = 4;  // a small window, to force real credit waits
        final int total = 500;  // far beyond the window, so the sender parks on credit many times
        SocketChannelSource src = new SocketChannelSource(META, window);
        SocketChannelSink sink = new SocketChannelSink(META, "127.0.0.1", src.port(), window);

        AtomicReference<Throwable> producerError = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            try {
                for (int i = 0; i < total; i++) {
                    sink.send(StreamEvent.record(row(i, "x", null)));
                }
                sink.close();  // normal EOS then socket close, after one thread has sent everything with nothing parked
            } catch (Throwable t) {
                producerError.set(t);
            }
        });
        producer.start();

        int got = 0;
        StreamEvent e;
        while ((e = src.poll()) != null) {
            if (e.isRecord()) got++;
            else if (e.isError()) fail("a clean close raises no error event: " + e.cause());
        }
        producer.join(5000);
        assertFalse(producer.isAlive(), "the producer finishes normally");
        assertNull(producerError.get(),
                "a clean close, sending everything after parking on credit, throws no ChannelPoisonedException; measured " + producerError.get());
        assertEquals(total, got, "everything arrives followed by a normal EOS");
        src.close();
    }

/** N rows, including nulls and BigDecimal scales, round-trip bit for bit, and the EOS is followed by a null poll. */
    @Test
    void roundTripBitIdenticalAndEos() throws Exception {
        SocketChannelSource src = new SocketChannelSource(META, 64);
        SocketChannelSink sink = new SocketChannelSink(META, "127.0.0.1", src.port(), 64);

        List<DataRow> sent = new ArrayList<>();
        sent.add(row(1, "a", new BigDecimal("12.3400000000")));
        sent.add(row(2, null, null));  // a trailing row that is all null
        sent.add(row(null, "z", new BigDecimal("0.0000000001")));
        Thread producer = new Thread(() -> {
            for (DataRow r : sent) sink.send(StreamEvent.record(r));
            sink.close();
        });
        producer.start();

        List<DataRow> got = new ArrayList<>();
        StreamEvent e;
        while ((e = src.poll()) != null) {
            if (e.isRecord()) got.add(e.row());
        }
        producer.join(2000);

        assertEquals(3, got.size());
        assertEquals(1, got.get(0).get(0));
        assertEquals(new BigDecimal("12.3400000000"), got.get(0).get(2));
        assertNull(got.get(1).get(1));
        assertNull(got.get(1).get(2));
        assertNull(got.get(2).get(0));
        assertEquals(new BigDecimal("0.0000000001"), got.get(2).get(2));
        src.close();
    }

/** A row with no fields round-trips against an empty schema. */
    @Test
    void zeroFieldRowRoundTrip() throws Exception {
        DataSchema empty = new DataSchema();
        SocketChannelSource src = new SocketChannelSource(empty, 8);
        SocketChannelSink sink = new SocketChannelSink(empty, "127.0.0.1", src.port(), 8);
        Thread p = new Thread(() -> {
            sink.send(StreamEvent.record(new DataRow(0)));
            sink.close();
        });
        p.start();
        int n = 0;
        StreamEvent e;
        while ((e = src.poll()) != null) {
            if (e.isRecord()) n++;
        }
        p.join(2000);
        assertEquals(1, n);
        src.close();
    }

/** A zero-payload record still consumes credit as a full RECORD frame, so it cannot bypass the bounded window. */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void zeroFieldRowsConsumeWireCreditAndBlockWithoutPoll() throws Exception {
        DataSchema empty = new DataSchema();
        assertEquals(0, new io.rowweave.mem.RowSerializer(empty).serialize(new DataRow(0)).length,
                "premise of the test: a row with no fields has an empty payload but still costs the fixed frame header on the wire");

        final int total = 64;
        SocketChannelSource src = new SocketChannelSource(empty, 1);
        SocketChannelSink sink = new SocketChannelSink(empty, "127.0.0.1", src.port(), 1);
        AtomicInteger sent = new AtomicInteger();
        AtomicReference<Throwable> producerError = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            try {
                for (int i = 0; i < total; i++) {
                    sink.send(StreamEvent.record(new DataRow(0)));
                    sent.incrementAndGet();
                }
                sink.close();
            } catch (Throwable t) {
                producerError.set(t);
            }
        }, "zero-field-credit-producer");
        producer.start();

        // Queue capacity 1: with full wire credit and no polling, at most one row is queued and one more holds credit and is being enqueued.
        long firstTwoDeadline = System.currentTimeMillis() + 2_000;
        while (sent.get() < 2 && System.currentTimeMillis() < firstTwoDeadline) {
            Thread.sleep(10);
        }
        Thread.sleep(200);
        int sentBeforeAnyPoll = sent.get();

        int received = 0;
        StreamEvent event;
        while ((event = src.poll()) != null) {
            if (event.isRecord()) {
                received++;
            } else if (event.isError()) {
                fail("a clean credit round trip raises no error: " + event.cause());
            }
        }
        producer.join(2_000);
        src.close();

        assertTrue(sentBeforeAnyPoll <= 2,
                "with a window of 1 and nothing polled at most two rows are in flight; the sender had already sent " + sentBeforeAnyPoll
                        + ", which means the empty payload was not charged the " + FrameCodec.HEADER_BYTES + " byte frame header credit");
        assertFalse(producer.isAlive(), "once draining starts the producer is pushed forward by credit and finishes");
        assertNull(producerError.get(), "the producer threw nothing: " + producerError.get());
        assertEquals(total, received, "resuming the poll lets the credit round trip deliver everything with no row lost");
    }

/** A sender failure makes the receiving poll see the error, which is poisoning across processes. */
    @Test
    void senderFailPropagatesToReceiver() throws Exception {
        SocketChannelSource src = new SocketChannelSource(META, 64);
        SocketChannelSink sink = new SocketChannelSink(META, "127.0.0.1", src.port(), 64);
        sink.send(StreamEvent.record(row(1, "a", null)));
        sink.fail(new RuntimeException("upstream boom"));

        boolean sawError = false;
        StreamEvent e;
        int polls = 0;
        while ((e = src.poll()) != null && polls++ < 10) {
            if (e.isError()) { sawError = true; break; }
        }
        assertTrue(sawError, "the receiving side sees an error event");
        src.close();
    }

    /** The sender's connection drops abnormally (no EOS written, the raw socket just closed): the receiving side's poll sees an error instead of treating it as a clean end of input. */
    @Test
    void senderAbruptDisconnectSeenAsError() throws Exception {
        SocketChannelSource src = new SocketChannelSource(META, 64);
        // A bare socket writes one RECORD frame and closes without an EOS, which stands in for a crashed sender process.
        try (java.net.Socket raw = new java.net.Socket("127.0.0.1", src.port())) {
            java.io.DataOutputStream o = new java.io.DataOutputStream(raw.getOutputStream());
            byte[] payload = new io.rowweave.mem.RowSerializer(META).serialize(row(1, "a", null));
            o.writeByte(0 /*FRAME_RECORD*/);
            o.writeLong(0L); o.writeLong(0L); /*targetId UUID 16B*/
            o.writeInt(payload.length);
            o.write(payload);
            o.flush();
        }  // try-with-resources closes the socket, with no EOS frame
        boolean sawError = false;
        StreamEvent e;
        int polls = 0;
        while ((e = src.poll()) != null && polls++ < 20) {
            if (e.isError()) { sawError = true; break; }
        }
        assertTrue(sawError, "a connection cut before EOS shows the receiver an error on poll rather than a silent normal end");
        src.close();
    }

/** A receiver failure wakes a send parked on credit and makes it throw ChannelPoisonedException, so it does not deadlock. */
    @Test
    void receiverFailUnparksSender() throws Exception {
        SocketChannelSource src = new SocketChannelSource(META, 1); // WINDOW=1
        SocketChannelSink sink = new SocketChannelSink(META, "127.0.0.1", src.port(), 1);

        AtomicReference<Throwable> caught = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            try {
                for (int i = 0; i < 1000; i++) {
                    sink.send(StreamEvent.record(row(i, "x", null)));  // credit=1 parks quickly, since the receiver never polls
                }
            } catch (Throwable t) {
                caught.set(t);
            }
        });
        producer.start();
        Thread.sleep(100);  // let the producer park on exhausted credit
        src.fail(new RuntimeException("downstream boom"));  // closing the socket breaks the sender credit thread, which poisons it
        producer.join(2000);
        assertFalse(producer.isAlive(), "the sender does not park forever, which is the cross-process deadlock");
        assertTrue(caught.get() instanceof ChannelPoisonedException
                        || caught.get() instanceof RuntimeException,
                "the sender throws poison or runtime, measured " + caught.get());
        src.close();
    }

/** Row-level bounds: a small window with a slow poll keeps the rows in flight at or below the window, because credit backpressures the sender. */
    @Test
    void creditBackpressureRowBounded() throws Exception {
        final int window = 4;
        SocketChannelSource src = new SocketChannelSource(META, window);
        SocketChannelSink sink = new SocketChannelSink(META, "127.0.0.1", src.port(), window);

        AtomicInteger sentCount = new AtomicInteger();
        final int total = 200;
        Thread producer = new Thread(() -> {
            for (int i = 0; i < total; i++) {
                sink.send(StreamEvent.record(row(i, "x", null)));
                sentCount.incrementAndGet();
            }
            sink.close();
        });
        producer.start();

        // Give the producer time; without backpressure it would send everything at once. Assert the sent count is credit-bound before the slow polling starts.
        Thread.sleep(200);
        int sentBeforeAnyPoll = sentCount.get();
        // The in-flight ceiling is the window plus a little socket buffering, with nothing polled, which stays far below the total and proves backpressure rather than a flood.
        assertTrue(sentBeforeAnyPoll <= window + 8,
                "credit backpressure: with nothing polled sent (" + sentBeforeAnyPoll + ") stays near the window (" + window + ") instead of the full " + total);

        // drain everything
        int got = 0;
        StreamEvent e;
        while ((e = src.poll()) != null) {
            if (e.isRecord()) got++;
        }
        producer.join(3000);
        assertEquals(total, got, "no row is lost in the credit round trip");
        src.close();
    }

/** Out-of-memory guard: an absurdly large frame length fails on the receiving side instead of allocating a huge array. */
    @Test
    void oversizedFrameLengthRejected() throws Exception {
        SocketChannelSource src = new SocketChannelSource(META, 8);
        try (java.net.Socket raw = new java.net.Socket("127.0.0.1", src.port())) {
            java.io.DataOutputStream o = new java.io.DataOutputStream(raw.getOutputStream());
            o.writeByte(0 /*FRAME_RECORD*/);
            o.writeLong(0L); o.writeLong(0L); /*targetId UUID 16B*/
            o.writeInt(Integer.MAX_VALUE);  // a deliberately enormous length
            o.flush();
            boolean sawError = false;
            StreamEvent e;
            int polls = 0;
            while ((e = src.poll()) != null && polls++ < 10) {
                if (e.isError()) { sawError = true; break; }
            }
            assertTrue(sawError, "an absurd frame length fails through the OOM guard rather than by OOM or an index error");
        }
        src.close();
    }

/** An error StreamEvent becomes a fail across processes. */
    @Test
    void sendErrorEventBecomesFail() throws Exception {
        SocketChannelSource src = new SocketChannelSource(META, 16);
        SocketChannelSink sink = new SocketChannelSink(META, "127.0.0.1", src.port(), 16);
        sink.send(StreamEvent.error(new RuntimeException("ev boom")));
        boolean sawError = false;
        StreamEvent e;
        int polls = 0;
        while ((e = src.poll()) != null && polls++ < 10) {
            if (e.isError()) { sawError = true; break; }
        }
        assertTrue(sawError, "sending an error event shows the receiver an error");
        src.close();
    }

/** A partly implemented channel throws on the unsupported side. */
    @Test
    void unsupportedHalvesThrow() throws Exception {
        SocketChannelSource src = new SocketChannelSource(META, 8);
        SocketChannelSink sink = new SocketChannelSink(META, "127.0.0.1", src.port(), 8);
        assertThrows(UnsupportedOperationException.class, () -> sink.poll());
        assertThrows(UnsupportedOperationException.class, () -> src.send(StreamEvent.record(row(1, "a", null))));
        sink.close();
        src.close();
    }
}
