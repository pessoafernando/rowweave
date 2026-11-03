package io.rowweave.sched.net;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.ChannelPoisonedException;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An N by M socket shuffle: M producers aggregate into one MultiSocketChannelSource.
 * Covers credit-on-enqueue row-level bounds, poisoning across several connections, the
 * EOS latch and the accept guard.
 */
class MultiSocketChannelTest {

    private static final DataSchema META = new DataSchema(
            new Field("pid", DataType.IntegerType),
            new Field("seq", DataType.IntegerType));

    private static DataRow row(int pid, int seq) {
        DataRow r = new DataRow(2);
        r.set(0, pid);
        r.set(1, seq);
        return r;
    }

    private static SocketChannelSink connect(int port, int window) {
        return new SocketChannelSink(META, "127.0.0.1", port, window);
    }

/** Three producers each send N, so all 3N rows arrive and only the three EOS end the stream. */
    @Test
    void mToOneAggregation() throws Exception {
        final int m = 3, n = 100;
        MultiSocketChannelSource src = new MultiSocketChannelSource(META, m, 64);
        List<Thread> producers = new ArrayList<>();
        for (int p = 0; p < m; p++) {
            final int pid = p;
            producers.add(new Thread(() -> {
                SocketChannelSink sink = connect(src.port(), 32);
                for (int i = 0; i < n; i++) sink.send(StreamEvent.record(row(pid, i)));
                sink.close();
            }));
        }
        producers.forEach(Thread::start);

        int got = 0;
        StreamEvent e;
        while ((e = src.poll()) != null) {
            if (e.isRecord()) got++;
        }
        for (Thread t : producers) t.join(3000);
        assertEquals(m * n, got, "all rows from M producers arrive (the M EOS latch)");
        src.close();
    }

/** Skewed input, where producer 0 sends a great deal, plus a slow poll: credit-on-enqueue bounds rows, so the total in flight stays finite instead of all rows arriving at once. */
    @Test
    void creditOnEnqueueRowBoundedUnderSkew() throws Exception {
        final int m = 2;
        final int cap = 4, perWindow = 4;
        MultiSocketChannelSource src = new MultiSocketChannelSource(META, m, cap);
        AtomicInteger p0Sent = new AtomicInteger();
        final int heavy = 500;
        Thread p0 = new Thread(() -> {
            SocketChannelSink sink = connect(src.port(), perWindow);
            for (int i = 0; i < heavy; i++) {
                sink.send(StreamEvent.record(row(0, i)));
                p0Sent.incrementAndGet();
            }
            sink.close();
        });
        Thread p1 = new Thread(() -> {
            SocketChannelSink sink = connect(src.port(), perWindow);
            sink.send(StreamEvent.record(row(1, 0)));
            sink.close();
        });
        p0.start();
        p1.start();
        Thread.sleep(200);  // stop polling, so the in-flight window fills
        // The in-flight ceiling is the merged-queue capacity plus m times perWindow on the network plus a little socket buffering, which stays far below the heavy count and shows backpressure held.
        assertTrue(p0Sent.get() <= cap + m * perWindow + 16,
                "row-level bounds with credit on enqueue: producer 0 sent (" + p0Sent.get() + ") stays bounded instead of reaching the full load " + heavy);

        int got = 0;
        StreamEvent e;
        while ((e = src.poll()) != null) {
            if (e.isRecord()) got++;
        }
        p0.join(3000);
        p1.join(3000);
        assertEquals(heavy + 1, got, "draining collects everything and no credit is lost in transit");
        src.close();
    }

/** One producer failing poisons the whole stream, so the receiving side sees the error. */
    @Test
    void oneProducerFailPoisonsAll() throws Exception {
        final int m = 3;
        MultiSocketChannelSource src = new MultiSocketChannelSource(META, m, 64);
        List<SocketChannelSink> sinks = new ArrayList<>();
        for (int p = 0; p < m; p++) {
            sinks.add(connect(src.port(), 32));
        }
        sinks.get(0).send(StreamEvent.record(row(0, 0)));
        sinks.get(1).fail(new RuntimeException("producer-1 boom"));  // one producer fails

        boolean sawError = false;
        StreamEvent e;
        int polls = 0;
        while ((e = src.poll()) != null && polls++ < 20) {
            if (e.isError()) { sawError = true; break; }
        }
        assertTrue(sawError, "a failing producer surfaces an error at the receiver");
        src.close();
    }

/** A downstream failure wakes the parked sends of all M producers, so nothing deadlocks. */
    @Test
    void downstreamFailUnparksAllProducers() throws Exception {
        final int m = 3;
        MultiSocketChannelSource src = new MultiSocketChannelSource(META, m, 1); // cap=1
        List<Thread> producers = new ArrayList<>();
        List<Throwable> caught = new java.util.concurrent.CopyOnWriteArrayList<>();
        for (int p = 0; p < m; p++) {
            final int pid = p;
            producers.add(new Thread(() -> {
                try {
                    SocketChannelSink sink = connect(src.port(), 1);  // window=1, so a park follows quickly
                    for (int i = 0; i < 1000; i++) sink.send(StreamEvent.record(row(pid, i)));
                } catch (Throwable t) {
                    caught.add(t);
                }
            }));
        }
        producers.forEach(Thread::start);
        Thread.sleep(200);  // let the producers park on exhausted credit
        src.fail(new RuntimeException("downstream boom"));
        for (Thread t : producers) t.join(3000);
        for (Thread t : producers) {
            assertFalse(t.isAlive(), "no producer parks forever, which is the cross-process deadlock");
        }
        assertTrue(caught.size() >= 1, "at least one producer throws a poison or runtime failure");
        src.close();
    }

/** Declaring M=3 but connecting only two must fail on the accept timeout rather than hang; a short timeout shows the guard really fires. */
    @Test
    void partialConnectFailsNotHang() throws Exception {
        // A short 300ms accept timeout: only two of three connect and the third never dials, so the timeout makes the guard fail the merged queue instead of hanging forever.
        MultiSocketChannelSource src = new MultiSocketChannelSource(META, 3, 64, 300);
        SocketChannelSink s0 = connect(src.port(), 8);
        SocketChannelSink s1 = connect(src.port(), 8);
        s0.send(StreamEvent.record(row(0, 0)));
        // No EOS is sent; past the 300ms accept timeout the guard fails the queue and poll sees the error, so a missing third connection does not hang.
        boolean sawError = false;
        StreamEvent e;
        int polls = 0;
        while ((e = src.poll()) != null && polls++ < 50) {
            if (e.isError()) { sawError = true; break; }
        }
        assertTrue(sawError, "the accept guard fails on a timeout instead of hanging when only 2 of 3 connections arrive");
        s0.close();
        s1.close();
        src.close();
    }

/** Unsupported operations throw on the send side, and the contract passes StreamEvent over the wire rather than a list of rows. */
    @Test
    void unsupportedSendThrows() {
        MultiSocketChannelSource src = new MultiSocketChannelSource(META, 1, 8);
        assertThrows(UnsupportedOperationException.class, () -> src.send(StreamEvent.record(row(0, 0))));
        src.close();
    }
}
