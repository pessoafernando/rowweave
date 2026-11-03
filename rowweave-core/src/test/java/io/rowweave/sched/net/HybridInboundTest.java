package io.rowweave.sched.net;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.Channel;
import io.rowweave.sched.LocalChannelSink;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A downstream inbound fans in both socket and local producers through one
 * {@link io.rowweave.sched.MultiProducerChannel}, where producerCount is socketN plus localN so
 * the EOS converges at one point. The socket side uses Netty credit, while the local side
 * passes DataRow references straight through {@link LocalChannelSink} with zero wire bytes and
 * backpressure from the row count alone.
 */
class HybridInboundTest {

    private static final DataSchema META = new DataSchema(
            new Field("pid", DataType.IntegerType), new Field("seq", DataType.IntegerType));

    private static DataRow row(int pid, int seq) {
        DataRow r = new DataRow(2);
        r.set(0, pid);
        r.set(1, seq);
        return r;
    }

    /** One socket producer plus one local producer: both paths must be drained, and the EOS needs both producers done. */
    @Test
    void socketPlusLocalAggregated() throws Exception {
        final int n = 100;
        // socketProducers=1 and localProducers=1, so a merged fan-in of two producers
        MultiSocketChannelSource src = new MultiSocketChannelSource(META, 1, 1, 64, 5000,
                SocketChannelSink.KEEPALIVE_DISABLED, SocketChannelSink.KEEPALIVE_DISABLED);
        Channel local = new LocalChannelSink(src.sharedChannel());

        Thread socketProducer = new Thread(() -> {
            SocketChannelSink sink = new SocketChannelSink(META, "127.0.0.1", src.port(), 32);
            for (int i = 0; i < n; i++) sink.send(StreamEvent.record(row(0, i)));
            sink.close();
        });
        Thread localProducer = new Thread(() -> {
            for (int i = 0; i < n; i++) local.send(StreamEvent.record(row(1, i)));
            local.close(); // → producerDone()
        });
        socketProducer.start();
        localProducer.start();

        int got = 0, socketRows = 0, localRows = 0;
        StreamEvent e;
        while ((e = src.poll()) != null) {
            if (e.isRecord()) {
                got++;
                if ((int) e.row().get(0) == 0) socketRows++; else localRows++;
            }
        }
        socketProducer.join(3000);
        localProducer.join(3000);
        assertEquals(2 * n, got, "the socket and local paths together deliver everything (producerCount=2 EOS latch)");
        assertEquals(n, socketRows, "the socket path carries 100 rows");
        assertEquals(n, localRows, "the local path carries 100 rows");
        src.close();
    }

    /** A local producer passes references: the DataRow a downstream poll gets is the same instance the upstream sent, which is reuse=true with no copy. */
    @Test
    void localPassesRowByReference() throws Exception {
        MultiSocketChannelSource src = new MultiSocketChannelSource(META, 0, 1, 64, 5000,
                SocketChannelSink.KEEPALIVE_DISABLED, SocketChannelSink.KEEPALIVE_DISABLED);
        Channel local = new LocalChannelSink(src.sharedChannel());
        DataRow sent = row(7, 7);
        local.send(StreamEvent.record(sent));
        local.close();
        StreamEvent e = src.poll();
        assertNotNull(e);
        assertTrue(e.isRecord());
        assertSame(sent, e.row(), "a local edge hands over the same reference with no serialization and no copy");
        assertNull(src.poll(), "1 local producer done → EOS");
        src.close();
    }

    /** reuse=false is an isolation boundary, so it must copy a mutable Timestamp cell as well. */
    @Test
    void localReuseOffSnapshotsMutableTimestamp() throws Exception {
        DataSchema timeMeta = new DataSchema(new Field("ts", DataType.TimestampType));
        MultiSocketChannelSource src = new MultiSocketChannelSource(timeMeta, 0, 1, 8, 5000,
                SocketChannelSink.KEEPALIVE_DISABLED, SocketChannelSink.KEEPALIVE_DISABLED);
        Channel local = new LocalChannelSink(src.sharedChannel(), false);
        Timestamp timestamp = new Timestamp(1_700_000_000_123L);
        timestamp.setNanos(123_456_789);
        DataRow sent = new DataRow(1);
        sent.set(0, timestamp);

        local.send(StreamEvent.record(sent));
        local.close();
        timestamp.setNanos(987_654_321);

        StreamEvent received = src.poll();
        assertNotNull(received);
        assertNotSame(sent, received.row());
        assertNotSame(timestamp, received.row().getTimestamp(0));
        assertEquals(123_456_789, received.row().getTimestamp(0).getNanos());
        assertNull(src.poll());
        src.close();
    }

    /** Holding the lock across a batch is allowed only for a single-producer channel; with several producers the rows must interleave fairly, so no lock convoy forms. */
    @Test
    void localBatchEligibilityRequiresSingleProducer() {
        assertTrue(new LocalChannelSink(new io.rowweave.sched.MultiProducerChannel(8, 1))
                .supportsRecordBatch());
        assertFalse(new LocalChannelSink(new io.rowweave.sched.MultiProducerChannel(8, 2))
                .supportsRecordBatch());
        LocalChannelSink snapshotting = new LocalChannelSink(
                new io.rowweave.sched.MultiProducerChannel(8, 1), false);
        assertFalse(snapshotting.supportsRecordBatch(),
                "reuse=false must snapshot each row immediately instead of deferring references");
        assertThrows(IllegalStateException.class,
                () -> snapshotting.sendRecords(new StreamEvent[]{StreamEvent.record(row(1, 1))}, 1));
    }

    /** Pure local with socketN=0: no socket is bound and the fan-in is direct. */
    @Test
    void pureLocalNoSocket() throws Exception {
        final int localN = 3, n = 50;
        MultiSocketChannelSource src = new MultiSocketChannelSource(META, 0, localN, 64, 5000,
                SocketChannelSink.KEEPALIVE_DISABLED, SocketChannelSink.KEEPALIVE_DISABLED);
        CopyOnWriteArrayList<Thread> ts = new CopyOnWriteArrayList<>();
        for (int p = 0; p < localN; p++) {
            final int pid = p;
            Channel local = new LocalChannelSink(src.sharedChannel());
            Thread t = new Thread(() -> {
                for (int i = 0; i < n; i++) local.send(StreamEvent.record(row(pid, i)));
                local.close();
            });
            ts.add(t);
            t.start();
        }
        int got = 0;
        StreamEvent e;
        while ((e = src.poll()) != null) {
            if (e.isRecord()) got++;
        }
        for (Thread t : ts) t.join(3000);
        assertEquals(localN * n, got, "3 local producers of 50 rows each give 150 rows and the three producer completions end with EOS");
        src.close();
    }

    /** A local producer failure poisons the merged queue, so a downstream poll sees the error, which is the same convergence as a socket failure. */
    @Test
    void localFailPoisonsInbound() throws Exception {
        MultiSocketChannelSource src = new MultiSocketChannelSource(META, 0, 2, 64, 5000,
                SocketChannelSink.KEEPALIVE_DISABLED, SocketChannelSink.KEEPALIVE_DISABLED);
        Channel local0 = new LocalChannelSink(src.sharedChannel());
        local0.send(StreamEvent.record(row(0, 0)));
        local0.fail(new RuntimeException("local boom"));
        boolean sawError = false;
        StreamEvent e;
        int polls = 0;
        while ((e = src.poll()) != null && polls++ < 10) {
            if (e.isError()) { sawError = true; break; }
        }
        assertTrue(sawError, "a failing local producer poisons the merged queue and downstream sees an error");
        src.close();
    }
}
