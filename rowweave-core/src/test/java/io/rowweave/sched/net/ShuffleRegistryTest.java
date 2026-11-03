package io.rowweave.sched.net;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ShuffleRegistry addressing: register, lookup and awaitPartitions, plus an end-to-end
 * closed loop of the registry with an N by M shuffle.
 */
class ShuffleRegistryTest {

    private static final DataSchema META = new DataSchema(
            new Field("pid", DataType.IntegerType),
            new Field("seq", DataType.IntegerType));

    private static DataRow row(int pid, int seq) {
        DataRow r = new DataRow(2);
        r.set(0, pid);
        r.set(1, seq);
        return r;
    }

/** Register M endpoints in partition order, then lookup returns the same M endpoints in that order. */
    @Test
    void registerLookupOrdered() {
        ShuffleRegistry reg = new ShuffleRegistry();
        reg.register("e1", 0, 3, new ShuffleEndpoint("h0", 100));
        reg.register("e1", 2, 3, new ShuffleEndpoint("h2", 102));
        reg.register("e1", 1, 3, new ShuffleEndpoint("h1", 101));
        List<ShuffleEndpoint> eps = reg.lookup("e1");
        assertEquals(3, eps.size());
        assertEquals(100, eps.get(0).port());
        assertEquals(101, eps.get(1).port());
        assertEquals(102, eps.get(2).port());
    }

/** awaitPartitions waits for asynchronous registration to complete, and throws on a timeout when fewer than M arrive rather than hanging. */
    @Test
    void awaitPartitionsBlocksThenReturns() throws Exception {
        ShuffleRegistry reg = new ShuffleRegistry();
        Thread late = new Thread(() -> {
            try { Thread.sleep(100); } catch (InterruptedException ignored) { }
            reg.register("e2", 0, 2, new ShuffleEndpoint("a", 1));
            reg.register("e2", 1, 2, new ShuffleEndpoint("b", 2));
        });
        late.start();
        List<ShuffleEndpoint> eps = reg.awaitPartitions("e2", 2, 2000);
        assertEquals(2, eps.size());
        late.join();
    }

    @Test
    void awaitPartitionsTimeoutThrows() {
        ShuffleRegistry reg = new ShuffleRegistry();
        reg.register("e3", 0, 3, new ShuffleEndpoint("a", 1));  // only one of three
        assertThrows(IllegalStateException.class, () -> reg.awaitPartitions("e3", 3, 150));
    }

/** Concurrent registers from several consumer threads lose nothing. */
    @Test
    void concurrentRegisterNoLoss() throws Exception {
        ShuffleRegistry reg = new ShuffleRegistry();
        final int m = 16;
        List<Thread> ts = new java.util.ArrayList<>();
        for (int i = 0; i < m; i++) {
            final int idx = i;
            ts.add(new Thread(() -> reg.register("e4", idx, m, new ShuffleEndpoint("h" + idx, 1000 + idx))));
        }
        ts.forEach(Thread::start);
        List<ShuffleEndpoint> eps = reg.awaitPartitions("e4", m, 2000);
        for (Thread t : ts) t.join();
        assertEquals(m, eps.size());
        for (int i = 0; i < m; i++) {
            assertEquals(1000 + i, eps.get(i).port(), "partition " + i + " is neither lost nor crossed over");
        }
    }

/** Several edges stay isolated. */
    @Test
    void multiEdgeIsolation() {
        ShuffleRegistry reg = new ShuffleRegistry();
        reg.register("eA", 0, 1, new ShuffleEndpoint("a", 1));
        reg.register("eB", 0, 1, new ShuffleEndpoint("b", 2));
        assertEquals(1, reg.lookup("eA").get(0).port());
        assertEquals(2, reg.lookup("eB").get(0).port());
    }

/** End to end: the consumer starts a MultiSocketChannelSource and registers its port, the producer awaits the partitions and connects, and all M streams arrive as one. */
    @Test
    void endToEndRegistryAddressedShuffle() throws Exception {
        final int m = 3, n = 50;
        ShuffleRegistry reg = new ShuffleRegistry();
        final String edge = "stage2->stage3";

        // One sink aggregates M producers: on the consumer side here there is a single MultiSocketChannelSource that M producers connect to.
        // This stands in for M consumer partitions, since the registry records this sink's endpoint at partition 0.
        // Normally each consumer registers its own port and a producer routes by partition, so this checks registry addressing plus the N by M data-plane loop.
        MultiSocketChannelSource sink = new MultiSocketChannelSource(META, m, 64);
        reg.register(edge, 0, 1, new ShuffleEndpoint("127.0.0.1", sink.port()));

        List<Thread> producers = new java.util.ArrayList<>();
        for (int p = 0; p < m; p++) {
            final int pid = p;
            producers.add(new Thread(() -> {
                // the producer addresses through the registry, waiting for the consumers to register
                List<ShuffleEndpoint> eps = reg.awaitPartitions(edge, 1, 2000);
                ShuffleEndpoint ep = eps.get(0);
                SocketChannelSink s = new SocketChannelSink(META, ep.host(), ep.port(), 16);
                for (int i = 0; i < n; i++) s.send(StreamEvent.record(row(pid, i)));
                s.close();
            }));
        }
        producers.forEach(Thread::start);

        int got = 0;
        StreamEvent e;
        while ((e = sink.poll()) != null) {
            if (e.isRecord()) got++;
        }
        for (Thread t : producers) t.join(3000);
        assertEquals(m * n, got, "registry addressing plus the N by M data plane delivers every row");
        sink.close();
    }
}
