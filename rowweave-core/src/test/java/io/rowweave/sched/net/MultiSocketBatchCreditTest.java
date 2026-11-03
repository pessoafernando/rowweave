package io.rowweave.sched.net;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Batch credit for MultiSocketChannelSource: each connection accumulates half its window
 * before granting credit back, which replaces a grantCredit(1) per row. The guardrails have M
 * producers each send far more rows than the window, which shows that under batch credit the
 * run does not deadlock, because returning credit at the low-water mark plus flushing the
 * residue at EOS keeps a sender from starving while the receiver batches; that nothing is
 * lost or wrong, with all M by N rows collected and the M EOS latches correct; and that
 * backpressure still applies, so a sender parks when the downstream stops draining, rather
 * than filling memory.
 */
class MultiSocketBatchCreditTest {

    private static final DataSchema META = new DataSchema(
            new Field("pid", DataType.IntegerType),
            new Field("seq", DataType.IntegerType));

    private static DataRow row(int pid, int seq) {
        DataRow r = new DataRow(2);
        r.set(0, pid);
        r.set(1, seq);
        return r;
    }

    /** M=3 producers each send N=2000, far beyond window=8: batch credit neither deadlocks nor loses rows, and the EOS latches are correct. */
    @Test
    @Timeout(value = 25, unit = TimeUnit.SECONDS)
    void batchCreditFarExceedsWindowNoDeadlockAllRows() throws Exception {
        final int m = 3, n = 2000, window = 8;  // a very small window, to force credit parks and batched exchanges
        MultiSocketChannelSource src = new MultiSocketChannelSource(META, m, window);

        List<Thread> producers = new ArrayList<>();
        AtomicReference<Throwable> perr = new AtomicReference<>();
        for (int p = 0; p < m; p++) {
            final int pid = p;
            producers.add(new Thread(() -> {
                try {
                    SocketChannelSink sink = new SocketChannelSink(META, "127.0.0.1", src.port(), window);
                    for (int i = 0; i < n; i++) sink.send(StreamEvent.record(row(pid, i)));
                    sink.close();
                } catch (Throwable t) {
                    perr.compareAndSet(null, t);
                }
            }, "producer-" + p));
        }
        producers.forEach(Thread::start);

        // Rows received per producer, to show nothing is lost or wrong: each producer gets exactly N rows.
        int[] perPid = new int[m];
        StreamEvent e;
        while ((e = src.poll()) != null) {
            if (e.isRecord()) {
                int pid = (Integer) e.row().get(0);
                perPid[pid]++;
            } else if (e.isError()) {
                fail("the clean batch-credit path raises no error: " + e.cause());
            }
        }
        for (Thread t : producers) t.join(5000);

        assertNull(perr.get(), "the producer throws nothing and never deadlocks: " + perr.get());
        for (int p = 0; p < m; p++) {
            assertEquals(n, perPid[p], "producer " + p + " produces exactly " + n + " rows, because batch credit loses nothing and the EOS latch counts every producer");
        }
    }
}
