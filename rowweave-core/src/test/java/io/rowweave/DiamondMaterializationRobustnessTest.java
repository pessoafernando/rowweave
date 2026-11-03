package io.rowweave;

import io.rowweave.mem.spill.DiskSpillStore;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import io.rowweave.sched.MaterializedRowBuffer;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Two hard edges of diamond materialization: two replay cursors over the same buffer read
 * **concurrently** and stay bit-identical (DiskSpillStore opens a fresh FileInputStream per read, so
 * there is no shared position); an upstream failure while filling makes commit **fail fast** rather
 * than leave a half-written run behind.
 */
class DiamondMaterializationRobustnessTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));

    /**
     * Two replay cursors over the same buffer, read concurrently by two threads, each staying bit-identical to the source row by row, since each holds its own FileInputStream and
     * shares no offset state. This runs against real disk through DiskSpillStore, with two handles open on the same file.
     */
    @Test
    void concurrentReplayCursorsBitIdentical(@TempDir Path tmp) throws Exception {
        List<DataRow> src = new ArrayList<>();
        for (int i = 0; i < 20_000; i++) {
            DataRow r = new DataRow(2);
            r.set(0, i % 100);
            r.set(1, new BigDecimal(i + ".1234567890"));
            src.add(r);
        }
        SpillStore store = new DiskSpillStore(new File(tmp.toFile(), "concurrent"));
        try {
            MaterializedRowBuffer buf = new MaterializedRowBuffer(META, store);
            Iterator<DataRow> it = src.iterator();
            buf.fill(() -> it.hasNext() ? StreamEvent.record(it.next()) : null);

            CountDownLatch start = new CountDownLatch(1);
            List<AtomicReference<List<DataRow>>> refs = new ArrayList<>();
            List<Thread> threads = new ArrayList<>();
            for (int c = 0; c < 4; c++) {
                AtomicReference<List<DataRow>> ref = new AtomicReference<>();
                refs.add(ref);
                RowStream replay = buf.replay();  // an independent cursor
                Thread t = new Thread(() -> {
                    try {
                        start.await();
                        List<DataRow> got = new ArrayList<>();
                        StreamEvent e;
                        while ((e = replay.next()) != null) {
                            if (e.isRecord()) {
                                got.add(e.row());
                            }
                        }
                        ref.set(got);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                });
                threads.add(t);
                t.start();
            }
            start.countDown();
            for (Thread t : threads) {
                t.join();
            }
            for (int c = 0; c < refs.size(); c++) {
                List<DataRow> got = refs.get(c).get();
                assertEquals(src.size(), got.size(), "concurrent cursor " + c + " rows");
                for (int i = 0; i < src.size(); i++) {
                    assertEquals(src.get(i).get(0), got.get(i).get(0), "cursor " + c + " k@" + i);
                    assertEquals(0, ((BigDecimal) src.get(i).get(1)).compareTo((BigDecimal) got.get(i).get(1)),
                            "cursor " + c + " amt@" + i);
                }
            }
        } finally {
            store.close();
        }
    }

    /**
     * When the upstream of a diamond fill fails partway, commit fails fast with an exception and yields no silently incomplete result.
     * (The Source throws a RuntimeException partway through iteration; fill runs on its own execution, where a failed upstream is detected and the join fails fast.)
     */
    @Test
    void fillUpstreamFailurePropagatesFailFast() {
        Source failing = new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() {
                return new Iterator<DataRow>() {
                    int i = 0;
                    @Override public boolean hasNext() { return true; }
                    @Override public DataRow next() {
                        if (i++ >= 100) {
                            throw new RuntimeException("boom: upstream read failure at row 100");
                        }
                        DataRow r = new DataRow(2);
                        r.set(0, i % 8);
                        r.set(1, new BigDecimal(i % 50));
                        return r;
                    }
                };
            }
        };
        BatchSession s = RowWeave.createBatchSession("fill-fail");
        DataSet mid = s.fromSource(failing).filter("amt >= 5");
        DataSet b1 = mid.filter("k >= 2");
        DataSet b2 = mid.filter("k <= 6");
        DataSet joined = b1.join(b2, SortMergeJoin.of(META, META, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
        WorkerCollectingOutput out = new WorkerCollectingOutput(joined.getSchema());
        joined.sinkTo(out);

        RuntimeException ex = assertThrows(RuntimeException.class, s::execute,
                "fill upstream failure must fail fast, never a partial write");
        // The root cause stays traceable to the upstream failure through the pipeline wrapper.
        assertTrue(rootCauseContains(ex, "boom"), "exception must carry the upstream failure root cause, actual: " + ex);
    }

    private static boolean rootCauseContains(Throwable t, String needle) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c.getMessage() != null && c.getMessage().contains(needle)) {
                return true;
            }
        }
        return false;
    }
}
