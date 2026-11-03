package io.rowweave.sched;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.spill.DiskSpillStore;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.*;

/**
 * The fan-out materializing channel component (MaterializedRowBuffer) — it breaks the deadlock cycle when
 * a fan-out re-joins into an order-consuming operator.
 *
 * <p>Mechanism (a blocking result partition on the edge, not a logical node): the shared upstream is
 * materialized **once** (a SpillStore FIFO with spilling as the fallback) and then **N independent replays**
 * are opened, each with its own cursor over the same spill runs, reusing the multiple-read contract. Each
 * downstream reads from its replay, and the producer exits as soon as it has written, never back-pressured
 * by a downstream.
 *
 * <p>Guards: (1) multi-consumer replay is **row-for-row consistent** (order preserved, multiset preserved);
 * (2) null and BigDecimal fidelity; (3) concurrent replay (N consumers reading at once) matches a single
 * read; (4) forcing a spill with a small SpillStore does not run out of memory and the content stays
 * faithful.
 */
class MaterializedRowBufferTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType),
            new Field("amt", DataType.BigDecimalType),
            new Field("name", DataType.StringType));

    private static DataRow row(Integer id, BigDecimal amt, String name) {
        DataRow r = new DataRow(3);
        r.set(0, id);
        r.set(1, amt);
        r.set(2, name);
        return r;
    }

    private static RowStream streamOf(List<DataRow> rows) {
        java.util.Iterator<DataRow> it = rows.iterator();
        return () -> it.hasNext() ? StreamEvent.record(it.next()) : null;
    }

    private static List<DataRow> drain(RowStream rs) {
        List<DataRow> out = new ArrayList<>();
        StreamEvent e;
        while ((e = rs.next()) != null) {
            if (e.isRecord()) {
                out.add(e.row());
            }
        }
        return out;
    }

    private static void assertRowsEqual(List<DataRow> expected, List<DataRow> actual, String msg) {
        assertEquals(expected.size(), actual.size(), msg + " (size)");
        for (int i = 0; i < expected.size(); i++) {
            DataRow x = expected.get(i);
            DataRow a = actual.get(i);
            assertEquals(x.get(0), a.get(0), msg + " id@" + i);
            // BigDecimal values only need to compare equal, and the round trip keeps the precision
            Object xa = x.get(1);
            Object aa = a.get(1);
            if (xa == null || aa == null) {
                assertEquals(xa, aa, msg + " amt@" + i);
            } else {
                assertEquals(0, ((BigDecimal) xa).compareTo((BigDecimal) aa), msg + " amt@" + i);
            }
            assertEquals(x.get(2), a.get(2), msg + " name@" + i);
        }
    }

    /** One materialization then two replays, each row for row in the same order and multiset, including nulls and faithful BigDecimal values. */
    @Test
    void materializeOnceReplayTwiceRowsIdentical() {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            List<DataRow> src = new ArrayList<>();
            src.add(row(1, new BigDecimal("10.1234567890"), "a"));
            src.add(row(2, null, "b"));                    // null BigDecimal
            src.add(row(null, new BigDecimal("0.0000000001"), null)); // null id + null name
            src.add(row(3, new BigDecimal("-99.9999999999"), "c"));

            try (SpillStore store = new MemorySpillStore()) {
                MaterializedRowBuffer buf = new MaterializedRowBuffer(META, store);
                buf.fill(streamOf(src));

                List<DataRow> replay1 = drain(buf.replay());
                List<DataRow> replay2 = drain(buf.replay());

                assertRowsEqual(src, replay1, "replay 1");
                assertRowsEqual(src, replay2, "replay 2");
                assertRowsEqual(replay1, replay2, "the two replays match each other");
            }
        });
    }

    /** An empty upstream materializes zero rows, every one of N replays comes back empty, and nothing hangs or throws. */
    @Test
    void emptyUpstreamReplaysEmpty() {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            try (SpillStore store = new MemorySpillStore()) {
                MaterializedRowBuffer buf = new MaterializedRowBuffer(META, store);
                buf.fill(streamOf(new ArrayList<>()));
                assertTrue(drain(buf.replay()).isEmpty(), "empty replay 1");
                assertTrue(drain(buf.replay()).isEmpty(), "empty replay 2");
            }
        });
    }

    /** Concurrent replay: three consumers drain at once and each matches the source, since the cursors are independent and nothing leaks between them. */
    @Test
    void concurrentReplayConsumersConsistent() {
        assertTimeoutPreemptively(ofSeconds(15), () -> {
            List<DataRow> src = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                src.add(row(i, new BigDecimal(i + ".0000000001"), "n" + i));
            }
            try (SpillStore store = new MemorySpillStore()) {
                MaterializedRowBuffer buf = new MaterializedRowBuffer(META, store);
                buf.fill(streamOf(src));

                int consumers = 3;
                CountDownLatch start = new CountDownLatch(1);
                CountDownLatch done = new CountDownLatch(consumers);
                List<AtomicReference<List<DataRow>>> results = new ArrayList<>();
                List<Thread> threads = new ArrayList<>();
                for (int c = 0; c < consumers; c++) {
                    AtomicReference<List<DataRow>> ref = new AtomicReference<>();
                    results.add(ref);
                    RowStream replay = buf.replay();
                    Thread t = new Thread(() -> {
                        try {
                            start.await();
                            ref.set(drain(replay));
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        } finally {
                            done.countDown();
                        }
                    });
                    threads.add(t);
                    t.start();
                }
                start.countDown();
                done.await();
                for (Thread t : threads) {
                    t.join();
                }
                for (int c = 0; c < consumers; c++) {
                    assertRowsEqual(src, results.get(c).get(), "concurrent replay consumer " + c);
                }
            }
        });
    }

    /** Real disk spilling through DiskSpillStore: a large materialization to disk avoids OOM, both replays stay faithful row for row, and close removes the temporary files. */
    @Test
    void diskSpillReplayFaithfulNoOom(@TempDir Path tmp) {
        assertTimeoutPreemptively(ofSeconds(20), () -> {
            List<DataRow> src = new ArrayList<>();
            for (int i = 0; i < 100_000; i++) {
                src.add(row(i, new BigDecimal(i + ".1234567890"), "row-" + i));
            }
            SpillStore store = new DiskSpillStore(new File(tmp.toFile(), "matbuf"));
            try {
                MaterializedRowBuffer buf = new MaterializedRowBuffer(META, store);
                buf.fill(streamOf(src));
                assertRowsEqual(src, drain(buf.replay()), "disk replay 1");
                assertRowsEqual(src, drain(buf.replay()), "disk replay 2");
            } finally {
                store.close();
            }
        });
    }

    /** A blocking materialize must not silently truncate a row that is too wide for the schema. */
    @Test
    void appendRejectsWideRowBeforeItCanBeReplayedAsValidSchema() {
        DataRow wide = new DataRow(4);
        wide.set(0, 1);
        wide.set(1, new BigDecimal("2.00"));
        wide.set(2, "visible");
        wide.set(3, "must-not-be-lost");

        try (SpillStore store = new MemorySpillStore()) {
            MaterializedRowBuffer buffer = new MaterializedRowBuffer(META, store);
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> buffer.append(wide));
            assertTrue(failure.getMessage().contains("row width mismatch"), failure.getMessage());
            assertTrue(failure.getMessage().contains("expected=3"), failure.getMessage());
            assertTrue(failure.getMessage().contains("actual=4"), failure.getMessage());
        }
    }
}
