package io.rowweave.ops.join;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.spill.DiskSpillStore;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The AUTO probe and the operator it selects share one managed memory ledger. */
class ManagedAutoJoinSelectorTest {

    private static final DataSchema LEFT = new DataSchema(
            new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));

    @TempDir
    Path tempDir;

    @Test
    void smallLeftAndSmallRightChooseTheirActualBuildSide() {
        assertBuildSide(5, 200, HashJoin.BuildSide.LEFT, "left-small");
        assertBuildSide(200, 5, HashJoin.BuildSide.RIGHT, "right-small");
    }

    @Test
    void wideProbeRowsSpillWithinBudgetAndReplayBeforeTail() {
        // One 4 KiB encoded row needs ~137 KiB decode upper-bound; 512 KiB fits one row but not 64×2 probes.
        MemoryManager memory = new MemoryManager(512L * 1024);
        HeapPool pool = new HeapPool();
        DiskSpillStore store = new DiskSpillStore(tempDir.resolve("wide").toFile());
        try {
            AutoJoinSelector selector = selector(64, memory, store, pool);
            List<DataRow> left = rows(200, "left-", 4096);
            List<DataRow> right = rows(5, "right-", 4096);
            CollectingRowSink sink = new CollectingRowSink();

            selector.join(stream(left), stream(right), sink);

            assertEquals(5, sink.rows().size());
            assertEquals(HashJoin.BuildSide.RIGHT, selector.selectedBuildSideForTest());
            assertTrue(store.runsCreated() > 0);
            assertTrue(memory.spillCount() > 0, "AUTO probe disk use must be visible in managed metrics");
            assertEquals(0L, memory.reservedBytes());
            assertEquals(0, pool.outstanding());
            assertTrue(memory.peakReservedBytes() <= memory.budget());
        } finally {
            store.close();
        }
    }

    @Test
    void probeErrorCancelsTheOtherSideAndDropsBufferedPrefix() {
        MemoryManager memory = new MemoryManager(1024L * 1024);
        HeapPool pool = new HeapPool();
        DiskSpillStore store = new DiskSpillStore(tempDir.resolve("terminal").toFile());
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicInteger records = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();
        RowStream left = new RowStream() {
            private boolean first = true;
            @Override public io.rowweave.sched.stream.StreamEvent next() {
                if (first) {
                    first = false;
                    return io.rowweave.sched.stream.StreamEvent.record(row(1, "buffered"));
                }
                return io.rowweave.sched.stream.StreamEvent.error(new IllegalStateException("probe failed"));
            }
        };
        RowStream right = new RowStream() {
            @Override public io.rowweave.sched.stream.StreamEvent next() {
                return io.rowweave.sched.stream.StreamEvent.record(row(1, "other"));
            }
            @Override public boolean supportsCancellation() { return true; }
            @Override public boolean cancel(Throwable cause) { cancelled.set(true); return true; }
        };
        try {
            AutoJoinSelector selector = selector(16, memory, store, pool);
            selector.join(left, right, event -> {
                if (event.isRecord()) records.incrementAndGet();
                if (event.isError()) errors.incrementAndGet();
            });

            assertEquals(0, records.get());
            assertEquals(1, errors.get());
            assertTrue(cancelled.get());
            assertEquals(0L, memory.reservedBytes());
            assertEquals(0, pool.outstanding());
            assertEquals(0, store.currentOpenReaders());
            assertEquals(0, store.currentOpenWriters());
        } finally {
            store.close();
        }
    }

    @Test
    void autoHashOverflowRebindsBothReplaySidesToSmjChildren() {
        MemoryManager memory = new MemoryManager(3L * 1024 * 1024);
        HeapPool pool = new HeapPool();
        DiskSpillStore store = new DiskSpillStore(tempDir.resolve("hash-handoff").toFile());
        try {
            AutoJoinSelector selector = selector(64, memory, store, pool);
            CollectingRowSink sink = new CollectingRowSink();

            selector.join(stream(rows(200, "left-", 0)),
                    stream(rows(63, "right-", 32 * 1024)), sink);

            assertEquals(63, sink.rows().size());
            assertEquals(HashJoin.BuildSide.RIGHT, selector.selectedBuildSideForTest());
            assertTrue(store.runsCreated() > 2, "AUTO replay + hash fallback + SMJ sort runs");
            assertEquals(0L, memory.reservedBytes());
            assertEquals(0, pool.outstanding());
        } finally {
            store.close();
        }
    }

    private void assertBuildSide(int leftCount, int rightCount, HashJoin.BuildSide expected, String dir) {
        MemoryManager memory = new MemoryManager(1024L * 1024);
        HeapPool pool = new HeapPool();
        DiskSpillStore store = new DiskSpillStore(tempDir.resolve(dir).toFile());
        try {
            AutoJoinSelector selector = selector(16, memory, store, pool);
            CollectingRowSink sink = new CollectingRowSink();
            selector.join(stream(rows(leftCount, "left-", 0)),
                    stream(rows(rightCount, "right-", 0)), sink);

            assertEquals(Math.min(leftCount, rightCount), sink.rows().size());
            assertTrue(selector.choseHashForTest());
            assertEquals(expected, selector.selectedBuildSideForTest());
            assertEquals(0L, memory.reservedBytes());
            assertEquals(0, pool.outstanding());
        } finally {
            store.close();
        }
    }

    private static AutoJoinSelector selector(int probeRows, MemoryManager memory,
                                             DiskSpillStore store, HeapPool pool) {
        return new AutoJoinSelector(LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"},
                JoinType.INNER, probeRows, memory, store, pool);
    }

    private static List<DataRow> rows(int count, String prefix, int padding) {
        List<DataRow> rows = new ArrayList<>();
        String suffix = repeated('x', padding);
        for (int i = 0; i < count; i++) rows.add(row(i, prefix + i + suffix));
        return rows;
    }

    private static RowStream stream(List<DataRow> rows) {
        return RowStream.fromIterable(rows::iterator);
    }

    private static DataRow row(int key, String value) {
        DataRow row = new DataRow(2);
        row.set(0, key);
        row.set(1, value);
        return row;
    }

    private static String repeated(char value, int count) {
        char[] chars = new char[count];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }
}
