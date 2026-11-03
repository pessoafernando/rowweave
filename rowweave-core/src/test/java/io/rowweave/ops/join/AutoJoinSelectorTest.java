package io.rowweave.ops.join;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Under AUTO, AutoJoinSelector probes both sides at run time and chooses hash or SMJ,
 * and the result is multiset-equivalent to SMJ. Probing alternates, concat is conserved,
 * a large cross small picks hash, and large cross large or a small left side falls back to SMJ.
 */
class AutoJoinSelectorTest {

    @TempDir
    Path tempDir;
    private final List<io.rowweave.mem.spill.DiskSpillStore> stores = new ArrayList<>();

    private static final DataSchema L = new DataSchema(
            new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema R = new DataSchema(
            new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));

    private static DataRow l(Integer k, String v) { DataRow r = new DataRow(2); r.set(0, k); r.set(1, v); return r; }
    private static RowStream s(List<DataRow> rows) { return RowStream.fromIterable(rows::iterator); }

    private static Map<String, Integer> multiset(List<DataRow> rows) {
        Map<String, Integer> m = new HashMap<>();
        for (DataRow row : rows) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < row.size(); i++) sb.append(row.get(i)).append('|');
            m.merge(sb.toString(), 1, Integer::sum);
        }
        return m;
    }

    private AutoJoinSelector selector(int probeN) {
        io.rowweave.mem.spill.DiskSpillStore store = new io.rowweave.mem.spill.DiskSpillStore(
                tempDir.resolve("auto-" + stores.size()).toFile());
        stores.add(store);
        return new AutoJoinSelector(L, R, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER, probeN,
                new io.rowweave.mem.MemoryManager(4L * 1024 * 1024), store,
                new io.rowweave.mem.seg.HeapPool());
    }

    @AfterEach
    void closeStores() {
        for (io.rowweave.mem.spill.DiskSpillStore store : stores) store.close();
        stores.clear();
    }

    private static List<DataRow> smjExpect(List<DataRow> left, List<DataRow> right) {
        CollectingRowSink sink = new CollectingRowSink();
        SortMergeJoin.of(L, R, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER).join(s(left), s(right), sink);
        return sink.rows();
    }

    @Test
    void smallRightSideChoosesHashMatchesSmj() {
        List<DataRow> left = new ArrayList<>();
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < 500; i++) left.add(l(i % 20, "l" + i));  // the large side is the left
        for (int i = 0; i < 10; i++) right.add(l(i % 20, "r" + i));  // the small side is the right, at most N
        AutoJoinSelector sel = selector(64);
        CollectingRowSink sink = new CollectingRowSink();
        sel.join(s(left), s(right), sink);
        assertTrue(sel.choseHashForTest(), "the smaller side is the right input so the hash join is chosen");
        assertEquals(multiset(smjExpect(left, right)), multiset(sink.rows()), "AUTO(hash) == SMJ");
    }

    @Test
    void bothLargeFallsBackToSmj() {
        List<DataRow> left = new ArrayList<>();
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < 500; i++) { left.add(l(i % 50, "l" + i)); right.add(l(i % 50, "r" + i)); }
        AutoJoinSelector sel = selector(64);  // both sides exceed 64, so fall back to SMJ
        CollectingRowSink sink = new CollectingRowSink();
        sel.join(s(left), s(right), sink);
        assertTrue(!sel.choseHashForTest(), "both sides are large so the plan falls back to a sort-merge join");
        assertEquals(multiset(smjExpect(left, right)), multiset(sink.rows()), "AUTO(SMJ) == SMJ");
    }

    @Test
    void smallLeftChoosesBuildLeftHash() {
        List<DataRow> left = new ArrayList<>();
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < 10; i++) left.add(l(i % 5, "l" + i));  // the small side is the left
        for (int i = 0; i < 500; i++) right.add(l(i % 5, "r" + i));  // the large side is the right
        AutoJoinSelector sel = selector(64);
        CollectingRowSink sink = new CollectingRowSink();
        sel.join(s(left), s(right), sink);
        assertTrue(sel.choseHashForTest(), "the smaller side is the left input so hash join builds on the left");
        assertEquals(HashJoin.BuildSide.LEFT, sel.selectedBuildSideForTest());
        assertEquals(multiset(smjExpect(left, right)), multiset(sink.rows()), "AUTO(HASH_LEFT) == SMJ");
    }

    @Test
    void concatBoundaryNoLossNoDup() {
        // probeN boundary: a right side of exactly N counts as small, and the concat across N neither loses nor repeats
        List<DataRow> left = new ArrayList<>();
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < 300; i++) left.add(l(i % 16, "l" + i));
        for (int i = 0; i < 16; i++) right.add(l(i, "r" + i));
        AutoJoinSelector sel = selector(16);  // right = 16 = N
        CollectingRowSink sink = new CollectingRowSink();
        sel.join(s(left), s(right), sink);
        assertEquals(multiset(smjExpect(left, right)), multiset(sink.rows()), "the concat at the boundary loses and duplicates nothing and matches the sort-merge join");
    }

    @Test
    @Timeout(3)
    void autoSelectedSmjPreservesCancellableConcatAndDrainsConcurrently() {
        System.setProperty("rowweave.binary.concurrentDrain", "true");
        try {
            CountDownLatch enteredAfterProbe = new CountDownLatch(2);
            RowStream left = new ProbeThenCoordinatedStream("l", enteredAfterProbe);
            RowStream right = new ProbeThenCoordinatedStream("r", enteredAfterProbe);
            AutoJoinSelector selector = selector(2);
            CollectingRowSink sink = new CollectingRowSink();

            selector.join(left, right, sink);

            assertTrue(!selector.choseHashForTest());
            assertTrue(sink.sawEndOfStream());
        } finally {
            System.clearProperty("rowweave.binary.concurrentDrain");
        }
    }

    private static final class ProbeThenCoordinatedStream implements RowStream {
        private final String side;
        private final CountDownLatch entered;
        private int index;

        private ProbeThenCoordinatedStream(String side, CountDownLatch entered) {
            this.side = side;
            this.entered = entered;
        }

        @Override
        public io.rowweave.sched.stream.StreamEvent next() {
            if (index >= 3) return null;
            if (index == 2) {
                entered.countDown();
                try {
                    if (!entered.await(1, TimeUnit.SECONDS)) {
                        throw new AssertionError("AUTO concat lost cancellation capability; SMJ drain stayed serial");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return io.rowweave.sched.stream.StreamEvent.cancel();
                }
            }
            return io.rowweave.sched.stream.StreamEvent.record(l(index++, side + index));
        }

        @Override public boolean supportsCancellation() { return true; }
        @Override public boolean cancel(Throwable cause) {
            while (entered.getCount() > 0) entered.countDown();
            return true;
        }
    }
}
