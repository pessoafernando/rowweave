package io.rowweave.ops.join;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The in-memory HashJoin (INNER) and SortMergeJoin produce equivalent result sets (as multisets, not row
 * by row — hash order is not merge order).
 * Correctness edges: null never matches null, composite keys, several rows per key forming a Cartesian
 * product, and an empty side.
 */
class HashJoinTest {

    @TempDir
    Path tempDir;

    private static final DataSchema L = new DataSchema(
            new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema R = new DataSchema(
            new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));

    private static DataRow l(Integer k, String v) { DataRow r = new DataRow(2); r.set(0, k); r.set(1, v); return r; }
    private static DataRow r(Integer k, String v) { DataRow r = new DataRow(2); r.set(0, k); r.set(1, v); return r; }

    private static RowStream stream(List<DataRow> rows) {
        return RowStream.fromIterable(rows::iterator);
    }

    /** Order-independent multiset fingerprint that counts each row's concatenated fields. */
    private static Map<String, Integer> multiset(List<DataRow> rows) {
        Map<String, Integer> m = new HashMap<>();
        for (DataRow row : rows) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < row.size(); i++) sb.append(row.get(i)).append('|');
            m.merge(sb.toString(), 1, Integer::sum);
        }
        return m;
    }

    private List<DataRow> runHash(List<DataRow> left, List<DataRow> right, JoinType jt) {
        HashJoin hj = HashJoin.of(L, R, new String[]{"lk"}, new String[]{"rk"}, jt);
        io.rowweave.mem.spill.DiskSpillStore store = new io.rowweave.mem.spill.DiskSpillStore(
                tempDir.resolve("hash-" + jt + "-" + System.nanoTime()).toFile());
        try {
            CollectingRowSink sink = new CollectingRowSink();
            hj.withSpill(new io.rowweave.mem.MemoryManager(64L * 1024 * 1024), store)
                    .join(stream(left), stream(right), sink);
            return sink.rows();
        } finally {
            store.close();
        }
    }

    private static List<DataRow> runSmj(List<DataRow> left, List<DataRow> right, JoinType jt) {
        SortMergeJoin smj = SortMergeJoin.of(L, R, new String[]{"lk"}, new String[]{"rk"}, jt);
        CollectingRowSink sink = new CollectingRowSink();
        smj.join(stream(left), stream(right), sink);
        return sink.rows();
    }

    private void assertSetEqual(List<DataRow> left, List<DataRow> right, JoinType jt, String msg) {
        assertEquals(multiset(runSmj(left, right, jt)), multiset(runHash(left, right, jt)),
                "the hash join and the sort-merge join produce equal multisets: " + msg);
    }

    @Test
    void innerBasicMatches() {
        List<DataRow> left = new ArrayList<>();
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < 100; i++) left.add(l(i % 20, "l" + i));
        for (int i = 0; i < 30; i++) right.add(r(i % 20, "r" + i));
        assertSetEqual(left, right, JoinType.INNER, "a large side against a small side with repeated keys forms the cartesian product");
    }

    @Test
    void innerNullKeyNeverMatches() {
        List<DataRow> left = new ArrayList<>();
        left.add(l(1, "a")); left.add(l(null, "nl")); left.add(l(2, "b"));
        List<DataRow> right = new ArrayList<>();
        right.add(r(1, "x")); right.add(r(null, "nr")); right.add(r(2, "y"));
        // INNER matches no null key on either side, so only k=1 and k=2 match
        List<DataRow> hash = runHash(left, right, JoinType.INNER);
        assertEquals(2, hash.size(), "a null key never matches so only k=1 and k=2 appear");
        assertSetEqual(left, right, JoinType.INNER, "null keys do not match and the result equals the sort-merge join");
    }

    @Test
    void innerEmptySide() {
        assertSetEqual(new ArrayList<>(), java.util.Arrays.asList(r(1, "x")), JoinType.INNER, "empty left side");
        assertSetEqual(java.util.Arrays.asList(l(1, "a")), new ArrayList<>(), JoinType.INNER, "empty right side");
    }

    @Test
    void outerFamilyMatchesSmj() {
        List<DataRow> left = new ArrayList<>();
        List<DataRow> right = new ArrayList<>();
        // built from matching rows, left-only rows, right-only rows and null keys on both sides
        left.add(l(1, "la")); left.add(l(1, "la2")); left.add(l(3, "lonly"));
        left.add(l(null, "lnull"));
        right.add(r(1, "ra")); right.add(r(2, "ronly")); right.add(r(null, "rnull"));
        assertSetEqual(left, right, JoinType.LEFT, "LEFT outer keeps left-only rows with nulls for the right and the left null-key rows");
        assertSetEqual(left, right, JoinType.RIGHT, "RIGHT outer keeps right-only rows with nulls for the left and the right null-key rows");
        assertSetEqual(left, right, JoinType.FULL, "FULL outer keeps rows unique to either side and the null-key rows from both sides");
    }

    /** A build side over a tiny budget spills through grace hash, stays multiset-equivalent to a large-budget in-memory run and does not OOM. */
    @Test
    void graceSpillEqualsInMemory() {
        List<DataRow> left = new ArrayList<>();
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < 2000; i++) left.add(l(i % 300, "l" + i));
        for (int i = 0; i < 1500; i++) right.add(r(i % 300, "r" + i));  // the right side builds, with 300 groups
        left.add(l(null, "ln")); right.add(r(null, "rn"));

        for (JoinType jt : JoinType.values()) {
            // in-memory run with a large budget, which never spills
            io.rowweave.mem.spill.DiskSpillStore memoryStore = new io.rowweave.mem.spill.DiskSpillStore(
                    tempDir.resolve("memory-" + jt).toFile());
            io.rowweave.mem.spill.DiskSpillStore fallbackStore = new io.rowweave.mem.spill.DiskSpillStore(
                    tempDir.resolve("fallback-" + jt).toFile());
            try {
                HashJoin mem = HashJoin.of(L, R, new String[]{"lk"}, new String[]{"rk"}, jt)
                        .withSpill(new io.rowweave.mem.MemoryManager(64L * 1024 * 1024), memoryStore);
                CollectingRowSink ms = new CollectingRowSink();
                mem.join(stream(new ArrayList<>(left)), stream(new ArrayList<>(right)), ms);
                HashJoin fallback = HashJoin.of(L, R, new String[]{"lk"}, new String[]{"rk"}, jt)
                        .withSpill(new io.rowweave.mem.MemoryManager(128L * 1024), fallbackStore);
                CollectingRowSink fs = new CollectingRowSink();
                fallback.join(stream(new ArrayList<>(left)), stream(new ArrayList<>(right)), fs);

                assertEquals(multiset(ms.rows()), multiset(fs.rows()), jt + " the spilled fallback matches the in-memory result");
            } finally {
                memoryStore.close();
                fallbackStore.close();
            }
        }
    }

    @Test
    void innerMultiKey() {
        DataSchema l2 = new DataSchema(new Field("a", DataType.IntegerType), new Field("b", DataType.IntegerType),
                new Field("v", DataType.StringType));
        DataSchema r2 = new DataSchema(new Field("a", DataType.IntegerType), new Field("b", DataType.IntegerType),
                new Field("w", DataType.StringType));
        DataRow la = new DataRow(3); la.set(0, 1); la.set(1, 2); la.set(2, "la");
        DataRow lb = new DataRow(3); lb.set(0, 1); lb.set(1, 9); lb.set(2, "lb");  // b differs
        DataRow lc = new DataRow(3); lc.set(0, 1); lc.set(1, null); lc.set(2, "lc");  // part of the key is null
        DataRow ra = new DataRow(3); ra.set(0, 1); ra.set(1, 2); ra.set(2, "ra");
        io.rowweave.mem.spill.DiskSpillStore store = new io.rowweave.mem.spill.DiskSpillStore(
                tempDir.resolve("multi-key").toFile());
        HashJoin hj = HashJoin.of(l2, r2, new String[]{"a", "b"}, new String[]{"a", "b"}, JoinType.INNER)
                .withSpill(new io.rowweave.mem.MemoryManager(1024L * 1024), store);
        SortMergeJoin smj = SortMergeJoin.of(l2, r2, new String[]{"a", "b"}, new String[]{"a", "b"}, JoinType.INNER);
        CollectingRowSink hs = new CollectingRowSink(), ss = new CollectingRowSink();
        try {
            hj.join(stream(java.util.Arrays.asList(la, lb, lc)), stream(java.util.Arrays.asList(ra)), hs);
            smj.join(stream(java.util.Arrays.asList(la, lb, lc)), stream(java.util.Arrays.asList(ra)), ss);
            assertEquals(multiset(ss.rows()), multiset(hs.rows()), "several keys with some nulls match the sort-merge join");
        } finally {
            store.close();
        }
    }
}
