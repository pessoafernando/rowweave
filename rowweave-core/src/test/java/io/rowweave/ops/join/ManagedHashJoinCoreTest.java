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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Semantics and closed resource accounting of the managed encoded hash table. */
class ManagedHashJoinCoreTest {

    private static final DataSchema LEFT = new DataSchema(
            new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));

    @TempDir
    Path tempDir;

    @Test
    void bothBuildSidesMatchSortMergeForEveryOuterJoin() {
        List<DataRow> left = Arrays.asList(
                row(1, "l1"), row(1, "l2"), row(3, "left-only"), row(null, "left-null"));
        List<DataRow> right = Arrays.asList(
                row(1, "r1"), row(1, "r2"), row(2, "right-only"), row(null, "right-null"));

        for (JoinType type : JoinType.values()) {
            Map<String, Integer> expected = multiset(runSortMerge(left, right, type));
            assertEquals(expected, multiset(runHash(left, right, type, HashJoin.BuildSide.RIGHT)),
                    type + " build-right");
            assertEquals(expected, multiset(runHash(left, right, type, HashJoin.BuildSide.LEFT)),
                    type + " build-left");
        }
    }

    @Test
    void duplicateProbeDecodesOneBuildRowAtATimeAndReleasesAllCredits() {
        List<DataRow> left = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            left.add(row(7, "probe-" + i));
        }
        List<DataRow> right = Arrays.asList(
                row(7, repeated('a', 4096)), row(7, repeated('b', 4096)), row(7, repeated('c', 4096)));

        MemoryManager memory = new MemoryManager(2L * 1024 * 1024);
        HeapPool pool = new HeapPool();
        DiskSpillStore store = new DiskSpillStore(tempDir.resolve("duplicates").toFile());
        try {
            HashJoin join = HashJoin.of(
                            LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER)
                    .withSpill(memory, store, pool)
                    .withBuildSide(HashJoin.BuildSide.RIGHT);
            CollectingRowSink sink = new CollectingRowSink();

            join.join(stream(left), stream(right), sink);

            assertEquals(600, sink.rows().size());
            assertEquals(0L, memory.reservedBytes(), "all exact HashJoin credits must be released");
            assertEquals(0, pool.outstanding(), "all encoded build segments must return to the pool");
            assertTrue(memory.peakReservedBytes() <= memory.budget());
            assertEquals(0, store.runsCreated(), "resident hash fast path must not spill");
        } finally {
            store.close();
            pool.trim();
        }
    }

    @Test
    void hashCollisionStillComparesTheDecodedKey() {
        DataSchema leftMeta = new DataSchema(
                new Field("lk", DataType.StringType), new Field("lv", DataType.StringType));
        DataSchema rightMeta = new DataSchema(
                new Field("rk", DataType.StringType), new Field("rv", DataType.StringType));
        DataRow left = stringRow("Aa", "left");
        DataRow sameHashDifferentKey = stringRow("BB", "wrong");
        DataRow matching = stringRow("Aa", "right");
        assertEquals("Aa".hashCode(), "BB".hashCode(), "fixture must exercise one hash chain collision");

        MemoryManager memory = new MemoryManager(1024L * 1024);
        HeapPool pool = new HeapPool();
        DiskSpillStore store = new DiskSpillStore(tempDir.resolve("collision").toFile());
        try {
            HashJoin join = HashJoin.of(leftMeta, rightMeta,
                            new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER)
                    .withSpill(memory, store, pool);
            CollectingRowSink sink = new CollectingRowSink();
            join.join(stream(Arrays.asList(left)),
                    stream(Arrays.asList(sameHashDifferentKey, matching)), sink);

            assertEquals(1, sink.rows().size());
            assertEquals("right", sink.rows().get(0).get(3));
            assertEquals(0L, memory.reservedBytes());
            assertEquals(0, pool.outstanding());
        } finally {
            store.close();
        }
    }

    @Test
    void integerAndBigDecimalKeysUseNumericEquality() {
        DataSchema decimalRight = new DataSchema(
                new Field("rk", DataType.BigDecimalType), new Field("rv", DataType.StringType));
        DataRow decimal = new DataRow(2);
        decimal.set(0, new BigDecimal("7.000"));
        decimal.set(1, "decimal");

        MemoryManager memory = new MemoryManager(1024L * 1024);
        HeapPool pool = new HeapPool();
        DiskSpillStore store = new DiskSpillStore(tempDir.resolve("numeric").toFile());
        try {
            HashJoin join = HashJoin.of(LEFT, decimalRight,
                            new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER)
                    .withSpill(memory, store, pool);
            CollectingRowSink sink = new CollectingRowSink();
            join.join(stream(Arrays.asList(row(7, "integer"))), stream(Arrays.asList(decimal)), sink);

            assertEquals(1, sink.rows().size());
            assertEquals(0L, memory.reservedBytes());
            assertEquals(0, pool.outstanding());
        } finally {
            store.close();
        }
    }

    @Test
    void outputAndSerializerFailuresReleaseTableAndScratch() {
        assertFailureReleases("output", true);
        assertFailureReleases("serializer", false);
    }

    private void assertFailureReleases(String directory, boolean failOutput) {
        MemoryManager memory = new MemoryManager(1024L * 1024);
        HeapPool pool = new HeapPool();
        DiskSpillStore store = new DiskSpillStore(tempDir.resolve(directory).toFile());
        try {
            HashJoin join = HashJoin.of(
                            LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER)
                    .withSpill(memory, store, pool);
            if (failOutput) {
                assertThrows(IllegalStateException.class, () -> join.join(
                        stream(Arrays.asList(row(1, "left"))),
                        stream(Arrays.asList(row(1, "right"))),
                        event -> { if (event.isRecord()) throw new IllegalStateException("sink failed"); }));
            } else {
                DataRow invalid = new DataRow(2);
                invalid.set(0, 1);
                invalid.set(1, 99); // schema says StringType
                assertThrows(ClassCastException.class, () -> join.join(
                        stream(new ArrayList<>()), stream(Arrays.asList(invalid)), new CollectingRowSink()));
            }
            assertEquals(0L, memory.reservedBytes());
            assertEquals(0, pool.outstanding());
        } finally {
            store.close();
        }
    }

    private List<DataRow> runHash(List<DataRow> left, List<DataRow> right, JoinType type, HashJoin.BuildSide side) {
        MemoryManager memory = new MemoryManager(1024L * 1024);
        HeapPool pool = new HeapPool();
        DiskSpillStore store = new DiskSpillStore(
                tempDir.resolve("hash-" + type + "-" + side).toFile());
        try {
            HashJoin join = HashJoin.of(
                            LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, type)
                    .withSpill(memory, store, pool)
                    .withBuildSide(side);
            CollectingRowSink sink = new CollectingRowSink();
            join.join(stream(left), stream(right), sink);
            assertEquals(0L, memory.reservedBytes());
            assertEquals(0, pool.outstanding());
            assertEquals(0, store.runsCreated());
            return sink.rows();
        } finally {
            store.close();
            pool.trim();
        }
    }

    private static List<DataRow> runSortMerge(List<DataRow> left, List<DataRow> right, JoinType type) {
        SortMergeJoin join = SortMergeJoin.of(
                LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, type);
        CollectingRowSink sink = new CollectingRowSink();
        join.join(stream(left), stream(right), sink);
        return sink.rows();
    }

    private static RowStream stream(List<DataRow> rows) {
        return RowStream.fromIterable(rows::iterator);
    }

    private static DataRow row(Integer key, String value) {
        DataRow row = new DataRow(2);
        row.set(0, key);
        row.set(1, value);
        return row;
    }

    private static DataRow stringRow(String key, String value) {
        DataRow row = new DataRow(2);
        row.set(0, key);
        row.set(1, value);
        return row;
    }

    private static String repeated(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static Map<String, Integer> multiset(List<DataRow> rows) {
        Map<String, Integer> result = new HashMap<>();
        for (DataRow row : rows) {
            StringBuilder fingerprint = new StringBuilder();
            for (int i = 0; i < row.size(); i++) {
                fingerprint.append(row.get(i)).append('|');
            }
            result.merge(fingerprint.toString(), 1, Integer::sum);
        }
        return result;
    }
}
