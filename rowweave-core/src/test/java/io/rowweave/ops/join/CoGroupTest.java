package io.rowweave.ops.join;

import io.rowweave.CoGroupFunction;
import io.rowweave.Collector;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.spill.DiskSpillStore;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CoGroupTest {

    private static final DataSchema LEFT = new DataSchema(
            new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));

    // output schema: [key(Integer), leftCount(Integer), rightCount(Integer)]
    private static final DataSchema OUT = new DataSchema(
            new Field("k", DataType.IntegerType),
            new Field("lc", DataType.IntegerType),
            new Field("rc", DataType.IntegerType));

    private static DataRow l(Integer k, String v) {
        DataRow r = new DataRow(2); r.set(0, k); r.set(1, v); return r;
    }

    private static DataRow r(Integer k, String v) {
        DataRow r = new DataRow(2); r.set(0, k); r.set(1, v); return r;
    }

    /** fn: emit [key, |left|, |right|] for each co-group. key taken from whichever side non-empty. */
    private static final CoGroupFunction COUNT_FN = new CoGroupFunction() {
        @Override
        public DataSchema getResultSchema() {
            return OUT;
        }

        @Override
        public void coGroup(Iterable<DataRow> iter1, Iterable<DataRow> iter2, Collector collector) {
            int lc = 0;
            Object key = "__none__";
            for (DataRow r : iter1) {
                lc++;
                key = r.get(0);
            }
            int rc = 0;
            for (DataRow r : iter2) {
                rc++;
                if ("__none__".equals(key)) {
                    key = r.get(0);
                }
            }
            DataRow out = new DataRow(3);
            out.set(0, key);
            out.set(1, lc);
            out.set(2, rc);
            collector.collect(out);
        }
    };

    private static List<DataRow> coGroup(CoGroup cg, List<DataRow> left, List<DataRow> right) {
        CollectingRowSink sink = new CollectingRowSink();
        cg.coGroup(RowStream.fromIterable(left), RowStream.fromIterable(right), sink);
        assertTrue(sink.sawEndOfStream());
        return sink.rows();
    }

    private static CoGroup newCoGroup() {
        return CoGroup.of(LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, COUNT_FN);
    }

    /** find result row by key (null key allowed). */
    private static DataRow byKey(List<DataRow> rows, Object key) {
        for (DataRow r : rows) {
            if (key == null ? r.get(0) == null : key.equals(r.get(0))) {
                return r;
            }
        }
        return null;
    }

    @Test
    void bothSidesSameKey() {
        List<DataRow> out = coGroup(newCoGroup(),
                Arrays.asList(l(1, "a"), l(1, "b")),
                Arrays.asList(r(1, "x")));
        assertEquals(1, out.size());
        DataRow g = byKey(out, 1);
        assertEquals(2, g.get(1)); // left count
        assertEquals(1, g.get(2)); // right count
    }

    @Test
    void leftOnlyKeyTriggersWithEmptyRight() {
        List<DataRow> out = coGroup(newCoGroup(),
                Arrays.asList(l(1, "a")),
                Arrays.asList(r(2, "x")));
        // key 1: left-only (lc=1, rc=0); key 2: right-only (lc=0, rc=1)
        assertEquals(2, out.size());
        assertEquals(1, byKey(out, 1).get(1));
        assertEquals(0, byKey(out, 1).get(2));
        assertEquals(0, byKey(out, 2).get(1));
        assertEquals(1, byKey(out, 2).get(2));
    }

    @Test
    void nullKeyParticipatesAsGroupBothSides() {
        // null participates (UNLIKE join's null != null): both sides' null-key rows form ONE co-group
        List<DataRow> out = coGroup(newCoGroup(),
                Arrays.asList(l(null, "a"), l(null, "b"), l(1, "c")),
                Arrays.asList(r(null, "x"), r(1, "y")));
        // null group: lc=2, rc=1 ; key 1 group: lc=1, rc=1
        DataRow nullGroup = byKey(out, null);
        assertNotNull(nullGroup, "null key must form its own co-group");
        assertEquals(2, nullGroup.get(1));
        assertEquals(1, nullGroup.get(2));
        assertEquals(1, byKey(out, 1).get(1));
        assertEquals(1, byKey(out, 1).get(2));
    }

    @Test
    void scaleSensitiveDistinctGroups() {
        // BigDecimal key: 5 (scale0) vs 5.00 (scale2) -> DIFFERENT groups (scale-sensitive)
        DataSchema lm = new DataSchema(new Field("k", DataType.BigDecimalType), new Field("v", DataType.StringType));
        DataSchema rm = new DataSchema(new Field("k", DataType.BigDecimalType), new Field("v", DataType.StringType));
        DataSchema outMeta = new DataSchema(new Field("k", DataType.BigDecimalType),
                new Field("lc", DataType.IntegerType), new Field("rc", DataType.IntegerType));
        CoGroupFunction fn = new CoGroupFunction() {
            @Override public DataSchema getResultSchema() { return outMeta; }
            @Override public void coGroup(Iterable<DataRow> a, Iterable<DataRow> b, Collector c) {
                int lc = 0; Object k = null;
                for (DataRow r : a) { lc++; k = r.get(0); }
                int rc = 0;
                for (DataRow r : b) { rc++; if (k == null) k = r.get(0); }
                DataRow o = new DataRow(3); o.set(0, k); o.set(1, lc); o.set(2, rc);
                c.collect(o);
            }
        };
        CoGroup cg = CoGroup.of(lm, rm, new String[]{"k"}, new String[]{"k"}, fn);
        DataRow l1 = new DataRow(2); l1.set(0, new BigDecimal("5")); l1.set(1, "a");
        DataRow l2 = new DataRow(2); l2.set(0, new BigDecimal("5.00")); l2.set(1, "b");
        DataRow r1 = new DataRow(2); r1.set(0, new BigDecimal("5")); r1.set(1, "x");
        CollectingRowSink sink = new CollectingRowSink();
        cg.coGroup(RowStream.fromIterable(Arrays.asList(l1, l2)),
                RowStream.fromIterable(Arrays.asList(r1)), sink);
        // 5 and 5.00 are distinct -> 2 groups
        assertEquals(2, sink.rows().size(), "scale-sensitive: 5 and 5.00 are different groups");
    }

    /** When the two sides interleave their scales in a different order, each exact key still forms one co-group. */
    @Test
    void interleavedBigDecimalScalesAlignAcrossBothSides() {
        DataSchema lm = new DataSchema(new Field("k", DataType.BigDecimalType), new Field("v", DataType.StringType));
        DataSchema rm = new DataSchema(new Field("k", DataType.BigDecimalType), new Field("v", DataType.StringType));
        DataSchema outMeta = new DataSchema(new Field("k", DataType.BigDecimalType),
                new Field("lc", DataType.IntegerType), new Field("rc", DataType.IntegerType));
        CoGroupFunction fn = new CoGroupFunction() {
            @Override public DataSchema getResultSchema() { return outMeta; }
            @Override public void coGroup(Iterable<DataRow> a, Iterable<DataRow> b, Collector c) {
                int lc = 0; BigDecimal key = null;
                for (DataRow value : a) { lc++; key = (BigDecimal) value.get(0); }
                int rc = 0;
                for (DataRow value : b) { rc++; if (key == null) key = (BigDecimal) value.get(0); }
                DataRow out = new DataRow(3); out.set(0, key); out.set(1, lc); out.set(2, rc);
                c.collect(out);
            }
        };
        java.util.function.BiFunction<String, String, DataRow> row = (key, value) -> {
            DataRow result = new DataRow(2);
            result.set(0, new BigDecimal(key));
            result.set(1, value);
            return result;
        };

        List<DataRow> out = coGroup(
                CoGroup.of(lm, rm, new String[]{"k"}, new String[]{"k"}, fn),
                Arrays.asList(row.apply("5.00", "l2a"), row.apply("5", "l0"), row.apply("5.00", "l2b")),
                Arrays.asList(row.apply("5", "r0a"), row.apply("5.00", "r2"), row.apply("5", "r0b")));

        assertEquals(2, out.size(), "each exact BigDecimal key must produce one co-group");
        DataRow scale0 = byKey(out, new BigDecimal("5"));
        DataRow scale2 = byKey(out, new BigDecimal("5.00"));
        assertNotNull(scale0);
        assertNotNull(scale2);
        assertEquals(1, scale0.get(1));
        assertEquals(2, scale0.get(2));
        assertEquals(2, scale2.get(1));
        assertEquals(1, scale2.get(2));
    }

    @Test
    void multiKeyCoGroup() {
        DataSchema lm = new DataSchema(new Field("k1", DataType.StringType),
                new Field("k2", DataType.IntegerType), new Field("v", DataType.StringType));
        DataSchema rm = new DataSchema(new Field("k1", DataType.StringType),
                new Field("k2", DataType.IntegerType), new Field("v", DataType.StringType));
        DataSchema outMeta = new DataSchema(new Field("lc", DataType.IntegerType), new Field("rc", DataType.IntegerType));
        CoGroupFunction fn = new CoGroupFunction() {
            @Override public DataSchema getResultSchema() { return outMeta; }
            @Override public void coGroup(Iterable<DataRow> a, Iterable<DataRow> b, Collector c) {
                int lc = 0; for (DataRow ignore : a) lc++;
                int rc = 0; for (DataRow ignore : b) rc++;
                DataRow o = new DataRow(2); o.set(0, lc); o.set(1, rc); c.collect(o);
            }
        };
        CoGroup cg = CoGroup.of(lm, rm, new String[]{"k1", "k2"}, new String[]{"k1", "k2"}, fn);
        DataRow l1 = new DataRow(3); l1.set(0, "a"); l1.set(1, 1); l1.set(2, "L");
        DataRow r1 = new DataRow(3); r1.set(0, "a"); r1.set(1, 1); r1.set(2, "R");
        DataRow r2 = new DataRow(3); r2.set(0, "a"); r2.set(1, 2); r2.set(2, "R2");
        CollectingRowSink sink = new CollectingRowSink();
        cg.coGroup(RowStream.fromIterable(Arrays.asList(l1)),
                RowStream.fromIterable(Arrays.asList(r1, r2)), sink);
        // (a,1): lc1 rc1 ; (a,2): lc0 rc1
        assertEquals(2, sink.rows().size());
    }

    @Test
    void fnCanIterateBlocksMultipleTimes() {
        CoGroupFunction fn = new CoGroupFunction() {
            @Override public DataSchema getResultSchema() { return OUT; }
            @Override public void coGroup(Iterable<DataRow> a, Iterable<DataRow> b, Collector c) {
                int l1 = 0; for (DataRow ignore : a) l1++;
                int l2 = 0; for (DataRow ignore : a) l2++; // second pass over left
                assertEquals(l1, l2, "left block must be re-readable");
                DataRow o = new DataRow(3); o.set(0, 1); o.set(1, l1); o.set(2, 0); c.collect(o);
            }
        };
        CoGroup cg = CoGroup.of(LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, fn);
        coGroup(cg, Arrays.asList(l(1, "a"), l(1, "b"), l(1, "c")), new ArrayList<>());
    }

    @Test
    void largeGroupSpillDoesNotOom(@TempDir Path tmp) {
        CoGroup cg = newCoGroup()
                .withSpill(new MemoryManager(128 * 1024), new DiskSpillStore(tmp.toFile()),
                        new io.rowweave.mem.seg.HeapPool())
                .withEqualKeyBlockThreshold(8);
        List<DataRow> left = new ArrayList<>();
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            left.add(l(1, "l" + i));
            right.add(r(1, "r" + i));
        }
        List<DataRow> out = coGroup(cg, left, right);
        assertEquals(1, out.size());
        assertEquals(100, out.get(0).get(1));
        assertEquals(100, out.get(0).get(2));
    }

    @Test
    void emptyBothSidesNoFnCall() {
        List<DataRow> out = coGroup(newCoGroup(), new ArrayList<>(), new ArrayList<>());
        assertTrue(out.isEmpty());
    }

    @Test
    void errorOnLeftForwardedAndStops() {
        CoGroup cg = newCoGroup();
        RuntimeException boom = new RuntimeException("boom");
        List<StreamEvent> events = Arrays.asList(
                StreamEvent.record(l(1, "a")), StreamEvent.error(boom));
        Iterator<StreamEvent> it = events.iterator();
        RowStream left = () -> it.hasNext() ? it.next() : null;
        CollectingRowSink sink = new CollectingRowSink();
        cg.coGroup(left, RowStream.fromIterable(Arrays.asList(r(1, "x"))), sink);
        assertTrue(sink.sawError());
        assertSame(boom, sink.error());
        assertFalse(sink.sawEndOfStream());
    }

    @Test
    void rejectsMismatchedKeyCounts() {
        assertThrows(IllegalArgumentException.class,
                () -> CoGroup.of(LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk", "rv"}, COUNT_FN));
    }

    /**
     * When the key types differ between the sides, the right block must read its DataType
     * from rightMeta rather than leftMeta. Here the left key is a String at index 0 and the
     * right key a BigDecimal at index 0, so the right rows 5 and 5.00 differ in scale and must
     * form two groups. Taking the String type from leftMeta instead fell through to the generic
     * Comparable path, where BigDecimal.compareTo treats them as equal and merges the groups.
     * An empty left side isolates the right-block boundary check, with no cross-side comparison.
     */
    @Test
    void rightSideScaleSensitiveWithDifferentLeftKeyType() {
        DataSchema lm = new DataSchema(new Field("lk", DataType.StringType), new Field("lv", DataType.StringType));
        DataSchema rm = new DataSchema(new Field("rk", DataType.BigDecimalType), new Field("rv", DataType.StringType));
        DataSchema outMeta = new DataSchema(new Field("lc", DataType.IntegerType), new Field("rc", DataType.IntegerType));
        CoGroupFunction fn = new CoGroupFunction() {
            @Override public DataSchema getResultSchema() { return outMeta; }
            @Override public void coGroup(Iterable<DataRow> a, Iterable<DataRow> b, Collector c) {
                int lc = 0; for (DataRow ignore : a) lc++;
                int rc = 0; for (DataRow ignore : b) rc++;
                DataRow o = new DataRow(2); o.set(0, lc); o.set(1, rc); c.collect(o);
            }
        };
        CoGroup cg = CoGroup.of(lm, rm, new String[]{"lk"}, new String[]{"rk"}, fn);
        DataRow r1 = new DataRow(2); r1.set(0, new BigDecimal("5")); r1.set(1, "x");
        DataRow r2 = new DataRow(2); r2.set(0, new BigDecimal("5.00")); r2.set(1, "y");
        CollectingRowSink sink = new CollectingRowSink();
        cg.coGroup(RowStream.fromIterable(new ArrayList<>()),
                RowStream.fromIterable(Arrays.asList(r1, r2)), sink);
        assertTrue(sink.sawEndOfStream());
        assertEquals(2, sink.rows().size(), "the right keys 5 and 5.00 are scale sensitive so they form two distinct groups (the right block reads its types from the right schema)");
    }
}
