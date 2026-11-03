package io.rowweave.ops.join;

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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SortMergeJoinTest {

    // left: (lk Integer key, lv String) ; right: (rk Integer key, rv String)
    private static final DataSchema LEFT = new DataSchema(
            new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));

    private static DataRow l(Integer k, String v) {
        DataRow r = new DataRow(2); r.set(0, k); r.set(1, v); return r;
    }

    private static DataRow r(Integer k, String v) {
        DataRow r = new DataRow(2); r.set(0, k); r.set(1, v); return r;
    }

    /** result row = [lk, lv, rk, rv]; render as "lk:lv|rk:rv" with N for null. */
    private static String render(DataRow row) {
        return cell(row.get(0)) + ":" + cell(row.get(1)) + "|" + cell(row.get(2)) + ":" + cell(row.get(3));
    }

    private static String cell(Object o) {
        return o == null ? "N" : o.toString();
    }

    private static Set<String> join(JoinType type, List<DataRow> left, List<DataRow> right) {
        SortMergeJoin smj = SortMergeJoin.of(LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, type);
        CollectingRowSink sink = new CollectingRowSink();
        smj.join(RowStream.fromIterable(left), RowStream.fromIterable(right), sink);
        assertTrue(sink.sawEndOfStream());
        Set<String> out = new HashSet<>();
        for (DataRow row : sink.rows()) {
            out.add(render(row));
        }
        return out;
    }

    @Test
    void innerJoinMatchingKeys() {
        Set<String> out = join(JoinType.INNER,
                Arrays.asList(l(1, "a"), l(2, "b"), l(3, "c")),
                Arrays.asList(r(2, "x"), r(3, "y"), r(4, "z")));
        assertEquals(new HashSet<>(Arrays.asList("2:b|2:x", "3:c|3:y")), out);
    }

    @Test
    void leftJoinFillsNullForUnmatched() {
        Set<String> out = join(JoinType.LEFT,
                Arrays.asList(l(1, "a"), l(2, "b")),
                Arrays.asList(r(2, "x")));
        assertEquals(new HashSet<>(Arrays.asList("1:a|N:N", "2:b|2:x")), out);
    }

    @Test
    void rightJoinFillsNullForUnmatched() {
        Set<String> out = join(JoinType.RIGHT,
                Arrays.asList(l(2, "b")),
                Arrays.asList(r(2, "x"), r(5, "w")));
        assertEquals(new HashSet<>(Arrays.asList("2:b|2:x", "N:N|5:w")), out);
    }

    @Test
    void fullJoinKeepsBothSides() {
        Set<String> out = join(JoinType.FULL,
                Arrays.asList(l(1, "a"), l(2, "b")),
                Arrays.asList(r(2, "x"), r(3, "y")));
        assertEquals(new HashSet<>(Arrays.asList("1:a|N:N", "2:b|2:x", "N:N|3:y")), out);
    }

    @Test
    void nullKeyNeverMatchesInner() {
        // both sides have null key -> must NOT match each other (null != null)
        Set<String> out = join(JoinType.INNER,
                Arrays.asList(l(null, "a"), l(1, "b")),
                Arrays.asList(r(null, "x"), r(1, "y")));
        assertEquals(new HashSet<>(Arrays.asList("1:b|1:y")), out); // null keys dropped, only 1 matches
    }

    @Test
    void nullKeyOuterFillsButNeverMatches() {
        // LEFT: left null-key row preserved with null right (NOT matched to right's null key)
        Set<String> outLeft = join(JoinType.LEFT,
                Arrays.asList(l(null, "a"), l(1, "b")),
                Arrays.asList(r(null, "x"), r(1, "y")));
        assertEquals(new HashSet<>(Arrays.asList("N:a|N:N", "1:b|1:y")), outLeft);

        // FULL: both null-key rows preserved separately, never matched
        Set<String> outFull = join(JoinType.FULL,
                Arrays.asList(l(null, "a")),
                Arrays.asList(r(null, "x")));
        assertEquals(new HashSet<>(Arrays.asList("N:a|N:N", "N:N|N:x")), outFull);
    }

    @Test
    void equalKeyBlockCartesianProduct() {
        // 2 left x 3 right with same key -> 6 rows
        SortMergeJoin smj = SortMergeJoin.of(LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER);
        CollectingRowSink sink = new CollectingRowSink();
        smj.join(
                RowStream.fromIterable(Arrays.asList(l(7, "a"), l(7, "b"))),
                RowStream.fromIterable(Arrays.asList(r(7, "x"), r(7, "y"), r(7, "z"))),
                sink);
        assertEquals(6, sink.rows().size());
    }

    @Test
    void integerAndBigDecimalKeysUseNumericEquality() {
        DataSchema decimalRight = new DataSchema(
                new Field("rk", DataType.BigDecimalType), new Field("rv", DataType.StringType));
        DataRow decimal = new DataRow(2);
        decimal.set(0, new BigDecimal("7.000"));
        decimal.set(1, "decimal");
        SortMergeJoin join = SortMergeJoin.of(
                LEFT, decimalRight, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER);
        CollectingRowSink sink = new CollectingRowSink();

        join.join(RowStream.fromIterable(Arrays.asList(l(7, "integer"))),
                RowStream.fromIterable(Arrays.asList(decimal)), sink);

        assertEquals(1, sink.rows().size());
    }

    /** group/coGroup are scale-sensitive, but SQL join deliberately remains numeric-value based. */
    @Test
    void bigDecimalJoinRemainsScaleInsensitiveThroughSpill(@TempDir Path tmp) {
        DataSchema lm = new DataSchema(
                new Field("lk", DataType.BigDecimalType), new Field("lv", DataType.StringType));
        DataSchema rm = new DataSchema(
                new Field("rk", DataType.BigDecimalType), new Field("rv", DataType.StringType));
        SortMergeJoin join = SortMergeJoin.of(
                        lm, rm, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER)
                .withSpill(new MemoryManager(128 * 1024), new DiskSpillStore(tmp.toFile()),
                        new io.rowweave.mem.seg.HeapPool());
        DataRow left = new DataRow(2); left.set(0, new BigDecimal("5.00")); left.set(1, "L");
        DataRow right = new DataRow(2); right.set(0, new BigDecimal("5")); right.set(1, "R");
        CollectingRowSink sink = new CollectingRowSink();

        join.join(RowStream.fromIterable(Arrays.asList(left)),
                RowStream.fromIterable(Arrays.asList(right)), sink);

        assertTrue(sink.sawEndOfStream());
        assertEquals(1, sink.rows().size(), "5.00 must join 5 by numeric value");
    }

    @Test
    void skewedEqualKeyBlockSpillsWithoutOom(@TempDir Path tmp) {
        SortMergeJoin smj = SortMergeJoin.of(LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER)
                .withSpill(new MemoryManager(128 * 1024), new DiskSpillStore(tmp.toFile()),
                        new io.rowweave.mem.seg.HeapPool())
                .withEqualKeyBlockThreshold(8); // tiny -> right block spills
        List<DataRow> left = new ArrayList<>();
        List<DataRow> right = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            left.add(l(1, "l" + i));
        }
        for (int i = 0; i < 100; i++) {
            right.add(r(1, "r" + i)); // big right block -> spills past threshold 8
        }
        CollectingRowSink sink = new CollectingRowSink();
        smj.join(RowStream.fromIterable(left), RowStream.fromIterable(right), sink);
        assertEquals(500, sink.rows().size()); // 5 x 100
    }

    @Test
    void multiKeyJoin() {
        DataSchema lm = new DataSchema(
                new Field("k1", DataType.StringType), new Field("k2", DataType.IntegerType),
                new Field("lv", DataType.StringType));
        DataSchema rm = new DataSchema(
                new Field("k1", DataType.StringType), new Field("k2", DataType.IntegerType),
                new Field("rv", DataType.StringType));
        SortMergeJoin smj = SortMergeJoin.of(lm, rm,
                new String[]{"k1", "k2"}, new String[]{"k1", "k2"}, JoinType.INNER);
        DataRow l1 = new DataRow(3); l1.set(0, "a"); l1.set(1, 1); l1.set(2, "L");
        DataRow l2 = new DataRow(3); l2.set(0, "a"); l2.set(1, 2); l2.set(2, "L2");
        DataRow r1 = new DataRow(3); r1.set(0, "a"); r1.set(1, 1); r1.set(2, "R");
        CollectingRowSink sink = new CollectingRowSink();
        smj.join(RowStream.fromIterable(Arrays.asList(l1, l2)),
                RowStream.fromIterable(Arrays.asList(r1)), sink);
        assertEquals(1, sink.rows().size()); // only (a,1) matches
        assertEquals("L", sink.rows().get(0).get(2));
        assertEquals("R", sink.rows().get(0).get(5));
    }

    @Test
    void emptySides() {
        assertTrue(join(JoinType.INNER, new ArrayList<>(), Arrays.asList(r(1, "x"))).isEmpty());
        assertTrue(join(JoinType.INNER, Arrays.asList(l(1, "a")), new ArrayList<>()).isEmpty());
        // LEFT with empty right -> all left with null right
        assertEquals(new HashSet<>(Arrays.asList("1:a|N:N")),
                join(JoinType.LEFT, Arrays.asList(l(1, "a")), new ArrayList<>()));
    }

    @Test
    void errorOnLeftForwardedAndStops() {
        SortMergeJoin smj = SortMergeJoin.of(LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER);
        RuntimeException boom = new RuntimeException("boom");
        List<StreamEvent> levents = Arrays.asList(
                StreamEvent.record(l(1, "a")), StreamEvent.error(boom));
        Iterator<StreamEvent> lit = levents.iterator();
        RowStream left = () -> lit.hasNext() ? lit.next() : null;
        CollectingRowSink sink = new CollectingRowSink();
        smj.join(left, RowStream.fromIterable(Arrays.asList(r(1, "x"))), sink);
        assertTrue(sink.rows().isEmpty());
        assertTrue(sink.sawError());
        assertSame(boom, sink.error());
        assertFalse(sink.sawEndOfStream());
    }

    @Test
    void rejectsMismatchedKeyCounts() {
        assertThrows(IllegalArgumentException.class,
                () -> SortMergeJoin.of(LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk", "rv"}, JoinType.INNER));
    }
}
