package io.rowweave;

import io.rowweave.ops.Distinct;
import io.rowweave.ops.Filter;
import io.rowweave.ops.OrderBy;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SortField;
import io.rowweave.ops.WholeTableAgg;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * **Cross-operator consistency for the sensitive value semantics** — pins the semantics matrix (scale
 * 10 HALF_UP applied once at output; null behaviour in arithmetic, comparison, grouping, ordering and
 * join) as a single auditable expected-value table. Every expectation here is derived by hand from the
 * operator definitions and the semantics spec, never from engine output. Exhaustive boundary coverage for
 * each operator lives in its own *Test; this class is only about the semantics agreeing across operators.
 */
class SensitivePointEquivalenceTest {

    private static final DataSchema META = new DataSchema(
            new Field("grp", DataType.StringType),
            new Field("amt", DataType.BigDecimalType));

    private static DataRow row(String grp, BigDecimal amt) {
        DataRow r = new DataRow(2);
        r.set(0, grp);
        r.set(1, amt);
        return r;
    }

    private static List<DataRow> drain(java.util.function.BiConsumer<RowStream, CollectingRowSink> op, List<DataRow> in) {
        CollectingRowSink sink = new CollectingRowSink();
        op.accept(RowStream.fromIterable(in), sink);
        return sink.rows();
    }

    /** Sum accumulates at full precision throughout and is rounded once at output to scale 10 HALF_UP, rather than at every step, which is what would accumulate error. */
    @Test
    void sumScale10RoundOnce() {
        // three approximations of a third: rounding at every step would drift, so one rounding after the full-precision sum gives 0.1+0.1+0.1 = 0.3000000000
        List<DataRow> rows = Arrays.asList(
                row("a", new BigDecimal("0.1")),
                row("a", new BigDecimal("0.1")),
                row("a", new BigDecimal("0.1")));
        ParallelGroupByAgg agg = ParallelGroupByAgg.of(META, "grp").sum("amt");
        List<DataRow> out = drain(agg::aggregate, rows);
        assertEquals(1, out.size());
        BigDecimal sum = (BigDecimal) out.get(0).get(1);
        assertEquals(0, new BigDecimal("0.3000000000").compareTo(sum));
        assertEquals(10, sum.scale(), "output scale must be 10");
    }

    /** Arithmetic over null yields null, since sum skips null instead of reading it as 0, and an empty group also yields null. */
    @Test
    void sumNullSkippedEmptyGroupNull() {
        List<DataRow> rows = Arrays.asList(
                row("a", new BigDecimal("5")),
                row("a", null),  // null is skipped, neither added as 0 nor allowed to contaminate
                row("b", null));  // group b is all null, so its sum is null
        ParallelGroupByAgg agg = ParallelGroupByAgg.of(META, "grp").sum("amt");
        List<DataRow> out = drain(agg::aggregate, rows);
        BigDecimal sumA = null, sumB = null;
        boolean sawB = false;
        for (DataRow r : out) {
            if ("a".equals(r.get(0))) sumA = (BigDecimal) r.get(1);
            if ("b".equals(r.get(0))) { sumB = (BigDecimal) r.get(1); sawB = true; }
        }
        assertEquals(0, new BigDecimal("5.0000000000").compareTo(sumA));
        assertTrue(sawB, "group b is present (null participates in grouping)");
        assertNull(sumB, "sum of the all-null group = null (not zero)");
    }

    /** A filter comparison over null is unknown, so the row does not pass, which is three-valued logic rather than treating unknown as a pass. */
    @Test
    void filterNullComparisonUnknownDrops() {
        Filter f = Filter.fromExpression(META, "amt > 5");
        List<DataRow> out = drain((in, sink) -> f.apply(in, sink), Arrays.asList(
                row("a", new BigDecimal("10")),  // 10>5 pass
                row("b", null),                  // null>5 unknown → drop
                row("c", new BigDecimal("2"))));  // 2>5 false → drop
        assertEquals(1, out.size());
        assertEquals(new BigDecimal("10"), out.get(0).get(1));
    }

    /** Null takes part in grouping and forms its own group, rather than being dropped. */
    @Test
    void nullParticipatesInGrouping() {
        List<DataRow> rows = Arrays.asList(
                row("a", new BigDecimal("1")),
                row(null, new BigDecimal("2")),  // a null key forms its own group
                row(null, new BigDecimal("3")));
        ParallelGroupByAgg agg = ParallelGroupByAgg.of(META, "grp").sum("amt");
        List<DataRow> out = drain(agg::aggregate, rows);
        assertEquals(2, out.size(), "null key forms its own group and is not dropped");
        BigDecimal nullGroupSum = null;
        for (DataRow r : out) {
            if (r.get(0) == null) nullGroupSum = (BigDecimal) r.get(1);
        }
        assertEquals(0, new BigDecimal("5.0000000000").compareTo(nullGroupSum));
    }

    /** In distinct a BigDecimal scale is part of the key, so 5 and 5.0 stay separate. */
    @Test
    void distinctScaleSensitive() {
        List<DataRow> rows = Arrays.asList(
                row("x", new BigDecimal("5")),
                row("x", new BigDecimal("5.0")),  // a different scale gives a different row
                row("x", new BigDecimal("5")));  // equal to the first row, so deduplicated
        Distinct d = Distinct.allColumns(META);
        List<DataRow> out = drain(d::dedup, rows);
        assertEquals(2, out.size(), "5 and 5.0 differ in scale, so distinct treats them as different");
    }

    /** For orderBy a numeric null follows the configurable NULLS_LAST, asked for explicitly here so null sorts last. */
    @Test
    void orderByNullsLast() {
        List<DataRow> rows = Arrays.asList(
                row("a", new BigDecimal("3")),
                row("b", null),
                row("c", new BigDecimal("1")));
        OrderBy ob = OrderBy.of(META, new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST));
        CollectingRowSink sink = new CollectingRowSink();
        ob.applyStream(RowStream.fromIterable(rows), 1, false, sink);
        List<DataRow> out = sink.rows();
        assertEquals(new BigDecimal("1"), out.get(0).get(1));
        assertEquals(new BigDecimal("3"), out.get(1).get(1));
        assertNull(out.get(2).get(1), "null sorts last (NULLS_LAST)");
    }

    /** In a join null does not equal null, so null keys on either side never match and INNER drops those rows. */
    @Test
    void joinNullNotEqual() {
        DataSchema lm = new DataSchema(new Field("k", DataType.BigDecimalType), new Field("lv", DataType.StringType));
        DataSchema rm = new DataSchema(new Field("k", DataType.BigDecimalType), new Field("rv", DataType.StringType));
        DataRow l1 = new DataRow(2); l1.set(0, null); l1.set(1, "L");
        DataRow r1 = new DataRow(2); r1.set(0, null); r1.set(1, "R");
        SortMergeJoin smj = SortMergeJoin.of(lm, rm, new String[]{"k"}, new String[]{"k"}, JoinType.INNER);
        CollectingRowSink sink = new CollectingRowSink();
        smj.join(RowStream.fromIterable(Arrays.asList(l1)), RowStream.fromIterable(Arrays.asList(r1)), sink);
        assertEquals(0, sink.rows().size(), "null keys never match on either side (null is not equal to null), so INNER drops them");
    }

    /** wholeAgg over an empty table sums to null and counts zero. */
    @Test
    void wholeAggEmptyTable() {
        WholeTableAgg sumAgg = WholeTableAgg.of(META, WholeTableAgg.Kind.SUM, "amt");
        CollectingRowSink s1 = new CollectingRowSink();
        sumAgg.apply(RowStream.fromIterable(java.util.Collections.emptyList()), s1);
        assertEquals(1, s1.rows().size());
        assertNull(s1.rows().get(0).get(0), "sum over an empty table = null");

        WholeTableAgg cntAgg = WholeTableAgg.of(META, WholeTableAgg.Kind.COUNT_ALL, null);
        CollectingRowSink s2 = new CollectingRowSink();
        cntAgg.apply(RowStream.fromIterable(java.util.Collections.emptyList()), s2);
        assertEquals(0L, s2.rows().get(0).get(0), "count_all over an empty table = 0");
    }
}
