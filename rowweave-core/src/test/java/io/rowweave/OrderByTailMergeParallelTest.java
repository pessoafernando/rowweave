package io.rowweave;

import io.rowweave.ops.OrderBy;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SortField;
import io.rowweave.ops.Top;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PushDagExecutor;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Parallelism 1 for the total-order operators, without collapsing the upstream keyed stage to serial.
 * Shape `SINK -> orderBy/top -> groupBy -> SOURCE` at P>1: the keyed stage (groupBy) still shuffles at
 * P-way parallelism and the tail OrderBy/Top is the P -> 1 merge, so the result is globally ordered and
 * row-for-row identical to the P=1 serial run.
 *
 * <p><b>Correctness edge</b>: the sort runs on the aggregate output column (the sum column, index 1),
 * DESC. If the tail merge mistakenly used a whole-row ordering (orderByAllFields, which sorts by k
 * ascending first), the output would be misordered against "sum descending" and nothing would raise.
 * This test pins the tail merge to the sort arguments of the orderBy node itself.
 */
class OrderByTailMergeParallelTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();

    private static DataRow row(String k, int amt) {
        DataRow r = new DataRow(2);
        r.set(0, k);
        r.set(1, new BigDecimal(amt));
        return r;
    }

    private static Source input(List<DataRow> rows) {
        return new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() { return new ArrayList<>(rows).iterator(); }
        };
    }

    /** groupBy(k, sum amt) then orderBy(sum DESC) at a given parallelism, collected in output order with no further sorting. */
    private static List<DataRow> runGroupByOrderBySumDesc(List<DataRow> rows, int parallelism) {
        ExecutionEnvironment session = RowWeave.createEnvironment("par2-ob");
        session.setParallelism(parallelism);
        DataSet grouped = session.fromSource(input(rows))
                .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"));
        // sorted descending on the aggregate sum at index 1, which is nothing like a whole-row total order by ascending k.
        DataSet ordered = grouped.orderBy(
                OrderBy.of(GB, new SortField(1, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST)),
                parallelism, parallelism > 1);
        WorkerCollectingOutput out = new WorkerCollectingOutput(ordered.getSchema());
        ordered.sinkTo(out);
        session.execute();
        return new ArrayList<>(out.getRows());
    }

    private static List<DataRow> runGroupByTopSumDesc(List<DataRow> rows, int parallelism, int n) {
        ExecutionEnvironment session = RowWeave.createEnvironment("par3-top");
        session.setParallelism(parallelism);
        DataSet grouped = session.fromSource(input(rows))
                .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"));
        DataSet topped = grouped.top(
                Top.of(GB, n, new SortField(1, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST)),
                parallelism);
        WorkerCollectingOutput out = new WorkerCollectingOutput(topped.getSchema());
        topped.sinkTo(out);
        session.execute();
        return new ArrayList<>(out.getRows());
    }

    /** orderBy(sum DESC) trailing a parallel groupBy: at P=3 the output is globally ordered and row for row the same as serial P=1, ordered on the sum column rather than resorted by whole row. */
    @Test
    void orderBySumDescTailOnParallelGroupBy() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            rows.add(row("k" + (i % 23), (i % 11) + 1));  // each group sum differs, so DESC yields a determinate order
        }
        List<DataRow> oracle = runGroupByOrderBySumDesc(rows, 1);
        // self-check: the oracle is already in descending sum order
        for (int i = 1; i < oracle.size(); i++) {
            assertTrue(((BigDecimal) oracle.get(i - 1).get(1)).compareTo((BigDecimal) oracle.get(i).get(1)) >= 0,
                    " expectation sorted by sum descending");
        }
        for (int p : new int[]{2, 3, 4}) {
            List<DataRow> got = runGroupByOrderBySumDesc(rows, p);
            assertEquals(oracle.size(), got.size(), "P=" + p + " group count matches");
            // globally ordered: the result is itself descending by sum, since the trailing OrderBy converges to a single stream.
            for (int i = 1; i < got.size(); i++) {
                assertTrue(((BigDecimal) got.get(i - 1).get(1)).compareTo((BigDecimal) got.get(i).get(1)) >= 0,
                        "P=" + p + " row " + i + " globally sorted by sum descending");
            }
            // the sum sequence matches the oracle row for row; sum is the sort key, so groups with equal sums have no fixed order and only the sequence of sums is pinned.
            for (int i = 0; i < oracle.size(); i++) {
                assertEquals(0, ((BigDecimal) oracle.get(i).get(1)).compareTo((BigDecimal) got.get(i).get(1)),
                        "P=" + p + " row " + i + " sum sequence matches");
            }
            // the (key,sum) multiset equals the oracle, losing nothing and mis-grouping nothing.
            assertEquals(multiset(oracle), multiset(got), "P=" + p + " (key,sum) multisets are equal");
        }
    }

    private static List<String> multiset(List<DataRow> rows) {
        List<String> ks = new ArrayList<>();
        for (DataRow r : rows) {
            ks.add(r.get(0) + "=" + ((BigDecimal) r.get(1)).stripTrailingZeros().toPlainString());
        }
        Collections.sort(ks);
        return ks;
    }

    /**
     * The shape a reviewer insisted on: SINK through orderBy through groupBy to SOURCE at P=3 has to take the parallel keyed shuffle, so groupBy must not fall back to serial.
     * The test is that ExecutionReport.partitionRowCounts has length P, since only the parallel path fills it and the serial path leaves it empty.
     */
    @Test
    void orderByTailKeepsUpstreamGroupByParallel() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            rows.add(row("k" + (i % 23), (i % 11) + 1));
        }
        WorkerCollectingOutput out = new WorkerCollectingOutput(GB);
        PlanNode src = PlanNode.sourceFromInputs(META, Collections.singletonList(input(rows)));
        PlanNode gb = PlanNode.unary("groupBySum", GB, 0)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode ob = PlanNode.unary("orderBy", GB, 1)
                .listParam("sort", Collections.singletonList("1:DESC:NULLS_LAST")).build();
        PlanNode sink = PlanNode.sink(GB, 2, out).build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, gb, ob, sink));

        int p = 3;
        ExecutionReport report = new PushDagExecutor(p).runSinkGraphReported(g);
        assertEquals(p, report.partitionRowCounts().length,
                "when orderBy follows groupBy the upstream groupBy must use a parallel keyed shuffle (P partitions) instead of falling back to serial");
        long totalIn = 0;
        for (long c : report.partitionRowCounts()) totalIn += c;
        assertEquals(600, totalIn, "input rows across partitions sum to the source row count");
    }

    /**
     * Guard: the groupBy output column order differs from the source order, since a source [amt, k] grouped by k (source idx1) outputs [k, sum]
     * (output idx0 is source idx1), and the trailing OrderBy sorts DESC on the output keyedMeta idx1, which is the sum. Using the source column space or renumbering every field
     * would shift the index onto a different column and fork away from the P=1 serial result, so the trailing convergence index space is pinned to keyedMeta.
     */
    @Test
    void tailOrderIndexSpaceIsKeyedMetaNotSource() {
        DataSchema srcMeta = new DataSchema(
                new Field("amt", DataType.BigDecimalType), new Field("k", DataType.StringType));
        DataSchema gbMeta = ParallelGroupByAgg.of(srcMeta, "k").sum("amt").getOutputSchema(); // [k, sum]
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 400; i++) {
            DataRow r = new DataRow(2);
            r.set(0, new BigDecimal((i % 13) + 1)); // amt
            r.set(1, "g" + (i % 17));               // k
            rows.add(r);
        }
        Source in = new Source() {
            @Override public DataSchema getSchema() { return srcMeta; }
            @Override public Iterator<DataRow> iterator() { return new ArrayList<>(rows).iterator(); }
        };

        java.util.function.BiFunction<Integer, Boolean, List<DataRow>> run = (p, parallel) -> {
            ExecutionEnvironment s = RowWeave.createEnvironment("par4-idx");
            s.setParallelism(p);
            DataSet g = s.fromSource(in).groupBy(ParallelGroupByAgg.of(srcMeta, "k").sum("amt"));
            DataSet o = g.orderBy(
                    OrderBy.of(gbMeta, new SortField(1, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST)),
                    p, parallel);
            WorkerCollectingOutput out = new WorkerCollectingOutput(o.getSchema());
            o.sinkTo(out);
            s.execute();
            return new ArrayList<>(out.getRows());
        };

        List<DataRow> oracle = run.apply(1, false);
        List<DataRow> got = run.apply(3, true);
        assertEquals(oracle.size(), got.size());
        for (int i = 1; i < got.size(); i++) {
            assertTrue(((BigDecimal) got.get(i - 1).get(1)).compareTo((BigDecimal) got.get(i).get(1)) >= 0,
                    " output sorted by sum (output index 1) descending, the index space is keyedMeta");
        }
        for (int i = 0; i < oracle.size(); i++) {
            assertEquals(0, ((BigDecimal) oracle.get(i).get(1)).compareTo((BigDecimal) got.get(i).get(1)),
                    "row " + i + " sum sequence matches");
        }
        assertEquals(multiset(oracle), multiset(got), "(key,sum) multisets are equal");
    }

    /** top(n, sum DESC) trailing a parallel groupBy: at P=3 the first n rows are globally right, row for row the same as P=1. */
    @Test
    void topSumDescTailOnParallelGroupBy() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            rows.add(row("k" + (i % 23), (i % 11) + 1));
        }
        int n = 5;
        List<DataRow> oracle = runGroupByTopSumDesc(rows, 1, n);
        assertEquals(n, oracle.size(), "top n rows");
        for (int p : new int[]{2, 3}) {
            List<DataRow> got = runGroupByTopSumDesc(rows, p, n);
            assertEquals(n, got.size(), "P=" + p + " top n rows");
            for (int i = 0; i < n; i++) {
                assertEquals(0, ((BigDecimal) oracle.get(i).get(1)).compareTo((BigDecimal) got.get(i).get(1)),
                        "P=" + p + " top row " + i + " sum matches row by row");
            }
        }
    }
}
