package io.rowweave;

import io.rowweave.ops.ParallelGroupByAgg;
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
 * Partition row counts, the basis for skew reporting: each router keeps a local long[P] inside routeKeyed, merged after the join into
 * ExecutionReport.partitionRowCounts, an array of length P whose entries add up to the input row count. A skewed input leaves one partition far larger, an even input spreads them evenly. Read-only, so the result is unchanged.
 */
class PartitionSkewMetricTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();

    private static DataRow row(int k, int amt) {
        DataRow r = new DataRow(2);
        r.set(0, k);
        r.set(1, new BigDecimal(amt));
        return r;
    }

    private static Source in(List<DataRow> rows) {
        return new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() { return new ArrayList<>(rows).iterator(); }
        };
    }

    private static Sink discard() {
        return new Sink() {
            @Override public DataSchema getSchema() { return GB; }
            @Override public void collect(DataRow r) { }
            @Override public void close() { }
            @Override public void abort() { }
        };
    }

    private static ExecutionReport runReported(List<DataRow> rows, int p) {
        PlanNode src = PlanNode.sourceFromInputs(META, Collections.singletonList(in(rows)));
        PlanNode gb = PlanNode.unary("groupBySum", GB, 0)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode sink = PlanNode.sink(GB, 1, discard()).build();
        return new PushDagExecutor(p).runSinkGraphReported(new PlanGraph(Arrays.asList(src, gb, sink)));
    }

    /** partitionRowCounts has length P and its entries sum to the input row count. */
    @Test
    void partitionCountsLengthAndSum() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 600; i++) rows.add(row(i % 20, 1));
        ExecutionReport rep = runReported(rows, 3);
        long[] counts = rep.partitionRowCounts();
        assertEquals(3, counts.length, "array length = P");
        long sum = 0;
        for (long c : counts) sum += c;
        assertEquals(600, sum, "sum of counts = input rows");
    }

    /** Skewed: one key everywhere sends every row to one partition and leaves the rest at 0, so partitionSkew is about P. */
    @Test
    void skewedInputOnePartitionDominant() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 500; i++) rows.add(row(7, 1));  // one key everywhere
        ExecutionReport rep = runReported(rows, 4);
        long[] counts = rep.partitionRowCounts();
        int nonZero = 0;
        long max = 0;
        for (long c : counts) { if (c > 0) nonZero++; if (c > max) max = c; }
        assertEquals(1, nonZero, "all rows share one key, so only one partition is non-zero");
        assertEquals(500, max, "that partition holds all 500");
        assertEquals(4.0, rep.partitionSkew(), 0.001, "skew = P (everything crowded into one partition)");
    }

    /** Evenly spread over many keys: each partition holds close to the average, so partitionSkew is about 1.0. */
    @Test
    void balancedInputLowSkew() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 4000; i++) rows.add(row(i % 400, 1));  // 400 distinct keys, evenly spread
        ExecutionReport rep = runReported(rows, 4);
        assertTrue(rep.partitionSkew() < 1.3,
                "balanced input should give skew close to 1.0, actual " + rep.partitionSkew());
    }

    /** The serial path at P=1 leaves partitionRowCounts empty and partitionSkew at 1.0, since there are no parallel partitions. */
    @Test
    void serialPathEmptyPartitionCounts() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) rows.add(row(i % 5, 1));
        ExecutionReport rep = runReported(rows, 1);
        assertEquals(0, rep.partitionRowCounts().length, "serial execution records no partition counts");
        assertEquals(1.0, rep.partitionSkew(), 0.001);
    }
}
