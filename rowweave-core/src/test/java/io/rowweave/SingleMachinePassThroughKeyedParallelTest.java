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
 * On a single machine, a keyed breaker preceded by a pass-through filter or select still has to take the keyed shuffle in parallel
 * over P keyed-operator instances rather than degrading to a serial single-threaded breaker, judged by
 * ExecutionReport.partitionRowCounts, an array of length P on the parallel path and null on the serial one.
 *
 * <p>Shape recognition sees through a pass-through, which is then moved ahead into each router read chain and executed in parallel.
 */
class SingleMachinePassThroughKeyedParallelTest {

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
            @Override public Iterator<DataRow> iterator() { return rows.iterator(); }
        };
    }

    /** A graph of source, filter, groupBy then SINK, where a pass-through filter precedes the keyed breaker. */
    private static PlanGraph filterThenGroupByGraph(List<DataRow> rows, Sink out) {
        PlanNode src = PlanNode.sourceFromInputs(META, Collections.singletonList(input(rows)));
        PlanNode filter = PlanNode.unary("filter", META, 0).param("expr", "amt >= 0").build();
        PlanNode gb = PlanNode.unary("groupBySum", GB, 1)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode sink = PlanNode.sink(GB, 2, out).build();
        return new PlanGraph(Arrays.asList(src, filter, gb, sink));
    }

    /**
     * A source through filter into groupBy at P=3 should take the parallel path, giving a partitionRowCounts of length 3.
     * At present the filter interrupts keyed shape recognition, so the run is serial and partitionRowCounts is null.
     */
    @Test
    void filterBeforeGroupByStillParallel() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 600; i++) rows.add(row("k" + (i % 17), i % 9));
        CollectingSink out = new CollectingSink(GB);
        ExecutionReport report = new PushDagExecutor(3).runSinkGraphReported(filterThenGroupByGraph(rows, out));
        // partitionRowCounts().length is P on the parallel path, counting the input rows of each keyed-operator instance, and 0 on the serial path, where the getter returns an empty array.
        assertEquals(3, report.partitionRowCounts().length,
                "filter->groupBy in a single process at P=3 must use a keyed shuffle (3 partitions); a serial path would report 0 partitions");
        long sum = 0; for (long c : report.partitionRowCounts()) sum += c;
        assertEquals(600, sum, "input rows across partitions sum to the total input");
    }

    /** After select moves column positions, the pass-through parallel result == the serial oracle: the partition key uses the metadata at the end of the pass-through chain, so it cannot hash the wrong column. */
    @Test
    void selectReorderThenGroupByCorrect() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 400; i++) rows.add(row("k" + (i % 11), i % 5));
        // source[k,amt], then select(amt, k) to swap the columns, then groupBy(k).sum(amt), where k moves to idx1 after the select.
        java.util.function.BiFunction<List<DataRow>, Integer, List<DataRow>> run = (data, par) -> {
            DataSchema swapped = new DataSchema(new Field("amt", DataType.BigDecimalType), new Field("k", DataType.StringType));
            DataSchema gbSwap = ParallelGroupByAgg.of(swapped, "k").sum("amt").getOutputSchema();
            PlanNode src = PlanNode.sourceFromInputs(META, Collections.singletonList(input(data)));
            PlanNode sel = PlanNode.unary("select", swapped, 0).listParam("exprs", Arrays.asList("amt", "k")).build();
            PlanNode gb = PlanNode.unary("groupBySum", gbSwap, 1).listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
            CollectingSink out = new CollectingSink(gbSwap);
            PlanNode sink = PlanNode.sink(gbSwap, 2, out).build();
            new PushDagExecutor(par).runSinkGraph(new PlanGraph(Arrays.asList(src, sel, gb, sink)));
            List<DataRow> got = new ArrayList<>(out.getRows());
            got.sort((a, b) -> String.valueOf(a.get(0)).compareTo(String.valueOf(b.get(0))));
            return got;
        };
        List<DataRow> oracle = run.apply(rows, 1);
        List<DataRow> par3 = run.apply(rows, 3);
        assertEquals(oracle.size(), par3.size(), "select reordering columns at P=3 gives the same group count as P=1");
        for (int i = 0; i < oracle.size(); i++) {
            assertEquals(oracle.get(i).get(0), par3.get(i).get(0), "row " + i + " key");
            assertEquals(0, ((BigDecimal) oracle.get(i).get(1)).compareTo((BigDecimal) par3.get(i).get(1)), "row " + i + " sum");
        }
    }
}
