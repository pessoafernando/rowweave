package io.rowweave.bench;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.ManagedMemory;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SortField;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PlanOps;
import io.rowweave.plan.PushDagExecutor;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The benchmark harness and the no-OOM assertion, which upgrades the peak staying within the
 * budget from an inference to an assertion. Under a bounded budget it runs groupBy and orderBy
 * over 50k rows and asserts directly that the peak bytes stay within the budget and the result
 * is multiset-equivalent to an in-heap oracle.
 */
class BenchmarkHarnessTest {

    private static final DataSchema META = new DataSchema(
            new Field("grp", DataType.StringType),
            new Field("amt", DataType.BigDecimalType));

    private static DataRow row(String grp, BigDecimal amt) {
        DataRow r = new DataRow(2);
        r.set(0, grp);
        r.set(1, amt);
        return r;
    }

    private static List<DataRow> bigRows(int n, int groups) {
        List<DataRow> rows = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            rows.add(row("g" + (i % groups), new BigDecimal(Integer.toString(i % 100))));
        }
        return rows;
    }

/** groupBy over 50k rows on a tiny pool: the harness returns a positive wall clock, a positive peak and the right row count, and the peak stays at or below the acquired quota. */
    @Test
    void groupByBenchmarkPeakWithinBudget() {
        List<DataRow> rows = bigRows(50_000, 50);
        DataSchema gbMeta = ParallelGroupByAgg.of(META, "grp").sum("amt").getOutputSchema();
        PlanGraph g = new PlanGraph(Arrays.asList(
                PlanNode.source(META, rows),
                PlanNode.unary("groupBySum", gbMeta, 0)
                        .listParam("keys", Arrays.asList("grp")).param("measure", "amt").build()));
        ManagedMemory pool = ManagedMemory.ofBytes(32 * 1024);

        BenchmarkHarness.BenchmarkResult res = BenchmarkHarness.run(g, gbMeta, pool, 1);

        assertTrue(res.wallNanos > 0, "wall clock > 0");
        assertTrue(res.peakBytes > 0, "peak > 0 (a reservation was actually made)");
        assertEquals(50, res.outRowCount, "50 groups");
        assertTrue(res.withinBudget(), "no-OOM invariant: peak " + res.peakBytes + " ≤ acquired " + res.acquiredBytes);
    }

/** orderBy over 50k rows on a tiny pool: the peak stays within the acquired quota and the full result is ordered. */
    @Test
    void orderByBenchmarkPeakWithinBudget() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 50_000; i++) {
            rows.add(row("g", new BigDecimal(Integer.toString((i * 7919) % 50_000))));
        }
        SortField sf = new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST);
        PlanGraph g = new PlanGraph(Arrays.asList(
                PlanNode.source(META, rows),
                PlanNode.unary("orderBy", META, 0)
                        .listParam("sort", Collections.singletonList(PlanOps.encodeSortField(sf))).build()));
        ManagedMemory pool = ManagedMemory.ofBytes(32 * 1024);

        BenchmarkHarness.BenchmarkResult res = BenchmarkHarness.run(g, META, pool, 1);

        assertEquals(50_000, res.outRowCount);
        assertTrue(res.withinBudget(), "no-OOM invariant: peak " + res.peakBytes + " ≤ acquired " + res.acquiredBytes);
    }

/** A spilled result on a tiny pool is multiset-equivalent to an in-heap oracle on a large pool that never spills, so spilling changes nothing about the outcome. */
    @Test
    void benchmarkResultMatchesHeapOracle() {
        List<DataRow> rows = bigRows(30_000, 40);
        DataSchema gbMeta = ParallelGroupByAgg.of(META, "grp").sum("amt").getOutputSchema();
        PlanGraph g = new PlanGraph(Arrays.asList(
                PlanNode.source(META, rows),
                PlanNode.unary("groupBySum", gbMeta, 0)
                        .listParam("keys", Arrays.asList("grp")).param("measure", "amt").build()));

/** the large-pool oracle, which stays on the in-heap path and never spills */
        TreeMap<String, BigDecimal> oracle = new TreeMap<>();
        new PushDagExecutor(1, ManagedMemory.ofBytes(256L * 1024 * 1024))
                .execute(g, r -> oracle.put((String) r.get(0), (BigDecimal) r.get(1)));

/** the tiny pool, which forces spilling */
        io.rowweave.CollectingSink out = new io.rowweave.CollectingSink(gbMeta);
        new PushDagExecutor(1, ManagedMemory.ofBytes(32 * 1024)).executeToOutput(g, out);

        TreeMap<String, BigDecimal> spilled = new TreeMap<>();
        for (DataRow r : out.getRows()) {
            spilled.put((String) r.get(0), (BigDecimal) r.get(1));
        }
        assertEquals(oracle.keySet(), spilled.keySet(), "group key sets are equivalent");
        for (String k : oracle.keySet()) {
            assertEquals(0, oracle.get(k).compareTo(spilled.get(k)), "sum[" + k + "] spill==heap");
        }
    }
}
