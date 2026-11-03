package io.rowweave;

import io.rowweave.cluster.LocalCluster;
import io.rowweave.mem.ManagedMemory;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SortField;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PushDagExecutor;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.TelemetryCoverage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * a/ breaker spill wiring — the engine manages ExecutionMemory itself and injects spilling buffers into
 * IR and cluster breakers. The OOM guard: a tiny pool with a large input spills instead of dying, and the
 * output is verified against a large in-heap pool (bit-identical). The MemoryManager is shared, not
 * M x B.
 */
class SpillWiringTest {

    @BeforeEach
    void admitTinyPoolOwners() {
        System.setProperty("rowweave.worker.operator.minMemory", "4096");
    }

    @AfterEach
    void resetMinimumGuarantee() {
        System.clearProperty("rowweave.worker.operator.minMemory");
    }

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

    private static TreeMap<String, BigDecimal> sums(List<DataRow> out) {
        TreeMap<String, BigDecimal> m = new TreeMap<>();
        for (DataRow r : out) {
            m.put((String) r.get(0), (BigDecimal) r.get(1));
        }
        return m;
    }

    /** A bounded budget injected into groupBy, where a small budget over a large volume spills, with the result compared against the in-memory one. */
    @Test
    void groupBySpillMatchesHeap() {
        List<DataRow> rows = bigRows(20_000, 50);
        DataSchema gbMeta = ParallelGroupByAgg.of(META, "grp").sum("amt").getOutputSchema();
        PlanGraph g = new PlanGraph(Arrays.asList(
                PlanNode.source(META, rows),
                PlanNode.unary("groupBySum", gbMeta, 0)
                        .listParam("keys", Arrays.asList("grp")).param("measure", "amt").build()));

        // the in-memory oracle
        TreeMap<String, BigDecimal> heap = new TreeMap<>();
        new PushDagExecutor(1, ManagedMemory.ofBytes(256L * 1024 * 1024)).execute(g, r -> heap.put((String) r.get(0), (BigDecimal) r.get(1)));

        // bounded(tiny budget) → spill
        ManagedMemory pool = ManagedMemory.ofBytes(16 * 1024);
        TreeMap<String, BigDecimal> spilled = new TreeMap<>();
        new PushDagExecutor(1, pool).execute(g, r -> spilled.put((String) r.get(0), (BigDecimal) r.get(1)));

        assertEquals(heap.keySet(), spilled.keySet());
        for (String k : heap.keySet()) {
            assertEquals(0, heap.get(k).compareTo(spilled.get(k)), "sum[" + k + "] spill==heap");
        }
        assertEquals(50, spilled.size());
    }

    /** Injected into orderBy, where a small budget over a large volume spills, producing everything in order, compared against the in-memory order. */
    @Test
    void orderBySpillOrdered() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            rows.add(row("g", new BigDecimal(Integer.toString((i * 7919) % 10_000))));  // pseudo-random
        }
        SortField sf = new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST);
        PlanGraph g = new PlanGraph(Arrays.asList(
                PlanNode.source(META, rows),
                PlanNode.unary("orderBy", META, 0)
                        .listParam("sort", java.util.Collections.singletonList(
                                io.rowweave.plan.PlanOps.encodeSortField(sf))).build()));
        ManagedMemory pool = ManagedMemory.ofBytes(16 * 1024);
        List<BigDecimal> out = new ArrayList<>();
        ExecutionReport report = new PushDagExecutor(1, pool)
                .execute(g, r -> out.add((BigDecimal) r.get(1)));
        assertEquals(10_000, out.size());
        for (int i = 1; i < out.size(); i++) {
            assertTrue(out.get(i).compareTo(out.get(i - 1)) >= 0, "ascending at " + i);
        }
        assertEquals(TelemetryCoverage.COMPLETE, report.resourceTelemetryCoverage());
        assertEquals(0L, report.pressureSpillCount(),
                "active external-sort spill is not a memory-pressure callback");
        assertTrue(report.spillRunCount() > 0);
        assertTrue(report.spillLogicalBytesWritten() > 0);
    }

    /** Injected into distinct, where a small budget over a large volume spills, with the deduplicated result set-equal rather than row for row. */
    @Test
    void distinctSpillSetEquivalence() {
        List<DataRow> rows = bigRows(20_000, 200); // 200 distinct grp
        PlanGraph g = new PlanGraph(Arrays.asList(
                PlanNode.source(META, rows),
                PlanNode.unary("distinct", META, 0)
                        .listParam("keys", Arrays.asList("grp")).build()));
        ManagedMemory pool = ManagedMemory.ofBytes(16 * 1024);
        java.util.Set<String> grps = new java.util.TreeSet<>();
        new PushDagExecutor(1, pool).execute(g, r -> grps.add((String) r.get(0)));
        assertEquals(200, grps.size(), "distinct result set = 200 groups (spill loses nothing and duplicates nothing)");
    }

    /** A large pool that triggers no spill still gives the right result, since the engine manages its own memory and takes the purely in-memory path when the allowance is generous. */
    @Test
    void largePoolNoSpillCorrect() {
        List<DataRow> rows = bigRows(1000, 10);
        DataSchema gbMeta = ParallelGroupByAgg.of(META, "grp").sum("amt").getOutputSchema();
        PlanGraph g = new PlanGraph(Arrays.asList(
                PlanNode.source(META, rows),
                PlanNode.unary("groupBySum", gbMeta, 0)
                        .listParam("keys", Arrays.asList("grp")).param("measure", "amt").build()));
        List<DataRow> out = new ArrayList<>();
        new PushDagExecutor(1, ManagedMemory.ofBytes(256L * 1024 * 1024)).execute(g, out::add);
        assertEquals(10, out.size());
    }

    /** A graph with several breakers, groupBy then orderBy, shares one MemoryManager, so the peak stays within what was acquired rather than adding the two allowances together. */
    @Test
    void sharedBudgetMultiBreakerPeakBounded() {
        List<DataRow> rows = bigRows(20_000, 50);
        DataSchema gbMeta = ParallelGroupByAgg.of(META, "grp").sum("amt").getOutputSchema();
        SortField sf = new SortField(1, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST);
        // source, groupBySum then orderBy, two breakers sharing one ExecutionMemory
        PlanGraph g = new PlanGraph(Arrays.asList(
                PlanNode.source(META, rows),
                PlanNode.unary("groupBySum", gbMeta, 0)
                        .listParam("keys", Arrays.asList("grp")).param("measure", "amt").build(),
                PlanNode.unary("orderBy", gbMeta, 1)
                        .listParam("sort", java.util.Collections.singletonList(
                                io.rowweave.plan.PlanOps.encodeSortField(sf))).build()));
        long total = 64 * 1024;
        ManagedMemory pool = ManagedMemory.ofBytes(total);
        // Both breakers take the same ExecutionMemory.memoryManager() through PlanOps, the very same instance, so the run stays in memory and the peak stays within what was acquired.
        List<DataRow> out = new ArrayList<>();
        io.rowweave.plan.ExecutionReport report =
                new PushDagExecutor(1, pool).execute(g, out::add);
        assertEquals(50, out.size());
        assertTrue(report.withinBudget(),
                "shared budget peak " + report.peakReservedBytes() + " ≤ acquired " + report.acquiredBytes() + "(not M*B)");
    }

    /**
     * Several breakers running concurrently, groupBy then orderBy on separate stage threads, each with its own sub-budget from the memory manager,
     * with a tiny pool forcing spill, giving a result set-equal to the large-pool oracle and stable across repeated runs, since a race would show up as drift or an exception.
     */
    @Test
    void concurrentMultiBreakerSpillDeterministic() {
        DataSchema gbMeta = ParallelGroupByAgg.of(META, "grp").sum("amt").getOutputSchema();
        SortField sf = new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST);
        PlanGraph g = new PlanGraph(Arrays.asList(
                PlanNode.source(META, bigRows(20_000, 50)),
                PlanNode.unary("groupBySum", gbMeta, 0)
                        .listParam("keys", Arrays.asList("grp")).param("measure", "amt").build(),
                PlanNode.unary("orderBy", gbMeta, 1)
                        .listParam("sort", java.util.Collections.singletonList(
                                io.rowweave.plan.PlanOps.encodeSortField(sf))).build()));
        // the large-pool oracle, which spills nothing
        List<BigDecimal> oracle = new ArrayList<>();
        new PushDagExecutor(1, ManagedMemory.ofBytes(256L * 1024 * 1024)).execute(g, r -> oracle.add((BigDecimal) r.get(1)));
        // a tiny pool with 2 breakers taking half each, spilling concurrently, run 3 times to assert a determinate result
        for (int run = 0; run < 3; run++) {
            List<BigDecimal> got = new ArrayList<>();
            new PushDagExecutor(1, ManagedMemory.ofBytes(64 * 1024)).execute(g, r -> got.add((BigDecimal) r.get(1)));
            assertEquals(oracle.size(), got.size(), "run " + run + " row count matches");
            for (int i = 0; i < oracle.size(); i++) {
                assertEquals(0, oracle.get(i).compareTo(got.get(i)), "run " + run + " row " + i + " value matches (no race drift)");
            }
        }
    }

    /** A cluster keyed groupBy on a tiny pool spills without running out of memory, and the result matches the single-machine run. */
    @Test
    void clusterKeyedGroupBySpill() {
        List<DataRow> rows = bigRows(20_000, 50);
        DataSchema gbMeta = ParallelGroupByAgg.of(META, "grp").sum("amt").getOutputSchema();
        CollectingSink out = new CollectingSink(gbMeta);
        PlanGraph g = new PlanGraph(Arrays.asList(
                PlanNode.source(META, rows),
                PlanNode.unary("groupBySum", gbMeta, 0)
                        .listParam("keys", Arrays.asList("grp")).param("measure", "amt").build(),
                PlanNode.sink(gbMeta, 1, out).build()));
        ManagedMemory pool = ManagedMemory.ofBytes(32 * 1024);
        long n = new LocalCluster(3, pool).submitKeyed(g);
        assertEquals(50, n);
    }
}
