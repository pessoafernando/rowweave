package io.rowweave;

import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SortField;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PushDagExecutor;
import io.rowweave.plan.StageExecutionTelemetry;
import io.rowweave.plan.StageExecutionTelemetryAccumulator;
import io.rowweave.plan.TelemetryCoverage;
import io.rowweave.mem.ManagedMemory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ExecutionReport extended with spillCount and rowsOut, with the parallel shuffle path returning the report through runSinkGraphReported.
 * runSinkGraph keeps returning a long, as the contract that the Worker writes the Sink requires. These are read-only metrics, so a bit-identical result is unaffected.
 */
class ExecutionReportMetricsTest {

    @AfterEach
    void resetMinimumGuarantee() {
        System.clearProperty("rowweave.worker.operator.minMemory");
    }

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();

    private static DataRow row(String k, int amt) {
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

    private static PlanGraph graph(List<DataRow> rows, Sink out) {
        PlanNode src = PlanNode.sourceFromInputs(META, Collections.singletonList(in(rows)));
        PlanNode gb = PlanNode.unary("groupBySum", GB, 0)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode sink = PlanNode.sink(GB, 1, out).build();
        return new PlanGraph(Arrays.asList(src, gb, sink));
    }

    private static Sink discard() {
        return new Sink() {
            @Override public DataSchema getSchema() { return GB; }
            @Override public void collect(DataRow r) { }
            @Override public void close() { }
            @Override public void abort() { }
        };
    }

    /** The older ExecutionReport constructor and withinBudget still behave, keeping compatibility. */
    @Test
    void backwardCompatibleConstructor() {
        ExecutionReport r = new ExecutionReport(100, 200);
        assertEquals(100, r.peakReservedBytes());
        assertEquals(200, r.acquiredBytes());
        assertTrue(r.withinBudget());
        assertEquals(0, r.spillCount(), "default spillCount=0");
        assertEquals(0, r.rowsOut(), "default rowsOut=0");
    }

    /** A parallel keyed shuffle through runSinkGraphReported returns a report whose rowsOut is the group count and whose peak stays within the acquired allowance. */
    @Test
    void parallelKeyedReturnsReport() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 300; i++) rows.add(row("k" + (i % 11), 1));
        ExecutionReport rep = runWholeReported(new PushDagExecutor(3), graph(rows, discard()));
        assertEquals(11, rep.rowsOut(), "11 groups");
        assertTrue(rep.withinBudget(), "no-OOM assertion holds");
        assertEquals(0, rep.spillCount(), "large pool does not spill");
        assertEquals(TelemetryCoverage.COMPLETE, rep.resourceTelemetryCoverage());
        assertEquals(TelemetryCoverage.COMPLETE, rep.partitionTelemetryCoverage());
        assertEquals(1, rep.partitionDistributions().size());
    }

    /** A tiny pool forces spill and report.spillCount rises above 0, since the parallel path collects spill metrics. */
    @Test
    void parallelKeyedReportsSpillUnderTinyPool() {
        System.setProperty("rowweave.worker.operator.minMemory", "1024");
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 20000; i++) rows.add(row("k" + (i % 5000), 1));  // 5000 distinct keys, a high cardinality that forces spill
        ManagedMemory tiny = ManagedMemory.ofBytes(8 * 1024);  // a very small pool, where perBreaker falls far short of 5000 groups
        ExecutionReport rep = new PushDagExecutor(2, tiny).runSinkGraphReported(graph(rows, discard()));
        assertEquals(5000, rep.rowsOut(), "5000 groups");
        assertTrue(rep.spillCount() > 0, "tiny pool triggers spill, spillCount > 0; actual " + rep.spillCount());
        assertTrue(rep.spillRunCount() > 0, "proactive spill runs must be counted separately from pressure spills");
        assertTrue(rep.spillLogicalBytesWritten() > 0, "logical bytes of successful appends must be visible");
        assertTrue(rep.spillPeakReservedBytes() > 0, "peak spill workspace reservation must be visible");
    }

    /** runSinkGraph still returns a long rowCount, keeping the contract that the Worker writes the Sink, and it equals report.rowsOut. */
    @Test
    void runSinkGraphStillReturnsLong() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 200; i++) rows.add(row("k" + (i % 7), 1));
        long count = new PushDagExecutor(2).runSinkGraph(graph(rows, discard()));
        ExecutionReport rep = new PushDagExecutor(2).runSinkGraphReported(graph(rows, discard()));
        assertEquals(7, count);
        assertEquals(count, rep.rowsOut(), "runSinkGraph long == report.rowsOut");
    }

    /** Multi-sink fan-out branches count concurrently; report aggregation must be race-free. */
    @Test
    void fanoutReportCountsEveryRowWrittenByConcurrentSinks() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 20_000; i++) rows.add(row("k" + i, 1));
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.source(META, rows),
                PlanNode.unary("filter", META, 0).param("expr", "amt >= 0").build(),
                PlanNode.sink(META, 1, new DiscardSink(META)).build(),
                PlanNode.sink(META, 1, new DiscardSink(META)).build()));

        ExecutionReport report = runWholeReported(new PushDagExecutor(1), graph);

        assertEquals(40_000L, report.rowsOut());
        assertEquals(TelemetryCoverage.COMPLETE, report.resourceTelemetryCoverage());
        assertEquals(0L, report.peakReservedBytes(), "pass-through fan-out reports a real 0 rather than a missing measurement");
    }

    @Test
    void disjointPerSinkRouteMergesCompleteFactsWithoutResettingThem() {
        ExecutionEnvironment session = RowWeave.createEnvironment("telemetry-disjoint");
        DataSet left = session.fromSource(in(Arrays.asList(row("a", 1), row("b", 2))))
                .filter("amt >= 0");
        DataSet right = session.fromSource(in(Collections.singletonList(row("c", 3))))
                .filter("amt >= 0");
        WorkerCollectingOutput leftOut = new WorkerCollectingOutput(META);
        WorkerCollectingOutput rightOut = new WorkerCollectingOutput(META);
        left.sinkTo(leftOut);
        right.sinkTo(rightOut);

        PlanGraph graph = session.toPlanGraph();
        ExecutionReport report = runWholeReported(new PushDagExecutor(1), graph);

        assertEquals(leftOut.getRows().size() + rightOut.getRows().size(), report.rowsOut());
        assertEquals(TelemetryCoverage.COMPLETE, report.resourceTelemetryCoverage());
        assertEquals(0L, report.acquiredBytes());
    }

    @Test
    void materializedMultiOutputRouteKeepsItsDiskFacts() {
        ExecutionEnvironment session = RowWeave.createEnvironment("telemetry-materialized");
        DataSet mid = session.fromSource(in(Arrays.asList(
                row("a", 1), row("a", 2), row("b", 3)))).filter("amt >= 0");
        DataSet left = mid.filter("amt >= 1");
        DataSet right = mid.filter("amt <= 3");
        DataSet joined = left.join(right, SortMergeJoin.of(
                META, META, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
        DataSet direct = mid.filter("amt >= 2");
        WorkerCollectingOutput joinedOut = new WorkerCollectingOutput(joined.getSchema());
        WorkerCollectingOutput directOut = new WorkerCollectingOutput(META);
        joined.sinkTo(joinedOut);
        direct.sinkTo(directOut);

        PlanGraph graph = session.toPlanGraph();
        ExecutionReport report = runWholeReported(new PushDagExecutor(1), graph);

        assertEquals(joinedOut.getRows().size() + directOut.getRows().size(), report.rowsOut());
        assertEquals(TelemetryCoverage.COMPLETE, report.resourceTelemetryCoverage());
        assertTrue(report.spillRunCount() > 0,
                "materialized fork disk run must survive buffer disposal and workspace close");
        assertTrue(report.spillLogicalBytesWritten() > 0);
    }

    @Test
    void disjointPerSinkRouteExactlyAddsAllSixNonZeroFacts() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 5_000; i++) rows.add(row("k" + i, i));

        PlanGraph expectedGraph = disjointBreakerGraph(rows);
        ExecutionReport first = new PushDagExecutor(1, ManagedMemory.ofBytes(16 * 1024))
                .runSinkGraphReported(expectedGraph.subGraphRootedAt(2));
        ExecutionReport second = new PushDagExecutor(1, ManagedMemory.ofBytes(16 * 1024))
                .runSinkGraphReported(expectedGraph.subGraphRootedAt(5));

        PlanGraph wholeGraph = disjointBreakerGraph(rows);
        ExecutionReport whole = runWholeReported(
                new PushDagExecutor(1, ManagedMemory.ofBytes(16 * 1024)), wholeGraph);

        assertEquals(Math.addExact(first.rowsOut(), second.rowsOut()), whole.rowsOut());
        assertEquals(Math.addExact(first.peakReservedBytes(), second.peakReservedBytes()),
                whole.peakReservedBytes());
        assertEquals(Math.addExact(first.acquiredBytes(), second.acquiredBytes()),
                whole.acquiredBytes());
        assertEquals(Math.addExact(first.pressureSpillCount(), second.pressureSpillCount()),
                whole.pressureSpillCount());
        assertEquals(Math.addExact(first.spillRunCount(), second.spillRunCount()),
                whole.spillRunCount());
        assertEquals(Math.addExact(first.spillLogicalBytesWritten(),
                second.spillLogicalBytesWritten()), whole.spillLogicalBytesWritten());
        assertEquals(Math.addExact(first.spillPeakReservedBytes(),
                second.spillPeakReservedBytes()), whole.spillPeakReservedBytes());
        assertEquals(TelemetryCoverage.COMPLETE, whole.resourceTelemetryCoverage());
        assertTrue(whole.spillRunCount() > 0);
    }

    @Test
    void reportedSourceShardedAndParallelBinaryRoutesFreezeExactlyTheirReturnedFacts() {
        WorkerCollectingOutput shardedOutput = new WorkerCollectingOutput(META);
        PlanGraph sourceSharded = new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META, Arrays.asList(
                        in(Collections.singletonList(row("a", 1))),
                        in(Collections.singletonList(row("b", 2))),
                        in(Collections.singletonList(row("c", 3))))),
                PlanNode.unary("filter", META, 0).param("expr", "amt >= 0").build(),
                PlanNode.sink(META, 1, shardedOutput).build()));

        ExecutionReport sharded = runWholeReported(new PushDagExecutor(3), sourceSharded);

        assertEquals(3L, sharded.rowsOut());
        assertEquals(3, shardedOutput.getRows().size());
        assertEquals(TelemetryCoverage.COMPLETE, sharded.resourceTelemetryCoverage());
        assertEquals(0L, sharded.acquiredBytes());

        ExecutionEnvironment session = RowWeave.createEnvironment("reported-parallel-binary");
        DataSet left = session.fromSource(in(Arrays.asList(row("a", 1), row("b", 2))));
        DataSet right = session.fromSource(in(Arrays.asList(row("a", 3), row("c", 4))));
        DataSet joined = left.join(right, SortMergeJoin.of(
                META, META, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
        WorkerCollectingOutput joinedOutput = new WorkerCollectingOutput(joined.getSchema());
        joined.sinkTo(joinedOutput);

        ExecutionReport binary = runWholeReported(
                new PushDagExecutor(2), session.toPlanGraph());

        assertEquals(1L, binary.rowsOut());
        assertEquals(1, joinedOutput.getRows().size());
        assertEquals(TelemetryCoverage.COMPLETE, binary.resourceTelemetryCoverage());
        assertTrue(binary.acquiredBytes() > 0);
    }

    private static ExecutionReport runWholeReported(PushDagExecutor executor, PlanGraph graph) {
        StageExecutionTelemetryAccumulator telemetry =
                new StageExecutionTelemetryAccumulator();
        ExecutionReport report = executor.runWholeGraphReported(graph, telemetry);
        StageExecutionTelemetry frozen = telemetry.freeze();
        assertEquals(report.resourceTelemetryCoverage(), frozen.coverage());
        assertEquals(report.peakReservedBytes(), frozen.managedPeakReservedBytes());
        assertEquals(report.acquiredBytes(), frozen.managedAcquiredBytes());
        assertEquals(report.pressureSpillCount(), frozen.pressureSpillCount());
        assertEquals(report.spillRunCount(), frozen.spillRunCount());
        assertEquals(report.spillLogicalBytesWritten(), frozen.spillLogicalBytesWritten());
        assertEquals(report.spillPeakReservedBytes(), frozen.spillPeakReservedBytes(),
                "reported route must merge every owner once and preserve every field");
        return report;
    }

    private static PlanGraph disjointBreakerGraph(List<DataRow> rows) {
        SortField sort = new SortField(1, SortField.Direction.ASC,
                SortField.NullOrder.NULLS_LAST);
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, rows),
                PlanNode.unary("orderBy", META, 0)
                        .listParam("sort", Collections.singletonList(
                                io.rowweave.plan.PlanOps.encodeSortField(sort))).build(),
                PlanNode.sink(META, 1, new DiscardSink(META)).build(),
                PlanNode.source(META, rows),
                PlanNode.unary("groupBySum", GB, 3)
                        .listParam("keys", Collections.singletonList("k"))
                        .param("measure", "amt").build(),
                PlanNode.sink(GB, 4, new DiscardSink(GB)).build()));
    }
}
