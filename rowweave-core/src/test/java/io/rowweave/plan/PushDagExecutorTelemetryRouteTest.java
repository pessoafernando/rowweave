package io.rowweave.plan;

import io.rowweave.WorkerCollectingOutput;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import io.rowweave.sched.PipelineExecutor;
import io.rowweave.sched.stream.RowStream;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** every local reported route must preserve one frozen six-field fact per owner. */
class PushDagExecutorTelemetryRouteTest {
    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType),
            new Field("v", DataType.IntegerType));
    private static final StageExecutionTelemetry MARKER =
            StageExecutionTelemetry.complete(11, 13, 17, 19, 23, 29);

    @Test
    void linearBoundReportedFreezesAndMergesItsOwnerExactlyOnce() {
        Probe probe = new Probe();
        PushDagExecutor executor = new PushDagExecutor(1, null, probe);
        PlanGraph fragment = new PlanGraph(Collections.singletonList(
                PlanNode.placeholderSource(META)));
        StageExecutionTelemetryAccumulator accumulator =
                new StageExecutionTelemetryAccumulator();

        executor.executeLinearBoundReported(
                fragment,
                Collections.singletonMap(0, RowStream.fromIterable(
                        Collections.singletonList(row("a", 1)))),
                PipelineExecutor.sinkStage(ignored -> { }),
                accumulator);

        assertTelemetry(accumulator.freeze(), 1);
        assertEquals(1, probe.calls.get());
    }

    @Test
    void serialSinkAndWholeReportedReuseOneFrozenSixFieldFact() {
        Probe sinkProbe = new Probe();
        ExecutionReport sink = new PushDagExecutor(1, null, sinkProbe)
                .runSinkGraphReported(serialGraph());
        assertTelemetry(sink, 1);
        assertEquals(1, sinkProbe.calls.get());

        Probe wholeProbe = new Probe();
        StageExecutionTelemetryAccumulator accumulator =
                new StageExecutionTelemetryAccumulator();
        ExecutionReport whole = new PushDagExecutor(1, null, wholeProbe)
                .runWholeGraphReported(serialGraph(), accumulator);
        assertTelemetry(whole, 1);
        assertTelemetry(accumulator.freeze(), 1);
        assertEquals(1, wholeProbe.calls.get());
    }

    @Test
    void parallelAndMultiOutputRoutesPreserveEveryFieldWithoutDuplicateFreeze() {
        assertWholeRoute(keyedGraph(), 2, 1);
        assertWholeRoute(sourceShardedGraph(), 3, 1);
        assertWholeRoute(binaryGraph(), 2, 1);
        assertWholeRoute(fanoutGraph(), 1, 1);
        assertWholeRoute(materializedGraph(), 1, 1);
        assertWholeRoute(disjointPerSinkGraph(), 1, 2);
    }

    @Test
    void snapshotFailureDowngradesFactsWithoutChangingTheDataResult() {
        AtomicInteger calls = new AtomicInteger();
        StageExecutionTelemetryAccumulator accumulator =
                new StageExecutionTelemetryAccumulator();
        PushDagExecutor executor = new PushDagExecutor(1, null, memory -> {
            memory.telemetrySnapshot();
            calls.incrementAndGet();
            throw new RuntimeException("telemetry probe failed");
        });

        ExecutionReport report = assertDoesNotThrow(
                () -> executor.runWholeGraphReported(serialGraph(), accumulator));

        assertEquals(2L, report.rowsOut());
        assertEquals(TelemetryCoverage.UNAVAILABLE, report.resourceTelemetryCoverage());
        assertEquals(TelemetryCoverage.UNAVAILABLE, accumulator.freeze().coverage());
        assertEquals(1, calls.get());
    }

    @Test
    void wholeGraphTerminalGateRejectsPreparedMultiOutputBeforePublication() {
        CoordinatedOutput first = new CoordinatedOutput();
        CoordinatedOutput second = new CoordinatedOutput();
        StageExecutionTelemetryAccumulator accumulator =
                new StageExecutionTelemetryAccumulator();

        RuntimeException rejected = assertThrows(RuntimeException.class,
                () -> new PushDagExecutor(1).runWholeGraphReported(
                        multiOutputGraph(first, second), accumulator, () -> false));

        assertEquals("whole graph terminal rejected by cancellation", rejected.getMessage());
        assertEquals(1, first.prepareCalls.get());
        assertEquals(1, second.prepareCalls.get());
        assertEquals(0, first.commitCalls.get());
        assertEquals(0, second.commitCalls.get());
        assertEquals(1, first.rollbackCalls.get());
        assertEquals(1, second.rollbackCalls.get());
        assertEquals(0, first.finalizeCalls.get());
        assertEquals(0, second.finalizeCalls.get());
    }

    private static void assertWholeRoute(PlanGraph graph, int parallelism, int ownerCount) {
        Probe probe = new Probe();
        StageExecutionTelemetryAccumulator accumulator =
                new StageExecutionTelemetryAccumulator();
        ExecutionReport report = new PushDagExecutor(parallelism, null, probe)
                .runWholeGraphReported(graph, accumulator);

        assertTelemetry(report, ownerCount);
        assertTelemetry(accumulator.freeze(), ownerCount);
        assertEquals(ownerCount, probe.calls.get(), "one snapshot per physical owner");
    }

    private static PlanGraph serialGraph() {
        WorkerCollectingOutput output = new WorkerCollectingOutput(META);
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, Arrays.asList(row("a", 1), row("b", 2))),
                PlanNode.unary("filter", META, 0).param("expr", "v >= 0").build(),
                PlanNode.sink(META, 1, output).build()));
    }

    private static PlanGraph keyedGraph() {
        ParallelGroupByAgg aggregate = ParallelGroupByAgg.of(META, "k").count();
        DataSchema grouped = aggregate.getOutputSchema();
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, Arrays.asList(
                        row("a", 1), row("a", 2), row("b", 3))),
                PlanNode.unary("groupByCount", grouped, 0)
                        .listParam("keys", Collections.singletonList("k")).build(),
                PlanNode.sink(grouped, 1, new WorkerCollectingOutput(grouped)).build()));
    }

    private static PlanGraph sourceShardedGraph() {
        return new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META, Arrays.asList(
                        input(Collections.singletonList(row("a", 1))),
                        input(Collections.singletonList(row("b", 2))),
                        input(Collections.singletonList(row("c", 3))))),
                PlanNode.unary("filter", META, 0).param("expr", "v >= 0").build(),
                PlanNode.sink(META, 1, new WorkerCollectingOutput(META)).build()));
    }

    private static PlanGraph binaryGraph() {
        DataSchema joined = SortMergeJoin.of(
                META, META, new String[]{"k"}, new String[]{"k"}, JoinType.INNER)
                .getOutputSchema();
        PlanNode join = PlanNode.binary("join", joined, 0, 1)
                .listParam("leftKeys", Collections.singletonList("k"))
                .listParam("rightKeys", Collections.singletonList("k"))
                .param("joinType", "INNER")
                .param("joinHint", "SORT_MERGE")
                .build();
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, Arrays.asList(row("a", 1), row("b", 2))),
                PlanNode.source(META, Arrays.asList(row("a", 3), row("c", 4))),
                join,
                PlanNode.sink(joined, 2, new WorkerCollectingOutput(joined)).build()));
    }

    private static PlanGraph fanoutGraph() {
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, Arrays.asList(row("a", 1), row("b", 2))),
                PlanNode.unary("filter", META, 0).param("expr", "v >= 0").build(),
                PlanNode.sink(META, 1, new WorkerCollectingOutput(META)).build(),
                PlanNode.sink(META, 1, new WorkerCollectingOutput(META)).build()));
    }

    private static PlanGraph materializedGraph() {
        ExecutionEnvironment session = io.rowweave.RowWeave.createEnvironment("telemetry-route-materialized");
        DataSet middle = session.fromSource(input(Arrays.asList(
                row("a", 1), row("a", 2), row("b", 3)))).filter("v >= 0");
        DataSet left = middle.filter("v >= 1");
        DataSet right = middle.filter("v <= 3");
        DataSet joined = left.join(right, SortMergeJoin.of(
                META, META, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
        joined.sinkTo(new WorkerCollectingOutput(joined.getSchema()));
        middle.filter("v >= 2").sinkTo(new WorkerCollectingOutput(META));
        return session.toPlanGraph();
    }

    private static PlanGraph disjointPerSinkGraph() {
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.singletonList(row("a", 1))),
                PlanNode.unary("filter", META, 0).param("expr", "v >= 0").build(),
                PlanNode.sink(META, 1, new WorkerCollectingOutput(META)).build(),
                PlanNode.source(META, Collections.singletonList(row("b", 2))),
                PlanNode.unary("filter", META, 3).param("expr", "v >= 0").build(),
                PlanNode.sink(META, 4, new WorkerCollectingOutput(META)).build()));
    }

    private static PlanGraph multiOutputGraph(Sink first, Sink second) {
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, Arrays.asList(row("a", 1), row("b", 2))),
                PlanNode.sink(META, 0, first).build(),
                PlanNode.sink(META, 0, second).build()));
    }

    private static Source input(List<DataRow> rows) {
        List<DataRow> copy = new ArrayList<>(rows);
        return new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() { return copy.iterator(); }
        };
    }

    private static DataRow row(String key, int value) {
        DataRow row = new DataRow(2);
        row.set(0, key);
        row.set(1, value);
        return row;
    }

    private static void assertTelemetry(ExecutionReport report, long multiplier) {
        assertEquals(TelemetryCoverage.COMPLETE, report.resourceTelemetryCoverage());
        assertEquals(11L * multiplier, report.peakReservedBytes());
        assertEquals(13L * multiplier, report.acquiredBytes());
        assertEquals(17L * multiplier, report.pressureSpillCount());
        assertEquals(19L * multiplier, report.spillRunCount());
        assertEquals(23L * multiplier, report.spillLogicalBytesWritten());
        assertEquals(29L * multiplier, report.spillPeakReservedBytes());
    }

    private static void assertTelemetry(StageExecutionTelemetry telemetry, long multiplier) {
        assertEquals(TelemetryCoverage.COMPLETE, telemetry.coverage());
        assertEquals(11L * multiplier, telemetry.managedPeakReservedBytes());
        assertEquals(13L * multiplier, telemetry.managedAcquiredBytes());
        assertEquals(17L * multiplier, telemetry.pressureSpillCount());
        assertEquals(19L * multiplier, telemetry.spillRunCount());
        assertEquals(23L * multiplier, telemetry.spillLogicalBytesWritten());
        assertEquals(29L * multiplier, telemetry.spillPeakReservedBytes());
    }

    private static final class CoordinatedOutput implements Sink {
        private final AtomicInteger prepareCalls = new AtomicInteger();
        private final AtomicInteger commitCalls = new AtomicInteger();
        private final AtomicInteger rollbackCalls = new AtomicInteger();
        private final AtomicInteger finalizeCalls = new AtomicInteger();

        @Override public DataSchema getSchema() { return META; }
        @Override public void collect(DataRow row) { }
        @Override public boolean supportsCoordinatedCommit() { return true; }
        @Override public void prepareCommit() { prepareCalls.incrementAndGet(); }
        @Override public void commitPrepared() { commitCalls.incrementAndGet(); }
        @Override public void rollbackCommit() { rollbackCalls.incrementAndGet(); }
        @Override public void finalizeCommit() { finalizeCalls.incrementAndGet(); }
    }

    private static final class Probe implements PushDagExecutor.TelemetrySnapshotter {
        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public StageExecutionTelemetry snapshot(io.rowweave.mem.ExecutionMemory memory) {
            memory.telemetrySnapshot();
            calls.incrementAndGet();
            return MARKER;
        }
    }
}
