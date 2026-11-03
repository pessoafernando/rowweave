package io.rowweave.cluster;

import io.rowweave.CollectingSink;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.join.JoinPhysicalStrategy;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.TelemetryCoverage;
import io.rowweave.sched.Channel;
import io.rowweave.sched.LocalShuffleRegistry;
import io.rowweave.sched.MultiProducerChannel;
import io.rowweave.sched.net.ShuffleEndpoint;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StageTaskTelemetryTest {
    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType),
            new Field("v", DataType.IntegerType));
    private static final int WINDOW = 32;

    @Test
    void keyedBreakerReportsManagedMemoryBeforeClosingIt() throws Exception {
        PlanGraph aggregate = groupByCountFragment();
        StageCommand command = StageCommand.keyedOp(
                        META, WINDOW, 1, aggregate, new ShuffleEndpoint("ignored", 1))
                .withLocalInbound(1, "keyed-telemetry");

        StageTask.StageResult result = runWithLocalInput(
                command, "keyed-telemetry", row("a", 1), row("a", 2), row("b", 3));

        assertEquals(StageTask.StopReason.EOS, result.reason());
        assertEquals(2L, result.rows());
        assertManagedBreakerFacts(result);
    }

    @Test
    void sinkBreakerReportsManagedMemoryAndCommitsOutput() throws Exception {
        PlanGraph aggregate = groupByCountFragment();
        DataSchema grouped = aggregate.root().outMeta();
        CollectingSink output = new CollectingSink(grouped);
        StageCommand command = StageCommand.sink(META, WINDOW, 1, aggregate, output)
                .withLocalInbound(1, "sink-telemetry");

        StageTask.StageResult result = runWithLocalInput(
                command, "sink-telemetry", row("a", 1), row("a", 2), row("b", 3));

        assertEquals(StageTask.StopReason.EOS, result.reason());
        assertEquals(2L, result.rows());
        assertEquals(2, output.getRows().size());
        assertManagedBreakerFacts(result);
    }

    @Test
    void everyProtectedJoinStrategyReportsItsOwnedManagedMemory() throws Exception {
        DataSchema joined = SortMergeJoin.of(
                META, META, new String[]{"k"}, new String[]{"k"}, JoinType.INNER)
                .getOutputSchema();
        for (JoinPhysicalStrategy strategy : Arrays.asList(
                JoinPhysicalStrategy.SORT_MERGE,
                JoinPhysicalStrategy.HASH,
                JoinPhysicalStrategy.AUTO)) {
            String prefix = "join-" + strategy.name().toLowerCase();
            StageCommand command = StageCommand.joinConsumer(
                            META, META, joined, WINDOW, 1, 1,
                            new String[]{"k"}, new String[]{"k"}, "INNER",
                            strategy, new ShuffleEndpoint("ignored", 1))
                    .withLocalInbound(1, prefix + "-left")
                    .withLocalInboundRight(1, prefix + "-right");

            StageTask.StageResult result = runJoinWithLocalInputs(
                    command, prefix + "-left", prefix + "-right",
                    new DataRow[]{row("a", 1), row("b", 2)},
                    new DataRow[]{row("a", 3), row("c", 4)});

            assertEquals(StageTask.StopReason.EOS, result.reason(), strategy.name());
            assertEquals(1L, result.rows(), strategy.name());
            assertManagedBreakerFacts(result);
        }
    }

    @Test
    void blockingFanoutReportsItsSingleMaterializationRun() {
        PlanGraph identity = new PlanGraph(Collections.singletonList(
                PlanNode.source(META, Collections.emptyList())));
        StageCommand command = StageCommand.sourceStage(
                        META, META, WINDOW,
                        input(Arrays.asList(row("a", 1), row("b", 2), row("c", 3))),
                        identity, new ShuffleEndpoint("ignored", 1))
                .withBroadcastGroups(Arrays.asList(
                        new StageCommand.BroadcastGroup(
                                Collections.singletonList(ShuffleTarget.socket(
                                        new ShuffleEndpoint("ignored", 1))),
                                new String[]{"k"}, false, true),
                        new StageCommand.BroadcastGroup(
                                Collections.singletonList(ShuffleTarget.socket(
                                        new ShuffleEndpoint("ignored", 2))),
                                new String[]{"k"}, false, false)));
        StageTask task = taskWithDiscardEndpoints(command);

        StageTask.StageResult result = task.run();

        assertEquals(StageTask.StopReason.EOS, result.reason());
        assertEquals(3L, result.rows());
        assertEquals(TelemetryCoverage.COMPLETE, result.telemetry().coverage());
        assertEquals(1L, result.telemetry().spillRunCount());
        assertTrue(result.telemetry().spillLogicalBytesWritten() > 0);
    }

    @Test
    void wholeGraphMergesSharedBreakerFactsOnceAcrossMultipleOutputs() {
        List<DataRow> rows = Arrays.asList(row("a", 1), row("a", 2), row("b", 3));
        ParallelGroupByAgg aggregate = ParallelGroupByAgg.of(META, "k").count();
        DataSchema grouped = aggregate.getOutputSchema();
        CollectingSink passThrough = new CollectingSink(META);
        CollectingSink groupedOutput = new CollectingSink(grouped);
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.source(META, rows),
                PlanNode.unary("groupByCount", grouped, 0)
                        .listParam("keys", Collections.singletonList("k"))
                        .build(),
                PlanNode.sink(META, 0, passThrough).build(),
                PlanNode.sink(grouped, 1, groupedOutput).build()));

        StageTask.StageResult result = new StageTask(
                StageCommand.wholeGraph(graph), StageTask.PortListener.NONE).run();

        assertEquals(StageTask.StopReason.EOS, result.reason());
        assertEquals(5L, result.rows());
        assertEquals(3, passThrough.getRows().size());
        assertEquals(2, groupedOutput.getRows().size());
        assertManagedBreakerFacts(result);
    }

    @Test
    void routerWithoutBreakerReportsCompleteZeroFacts() {
        StageCommand command = StageCommand.router(
                META, WINDOW,
                input(Arrays.asList(row("a", 1), row("b", 2))),
                new String[]{"k"},
                Arrays.asList(
                        new ShuffleEndpoint("ignored", 1),
                        new ShuffleEndpoint("ignored", 2)));

        StageTask.StageResult result = taskWithDiscardEndpoints(command).run();

        assertEquals(StageTask.StopReason.EOS, result.reason());
        assertCompleteZeroFacts(result);
    }

    @Test
    void sinkSuccessUsesCloseOrPrepareAccordingToCommitMode() throws Exception {
        PlanGraph identity = new PlanGraph(Collections.singletonList(
                PlanNode.source(META, Collections.emptyList())));
        LifecycleOutput ordinary = new LifecycleOutput(null, null);
        StageTask.StageResult ordinaryResult = runWithLocalInput(
                StageCommand.sink(META, WINDOW, 1, identity, ordinary)
                        .withLocalInbound(1, "ordinary-output"),
                "ordinary-output", row("a", 1));

        assertEquals(StageTask.StopReason.EOS, ordinaryResult.reason());
        assertEquals(1, ordinary.closeCalls.get());
        assertEquals(0, ordinary.prepareCalls.get());
        assertEquals(0, ordinary.rollbackCalls.get());

        LifecycleOutput coordinated = new LifecycleOutput(null, null);
        StageTask.StageResult coordinatedResult = runWithLocalInput(
                StageCommand.sink(META, WINDOW, 1, identity, coordinated)
                        .withLocalInbound(1, "coordinated-output")
                        .withCommitGroupId("group-1"),
                "coordinated-output", row("a", 1));

        assertEquals(StageTask.StopReason.EOS, coordinatedResult.reason());
        assertEquals(0, coordinated.closeCalls.get());
        assertEquals(1, coordinated.prepareCalls.get());
        assertEquals(0, coordinated.rollbackCalls.get());
    }

    @Test
    void coordinatedSinkCancellationDuringReversiblePrepareRollsBack() throws Exception {
        CountDownLatch prepareStarted = new CountDownLatch(1);
        CountDownLatch releasePrepare = new CountDownLatch(1);
        AtomicInteger prepareCalls = new AtomicInteger();
        AtomicInteger rollbackCalls = new AtomicInteger();
        Sink output = new Sink() {
            @Override public DataSchema getSchema() { return META; }
            @Override public void collect(DataRow row) { }
            @Override public boolean supportsCoordinatedCommit() { return true; }
            @Override public void prepareCommit() {
                prepareCalls.incrementAndGet();
                prepareStarted.countDown();
                try {
                    if (!releasePrepare.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("prepare release timed out");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("prepare interrupted", interrupted);
                }
            }
            @Override public void commitPrepared() { }
            @Override public void rollbackCommit() { rollbackCalls.incrementAndGet(); }
            @Override public void finalizeCommit() { }
        };
        PlanGraph identity = new PlanGraph(Collections.singletonList(
                PlanNode.source(META, Collections.emptyList())));
        StageCommand command = StageCommand.sink(META, WINDOW, 1, identity, output)
                .withLocalInbound(1, "cancel-during-prepare")
                .withCommitGroupId("group-cancel-prepare");
        LocalShuffleRegistry registry = new LocalShuffleRegistry();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Thread stopping = null;
        try {
            StageTask task = taskWithDiscardEndpoints(command);
            task.setLocalShuffle(registry);
            Future<StageTask.StageResult> running = executor.submit(task::run);
            MultiProducerChannel input = (MultiProducerChannel) registry.await(
                    "cancel-during-prepare", 2_000);
            input.send(StreamEvent.record(row("a", 1)));
            input.producerDone();
            assertTrue(prepareStarted.await(10, TimeUnit.SECONDS));
            stopping = new Thread(
                    () -> task.stop(StageTask.StopReason.CANCELLED),
                    "cancel-during-reversible-prepare");
            stopping.start();
            long blockedDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (stopping.getState() != Thread.State.BLOCKED
                    && stopping.isAlive() && System.nanoTime() < blockedDeadline) {
                Thread.yield();
            }
            assertEquals(Thread.State.BLOCKED, stopping.getState(),
                    "stop must have accepted cancellation and wait behind in-flight prepare");
            releasePrepare.countDown();
            stopping.join(10_000);
            assertTrue(!stopping.isAlive());

            StageTask.StageResult result = running.get(10, TimeUnit.SECONDS);

            assertEquals(StageTask.StopReason.CANCELLED, result.reason());
            assertEquals(1, prepareCalls.get());
            assertEquals(1, rollbackCalls.get());
        } finally {
            releasePrepare.countDown();
            if (stopping != null) stopping.join(10_000);
            executor.shutdownNow();
            registry.close();
        }
    }

    @Test
    void sinkFailureRollsBackAndKeepsRollbackFailureSuppressed() throws Exception {
        RuntimeException collectFailure = new RuntimeException("collect failed");
        RuntimeException rollbackFailure = new RuntimeException("rollback failed");
        LifecycleOutput output = new LifecycleOutput(collectFailure, rollbackFailure);
        PlanGraph identity = new PlanGraph(Collections.singletonList(
                PlanNode.source(META, Collections.emptyList())));
        StageCommand command = StageCommand.sink(META, WINDOW, 1, identity, output)
                .withLocalInbound(1, "failing-output")
                .withCommitGroupId("group-2");

        StageTask.StageResult result = runWithLocalInput(
                command, "failing-output", row("a", 1));

        assertEquals(StageTask.StopReason.FAILED, result.reason());
        assertSameInCauseChain(collectFailure, result.failure());
        assertEquals(1, output.rollbackCalls.get());
        assertEquals(1, collectFailure.getSuppressed().length);
        assertEquals(rollbackFailure, collectFailure.getSuppressed()[0]);
        assertCompleteZeroFacts(result);
    }

    private static PlanGraph groupByCountFragment() {
        ParallelGroupByAgg aggregate = ParallelGroupByAgg.of(META, "k").count();
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.emptyList()),
                PlanNode.unary("groupByCount", aggregate.getOutputSchema(), 0)
                        .listParam("keys", Collections.singletonList("k"))
                        .build()));
    }

    private static StageTask.StageResult runWithLocalInput(
            StageCommand command, String localKey, DataRow... rows) throws Exception {
        LocalShuffleRegistry registry = new LocalShuffleRegistry();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            StageTask task = taskWithDiscardEndpoints(command);
            task.setLocalShuffle(registry);
            Future<StageTask.StageResult> running = executor.submit(task::run);
            MultiProducerChannel input = (MultiProducerChannel) registry.await(localKey, 2_000);
            for (DataRow row : rows) input.send(StreamEvent.record(row));
            input.producerDone();
            return running.get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            registry.close();
        }
    }

    private static StageTask.StageResult runJoinWithLocalInputs(
            StageCommand command, String leftKey, String rightKey,
            DataRow[] leftRows, DataRow[] rightRows) throws Exception {
        LocalShuffleRegistry registry = new LocalShuffleRegistry();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            StageTask task = taskWithDiscardEndpoints(command);
            task.setLocalShuffle(registry);
            Future<StageTask.StageResult> running = executor.submit(task::run);
            MultiProducerChannel left = (MultiProducerChannel) registry.await(leftKey, 2_000);
            MultiProducerChannel right = (MultiProducerChannel) registry.await(rightKey, 2_000);
            for (DataRow row : leftRows) left.send(StreamEvent.record(row));
            left.producerDone();
            for (DataRow row : rightRows) right.send(StreamEvent.record(row));
            right.producerDone();
            return running.get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            registry.close();
        }
    }

    private static StageTask taskWithDiscardEndpoints(StageCommand command) {
        return new StageTask(command, StageTask.PortListener.NONE,
                new StageTask.SetupFactory() {
                    @Override
                    public Channel endpoint(
                            ShuffleEndpoint endpoint, DataSchema meta,
                            java.util.function.Supplier<Channel> productionFactory) {
                        return discardChannel();
                    }
                });
    }

    private static Source input(List<DataRow> rows) {
        return new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public Iterator<DataRow> iterator() { return rows.iterator(); }
        };
    }

    private static Channel discardChannel() {
        return new Channel() {
            @Override public void send(StreamEvent event) { }
            @Override public StreamEvent poll() { return null; }
            @Override public void close() { }
            @Override public void fail(Throwable cause) { }
        };
    }

    private static final class LifecycleOutput implements Sink {
        private final RuntimeException collectFailure;
        private final RuntimeException rollbackFailure;
        private final AtomicInteger closeCalls = new AtomicInteger();
        private final AtomicInteger prepareCalls = new AtomicInteger();
        private final AtomicInteger rollbackCalls = new AtomicInteger();

        private LifecycleOutput(RuntimeException collectFailure, RuntimeException rollbackFailure) {
            this.collectFailure = collectFailure;
            this.rollbackFailure = rollbackFailure;
        }

        @Override public DataSchema getSchema() { return META; }
        @Override public void collect(DataRow row) {
            if (collectFailure != null) throw collectFailure;
        }
        @Override public void close() { closeCalls.incrementAndGet(); }
        @Override public void abort() { rollbackCommit(); }
        @Override public boolean supportsCoordinatedCommit() { return true; }
        @Override public void prepareCommit() { prepareCalls.incrementAndGet(); }
        @Override public void commitPrepared() { }
        @Override public void rollbackCommit() {
            rollbackCalls.incrementAndGet();
            if (rollbackFailure != null) throw rollbackFailure;
        }
        @Override public void finalizeCommit() { }
    }

    private static DataRow row(String key, int value) {
        DataRow row = new DataRow(2);
        row.set(0, key);
        row.set(1, value);
        return row;
    }

    private static void assertManagedBreakerFacts(StageTask.StageResult result) {
        assertEquals(TelemetryCoverage.COMPLETE, result.telemetry().coverage());
        assertTrue(result.telemetry().managedAcquiredBytes() > 0);
        assertTrue(result.telemetry().managedPeakReservedBytes() > 0);
    }

    private static void assertCompleteZeroFacts(StageTask.StageResult result) {
        assertEquals(TelemetryCoverage.COMPLETE, result.telemetry().coverage());
        assertEquals(0L, result.telemetry().managedPeakReservedBytes());
        assertEquals(0L, result.telemetry().managedAcquiredBytes());
        assertEquals(0L, result.telemetry().pressureSpillCount());
        assertEquals(0L, result.telemetry().spillRunCount());
        assertEquals(0L, result.telemetry().spillLogicalBytesWritten());
        assertEquals(0L, result.telemetry().spillPeakReservedBytes());
    }

    private static void assertSameInCauseChain(Throwable expected, Throwable actual) {
        Throwable cursor = actual;
        while (cursor != null && cursor != expected) cursor = cursor.getCause();
        assertEquals(expected, cursor);
    }
}

