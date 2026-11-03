package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.plan.ExecutionTelemetrySummary;
import io.rowweave.plan.ExecutionTelemetrySummaryCodec;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.StageExecutionTelemetry;
import io.rowweave.plan.StageExecutionTelemetryCodec;
import io.rowweave.plan.TelemetryCoverage;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.monitor.JobMonitorRegistry;
import io.rowweave.monitor.JobView;
import io.rowweave.monitor.StageView;
import io.rowweave.monitor.WorkerView;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MasterEphemeralTelemetryReportTest {

    @Test
    void onlyCurrentFirstTerminalContributesToEphemeralJobSummary() throws Exception {
        String host = "telemetry-worker";
        int port = 62_208;
        String incarnation = "inc-current";
        AtomicReference<RpcMessage> assignment = new AtomicReference<>();
        CountDownLatch assigned = new CountDownLatch(1);
        ExecutorService submitter = Executors.newSingleThreadExecutor();
        try (MasterServer master = new MasterServer(60_000L, 0, new InProcessRegistry())) {
            InProcessRpcRegistry.Registration endpoint = InProcessRpcRegistry.register(
                    host, port, request -> {
                        if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                            assignment.set(request);
                            assigned.countDown();
                        }
                        return RpcMessage.ack();
                    });
            try {
                master.handler().apply(RpcMessage.registerWorker(
                        "w", host, port, 1, -1, incarnation));
                Future<RpcMessage> submitted = submitter.submit(() ->
                        master.handler().apply(RpcMessage.submitJob(emptyGraph().toBytes())));
                assertTrue(assigned.await(5, TimeUnit.SECONDS));
                RpcMessage assign = assignment.get();
                RunAttemptFence current = assign.runAttemptFence();
                byte[] poisoned = StageExecutionTelemetryCodec.encode(
                        StageExecutionTelemetry.complete(999, 999, 999, 999, 999, 999));
                RunAttemptFence stale = new RunAttemptFence(
                        current.runId(), current.attemptId(), current.fenceToken() + 1L);
                master.handler().apply(RpcMessage.stageReport(
                        "w", assign.stageId(), true, 999, 999, null,
                        incarnation, stale, poisoned));
                master.handler().apply(RpcMessage.stageReport(
                        "w", assign.stageId(), true, 999, 999, null,
                        "old-incarnation", current, poisoned));
                Thread.sleep(50L);
                assertFalse(submitted.isDone());

                StageExecutionTelemetry expected = StageExecutionTelemetry.complete(
                        10, 20, 3, 4, 50, 16);
                byte[] expectedBytes = StageExecutionTelemetryCodec.encode(expected);
                master.handler().apply(RpcMessage.stageReport(
                        "w", assign.stageId(), true, 7, 5, null,
                        incarnation, current, expectedBytes));
                master.handler().apply(RpcMessage.stageReport(
                        "w", assign.stageId(), true, 999, 999, null,
                        incarnation, current, poisoned));

                RpcMessage result = submitted.get(5, TimeUnit.SECONDS);
                assertTrue(result.success(), result.errorMessage());
                assertEquals(5L, result.rowCount());
                ExecutionTelemetrySummary summary = ExecutionTelemetrySummaryCodec.decode(
                        result.executionTelemetryBytes());
                assertEquals(TelemetryCoverage.COMPLETE, summary.resourceCoverage());
                assertEquals(TelemetryCoverage.NOT_APPLICABLE, summary.partitionCoverage());
                assertEquals(10L, summary.managedPeakReservedBytes());
                assertEquals(20L, summary.managedAcquiredBytes());
                assertEquals(4L, summary.spillRunCount());
            } finally {
                endpoint.close();
            }
        } finally {
            submitter.shutdownNow();
        }
    }

    @Test
    void acceptedTerminalMonitorUpdatePrecedesJobCompletionPublication(@TempDir Path temp)
            throws Exception {
        BlockingTerminalMonitor monitor = new BlockingTerminalMonitor();
        ExecutorService threads = Executors.newSingleThreadExecutor();
        try (MasterServer master = new MasterServer(
                60_000L, 0, new InProcessRegistry(), monitor);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "ordered-w", 8, "127.0.0.1", master.port(), temp.toFile())) {
            DataSchema meta = new DataSchema(new Field("v", DataType.IntegerType));
            DataRow one = new DataRow(1); one.set(0, 1);
            DataRow two = new DataRow(1); two.set(0, 2);
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    PlanNode.source(meta, Arrays.asList(one, two)),
                    PlanNode.unary("distinct", meta, 0)
                            .listParam("keys", Collections.singletonList("v")).build(),
                    PlanNode.sink(meta, 1, new DiscardSink(meta)).build()), 2);
            Future<io.rowweave.plan.ExecutionReport> submitted = threads.submit(() -> {
                try (RealClusterJobClient client = new RealClusterJobClient(
                        "127.0.0.1", master.port())) {
                    return client.submitAttempt(graph, "ordered-monitor-attempt");
                }
            });

            assertTrue(monitor.terminalEntered.await(5, TimeUnit.SECONDS));
            assertFalse(monitor.jobCompleted.await(200, TimeUnit.MILLISECONDS),
                    "job completion must not overtake the accepted terminal stage update");
            assertFalse(submitted.isDone());
            monitor.releaseTerminal.countDown();

            assertEquals(2L, submitted.get(5, TimeUnit.SECONDS).rowsOut());
            assertTrue(monitor.jobCompleted.await(5, TimeUnit.SECONDS));
        } finally {
            monitor.releaseTerminal.countDown();
            threads.shutdownNow();
        }
    }

    private static final class BlockingTerminalMonitor implements JobMonitorRegistry {
        final CountDownLatch terminalEntered = new CountDownLatch(1);
        final CountDownLatch releaseTerminal = new CountDownLatch(1);
        final CountDownLatch jobCompleted = new CountDownLatch(1);

        @Override public void onJobSubmit(JobView job) { }

        @Override public void onStageUpdate(String jobId, StageView stage) {
            if (!"SINK".equals(stage.role())
                    || (!"FINISHED".equals(stage.state())
                    && !"FAILED".equals(stage.state()))) return;
            terminalEntered.countDown();
            try {
                if (!releaseTerminal.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("terminal monitor test release timed out");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
        }

        @Override public void onJobComplete(String jobId, String state, String error) {
            jobCompleted.countDown();
        }

        @Override public java.util.List<JobView> jobs() {
            return Collections.emptyList();
        }

        @Override public JobView job(String jobId) { return null; }

        @Override public java.util.List<WorkerView> workers() {
            return Collections.emptyList();
        }
    }

    private static PlanGraph emptyGraph() {
        DataSchema meta = new DataSchema(new Field("v", DataType.IntegerType));
        return new PlanGraph(Arrays.asList(
                PlanNode.source(meta, Collections.emptyList()),
                PlanNode.sink(meta, 0, new DiscardSink(meta)).build()));
    }
}
