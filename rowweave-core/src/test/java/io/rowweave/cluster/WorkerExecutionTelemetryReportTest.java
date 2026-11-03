package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcServer;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.StageExecutionTelemetry;
import io.rowweave.plan.StageExecutionTelemetryCodec;
import io.rowweave.plan.TelemetryCoverage;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.sched.LocalShuffleRegistry;
import io.rowweave.sched.MultiProducerChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class WorkerExecutionTelemetryReportTest {

    @Test
    void successfulWorkerStageReportsFrozenCompleteZeroTelemetry(@TempDir Path temp)
            throws Exception {
        ConcurrentLinkedQueue<RpcMessage> received = new ConcurrentLinkedQueue<>();
        try (RpcServer master = new RpcServer(message -> {
            received.add(message);
            return message.type() == RpcMessage.Type.HEARTBEAT ? null : RpcMessage.ack();
        }); WorkerServer worker = WorkerServer.newUnguarded(
                "telemetry-worker", 1, "127.0.0.1", master.port(), temp.toFile())) {
            RunAttemptFence fence = new RunAttemptFence("run-worker", "attempt-1", 1L);
            StageCommand command = StageCommand.wholeGraph(emptyGraph())
                    .withRunAttemptFence(fence);
            assertEquals(RpcMessage.Type.ACK, worker.handler().apply(
                    RpcMessage.assignStageInProcess("telemetry-worker", 91L, command,
                            worker.incarnationId(), fence)).type());

            RpcMessage report = awaitStageReport(received, 91L);
            assertNotNull(report.executionTelemetryBytes());
            StageExecutionTelemetry telemetry = StageExecutionTelemetryCodec.decode(
                    report.executionTelemetryBytes());
            assertEquals(TelemetryCoverage.COMPLETE, telemetry.coverage());
            assertEquals(0L, telemetry.managedPeakReservedBytes());
            assertEquals(0L, telemetry.spillRunCount());
            assertEquals(0, assignments(worker).frozenTelemetryCount(),
                    "ACKed ephemeral terminal telemetry must not remain in the ledger");
        }
    }

    @Test
    void eosThenRejectedPreparedRegistrationKeepsExecutedTelemetry(@TempDir Path temp)
            throws Exception {
        ConcurrentLinkedQueue<RpcMessage> received = new ConcurrentLinkedQueue<>();
        try (RpcServer master = new RpcServer(message -> {
            received.add(message);
            return message.type() == RpcMessage.Type.HEARTBEAT ? null : RpcMessage.ack();
        }); WorkerServer worker = WorkerServer.newUnguarded(
                "late-register-worker", 1, "127.0.0.1", master.port(), temp.toFile())) {
            RunAttemptFence fence = new RunAttemptFence("run-late", "attempt-1", 1L);
            long stageId = 92L;
            String groupId = "late-register-group";
            assertEquals(RpcMessage.Type.ACK, worker.handler().apply(
                    RpcMessage.rollbackPreparedOutput(
                            "late-register-worker", stageId, groupId,
                            worker.incarnationId(), fence)).type());
            DataSchema meta = new DataSchema(new Field("v", DataType.IntegerType));
            PlanGraph identity = new PlanGraph(Collections.singletonList(
                    PlanNode.source(meta, Collections.emptyList())));
            String localKey = "late-register-empty";
            StageCommand command = StageCommand.sink(
                            meta, 16, 1, identity, new PreparedDiscardOutput())
                    .withLocalInbound(1, localKey)
                    .withCommitGroupId(groupId)
                    .withRunAttemptFence(fence);

            assertEquals(RpcMessage.Type.ACK, worker.handler().apply(
                    RpcMessage.assignStageInProcess(
                            "late-register-worker", stageId, command,
                            worker.incarnationId(), fence)).type());
            LocalShuffleRegistry local = localShuffle(worker);
            MultiProducerChannel input = (MultiProducerChannel) local.await(localKey, 2_000L);
            input.producerDone();

            RpcMessage report = awaitStageReport(received, stageId);
            assertFalse(report.success(), "tombstone must still reject the late prepared handoff");
            assertEquals(TelemetryCoverage.COMPLETE,
                    report.stageExecutionTelemetry().coverage(),
                    "EOS execution facts must survive the later registration failure");
        }
    }

    private static RpcMessage awaitStageReport(
            ConcurrentLinkedQueue<RpcMessage> received, long stageId) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline) {
            for (RpcMessage message : received) {
                if (message.type() == RpcMessage.Type.STAGE_REPORT
                        && message.stageId() == stageId) return message;
            }
            Thread.sleep(10L);
        }
        throw new AssertionError("timed out waiting for stage report");
    }

    private static PlanGraph emptyGraph() {
        DataSchema meta = new DataSchema(new Field("v", DataType.IntegerType));
        return new PlanGraph(Arrays.asList(
                PlanNode.source(meta, Collections.emptyList()),
                PlanNode.sink(meta, 0, new DiscardSink(meta)).build()));
    }

    private static LocalShuffleRegistry localShuffle(WorkerServer worker) throws Exception {
        java.lang.reflect.Field field = WorkerServer.class.getDeclaredField("localShuffle");
        field.setAccessible(true);
        return (LocalShuffleRegistry) field.get(worker);
    }

    private static WorkerAssignmentLedger assignments(WorkerServer worker) throws Exception {
        java.lang.reflect.Field field = WorkerServer.class.getDeclaredField("assignments");
        field.setAccessible(true);
        return (WorkerAssignmentLedger) field.get(worker);
    }

    private static final class PreparedDiscardOutput implements Sink {
        private static final long serialVersionUID = 1L;
        @Override public DataSchema getSchema() {
            return new DataSchema(new Field("v", DataType.IntegerType));
        }
        @Override public void collect(DataRow row) { }
        @Override public boolean supportsCoordinatedCommit() { return true; }
        @Override public void prepareCommit() { }
        @Override public void commitPrepared() { }
        @Override public void rollbackCommit() { }
        @Override public void finalizeCommit() { }
        @Override public boolean isWorkerWritable() { return true; }
    }
}
