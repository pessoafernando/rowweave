package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcServer;
import io.rowweave.run.RunAttemptFence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerReportRunAttemptFenceTest {

    @Test
    void portProgressAndCancelledTerminalReportsKeepAssignmentFence(@TempDir Path tmp) throws Exception {
        ConcurrentLinkedQueue<RpcMessage> received = new ConcurrentLinkedQueue<>();
        try (RpcServer master = new RpcServer(message -> {
            received.add(message);
            if (message.type() == RpcMessage.Type.PROGRESS_REPORT
                    || message.type() == RpcMessage.Type.HEARTBEAT
                    || message.type() == RpcMessage.Type.DEREGISTER_WORKER) {
                return null;
            }
            return RpcMessage.ack();
        });
             WorkerServer worker = WorkerServer.newUnguarded(
                     "report-worker", 1, "127.0.0.1", master.port(), tmp.toFile())) {
            RunAttemptFence fence = new RunAttemptFence("run-report", "attempt-1", 7L);
            long stageId = 92_001L;
            DataSchema meta = new DataSchema(new Field("v", DataType.IntegerType));
            StageCommand command = StageCommand.consumer(meta, 64, new DiscardSink(meta))
                    .withRunAttemptFence(fence);

            assertEquals(RpcMessage.Type.ACK, worker.handler().apply(
                    RpcMessage.assignStageInProcess(
                            "report-worker", stageId, command, worker.incarnationId(), fence)).type());
            awaitReports(received, stageId,
                    EnumSet.of(RpcMessage.Type.PORT_REPORT, RpcMessage.Type.PROGRESS_REPORT));
            assertEquals(RpcMessage.Type.ACK, worker.handler().apply(
                    RpcMessage.cancelStage(
                            "report-worker", stageId, worker.incarnationId(), fence)).type());
            awaitReports(received, stageId, EnumSet.of(RpcMessage.Type.STAGE_REPORT));

            Set<RpcMessage.Type> reportTypes = EnumSet.noneOf(RpcMessage.Type.class);
            for (RpcMessage report : received) {
                if (report.stageId() != stageId) continue;
                reportTypes.add(report.type());
                assertEquals(fence, report.runAttemptFence(), report.type().name());
            }
            assertTrue(reportTypes.containsAll(EnumSet.of(
                    RpcMessage.Type.PORT_REPORT,
                    RpcMessage.Type.PROGRESS_REPORT,
                    RpcMessage.Type.STAGE_REPORT)), reportTypes.toString());
        }
    }

    private static void awaitReports(
            ConcurrentLinkedQueue<RpcMessage> received, long stageId,
            Set<RpcMessage.Type> expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline) {
            Set<RpcMessage.Type> actual = EnumSet.noneOf(RpcMessage.Type.class);
            for (RpcMessage message : received) {
                if (message.stageId() == stageId) actual.add(message.type());
            }
            if (actual.containsAll(expected)) return;
            Thread.sleep(10L);
        }
        Set<RpcMessage.Type> actual = EnumSet.noneOf(RpcMessage.Type.class);
        for (RpcMessage message : received) {
            if (message.stageId() == stageId) actual.add(message.type());
        }
        throw new AssertionError("timed out waiting for reports " + expected + "; actual=" + actual);
    }
}
