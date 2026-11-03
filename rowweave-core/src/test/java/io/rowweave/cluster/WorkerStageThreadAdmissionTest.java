package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** CAP-005 must reject before StageTask and its data-plane resources exist. */
class WorkerStageThreadAdmissionTest {

    @Test
    void rejectsBeforeStageTaskPortOrProgressSideEffects(@TempDir Path temporary) {
        ConcurrentLinkedQueue<RpcMessage> masterMessages = new ConcurrentLinkedQueue<>();
        try (RpcServer master = new RpcServer(message -> {
            masterMessages.add(message);
            return message.type() == RpcMessage.Type.HEARTBEAT ? null : RpcMessage.ack();
        });
             WorkerServer worker = WorkerServer.newUnguarded(
                     "thread-gate-worker", 1, "127.0.0.1", master.port(),
                     temporary.toFile(), 2)) {
            DataSchema meta = new DataSchema(new Field("v", DataType.IntegerType));
            StageCommand command = StageCommand.consumer(
                    meta, 64, new DiscardSink(meta));

            RpcMessage response = worker.handler().apply(
                    RunAttemptFenceTestSupport.inProcess(
                            "thread-gate-worker", 73_001L, command,
                            worker.incarnationId()));

            assertEquals(RpcMessage.Type.JOB_RESULT, response.type());
            assertTrue(response.errorMessage().contains("ROWWEAVE-CAP-005"),
                    response.errorMessage());
            assertEquals(0, worker.runningStageCount());
            assertFalse(masterMessages.stream().anyMatch(message ->
                    message.stageId() == 73_001L
                            && (message.type() == RpcMessage.Type.PORT_REPORT
                            || message.type() == RpcMessage.Type.PROGRESS_REPORT
                            || message.type() == RpcMessage.Type.STAGE_REPORT)));
        }
    }
}
