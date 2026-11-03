package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.RunAttemptFence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerAssignmentRunAttemptFenceTest {

    @Test
    void rejectsMalformedOrMismatchedFenceBeforeAckAndSlotRegistration(@TempDir Path tmp) {
        RunAttemptFence current = new RunAttemptFence("run", "attempt-1", 1L);
        RunAttemptFence stale = new RunAttemptFence("run", "attempt-0", 2L);
        StageCommand currentCommand = StageCommand.wholeGraph(emptyGraph()).withRunAttemptFence(current);
        StageCommand legacyCommand = StageCommand.wholeGraph(emptyGraph());

        try (MasterServer master = new MasterServer(60_000L);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "fenced-worker", 2, "127.0.0.1", master.port(), tmp.toFile())) {
            assertRejected(worker.handler().apply(RpcMessage.assignStageInProcess(
                    "fenced-worker", 91_001L, currentCommand, worker.incarnationId(), stale)), "mismatch");
            assertRejected(worker.handler().apply(RpcMessage.assignStageInProcess(
                    "fenced-worker", 91_002L, legacyCommand, worker.incarnationId(), current)), "missing");
            assertRejected(worker.handler().apply(RpcMessage.assignStageInProcess(
                    "fenced-worker", 91_003L, currentCommand, worker.incarnationId())), "missing");
            assertRejected(worker.handler().apply(RpcMessage.assignStage(
                    "fenced-worker", 91_004L, new byte[] { 1, 2, 3 },
                    worker.incarnationId(), current)), "command");
            byte[] oldUid = master.serialize(currentCommand);
            oldUid[oldUid.length - 1] ^= 1;
            assertRejected(worker.handler().apply(RpcMessage.assignStage(
                    "fenced-worker", 91_005L, oldUid,
                    worker.incarnationId(), current)), "command");

            assertEquals(0, worker.runningStageCount(), "the rejection must happen before any ledger or active slot is taken");
            assertEquals(2L, worker.freeSlots());
        }
    }

    private static void assertRejected(RpcMessage response, String diagnostic) {
        assertEquals(RpcMessage.Type.JOB_RESULT, response.type());
        assertFalse(response.success());
        assertTrue(response.errorMessage().toLowerCase(java.util.Locale.ROOT).contains(diagnostic),
                response.errorMessage());
    }

    private static PlanGraph emptyGraph() {
        DataSchema meta = new DataSchema(new Field("v", DataType.IntegerType));
        return new PlanGraph(Arrays.asList(
                PlanNode.source(meta, Collections.emptyList()),
                PlanNode.sink(meta, 0, new DiscardSink(meta)).build()));
    }

}
