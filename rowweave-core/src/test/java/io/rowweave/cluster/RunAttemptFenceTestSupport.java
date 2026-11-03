package io.rowweave.cluster;

import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.run.RunAttemptFence;

/** Shared authority for direct Worker tests; production assignments obtain their fence from Master. */
final class RunAttemptFenceTestSupport {
    static final RunAttemptFence FENCE =
            new RunAttemptFence("test-run", "attempt-1", 1L);

    private RunAttemptFenceTestSupport() {
    }

    static StageCommand bind(StageCommand command) {
        return command.withRunAttemptFence(FENCE);
    }

    static RpcMessage inProcess(
            String workerId, long stageId, StageCommand command, String incarnationId) {
        return RpcMessage.assignStageInProcess(
                workerId, stageId, bind(command), incarnationId, FENCE);
    }

    static RpcMessage inProcess(
            String workerId, long stageId, StageCommand command,
            Endpoint leader, long termToken, String incarnationId) {
        return RpcMessage.assignStageInProcess(
                workerId, stageId, bind(command), leader, termToken, incarnationId, FENCE);
    }

    static RpcMessage socket(
            String workerId, long stageId, byte[] command, String incarnationId) {
        return RpcMessage.assignStage(workerId, stageId, command, incarnationId, FENCE);
    }

    static RpcMessage socket(
            String workerId, long stageId, byte[] command,
            Endpoint leader, long termToken, String incarnationId) {
        return RpcMessage.assignStage(
                workerId, stageId, command, leader, termToken, incarnationId, FENCE);
    }
}
