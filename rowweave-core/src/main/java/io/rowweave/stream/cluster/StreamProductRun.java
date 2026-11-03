package io.rowweave.stream.cluster;

import io.rowweave.StreamSavepointRequest;
import io.rowweave.stream.api.StreamCdcPhase;
import io.rowweave.stream.api.StreamCommandId;

/** Master-owned lifecycle control for one already-admitted cluster run. */
public interface StreamProductRun extends AutoCloseable {
    void start();
    void cancel();
    default void triggerCheckpoint(
            StreamCommandId commandId, String checkpointId) {
        throw new UnsupportedOperationException(
                "checkpoint data plane is unavailable");
    }
    default boolean ownsCheckpointDeadline() { return false; }
    default long assignmentGeneration() { return 1L; }
    default StreamProductRun liveRescale(
            StreamCommandId commandId, String rescaleId,
            int targetParallelism, long expectedSourceGeneration,
            long expectedTargetGeneration) {
        throw new UnsupportedOperationException(
                "live rescale data plane is unavailable");
    }
    default void stopWithSavepoint(
            StreamCommandId commandId, String savepointId,
            StreamSavepointRequest request) {
        throw new UnsupportedOperationException(
                "savepoint data plane is unavailable");
    }
    /** Recovers an already durable savepoint cut without replaying its request. */
    default boolean recoverSavepoint(
            StreamCommandId commandId, String savepointId) {
        return false;
    }
    default Throwable failure() { return null; }
    /**
     * Detects failure without acquiring a monitor owned by an active checkpoint.
     * Runs without such a probe return unknown, not a healthy-run assertion.
     */
    default Throwable failureDuringCheckpoint() { return null; }
    /** True only after every task of a fully bounded run ended normally. */
    default boolean finished() { return false; }
    /** True when bounded Sources ended and a transactional terminal cut is due. */
    default boolean terminalCheckpointReady() { return false; }
    default StreamCdcPhase cdcPhase() {
        return StreamCdcPhase.NOT_APPLICABLE;
    }
    /** Advisory observation; null means this run cannot report progress. */
    default StreamRunProgressSnapshot progressSnapshot() { return null; }
    /** Latest canonical checkpoint observation, only after durable completion. */
    default StreamCompletedCheckpointEvidence completedCheckpointEvidence() {
        return null;
    }
    @Override void close();
}
