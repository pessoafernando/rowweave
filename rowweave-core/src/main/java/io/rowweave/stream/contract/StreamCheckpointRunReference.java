package io.rowweave.stream.contract;

/** Pure Run-level checkpoint sequencing model for 1 safety exploration. */
public final class StreamCheckpointRunReference {
    private final String runId;
    private final String attemptId;
    private final long fenceToken;
    private long lastCheckpointSequence;
    private CheckpointAuthorityReference active;
    private CheckpointDecision latestRestorePoint;

    public StreamCheckpointRunReference(
            String runId, String attemptId, long fenceToken) {
        this.runId = StreamContractSupport.text(runId,
                "checkpoint.run.runId", 256);
        this.attemptId = StreamContractSupport.text(attemptId,
                "checkpoint.run.attemptId", 256);
        if (fenceToken <= 0L) {
            throw StreamContractSupport.failure("INVALID_CHECKPOINT_RUN",
                    "checkpoint.run.fenceToken", "positive fence token is required");
        }
        this.fenceToken = fenceToken;
    }

    public synchronized void trigger(CheckpointExpectation expectation) {
        if (expectation == null || !sameAttempt(expectation.checkpointIdentity())) {
            throw failure("STALE_CHECKPOINT", "checkpoint.identity",
                    "checkpoint does not belong to the active Run attempt");
        }
        long sequence = expectation.checkpointIdentity().checkpointSequence();
        if (sequence <= lastCheckpointSequence) {
            throw failure("CHECKPOINT_SEQUENCE_REPLAY", "checkpoint.sequence",
                    "checkpoint sequence must strictly increase and never be reused");
        }
        if (active != null && !isTerminal(active.snapshot().status())) {
            throw failure("CHECKPOINT_IN_FLIGHT", "checkpoint.active",
                    "Stream allows one checkpoint in flight");
        }
        active = new CheckpointAuthorityReference(expectation);
        lastCheckpointSequence = sequence;
    }

    public synchronized void acknowledge(CheckpointTaskReceipt receipt) {
        active().acknowledge(receipt);
    }

    public synchronized CheckpointDecision decideCommit() {
        return active().decideCommit();
    }

    public synchronized CheckpointDecision abort() {
        return active().abort();
    }

    public synchronized void complete(CheckpointDecision commitDecision) {
        CheckpointAuthorityReference current = active();
        current.complete(commitDecision);
        latestRestorePoint = current.snapshot();
    }

    public synchronized CheckpointDecision latestRestorePoint() {
        return latestRestorePoint;
    }

    private CheckpointAuthorityReference active() {
        if (active == null) {
            throw failure("NO_ACTIVE_CHECKPOINT", "checkpoint.active",
                    "checkpoint operation requires an active checkpoint");
        }
        return active;
    }

    private boolean sameAttempt(CheckpointIdentity checkpoint) {
        return runId.equals(checkpoint.runId())
                && attemptId.equals(checkpoint.attemptId())
                && fenceToken == checkpoint.fenceToken();
    }

    private static boolean isTerminal(CheckpointDecision.Status status) {
        return status == CheckpointDecision.Status.COMPLETED
                || status == CheckpointDecision.Status.ABORTED;
    }

    private static StreamContractException failure(
            String code, String path, String message) {
        return StreamContractSupport.failure(code, path, message);
    }
}
