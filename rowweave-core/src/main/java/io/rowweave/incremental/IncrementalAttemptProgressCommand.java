package io.rowweave.incremental;

import io.rowweave.run.AttemptStatus;
import io.rowweave.run.RunAttemptFence;

/** Narrow nonterminal Run progress request bound to the current active interval. */
public final class IncrementalAttemptProgressCommand {
    private final String feedId;
    private final String intervalId;
    private final long expectedFrontierRevision;
    private final long expectedRunRevision;
    private final RunAttemptFence fence;
    private final AttemptStatus targetStatus;
    private final String commandId;
    private final long eventTimeMillis;

    public IncrementalAttemptProgressCommand(
            String feedId, String intervalId, long expectedFrontierRevision,
            long expectedRunRevision, RunAttemptFence fence,
            AttemptStatus targetStatus, String commandId, long eventTimeMillis) {
        if (expectedFrontierRevision < 0L || expectedRunRevision <= 0L
                || eventTimeMillis < 0L || fence == null) {
            throw new IllegalArgumentException("progress revision, fence or time is invalid");
        }
        if (targetStatus != AttemptStatus.DISPATCHING
                && targetStatus != AttemptStatus.RUNNING
                && targetStatus != AttemptStatus.COMMITTING) {
            throw new IllegalArgumentException(
                    "incremental progress cannot perform control or terminal transitions");
        }
        this.feedId = IncrementalAuthorityValue.text(feedId, "progress feed id");
        this.intervalId = IncrementalAuthorityValue.hexDigest(
                intervalId, "progress interval id");
        this.expectedFrontierRevision = expectedFrontierRevision;
        this.expectedRunRevision = expectedRunRevision;
        this.fence = fence;
        this.targetStatus = targetStatus;
        this.commandId = IncrementalAuthorityValue.text(
                commandId, "progress command id");
        this.eventTimeMillis = eventTimeMillis;
    }

    public String feedId() { return feedId; }
    public String intervalId() { return intervalId; }
    public long expectedFrontierRevision() { return expectedFrontierRevision; }
    public long expectedRunRevision() { return expectedRunRevision; }
    public RunAttemptFence fence() { return fence; }
    public AttemptStatus targetStatus() { return targetStatus; }
    public String commandId() { return commandId; }
    public long eventTimeMillis() { return eventTimeMillis; }
}
