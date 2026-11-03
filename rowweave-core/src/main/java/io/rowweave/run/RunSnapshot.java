package io.rowweave.run;

/** Bounded, immutable Run snapshot from which scheduling decisions are made. */
public final class RunSnapshot {
    private final String runId;
    private final String definitionId;
    private final String artifactId;
    private final int maxAttempts;
    private final int attemptCount;
    private final long revision;
    private final RunStatus status;
    private final long deadlineEpochMillis;
    private final long createdAtMillis;
    private final long updatedAtMillis;
    private final AttemptSnapshot currentAttempt;

    RunSnapshot(String runId, String definitionId, String artifactId,
                int maxAttempts, int attemptCount, long revision, RunStatus status,
                long deadlineEpochMillis, long createdAtMillis, long updatedAtMillis,
                AttemptSnapshot currentAttempt) {
        this.runId = runId;
        this.definitionId = definitionId;
        this.artifactId = artifactId;
        this.maxAttempts = maxAttempts;
        this.attemptCount = attemptCount;
        this.revision = revision;
        this.status = status;
        this.deadlineEpochMillis = deadlineEpochMillis;
        this.createdAtMillis = createdAtMillis;
        this.updatedAtMillis = updatedAtMillis;
        this.currentAttempt = currentAttempt;
    }

    RunSnapshot next(RunStatus nextStatus, int nextAttemptCount,
                     AttemptSnapshot nextAttempt, long eventTime) {
        return new RunSnapshot(runId, definitionId, artifactId, maxAttempts,
                nextAttemptCount, revision + 1L, nextStatus, deadlineEpochMillis,
                createdAtMillis, eventTime, nextAttempt);
    }

    public String runId() { return runId; }
    public String definitionId() { return definitionId; }
    public String artifactId() { return artifactId; }
    public int maxAttempts() { return maxAttempts; }
    public int attemptCount() { return attemptCount; }
    public long revision() { return revision; }
    public RunStatus status() { return status; }
    public long deadlineEpochMillis() { return deadlineEpochMillis; }
    public long createdAtMillis() { return createdAtMillis; }
    public long updatedAtMillis() { return updatedAtMillis; }
    public AttemptSnapshot currentAttempt() { return currentAttempt; }
    public RunResultSummary resultSummary() {
        return currentAttempt == null ? null : currentAttempt.resultSummary();
    }
}
