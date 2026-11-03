package io.rowweave.run;

/** Identity and policy of one Artifact Run submission that can be safely replayed. */
public final class RunSubmission {
    private final String runId;
    private final String attemptId;
    private final String publicationId;
    private final int maxAttempts;
    private final long deadlineEpochMillis;
    private final long submittedAtMillis;

    public RunSubmission(String runId, String attemptId, String publicationId,
                         int maxAttempts, long deadlineEpochMillis,
                         long submittedAtMillis) {
        this.runId = RunText.required(runId, "submission.runId", RunText.MAX_ID_BYTES);
        this.attemptId = RunText.required(
                attemptId, "submission.attemptId", RunText.MAX_ID_BYTES);
        this.publicationId = RunText.required(
                publicationId, "submission.publicationId", RunText.MAX_ID_BYTES);
        if (maxAttempts < 1 || maxAttempts > 1000) {
            throw new IllegalArgumentException("maxAttempts must be in [1,1000]");
        }
        if (submittedAtMillis < 0L) {
            throw new IllegalArgumentException("submittedAtMillis must be >= 0");
        }
        if (deadlineEpochMillis < 0L
                || (deadlineEpochMillis != 0L && deadlineEpochMillis < submittedAtMillis)) {
            throw new IllegalArgumentException(
                    "deadline must be zero or >= submittedAtMillis");
        }
        this.maxAttempts = maxAttempts;
        this.deadlineEpochMillis = deadlineEpochMillis;
        this.submittedAtMillis = submittedAtMillis;
    }

    public String runId() { return runId; }
    public String attemptId() { return attemptId; }
    public String publicationId() { return publicationId; }
    public int maxAttempts() { return maxAttempts; }
    public long deadlineEpochMillis() { return deadlineEpochMillis; }
    public long submittedAtMillis() { return submittedAtMillis; }
}
