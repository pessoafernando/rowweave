package io.rowweave.run;

/** Immutable view of the latest Attempt; run ExecutionEvent pages for the full history. */
public final class AttemptSnapshot {
    private final int attemptNumber;
    private final String attemptId;
    private final long fenceToken;
    private final String publicationId;
    private final AttemptStatus status;
    private final CancellationCause cancellationCause;
    private final long createdAtMillis;
    private final long updatedAtMillis;
    private final String reasonCode;
    private final String message;
    private final RunResultSummary resultSummary;

    AttemptSnapshot(int attemptNumber, String attemptId, long fenceToken, String publicationId,
                    AttemptStatus status, CancellationCause cancellationCause,
                    long createdAtMillis, long updatedAtMillis, String reasonCode, String message) {
        this.attemptNumber = attemptNumber;
        this.attemptId = attemptId;
        this.fenceToken = fenceToken;
        this.publicationId = publicationId;
        this.status = status;
        this.cancellationCause = cancellationCause;
        this.createdAtMillis = createdAtMillis;
        this.updatedAtMillis = updatedAtMillis;
        this.reasonCode = reasonCode;
        this.message = message;
        this.resultSummary = null;
    }

    AttemptSnapshot(int attemptNumber, String attemptId, long fenceToken, String publicationId,
                    AttemptStatus status, CancellationCause cancellationCause,
                    long createdAtMillis, long updatedAtMillis, String reasonCode, String message,
                    RunResultSummary resultSummary) {
        this.attemptNumber = attemptNumber;
        this.attemptId = attemptId;
        this.fenceToken = fenceToken;
        this.publicationId = publicationId;
        this.status = status;
        this.cancellationCause = cancellationCause;
        this.createdAtMillis = createdAtMillis;
        this.updatedAtMillis = updatedAtMillis;
        this.reasonCode = reasonCode;
        this.message = message;
        this.resultSummary = resultSummary;
    }

    AttemptSnapshot transition(AttemptStatus next, CancellationCause cause, long eventTime,
                               String nextReasonCode, String nextMessage) {
        return new AttemptSnapshot(attemptNumber, attemptId, fenceToken, publicationId,
                next, cause, createdAtMillis, eventTime, nextReasonCode, nextMessage, null);
    }

    AttemptSnapshot transition(AttemptStatus next, CancellationCause cause, long eventTime,
                               String nextReasonCode, String nextMessage,
                               RunResultSummary nextResultSummary) {
        return new AttemptSnapshot(attemptNumber, attemptId, fenceToken, publicationId,
                next, cause, createdAtMillis, eventTime, nextReasonCode, nextMessage,
                nextResultSummary);
    }

    public int attemptNumber() { return attemptNumber; }
    public String attemptId() { return attemptId; }
    public long fenceToken() { return fenceToken; }
    public String publicationId() { return publicationId; }
    public AttemptStatus status() { return status; }
    public CancellationCause cancellationCause() { return cancellationCause; }
    public long createdAtMillis() { return createdAtMillis; }
    public long updatedAtMillis() { return updatedAtMillis; }
    public String reasonCode() { return reasonCode; }
    public String message() { return message; }
    public RunResultSummary resultSummary() { return resultSummary; }
}
