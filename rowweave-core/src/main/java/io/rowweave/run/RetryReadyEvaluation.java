package io.rowweave.run;

/** Read-only backend-time view of a Run waiting for a bounded retry. */
public final class RetryReadyEvaluation {
    public enum Code {
        DUE,
        NOT_DUE,
        NOT_RETRY_WAIT,
        TOO_LATE,
        STALE_RECOVERY_OWNER,
        NOT_FOUND,
        RETIRING,
        UNAVAILABLE
    }

    private final Code code;
    private final RunSnapshot snapshot;
    private final RetryAttemptIdentity identity;
    private final long observedAtMillis;
    private final long notBeforeMillis;
    private final int remainingAttempts;

    RetryReadyEvaluation(Code code, RunSnapshot snapshot,
                         RetryAttemptIdentity identity,
                         long observedAtMillis, long notBeforeMillis,
                         int remainingAttempts) {
        if (code == null) throw new IllegalArgumentException("code is required");
        if (observedAtMillis < 0L || notBeforeMillis < 0L
                || remainingAttempts < 0) {
            throw new IllegalArgumentException("retry timing fields are invalid");
        }
        this.code = code;
        this.snapshot = snapshot;
        this.identity = identity;
        this.observedAtMillis = observedAtMillis;
        this.notBeforeMillis = notBeforeMillis;
        this.remainingAttempts = remainingAttempts;
    }

    public Code code() { return code; }
    public RunSnapshot snapshot() { return snapshot; }
    public RetryAttemptIdentity identity() { return identity; }
    public long observedAtMillis() { return observedAtMillis; }
    public long notBeforeMillis() { return notBeforeMillis; }
    public int remainingAttempts() { return remainingAttempts; }
}
