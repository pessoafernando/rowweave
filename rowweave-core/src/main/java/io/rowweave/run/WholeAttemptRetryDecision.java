package io.rowweave.run;

/** Result of atomically closing one attempt for retry or final exhaustion. */
public final class WholeAttemptRetryDecision {
    public enum Code {
        SCHEDULED,
        ALREADY_SCHEDULED,
        EXHAUSTED,
        NOT_RETRYABLE,
        CLOSURE_PENDING,
        TOO_LATE,
        STALE_RUN,
        STALE_RECOVERY_OWNER,
        NOT_FOUND,
        RETIRING,
        UNAVAILABLE
    }

    private final Code code;
    private final RunSnapshot snapshot;
    private final WholeAttemptRetryClass retryClass;
    private final long observedAtMillis;

    WholeAttemptRetryDecision(Code code, RunSnapshot snapshot,
                              WholeAttemptRetryClass retryClass,
                              long observedAtMillis) {
        if (code == null) throw new IllegalArgumentException("code is required");
        if (observedAtMillis < 0L) {
            throw new IllegalArgumentException("observedAtMillis must be >= 0");
        }
        this.code = code;
        this.snapshot = snapshot;
        this.retryClass = retryClass;
        this.observedAtMillis = observedAtMillis;
    }

    public Code code() { return code; }
    public RunSnapshot snapshot() { return snapshot; }
    public WholeAttemptRetryClass retryClass() { return retryClass; }
    public long observedAtMillis() { return observedAtMillis; }
}
