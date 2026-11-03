package io.rowweave.run;

/** Atomic backend-time claim of one due successor attempt. */
public final class RetryAttemptClaim {
    public enum Code {
        CLAIMED,
        ALREADY_CLAIMED,
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

    RetryAttemptClaim(Code code, RunSnapshot snapshot,
                      RetryAttemptIdentity identity,
                      long observedAtMillis, long notBeforeMillis) {
        if (code == null) throw new IllegalArgumentException("code is required");
        if (observedAtMillis < 0L || notBeforeMillis < 0L) {
            throw new IllegalArgumentException("retry timing fields are invalid");
        }
        this.code = code;
        this.snapshot = snapshot;
        this.identity = identity;
        this.observedAtMillis = observedAtMillis;
        this.notBeforeMillis = notBeforeMillis;
    }

    public Code code() { return code; }
    public RunSnapshot snapshot() { return snapshot; }
    public RetryAttemptIdentity identity() { return identity; }
    public long observedAtMillis() { return observedAtMillis; }
    public long notBeforeMillis() { return notBeforeMillis; }
}
