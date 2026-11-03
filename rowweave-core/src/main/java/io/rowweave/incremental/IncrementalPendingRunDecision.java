package io.rowweave.incremental;

import io.rowweave.run.RunSnapshot;

/** Result of publishing an exact, non-dispatchable incremental pending Run binding. */
public final class IncrementalPendingRunDecision {
    public enum Code {
        PUBLISHED,
        ALREADY_PUBLISHED,
        NO_OP,
        FEED_NOT_FOUND,
        STALE_OWNERSHIP,
        ACTIVE_OR_PENDING_EXISTS,
        PENDING_CONFLICT,
        CONTRACT_CONFLICT,
        RUN_CONFLICT
    }

    private final Code code;
    private final RunSnapshot runSnapshot;

    public IncrementalPendingRunDecision(Code code, RunSnapshot runSnapshot) {
        if (code == null) throw new IllegalArgumentException("pending decision code is required");
        this.code = code;
        this.runSnapshot = runSnapshot;
    }

    public Code code() { return code; }
    public RunSnapshot runSnapshot() { return runSnapshot; }
}
