package io.rowweave.incremental;

import io.rowweave.run.RunSnapshot;

/** Closed result of one in-memory frontier/Run authority mutation. */
public final class IncrementalAuthorityDecision {
    public enum Code {
        ACCEPTED,
        ALREADY_APPLIED,
        FEED_NOT_FOUND,
        FEED_CONFLICT,
        GUARD_SHARD_FULL,
        STALE_REVISION,
        STALE_OWNERSHIP,
        COMMAND_ID_CONFLICT,
        ACTIVE_EXISTS,
        PENDING_NOT_FOUND,
        PENDING_CONFLICT,
        RUN_CONFLICT,
        CONTRACT_CONFLICT
    }

    private final Code code;
    private final IncrementalAuthorityRecord record;
    private final RunSnapshot runSnapshot;

    private IncrementalAuthorityDecision(
            Code code, IncrementalAuthorityRecord record, RunSnapshot runSnapshot) {
        if (code == null) throw new IllegalArgumentException("decision code is required");
        this.code = code;
        this.record = record;
        this.runSnapshot = runSnapshot;
    }

    public static IncrementalAuthorityDecision accepted(
            IncrementalAuthorityRecord record, RunSnapshot runSnapshot) {
        return new IncrementalAuthorityDecision(Code.ACCEPTED, record, runSnapshot);
    }

    public static IncrementalAuthorityDecision replayed(
            IncrementalAuthorityRecord record, RunSnapshot runSnapshot) {
        return new IncrementalAuthorityDecision(Code.ALREADY_APPLIED, record, runSnapshot);
    }

    public static IncrementalAuthorityDecision rejected(
            Code code, IncrementalAuthorityRecord record, RunSnapshot runSnapshot) {
        if (code == Code.ACCEPTED || code == Code.ALREADY_APPLIED) {
            throw new IllegalArgumentException("rejection code is not rejected");
        }
        return new IncrementalAuthorityDecision(code, record, runSnapshot);
    }

    public Code code() { return code; }
    public IncrementalAuthorityRecord record() { return record; }
    public RunSnapshot runSnapshot() { return runSnapshot; }
}
