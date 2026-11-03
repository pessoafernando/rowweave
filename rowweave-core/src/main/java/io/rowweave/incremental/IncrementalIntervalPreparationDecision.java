package io.rowweave.incremental;

import io.rowweave.run.RunSnapshot;

/** Result of capture/materialization/pending publication/joint claim, before dispatch. */
public final class IncrementalIntervalPreparationDecision {
    public enum Code {
        CLAIMED,
        NO_OP,
        BACKFILL_REPLAY,
        STALE_BACKFILL,
        LOST_RACE
    }

    private final Code code;
    private final String artifactId;
    private final IncrementalClaimRequest claimRequest;
    private final IncrementalAuthorityRecord record;
    private final RunSnapshot runSnapshot;

    private IncrementalIntervalPreparationDecision(
            Code code, String artifactId,
            IncrementalClaimRequest claimRequest,
            IncrementalAuthorityRecord record, RunSnapshot runSnapshot) {
        this.code = code;
        this.artifactId = artifactId;
        this.claimRequest = claimRequest;
        this.record = record;
        this.runSnapshot = runSnapshot;
    }

    static IncrementalIntervalPreparationDecision claimed(
            String artifactId, IncrementalClaimRequest request,
            IncrementalAuthorityDecision decision) {
        return new IncrementalIntervalPreparationDecision(
                Code.CLAIMED, artifactId, request,
                decision.record(), decision.runSnapshot());
    }

    static IncrementalIntervalPreparationDecision noOp(
            IncrementalAuthorityRecord record) {
        return new IncrementalIntervalPreparationDecision(
                Code.NO_OP, null, null, record, null);
    }

    static IncrementalIntervalPreparationDecision backfillReplay(
            IncrementalAuthorityRecord record) {
        return new IncrementalIntervalPreparationDecision(
                Code.BACKFILL_REPLAY, null, null, record, null);
    }

    static IncrementalIntervalPreparationDecision staleBackfill(
            IncrementalAuthorityRecord record) {
        return new IncrementalIntervalPreparationDecision(
                Code.STALE_BACKFILL, null, null, record, null);
    }

    static IncrementalIntervalPreparationDecision lostRace(
            String artifactId, IncrementalClaimRequest request,
            IncrementalAuthorityRecord record, RunSnapshot runSnapshot) {
        return new IncrementalIntervalPreparationDecision(
                Code.LOST_RACE, artifactId, request, record, runSnapshot);
    }

    public Code code() { return code; }
    public String artifactId() { return artifactId; }
    public IncrementalClaimRequest claimRequest() { return claimRequest; }
    public IncrementalAuthorityRecord record() { return record; }
    public RunSnapshot runSnapshot() { return runSnapshot; }
}
