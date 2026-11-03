package io.rowweave.incremental;

import io.rowweave.plan.ExecutionReport;
import io.rowweave.run.RunSnapshot;

/** Bounded control result for one local incremental trigger. */
public final class IncrementalJobResult {
    public enum Code {
        SUCCEEDED,
        NO_OP,
        BACKFILL_REPLAY,
        STALE_BACKFILL,
        CANCELED,
        TIMED_OUT,
        RECOVERED,
        LOST_RACE
    }

    private final Code code;
    private final String artifactId;
    private final String runId;
    private final String intervalId;
    private final ExecutionReport executionReport;
    private final IncrementalAuthorityRecord record;
    private final RunSnapshot runSnapshot;

    private IncrementalJobResult(
            Code code, String artifactId, String runId, String intervalId,
            ExecutionReport executionReport,
            IncrementalAuthorityRecord record, RunSnapshot runSnapshot) {
        this.code = code;
        this.artifactId = artifactId;
        this.runId = runId;
        this.intervalId = intervalId;
        this.executionReport = executionReport;
        this.record = record;
        this.runSnapshot = runSnapshot;
    }

    static IncrementalJobResult prepared(
            IncrementalIntervalPreparationDecision decision) {
        switch (decision.code()) {
            case NO_OP:
                return new IncrementalJobResult(
                        Code.NO_OP, null, null, null, null,
                        decision.record(), null);
            case LOST_RACE:
                return new IncrementalJobResult(
                        Code.LOST_RACE, decision.artifactId(),
                        decision.claimRequest() == null ? null
                                : decision.claimRequest().runId(),
                        decision.claimRequest() == null ? null
                                : decision.claimRequest().intervalId(),
                        null, decision.record(), decision.runSnapshot());
            case BACKFILL_REPLAY:
                IncrementalCommitReceipt receipt =
                        decision.record().lastBackfillReceipt();
                return new IncrementalJobResult(
                        Code.BACKFILL_REPLAY, receipt.artifactId(),
                        receipt.fence().runId(), receipt.intervalId(), null,
                        decision.record(), null);
            case STALE_BACKFILL:
                return new IncrementalJobResult(
                        Code.STALE_BACKFILL, null, null, null, null,
                        decision.record(), null);
            default:
                throw new IllegalArgumentException(
                        "claimed preparation requires execution");
        }
    }

    static IncrementalJobResult succeeded(
            IncrementalIntervalPreparationDecision prepared,
            ExecutionReport report, IncrementalAuthorityDecision committed) {
        return new IncrementalJobResult(
                Code.SUCCEEDED, prepared.artifactId(),
                prepared.claimRequest().runId(),
                prepared.claimRequest().intervalId(), report,
                committed.record(), committed.runSnapshot());
    }

    static IncrementalJobResult terminal(
            IncrementalIntervalPreparationDecision prepared,
            io.rowweave.run.AttemptStatus terminalStatus,
            IncrementalAuthorityDecision decision) {
        Code code = terminalStatus == io.rowweave.run.AttemptStatus.TIMED_OUT
                ? Code.TIMED_OUT : Code.CANCELED;
        return new IncrementalJobResult(
                code, prepared.artifactId(),
                prepared.claimRequest().runId(),
                prepared.claimRequest().intervalId(), null,
                decision.record(), decision.runSnapshot());
    }

    static IncrementalJobResult recovered(
            IncrementalActiveInterval active,
            IncrementalAuthorityDecision decision) {
        return new IncrementalJobResult(
                Code.RECOVERED, active.artifactId(),
                active.fence().runId(), active.identity().intervalId(),
                null, decision.record(), decision.runSnapshot());
    }

    public Code code() { return code; }
    public String artifactId() { return artifactId; }
    public String runId() { return runId; }
    public String intervalId() { return intervalId; }
    public ExecutionReport executionReport() { return executionReport; }
    public IncrementalAuthorityRecord record() { return record; }
    public RunSnapshot runSnapshot() { return runSnapshot; }
}
