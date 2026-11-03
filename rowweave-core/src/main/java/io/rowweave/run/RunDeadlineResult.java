package io.rowweave.run;

/** Bounded result, from a closed set, of one deadline authority evaluation. */
public final class RunDeadlineResult {
    public enum Disposition {
        NO_DEADLINE,
        NOT_DUE,
        ACCEPTED,
        ALREADY_REQUESTED,
        TOO_LATE,
        ALREADY_TERMINAL,
        NOT_FOUND,
        RETIRING
    }

    private final String runId;
    private final Disposition disposition;
    private final RunStatus status;
    private final long revision;
    private final long deadlineEpochMillis;
    private final long observedAuthorityTimeMillis;

    public RunDeadlineResult(String runId, Disposition disposition,
                             RunStatus status, long revision,
                             long deadlineEpochMillis,
                             long observedAuthorityTimeMillis) {
        this.runId = RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
        if (disposition == null) {
            throw new IllegalArgumentException("deadline disposition is required");
        }
        boolean absent = disposition == Disposition.NOT_FOUND
                || disposition == Disposition.RETIRING;
        if (absent) {
            if (status != null || revision != 0L || deadlineEpochMillis != 0L
                    || observedAuthorityTimeMillis != 0L) {
                throw new IllegalArgumentException(
                        "absent deadline result must not expose authority state");
            }
        } else {
            if (status == null || revision <= 0L || deadlineEpochMillis < 0L
                    || observedAuthorityTimeMillis < 0L) {
                throw new IllegalArgumentException("invalid found deadline result");
            }
            if (disposition == Disposition.NO_DEADLINE) {
                if (deadlineEpochMillis != 0L || observedAuthorityTimeMillis != 0L) {
                    throw new IllegalArgumentException("NO_DEADLINE shape mismatch");
                }
            } else {
                if (deadlineEpochMillis <= 0L || observedAuthorityTimeMillis <= 0L) {
                    throw new IllegalArgumentException("deadline authority time is required");
                }
                if (disposition == Disposition.NOT_DUE
                        && observedAuthorityTimeMillis >= deadlineEpochMillis) {
                    throw new IllegalArgumentException("NOT_DUE requires authority time before deadline");
                }
                if (disposition == Disposition.ACCEPTED
                        && observedAuthorityTimeMillis < deadlineEpochMillis) {
                    throw new IllegalArgumentException("accepted deadline must be due");
                }
            }
            if (disposition == Disposition.ACCEPTED
                    && status != RunStatus.CANCELING && status != RunStatus.FAILED
                    && status != RunStatus.RECONCILING) {
                throw new IllegalArgumentException(
                        "ACCEPTED requires CANCELING, FAILED, or RECONCILING");
            }
            if (disposition == Disposition.ALREADY_REQUESTED
                    && status != RunStatus.CANCELING && status != RunStatus.FAILED) {
                throw new IllegalArgumentException(
                        "ALREADY_REQUESTED requires CANCELING or FAILED");
            }
            if (disposition == Disposition.ALREADY_TERMINAL && !status.isTerminal()) {
                throw new IllegalArgumentException("ALREADY_TERMINAL requires terminal status");
            }
        }
        this.disposition = disposition;
        this.status = status;
        this.revision = revision;
        this.deadlineEpochMillis = deadlineEpochMillis;
        this.observedAuthorityTimeMillis = observedAuthorityTimeMillis;
    }

    public String runId() { return runId; }
    public Disposition disposition() { return disposition; }
    public RunStatus status() { return status; }
    public long revision() { return revision; }
    public long deadlineEpochMillis() { return deadlineEpochMillis; }
    public long observedAuthorityTimeMillis() { return observedAuthorityTimeMillis; }
}
