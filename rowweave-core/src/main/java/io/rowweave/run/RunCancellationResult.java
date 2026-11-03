package io.rowweave.run;

/** Authoritative result, from a closed set, of one durable USER cancellation of a Run. */
public final class RunCancellationResult {
    public enum Disposition {
        ACCEPTED,
        ALREADY_REQUESTED,
        ALREADY_CANCELED,
        ALREADY_TERMINAL,
        TOO_LATE,
        NOT_FOUND,
        RETIRING
    }

    private final String runId;
    private final Disposition disposition;
    private final RunStatus status;
    private final long revision;

    public RunCancellationResult(String runId, Disposition disposition,
                                 RunStatus status, long revision) {
        this.runId = RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
        if (disposition == null) {
            throw new IllegalArgumentException("disposition is required");
        }
        boolean absent = disposition == Disposition.NOT_FOUND
                || disposition == Disposition.RETIRING;
        if (absent != (status == null) || absent != (revision == 0L)) {
            throw new IllegalArgumentException(
                    "absent results require null status/revision zero; found results require status/positive revision");
        }
        if (!absent && revision <= 0L) {
            throw new IllegalArgumentException("revision must be positive for a found Run");
        }
        if (disposition == Disposition.ACCEPTED
                && status != RunStatus.CANCELING && status != RunStatus.CANCELED
                && status != RunStatus.RECONCILING) {
            throw new IllegalArgumentException(
                    "ACCEPTED requires CANCELING, CANCELED, or RECONCILING status");
        }
        if (disposition == Disposition.ALREADY_CANCELED
                && status != RunStatus.CANCELED) {
            throw new IllegalArgumentException("ALREADY_CANCELED requires CANCELED status");
        }
        if (disposition == Disposition.ALREADY_TERMINAL
                && status != RunStatus.SUCCEEDED && status != RunStatus.FAILED) {
            throw new IllegalArgumentException(
                    "ALREADY_TERMINAL requires SUCCEEDED or FAILED status");
        }
        this.disposition = disposition;
        this.status = status;
        this.revision = revision;
    }

    public String runId() { return runId; }
    public Disposition disposition() { return disposition; }
    public RunStatus status() { return status; }
    public long revision() { return revision; }

    @Override
    public String toString() {
        return "RunCancellationResult{runId='" + runId + "', disposition="
                + disposition + ", status=" + status + ", revision=" + revision + '}';
    }
}
