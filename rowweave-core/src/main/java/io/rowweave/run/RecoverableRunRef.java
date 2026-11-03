package io.rowweave.run;

/** Immutable discovery reference; the RunStateStore authority must still be re-read before it is used. */
public final class RecoverableRunRef {
    private final String runId;
    private final long revision;
    private final RunStatus status;

    public RecoverableRunRef(String runId, long revision, RunStatus status) {
        RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
        if (revision <= 0L) throw new IllegalArgumentException("revision must be > 0");
        if (status == null || status.isTerminal()) {
            throw new IllegalArgumentException("status must be non-terminal");
        }
        this.runId = runId;
        this.revision = revision;
        this.status = status;
    }

    public String runId() { return runId; }
    public long revision() { return revision; }
    public RunStatus status() { return status; }
}
