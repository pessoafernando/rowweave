package io.rowweave.run;

/** An immutable, checkable Run reference held in the lifecycle catalog. */
public final class RunLifecycleRef {
    private final String runId;
    private final long revision;
    private final RunStatus status;
    private final String artifactId;
    private final RunLifecycleState state;
    private final long terminalAtMillis;
    private final RetentionToken retentionToken;

    private RunLifecycleRef(String runId, long revision, RunStatus status,
                            String artifactId, RunLifecycleState state,
                            long terminalAtMillis, RetentionToken retentionToken) {
        this.runId = RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
        this.artifactId = RunText.required(
                artifactId, "artifactId", RunText.MAX_ID_BYTES);
        if (revision <= 0L) throw new IllegalArgumentException("revision must be > 0");
        if (status == null) throw new IllegalArgumentException("status is required");
        if (state == null) throw new IllegalArgumentException("state is required");
        if (state == RunLifecycleState.ACTIVE) {
            if (status.isTerminal() || terminalAtMillis != 0L || retentionToken != null) {
                throw new IllegalArgumentException("invalid active lifecycle ref");
            }
        } else {
            if (!status.isTerminal() || terminalAtMillis <= 0L) {
                throw new IllegalArgumentException("invalid terminal lifecycle ref");
            }
            if ((state == RunLifecycleState.DELETING) != (retentionToken != null)) {
                throw new IllegalArgumentException("retention token does not match lifecycle state");
            }
        }
        if (retentionToken != null
                && (!runId.equals(retentionToken.runId())
                || revision != retentionToken.terminalRevision()
                || terminalAtMillis != retentionToken.terminalAtMillis())) {
            throw new IllegalArgumentException("retention token identity mismatch");
        }
        this.revision = revision;
        this.status = status;
        this.state = state;
        this.terminalAtMillis = terminalAtMillis;
        this.retentionToken = retentionToken;
    }

    public static RunLifecycleRef active(String runId, long revision,
                                         RunStatus status, String artifactId) {
        return new RunLifecycleRef(runId, revision, status, artifactId,
                RunLifecycleState.ACTIVE, 0L, null);
    }

    public static RunLifecycleRef terminal(String runId, long revision,
                                           RunStatus status, String artifactId,
                                           long terminalAtMillis) {
        return new RunLifecycleRef(runId, revision, status, artifactId,
                RunLifecycleState.TERMINAL, terminalAtMillis, null);
    }

    public static RunLifecycleRef deleting(String runId, long revision,
                                           RunStatus status, String artifactId,
                                           long terminalAtMillis,
                                           RetentionToken retentionToken) {
        return new RunLifecycleRef(runId, revision, status, artifactId,
                RunLifecycleState.DELETING, terminalAtMillis, retentionToken);
    }

    public String runId() { return runId; }
    public long revision() { return revision; }
    public RunStatus status() { return status; }
    public String artifactId() { return artifactId; }
    public RunLifecycleState state() { return state; }
    public long terminalAtMillis() { return terminalAtMillis; }
    public RetentionToken retentionToken() { return retentionToken; }
}
