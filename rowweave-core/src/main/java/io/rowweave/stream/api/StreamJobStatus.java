package io.rowweave.stream.api;

/** Bounded public lifecycle projection; terminal states never imply client timeout. */
public enum StreamJobStatus {
    SUBMITTED(1, false),
    STARTING(2, false),
    RUNNING(3, false),
    RESTARTING(4, false),
    STOPPING(5, false),
    CANCELED(6, true),
    FAILED(7, true),
    STOPPED_WITH_SAVEPOINT(8, true),
    FINISHED(9, true);

    private final int wireId;
    private final boolean terminal;

    StreamJobStatus(int wireId, boolean terminal) {
        this.wireId = wireId;
        this.terminal = terminal;
    }

    public int wireId() { return wireId; }
    public boolean terminal() { return terminal; }
}
