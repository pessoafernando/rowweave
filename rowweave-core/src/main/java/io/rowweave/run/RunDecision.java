package io.rowweave.run;

/** Structured outcome of one command; a rejection costs no revision and appends no event. */
public final class RunDecision {
    public enum Code {
        ACCEPTED,
        ALREADY_APPLIED,
        RUN_NOT_FOUND,
        RUN_ALREADY_EXISTS,
        STALE_REVISION,
        STALE_FENCE,
        ILLEGAL_TRANSITION,
        LIMIT_EXCEEDED,
        COMMAND_ID_CONFLICT,
        RETENTION_IN_PROGRESS,
        RECOVERY_OWNED,
        STALE_RECOVERY_OWNER
    }

    private final Code code;
    private final RunSnapshot snapshot;
    private final ExecutionEvent event;

    private RunDecision(Code code, RunSnapshot snapshot, ExecutionEvent event) {
        this.code = code;
        this.snapshot = snapshot;
        this.event = event;
    }

    static RunDecision accepted(RunSnapshot snapshot, ExecutionEvent event) {
        return new RunDecision(Code.ACCEPTED, snapshot, event);
    }

    static RunDecision rejected(Code code, RunSnapshot snapshot) {
        return new RunDecision(code, snapshot, null);
    }

    static RunDecision replayed(RunSnapshot snapshot, ExecutionEvent originalEvent) {
        return new RunDecision(Code.ALREADY_APPLIED, snapshot, originalEvent);
    }

    public Code code() { return code; }
    public boolean accepted() { return code == Code.ACCEPTED; }
    public RunSnapshot snapshot() { return snapshot; }
    public ExecutionEvent event() { return event; }
}
