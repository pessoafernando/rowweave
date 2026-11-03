package io.rowweave.run;

/** Structured audit event committed atomically with the accepted mutation it describes. */
public final class ExecutionEvent {
    public static final int SCHEMA_VERSION = 1;

    public enum Type {
        RUN_CREATED,
        ATTEMPT_CREATED,
        ATTEMPT_ADVANCED,
        CANCEL_REQUESTED,
        TIMEOUT_REQUESTED,
        FENCE_REQUESTED
    }

    private final long sequence;
    private final Type type;
    private final String runId;
    private final String attemptId;
    private final long fenceToken;
    private final RunStatus runFrom;
    private final RunStatus runTo;
    private final AttemptStatus attemptFrom;
    private final AttemptStatus attemptTo;
    private final long eventTimeMillis;
    private final String reasonCode;
    private final String message;
    private final String commandId;
    private final byte[] commandDigest;

    ExecutionEvent(long sequence, Type type, String runId, String attemptId, long fenceToken,
                   RunStatus runFrom, RunStatus runTo,
                   AttemptStatus attemptFrom, AttemptStatus attemptTo,
                   long eventTimeMillis, String reasonCode, String message,
                   String commandId, byte[] commandDigest) {
        this.sequence = sequence;
        this.type = type;
        this.runId = runId;
        this.attemptId = attemptId;
        this.fenceToken = fenceToken;
        this.runFrom = runFrom;
        this.runTo = runTo;
        this.attemptFrom = attemptFrom;
        this.attemptTo = attemptTo;
        this.eventTimeMillis = eventTimeMillis;
        this.reasonCode = reasonCode;
        this.message = message;
        this.commandId = commandId;
        this.commandDigest = commandDigest.clone();
    }

    public int schemaVersion() { return SCHEMA_VERSION; }
    public long sequence() { return sequence; }
    public Type type() { return type; }
    public String runId() { return runId; }
    public String attemptId() { return attemptId; }
    public long fenceToken() { return fenceToken; }
    public RunStatus runFrom() { return runFrom; }
    public RunStatus runTo() { return runTo; }
    public AttemptStatus attemptFrom() { return attemptFrom; }
    public AttemptStatus attemptTo() { return attemptTo; }
    public long eventTimeMillis() { return eventTimeMillis; }
    public String reasonCode() { return reasonCode; }
    public String message() { return message; }
    public String commandId() { return commandId; }
    public byte[] commandDigest() { return commandDigest.clone(); }
    byte[] commandDigestUnsafe() { return commandDigest; }

    /** Public projection that omits Run and Attempt identity, the fence, raw diagnostic text and the full digest. */
    public RunExecutionEvent publicView() { return RunExecutionEvent.project(this); }
}
