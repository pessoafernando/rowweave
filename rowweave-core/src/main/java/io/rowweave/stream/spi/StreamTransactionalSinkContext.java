package io.rowweave.stream.spi;

/** Bounded transactional Sink identity with optional ephemeral secrets. */
public final class StreamTransactionalSinkContext {
    private static final int MAX_FRAME_BYTES = 4 * 1024 * 1024;

    private final String operatorUid;
    private final int subtaskIndex;
    private final String runId;
    private final String attemptId;
    private final long fenceToken;
    private final long assignmentGeneration;
    private final byte[] configuration;
    private final StreamSecretAccess secrets;

    public StreamTransactionalSinkContext(
            String operatorUid, int subtaskIndex,
            String runId, String attemptId, long fenceToken,
            long assignmentGeneration, byte[] configuration) {
        this(operatorUid, subtaskIndex, runId, attemptId, fenceToken,
                assignmentGeneration, configuration,
                StreamSecretAccess.unavailable());
    }

    public StreamTransactionalSinkContext(
            String operatorUid, int subtaskIndex,
            String runId, String attemptId, long fenceToken,
            long assignmentGeneration, byte[] configuration,
            StreamSecretAccess secrets) {
        if (empty(operatorUid) || subtaskIndex < 0 || empty(runId)
                || empty(attemptId) || fenceToken <= 0L
                || assignmentGeneration <= 0L || configuration == null
                || configuration.length > MAX_FRAME_BYTES
                || secrets == null) {
            throw new IllegalArgumentException(
                    "Stream transactional Sink context is invalid");
        }
        this.operatorUid = operatorUid;
        this.subtaskIndex = subtaskIndex;
        this.runId = runId;
        this.attemptId = attemptId;
        this.fenceToken = fenceToken;
        this.assignmentGeneration = assignmentGeneration;
        this.configuration = configuration.clone();
        this.secrets = secrets;
    }

    public String operatorUid() { return operatorUid; }
    public int subtaskIndex() { return subtaskIndex; }
    public String runId() { return runId; }
    public String attemptId() { return attemptId; }
    public long fenceToken() { return fenceToken; }
    public long assignmentGeneration() { return assignmentGeneration; }
    public byte[] configurationFrame() { return configuration.clone(); }
    public StreamSecretAccess secrets() { return secrets; }

    private static boolean empty(String value) {
        return value == null || value.isEmpty();
    }
}
