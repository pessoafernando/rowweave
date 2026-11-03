package io.rowweave.stream.spi;

/** Data-free eager Sink open context with optional ephemeral secrets. */
public final class StreamSinkContext {
    private static final int MAX_FRAME_BYTES = 4 * 1024 * 1024;
    private final String operatorUid;
    private final byte[] configuration;
    private final StreamSecretAccess secrets;
    public StreamSinkContext(String operatorUid, byte[] configuration) {
        this(operatorUid, configuration, StreamSecretAccess.unavailable());
    }
    public StreamSinkContext(String operatorUid, byte[] configuration,
            StreamSecretAccess secrets) {
        if (operatorUid == null || configuration == null
                || configuration.length > MAX_FRAME_BYTES
                || secrets == null) {
            throw new IllegalArgumentException("Stream Sink context is invalid");
        }
        this.operatorUid = operatorUid;
        this.configuration = configuration.clone();
        this.secrets = secrets;
    }
    public String operatorUid() { return operatorUid; }
    public byte[] configurationFrame() { return configuration.clone(); }
    public StreamSecretAccess secrets() { return secrets; }
}
