package io.rowweave.stream.spi;

/** Data-free source open context with optional ephemeral Worker-local secrets. */
public final class StreamSourceContext {
    private static final int MAX_FRAME_BYTES = 4 * 1024 * 1024;
    private final String operatorUid;
    private final byte[] configuration;
    private final String splitId;
    private final String positionCodecId;
    private final byte[] splitDescriptor;
    private final byte[] restoredPosition;
    private final long restoredLogicalPosition;
    private final StreamSecretAccess secrets;

    public StreamSourceContext(String operatorUid, byte[] configuration,
            String splitId, String positionCodecId, byte[] splitDescriptor,
            byte[] restoredPosition, long restoredLogicalPosition) {
        this(operatorUid, configuration, splitId, positionCodecId,
                splitDescriptor, restoredPosition, restoredLogicalPosition,
                StreamSecretAccess.unavailable());
    }

    public StreamSourceContext(String operatorUid, byte[] configuration,
            String splitId, String positionCodecId, byte[] splitDescriptor,
            byte[] restoredPosition, long restoredLogicalPosition,
            StreamSecretAccess secrets) {
        if (operatorUid == null || configuration == null || splitId == null
                || positionCodecId == null || splitDescriptor == null
                || secrets == null
                || configuration.length > MAX_FRAME_BYTES
                || splitDescriptor.length == 0
                || splitDescriptor.length > MAX_FRAME_BYTES
                || (restoredPosition != null
                && (restoredPosition.length == 0
                || restoredPosition.length > MAX_FRAME_BYTES))
                || (restoredPosition == null) != (restoredLogicalPosition < 0L)) {
            throw new IllegalArgumentException("Stream source context is invalid");
        }
        this.operatorUid = operatorUid;
        this.configuration = configuration.clone();
        this.splitId = splitId;
        this.positionCodecId = positionCodecId;
        this.splitDescriptor = splitDescriptor.clone();
        this.restoredPosition = restoredPosition == null ? null
                : restoredPosition.clone();
        this.restoredLogicalPosition = restoredLogicalPosition;
        this.secrets = secrets;
    }
    public String operatorUid() { return operatorUid; }
    public byte[] configurationFrame() { return configuration.clone(); }
    public String splitId() { return splitId; }
    public String positionCodecId() { return positionCodecId; }
    public byte[] splitDescriptorFrame() { return splitDescriptor.clone(); }
    public boolean hasRestoredPosition() { return restoredPosition != null; }
    public byte[] restoredPositionFrame() {
        if (restoredPosition == null) throw new IllegalStateException("no restored position");
        return restoredPosition.clone();
    }
    public long restoredLogicalPosition() {
        if (restoredPosition == null) throw new IllegalStateException("no restored position");
        return restoredLogicalPosition;
    }
    public StreamSecretAccess secrets() { return secrets; }
}
