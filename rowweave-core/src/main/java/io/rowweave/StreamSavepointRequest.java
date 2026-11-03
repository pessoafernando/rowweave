package io.rowweave;

/** Connector-neutral destination contract; contains no live store or credential. */
public final class StreamSavepointRequest {
    private final String providerKey;
    private final byte[] configurationFrame;

    public StreamSavepointRequest(String providerKey, byte[] configurationFrame) {
        if (providerKey == null || providerKey.trim().isEmpty()
                || providerKey.length() > 256 || configurationFrame == null) {
            throw new IllegalArgumentException("savepoint provider and configuration are required");
        }
        this.providerKey = providerKey;
        this.configurationFrame = configurationFrame.clone();
    }
    public String providerKey() { return providerKey; }
    public byte[] configurationFrame() { return configurationFrame.clone(); }
}
