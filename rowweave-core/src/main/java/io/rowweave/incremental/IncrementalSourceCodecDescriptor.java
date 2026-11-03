package io.rowweave.incremental;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Objects;

/** Identity and hard payload ceiling of one provider-owned canonical codec. */
public final class IncrementalSourceCodecDescriptor implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final int MAX_CODEC_ID_BYTES = 128;
    public static final int MAX_PAYLOAD_BYTES = 8 * 1024 * 1024;

    private final String codecId;
    private final int codecVersion;
    private final int maxPayloadBytes;

    public IncrementalSourceCodecDescriptor(
            String codecId, int codecVersion, int maxPayloadBytes) {
        this.codecId = IncrementalSourceText.require(
                codecId, "codecId", MAX_CODEC_ID_BYTES);
        if (codecVersion <= 0) {
            throw new IllegalArgumentException(
                    "incremental source codecVersion must be positive");
        }
        if (maxPayloadBytes <= 0 || maxPayloadBytes > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException(
                    "incremental source codec payload bound is invalid");
        }
        this.codecVersion = codecVersion;
        this.maxPayloadBytes = maxPayloadBytes;
    }

    public String codecId() { return codecId; }
    public int codecVersion() { return codecVersion; }
    public int maxPayloadBytes() { return maxPayloadBytes; }

    private Object readResolve() throws ObjectStreamException {
        return new IncrementalSourceCodecDescriptor(
                codecId, codecVersion, maxPayloadBytes);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalSourceCodecDescriptor)) return false;
        IncrementalSourceCodecDescriptor that =
                (IncrementalSourceCodecDescriptor) other;
        return codecVersion == that.codecVersion
                && maxPayloadBytes == that.maxPayloadBytes
                && codecId.equals(that.codecId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(codecId, codecVersion, maxPayloadBytes);
    }
}
