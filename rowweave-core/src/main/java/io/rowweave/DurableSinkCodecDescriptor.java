package io.rowweave;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Objects;

/** Immutable identity and payload bound for one provider-owned canonical codec. */
public final class DurableSinkCodecDescriptor implements Serializable {

    private static final long serialVersionUID = 1L;
    public static final int MAX_CODEC_ID_BYTES = 128;
    public static final int MAX_PAYLOAD_BYTES = 16 * 1024 * 1024;

    private final String codecId;
    private final int codecVersion;
    private final int maxPayloadBytes;

    public DurableSinkCodecDescriptor(
            String codecId, int codecVersion, int maxPayloadBytes) {
        this.codecId = DurableSinkContractText.require(
                codecId, "codecId", MAX_CODEC_ID_BYTES);
        if (codecVersion <= 0) {
            throw new IllegalArgumentException(
                    "durable Sink codecVersion must be positive");
        }
        if (maxPayloadBytes <= 0 || maxPayloadBytes > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException(
                    "durable Sink codec payload bound is invalid");
        }
        this.codecVersion = codecVersion;
        this.maxPayloadBytes = maxPayloadBytes;
    }

    public String codecId() { return codecId; }
    public int codecVersion() { return codecVersion; }
    public int maxPayloadBytes() { return maxPayloadBytes; }

    private Object readResolve() throws ObjectStreamException {
        return new DurableSinkCodecDescriptor(
                codecId, codecVersion, maxPayloadBytes);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof DurableSinkCodecDescriptor)) return false;
        DurableSinkCodecDescriptor that = (DurableSinkCodecDescriptor) other;
        return codecVersion == that.codecVersion
                && maxPayloadBytes == that.maxPayloadBytes
                && codecId.equals(that.codecId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(codecId, codecVersion, maxPayloadBytes);
    }

    @Override
    public String toString() {
        return "DurableSinkCodecDescriptor{" + codecId + '/' + codecVersion
                + ", maxPayloadBytes=" + maxPayloadBytes + '}';
    }
}
