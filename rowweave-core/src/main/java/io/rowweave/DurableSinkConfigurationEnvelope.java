package io.rowweave;

import java.util.Arrays;
import java.util.Objects;

/** Immutable codec identity plus canonical provider configuration bytes. */
public final class DurableSinkConfigurationEnvelope {

    private final DurableSinkCodecDescriptor configurationCodec;
    private final byte[] configurationBytes;

    public DurableSinkConfigurationEnvelope(
            DurableSinkCodecDescriptor configurationCodec,
            byte[] configurationBytes) {
        if (configurationCodec == null || configurationBytes == null) {
            throw new IllegalArgumentException(
                    "provider configuration codec and bytes are required");
        }
        if (configurationBytes.length > configurationCodec.maxPayloadBytes()
                || configurationBytes.length
                > DurableBatchSinkProviderContract.MAX_CONFIGURATION_BYTES) {
            throw new IllegalArgumentException(
                    "provider configuration exceeds codec bound");
        }
        this.configurationCodec = configurationCodec;
        this.configurationBytes = configurationBytes.clone();
    }

    public DurableSinkCodecDescriptor configurationCodec() {
        return configurationCodec;
    }

    public byte[] configurationBytes() { return configurationBytes.clone(); }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof DurableSinkConfigurationEnvelope)) return false;
        DurableSinkConfigurationEnvelope that =
                (DurableSinkConfigurationEnvelope) other;
        return configurationCodec.equals(that.configurationCodec)
                && Arrays.equals(configurationBytes, that.configurationBytes);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(configurationCodec)
                + Arrays.hashCode(configurationBytes);
    }
}
