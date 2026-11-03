package io.rowweave.incremental;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/** Inert Plan/Artifact proxy; only an admitted Worker provider may act on it. */
public final class IncrementalSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final int DIGEST_BYTES = 32;

    private final IncrementalSourceProviderKey providerKey;
    private final byte[] contractDigest;
    private final byte[] configurationBytes;

    public IncrementalSourceConfig(
            IncrementalSourceProviderKey providerKey,
            byte[] contractDigest, byte[] configurationBytes) {
        if (providerKey == null || contractDigest == null
                || contractDigest.length != DIGEST_BYTES
                || configurationBytes == null
                || configurationBytes.length
                > IncrementalSourceProviderContract.MAX_CONFIGURATION_BYTES) {
            throw new IllegalArgumentException(
                    "incremental source configuration fields are invalid");
        }
        this.providerKey = providerKey;
        this.contractDigest = contractDigest.clone();
        this.configurationBytes = configurationBytes.clone();
    }

    public IncrementalSourceProviderKey providerKey() { return providerKey; }
    public byte[] contractDigest() { return contractDigest.clone(); }
    public byte[] configurationBytes() { return configurationBytes.clone(); }
    public int configurationSize() { return configurationBytes.length; }

    private Object readResolve() throws ObjectStreamException {
        return new IncrementalSourceConfig(
                providerKey, contractDigest, configurationBytes);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalSourceConfig)) return false;
        IncrementalSourceConfig that = (IncrementalSourceConfig) other;
        return providerKey.equals(that.providerKey)
                && Arrays.equals(contractDigest, that.contractDigest)
                && Arrays.equals(configurationBytes, that.configurationBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(providerKey, Arrays.hashCode(contractDigest),
                Arrays.hashCode(configurationBytes));
    }
}
