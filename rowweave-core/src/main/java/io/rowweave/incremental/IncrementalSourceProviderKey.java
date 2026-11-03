package io.rowweave.incremental;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Objects;

/** Exact identity of one versioned incremental source provider. */
public final class IncrementalSourceProviderKey implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final int MAX_CONNECTOR_TYPE_BYTES = 128;

    private final String connectorType;
    private final int connectorVersion;

    public IncrementalSourceProviderKey(
            String connectorType, int connectorVersion) {
        this.connectorType = IncrementalSourceText.require(
                connectorType, "connectorType", MAX_CONNECTOR_TYPE_BYTES);
        if (connectorVersion <= 0) {
            throw new IllegalArgumentException(
                    "incremental source connectorVersion must be positive");
        }
        this.connectorVersion = connectorVersion;
    }

    public String connectorType() { return connectorType; }
    public int connectorVersion() { return connectorVersion; }

    private Object readResolve() throws ObjectStreamException {
        return new IncrementalSourceProviderKey(
                connectorType, connectorVersion);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalSourceProviderKey)) return false;
        IncrementalSourceProviderKey that =
                (IncrementalSourceProviderKey) other;
        return connectorVersion == that.connectorVersion
                && connectorType.equals(that.connectorType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorType, connectorVersion);
    }

    @Override
    public String toString() {
        return "IncrementalSourceProviderKey{" + connectorType + '/'
                + connectorVersion + '}';
    }
}
