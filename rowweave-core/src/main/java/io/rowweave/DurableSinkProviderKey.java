package io.rowweave;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Objects;

/** Exact identity of a versioned durable batch Sink provider. */
public final class DurableSinkProviderKey implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String connectorType;
    private final int connectorVersion;
    private final BatchSinkDescriptor.CapabilityProfile capabilityProfile;

    public DurableSinkProviderKey(
            String connectorType, int connectorVersion,
            BatchSinkDescriptor.CapabilityProfile capabilityProfile) {
        this.connectorType = DurableSinkContractText.require(connectorType,
                "connectorType", BatchSinkDescriptor.MAX_CONNECTOR_TYPE_BYTES);
        if (connectorVersion <= 0) {
            throw new IllegalArgumentException(
                    "durable Sink connectorVersion must be positive");
        }
        if (capabilityProfile == null) {
            throw new IllegalArgumentException(
                    "durable Sink capabilityProfile is required");
        }
        this.connectorVersion = connectorVersion;
        this.capabilityProfile = capabilityProfile;
    }

    public String connectorType() { return connectorType; }
    public int connectorVersion() { return connectorVersion; }
    public BatchSinkDescriptor.CapabilityProfile capabilityProfile() {
        return capabilityProfile;
    }

    public boolean matches(BatchSinkDescriptor descriptor) {
        return descriptor != null
                && connectorType.equals(descriptor.connectorType())
                && connectorVersion == descriptor.connectorVersion()
                && capabilityProfile == descriptor.capabilityProfile();
    }

    private Object readResolve() throws ObjectStreamException {
        return new DurableSinkProviderKey(
                connectorType, connectorVersion, capabilityProfile);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof DurableSinkProviderKey)) return false;
        DurableSinkProviderKey that = (DurableSinkProviderKey) other;
        return connectorVersion == that.connectorVersion
                && connectorType.equals(that.connectorType)
                && capabilityProfile == that.capabilityProfile;
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorType, connectorVersion, capabilityProfile);
    }

    @Override
    public String toString() {
        return "DurableSinkProviderKey{" + connectorType + '/' + connectorVersion
                + ", capability=" + capabilityProfile + '}';
    }
}
