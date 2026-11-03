package io.rowweave.portable;

import java.util.Objects;

/** Static source/sink provider limits stored as a signed deployment resource. */
public final class PortableConnectorProviderContract {
    public static final int CURRENT_VERSION = 1;

    private final int contractVersion;
    private final PortableOperatorCode.BindingKind bindingKind;
    private final String providerKey;
    private final String configurationCodecId;
    private final int maximumConfigurationBytes;
    private final int maximumPayloadBytes;
    private final int maximumInstances;

    public PortableConnectorProviderContract(
            int contractVersion,
            PortableOperatorCode.BindingKind bindingKind,
            String providerKey, String configurationCodecId,
            int maximumConfigurationBytes, int maximumPayloadBytes,
            int maximumInstances) {
        if (contractVersion != CURRENT_VERSION) {
            throw PortablePlanText.failure(
                    "UNSUPPORTED_CONNECTOR_CONTRACT",
                    "connectorContract.version",
                    "connector provider contract version is not supported");
        }
        if (bindingKind != PortableOperatorCode.BindingKind.SOURCE
                && bindingKind != PortableOperatorCode.BindingKind.SINK) {
            throw PortablePlanText.failure("INVALID_CONNECTOR_CONTRACT",
                    "connectorContract.kind",
                    "connector contract must be SOURCE or SINK");
        }
        this.providerKey = PortableBindingSupport.key(
                providerKey, "connectorContract.providerKey");
        this.configurationCodecId = PortableBindingSupport.key(
                configurationCodecId,
                "connectorContract.configurationCodecId");
        int payloadCeiling = bindingKind
                == PortableOperatorCode.BindingKind.SOURCE
                ? PortableBindingLimits.MAX_SPLIT_FRAME_BYTES
                : PortableBindingLimits.MAX_DESCRIPTOR_BYTES;
        int instanceCeiling = bindingKind
                == PortableOperatorCode.BindingKind.SOURCE
                ? PortableBindingLimits.MAX_SPLITS : 1;
        if (maximumConfigurationBytes <= 0
                || maximumConfigurationBytes
                > PortableBindingLimits.MAX_CONFIGURATION_BYTES
                || maximumPayloadBytes <= 0
                || maximumPayloadBytes > payloadCeiling
                || maximumInstances <= 0
                || maximumInstances > instanceCeiling) {
            throw PortablePlanText.failure("LIMIT_EXCEEDED",
                    "connectorContract.limits",
                    "connector provider limits exceed portable v1 ceilings");
        }
        if (bindingKind == PortableOperatorCode.BindingKind.SINK
                && maximumInstances != 1) {
            throw PortablePlanText.failure("INVALID_CONNECTOR_CONTRACT",
                    "connectorContract.maximumInstances",
                    "sink provider contract requires one output instance");
        }
        this.contractVersion = contractVersion;
        this.bindingKind = bindingKind;
        this.maximumConfigurationBytes = maximumConfigurationBytes;
        this.maximumPayloadBytes = maximumPayloadBytes;
        this.maximumInstances = maximumInstances;
    }

    public int contractVersion() { return contractVersion; }
    public PortableOperatorCode.BindingKind bindingKind() {
        return bindingKind;
    }
    public String providerKey() { return providerKey; }
    public String configurationCodecId() { return configurationCodecId; }
    public int maximumConfigurationBytes() {
        return maximumConfigurationBytes;
    }
    public int maximumPayloadBytes() { return maximumPayloadBytes; }
    public int maximumInstances() { return maximumInstances; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof PortableConnectorProviderContract)) return false;
        PortableConnectorProviderContract that =
                (PortableConnectorProviderContract) other;
        return contractVersion == that.contractVersion
                && maximumConfigurationBytes
                == that.maximumConfigurationBytes
                && maximumPayloadBytes == that.maximumPayloadBytes
                && maximumInstances == that.maximumInstances
                && bindingKind == that.bindingKind
                && providerKey.equals(that.providerKey)
                && configurationCodecId.equals(that.configurationCodecId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(contractVersion, bindingKind, providerKey,
                configurationCodecId, maximumConfigurationBytes,
                maximumPayloadBytes, maximumInstances);
    }
}
