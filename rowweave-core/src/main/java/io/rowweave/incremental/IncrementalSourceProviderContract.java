package io.rowweave.incremental;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Objects;

/** Closed immutable capability contract for an incremental source provider. */
public final class IncrementalSourceProviderContract implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int CURRENT_SPI_VERSION = 1;
    public static final int MAX_CONFIGURATION_BYTES = 16 * 1024;
    public static final int MAX_TARGET_BYTES = 8 * 1024;
    public static final int MAX_CAPTURE_RESPONSE_BYTES = 8 * 1024 * 1024;
    public static final int MAX_MATERIALIZATION_RESPONSE_BYTES =
            8 * 1024 * 1024;

    public enum OrderingGuarantee {
        TOTAL_ORDER_NO_LATE_LEFT_V1(1);
        private final int wireId;
        OrderingGuarantee(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static OrderingGuarantee fromWire(int wireId) {
            for (OrderingGuarantee value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw new IllegalArgumentException(
                    "unknown incremental source ordering guarantee");
        }
    }

    public enum SnapshotGuarantee {
        EXACT_REOPENABLE_V1(1);
        private final int wireId;
        SnapshotGuarantee(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static SnapshotGuarantee fromWire(int wireId) {
            for (SnapshotGuarantee value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw new IllegalArgumentException(
                    "unknown incremental source snapshot guarantee");
        }
    }

    public enum DeploymentCapability {
        CORE_BUILTIN_V1(1), SIGNED_TEST_CONFORMANCE_V1(2);
        private final int wireId;
        DeploymentCapability(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static DeploymentCapability fromWire(int wireId) {
            for (DeploymentCapability value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw new IllegalArgumentException(
                    "unknown incremental source deployment capability");
        }
    }

    private final int spiVersion;
    private final IncrementalSourceProviderKey providerKey;
    private final IncrementalSourceCodecDescriptor configurationCodec;
    private final IncrementalSourceCodecDescriptor captureResponseCodec;
    private final IncrementalSourceCodecDescriptor materializationResponseCodec;
    private final int maxTargetBytes;
    private final IncrementalFrontier.Kind frontierKind;
    private final OrderingGuarantee orderingGuarantee;
    private final SnapshotGuarantee snapshotGuarantee;
    private final boolean tombstoneCapable;
    private final IncrementalSourceActionBudget actionBudget;
    private final DeploymentCapability deploymentCapability;

    public IncrementalSourceProviderContract(
            int spiVersion, IncrementalSourceProviderKey providerKey,
            IncrementalSourceCodecDescriptor configurationCodec,
            IncrementalSourceCodecDescriptor captureResponseCodec,
            IncrementalSourceCodecDescriptor materializationResponseCodec,
            int maxTargetBytes, IncrementalFrontier.Kind frontierKind,
            OrderingGuarantee orderingGuarantee,
            SnapshotGuarantee snapshotGuarantee, boolean tombstoneCapable,
            IncrementalSourceActionBudget actionBudget,
            DeploymentCapability deploymentCapability) {
        if (spiVersion != CURRENT_SPI_VERSION) {
            throw new IllegalArgumentException(
                    "incremental source SPI version must be "
                            + CURRENT_SPI_VERSION);
        }
        if (providerKey == null || configurationCodec == null
                || captureResponseCodec == null
                || materializationResponseCodec == null
                || frontierKind == null || orderingGuarantee == null
                || snapshotGuarantee == null || actionBudget == null
                || deploymentCapability == null) {
            throw new IllegalArgumentException(
                    "incremental source provider contract fields are required");
        }
        if (configurationCodec.maxPayloadBytes() > MAX_CONFIGURATION_BYTES
                || captureResponseCodec.maxPayloadBytes()
                > MAX_CAPTURE_RESPONSE_BYTES
                || materializationResponseCodec.maxPayloadBytes()
                > MAX_MATERIALIZATION_RESPONSE_BYTES) {
            throw new IllegalArgumentException(
                    "incremental source codec bounds exceed engine ceiling");
        }
        if (maxTargetBytes <= 0 || maxTargetBytes > MAX_TARGET_BYTES) {
            throw new IllegalArgumentException(
                    "incremental source target bound is invalid");
        }
        if (frontierKind == IncrementalFrontier.Kind.BEGIN_V1) {
            throw new IllegalArgumentException(
                    "incremental source requires a concrete frontier kind");
        }
        this.spiVersion = spiVersion;
        this.providerKey = providerKey;
        this.configurationCodec = configurationCodec;
        this.captureResponseCodec = captureResponseCodec;
        this.materializationResponseCodec = materializationResponseCodec;
        this.maxTargetBytes = maxTargetBytes;
        this.frontierKind = frontierKind;
        this.orderingGuarantee = orderingGuarantee;
        this.snapshotGuarantee = snapshotGuarantee;
        this.tombstoneCapable = tombstoneCapable;
        this.actionBudget = actionBudget;
        this.deploymentCapability = deploymentCapability;
    }

    public int spiVersion() { return spiVersion; }
    public IncrementalSourceProviderKey providerKey() { return providerKey; }
    public IncrementalSourceCodecDescriptor configurationCodec() {
        return configurationCodec;
    }
    public IncrementalSourceCodecDescriptor captureResponseCodec() {
        return captureResponseCodec;
    }
    public IncrementalSourceCodecDescriptor materializationResponseCodec() {
        return materializationResponseCodec;
    }
    public int maxTargetBytes() { return maxTargetBytes; }
    public IncrementalFrontier.Kind frontierKind() { return frontierKind; }
    public OrderingGuarantee orderingGuarantee() { return orderingGuarantee; }
    public SnapshotGuarantee snapshotGuarantee() { return snapshotGuarantee; }
    public boolean tombstoneCapable() { return tombstoneCapable; }
    public IncrementalSourceActionBudget actionBudget() { return actionBudget; }
    public DeploymentCapability deploymentCapability() {
        return deploymentCapability;
    }

    private Object readResolve() throws ObjectStreamException {
        return new IncrementalSourceProviderContract(
                spiVersion, providerKey, configurationCodec,
                captureResponseCodec, materializationResponseCodec,
                maxTargetBytes, frontierKind, orderingGuarantee,
                snapshotGuarantee, tombstoneCapable, actionBudget,
                deploymentCapability);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalSourceProviderContract)) return false;
        IncrementalSourceProviderContract that =
                (IncrementalSourceProviderContract) other;
        return spiVersion == that.spiVersion
                && maxTargetBytes == that.maxTargetBytes
                && tombstoneCapable == that.tombstoneCapable
                && providerKey.equals(that.providerKey)
                && configurationCodec.equals(that.configurationCodec)
                && captureResponseCodec.equals(that.captureResponseCodec)
                && materializationResponseCodec.equals(
                that.materializationResponseCodec)
                && frontierKind == that.frontierKind
                && orderingGuarantee == that.orderingGuarantee
                && snapshotGuarantee == that.snapshotGuarantee
                && actionBudget.equals(that.actionBudget)
                && deploymentCapability == that.deploymentCapability;
    }

    @Override
    public int hashCode() {
        return Objects.hash(spiVersion, providerKey, configurationCodec,
                captureResponseCodec, materializationResponseCodec,
                maxTargetBytes, frontierKind, orderingGuarantee,
                snapshotGuarantee, tombstoneCapable, actionBudget,
                deploymentCapability);
    }
}
