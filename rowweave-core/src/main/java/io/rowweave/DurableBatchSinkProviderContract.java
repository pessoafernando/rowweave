package io.rowweave;

import io.rowweave.run.PreparedSinkLocator;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Objects;

/** Closed, immutable capability contract for a durable batch Sink provider. */
public final class DurableBatchSinkProviderContract implements Serializable {

    private static final long serialVersionUID = 1L;
    public static final int CURRENT_SPI_VERSION = 1;
    public static final int MAX_CONFIGURATION_BYTES = 1024 * 1024;
    public static final int MAX_OUTCOME_BYTES = 16 * 1024;

    public enum IdempotencyScope {
        ATTEMPT_AND_PUBLICATION_V1(1);
        private final int wireId;
        IdempotencyScope(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static IdempotencyScope fromWire(int id) {
            for (IdempotencyScope value : values()) {
                if (value.wireId == id) return value;
            }
            throw new IllegalArgumentException("unknown provider idempotency scope: " + id);
        }
    }

    public enum OrphanOwnership {
        ATTEMPT_PRIVATE_V1(1);
        private final int wireId;
        OrphanOwnership(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static OrphanOwnership fromWire(int id) {
            for (OrphanOwnership value : values()) {
                if (value.wireId == id) return value;
            }
            throw new IllegalArgumentException("unknown provider orphan ownership: " + id);
        }
    }

    public enum QuarantinePolicy {
        PRESERVE_ON_UNKNOWN_OR_CONFLICT(1);
        private final int wireId;
        QuarantinePolicy(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static QuarantinePolicy fromWire(int id) {
            for (QuarantinePolicy value : values()) {
                if (value.wireId == id) return value;
            }
            throw new IllegalArgumentException("unknown provider quarantine policy: " + id);
        }
    }

    public enum SafeDeleteProof {
        ABORT_DECISION_OR_UNREFERENCED_RETENTION_V1(1);
        private final int wireId;
        SafeDeleteProof(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static SafeDeleteProof fromWire(int id) {
            for (SafeDeleteProof value : values()) {
                if (value.wireId == id) return value;
            }
            throw new IllegalArgumentException("unknown provider safe-delete proof: " + id);
        }
    }

    public enum DeploymentCapability {
        CORE_BUILTIN_V1(1),
        SIGNED_TEST_CONFORMANCE_V1(2);
        private final int wireId;
        DeploymentCapability(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static DeploymentCapability fromWire(int id) {
            for (DeploymentCapability value : values()) {
                if (value.wireId == id) return value;
            }
            throw new IllegalArgumentException("unknown provider deployment capability: " + id);
        }
    }

    private final int spiVersion;
    private final DurableSinkProviderKey providerKey;
    private final DurableSinkCodecDescriptor configurationCodec;
    private final DurableSinkCodecDescriptor locatorCodec;
    private final DurableSinkCodecDescriptor outcomeCodec;
    private final IdempotencyScope idempotencyScope;
    private final DurableSinkCleanupContract cleanupContract;
    private final DurableSinkAttemptBudget attemptBudget;
    private final DeploymentCapability deploymentCapability;

    public DurableBatchSinkProviderContract(
            int spiVersion, DurableSinkProviderKey providerKey,
            DurableSinkCodecDescriptor configurationCodec,
            DurableSinkCodecDescriptor locatorCodec,
            DurableSinkCodecDescriptor outcomeCodec,
            IdempotencyScope idempotencyScope,
            DurableSinkCleanupContract cleanupContract,
            DurableSinkAttemptBudget attemptBudget,
            DeploymentCapability deploymentCapability) {
        if (spiVersion != CURRENT_SPI_VERSION) {
            throw new IllegalArgumentException(
                    "durable Sink SPI version must be " + CURRENT_SPI_VERSION);
        }
        if (providerKey == null || configurationCodec == null
                || locatorCodec == null || outcomeCodec == null
                || idempotencyScope == null || cleanupContract == null
                || attemptBudget == null || deploymentCapability == null) {
            throw new IllegalArgumentException(
                    "durable Sink provider contract fields are required");
        }
        if (providerKey.capabilityProfile()
                != BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1
                && providerKey.capabilityProfile()
                != BatchSinkDescriptor.CapabilityProfile.SIDE_EFFECT_FREE_V1) {
            throw new IllegalArgumentException(
                    "durable Sink provider capability is unsupported");
        }
        if (configurationCodec.maxPayloadBytes() > MAX_CONFIGURATION_BYTES
                || locatorCodec.maxPayloadBytes()
                > PreparedSinkLocator.MAX_PAYLOAD_BYTES
                || locatorCodec.maxPayloadBytes() > attemptBudget.maxLocatorBytes()
                || outcomeCodec.maxPayloadBytes() > MAX_OUTCOME_BYTES
                || outcomeCodec.maxPayloadBytes() > attemptBudget.maxControlBytes()) {
            throw new IllegalArgumentException(
                    "durable Sink codec bounds exceed action budget");
        }
        this.spiVersion = spiVersion;
        this.providerKey = providerKey;
        this.configurationCodec = configurationCodec;
        this.locatorCodec = locatorCodec;
        this.outcomeCodec = outcomeCodec;
        this.idempotencyScope = idempotencyScope;
        this.cleanupContract = cleanupContract;
        this.attemptBudget = attemptBudget;
        this.deploymentCapability = deploymentCapability;
    }

    public int spiVersion() { return spiVersion; }
    public DurableSinkProviderKey providerKey() { return providerKey; }
    public DurableSinkCodecDescriptor configurationCodec() { return configurationCodec; }
    public DurableSinkCodecDescriptor locatorCodec() { return locatorCodec; }
    public DurableSinkCodecDescriptor outcomeCodec() { return outcomeCodec; }
    public IdempotencyScope idempotencyScope() { return idempotencyScope; }
    public DurableSinkCleanupContract cleanupContract() { return cleanupContract; }
    public DurableSinkAttemptBudget attemptBudget() { return attemptBudget; }
    public DeploymentCapability deploymentCapability() { return deploymentCapability; }

    private Object readResolve() throws ObjectStreamException {
        return new DurableBatchSinkProviderContract(spiVersion, providerKey,
                configurationCodec, locatorCodec, outcomeCodec, idempotencyScope,
                cleanupContract, attemptBudget, deploymentCapability);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof DurableBatchSinkProviderContract)) return false;
        DurableBatchSinkProviderContract that =
                (DurableBatchSinkProviderContract) other;
        return spiVersion == that.spiVersion
                && providerKey.equals(that.providerKey)
                && configurationCodec.equals(that.configurationCodec)
                && locatorCodec.equals(that.locatorCodec)
                && outcomeCodec.equals(that.outcomeCodec)
                && idempotencyScope == that.idempotencyScope
                && cleanupContract.equals(that.cleanupContract)
                && attemptBudget.equals(that.attemptBudget)
                && deploymentCapability == that.deploymentCapability;
    }

    @Override
    public int hashCode() {
        return Objects.hash(spiVersion, providerKey, configurationCodec,
                locatorCodec, outcomeCodec, idempotencyScope, cleanupContract,
                attemptBudget, deploymentCapability);
    }
}
