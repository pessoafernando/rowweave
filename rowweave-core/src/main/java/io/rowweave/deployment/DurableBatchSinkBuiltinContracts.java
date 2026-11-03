package io.rowweave.deployment;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.DurableBatchSinkProviderContract;
import io.rowweave.DurableSinkAttemptBudget;
import io.rowweave.DurableSinkCleanupContract;
import io.rowweave.DurableSinkCodecDescriptor;
import io.rowweave.DurableSinkProviderKey;

import java.util.Arrays;
import java.util.List;

/** Exact same-build contracts for the two Phase-1 core durable backends. */
final class DurableBatchSinkBuiltinContracts {
    private DurableBatchSinkBuiltinContracts() { }

    static List<DurableBatchSinkProviderContract> all() {
        return Arrays.asList(discard(), manifest());
    }

    private static DurableBatchSinkProviderContract discard() {
        return contract(
                new DurableSinkProviderKey("rowweave.discard", 1,
                        BatchSinkDescriptor.CapabilityProfile.SIDE_EFFECT_FREE_V1),
                new DurableSinkCodecDescriptor("rowweave.discard-config", 1, 1),
                new DurableSinkCodecDescriptor("rowweave.discard-locator", 1, 1));
    }

    static DurableBatchSinkProviderContract manifest() {
        return contract(
                new DurableSinkProviderKey("rowweave.manifest-file", 1,
                        BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1),
                new DurableSinkCodecDescriptor("rowweave.manifest-config", 1,
                        DurableBatchSinkProviderContract.MAX_CONFIGURATION_BYTES),
                new DurableSinkCodecDescriptor("rowweave.manifest-member", 1, 4096));
    }

    private static DurableBatchSinkProviderContract contract(
            DurableSinkProviderKey key, DurableSinkCodecDescriptor config,
            DurableSinkCodecDescriptor locator) {
        return new DurableBatchSinkProviderContract(
                DurableBatchSinkProviderContract.CURRENT_SPI_VERSION,
                key, config, locator,
                new DurableSinkCodecDescriptor(
                        "rowweave.batch-sink-outcome", 1, 1024),
                DurableBatchSinkProviderContract.IdempotencyScope
                        .ATTEMPT_AND_PUBLICATION_V1,
                new DurableSinkCleanupContract(
                        60_000L, 16, 8, true,
                        DurableBatchSinkProviderContract.OrphanOwnership
                                .ATTEMPT_PRIVATE_V1,
                        DurableBatchSinkProviderContract.QuarantinePolicy
                                .PRESERVE_ON_UNKNOWN_OR_CONFLICT,
                        DurableBatchSinkProviderContract.SafeDeleteProof
                                .ABORT_DECISION_OR_UNREFERENCED_RETENTION_V1),
                new DurableSinkAttemptBudget(
                        1024L * 1024L, 8, 4096, 16 * 1024, 30_000L),
                DurableBatchSinkProviderContract.DeploymentCapability
                        .CORE_BUILTIN_V1);
    }
}
