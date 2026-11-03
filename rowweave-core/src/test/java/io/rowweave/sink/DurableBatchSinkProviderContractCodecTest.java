package io.rowweave.sink;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.DataType;
import io.rowweave.DurableBatchSinkProviderContract;
import io.rowweave.DurableBatchSinkProviderContractCodec;
import io.rowweave.DurableSinkAttemptBudget;
import io.rowweave.DurableSinkCleanupContract;
import io.rowweave.DurableSinkCodecDescriptor;
import io.rowweave.DurableSinkProviderKey;
import io.rowweave.Field;
import io.rowweave.ProviderBatchSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.Test;

import java.security.MessageDigest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** canonical provider contract and inert config carrier. */
class DurableBatchSinkProviderContractCodecTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));

    @Test
    void contractRoundTripIsCanonicalBoundedAndDigestStable() {
        DurableBatchSinkProviderContract contract = contract();
        byte[] encoded = DurableBatchSinkProviderContractCodec.encode(contract);
        assertTrue(encoded.length <= DurableBatchSinkProviderContractCodec.MAX_FRAME_BYTES);
        assertEquals(contract, DurableBatchSinkProviderContractCodec.decode(encoded));
        assertArrayEquals(sha256(encoded),
                DurableBatchSinkProviderContractCodec.digest(contract));
        assertArrayEquals(encoded, DurableBatchSinkProviderContractCodec.encode(
                DurableBatchSinkProviderContractCodec.decode(encoded)));

        byte[] corrupt = encoded.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> DurableBatchSinkProviderContractCodec.decode(corrupt));
        byte[] trailing = java.util.Arrays.copyOf(encoded, encoded.length + 1);
        assertThrows(IllegalArgumentException.class,
                () -> DurableBatchSinkProviderContractCodec.decode(trailing));
    }

    @Test
    void componentBoundsFailBeforeTheyCanEnterAContractFrame() {
        assertThrows(IllegalArgumentException.class,
                () -> new DurableSinkProviderKey("", 1,
                        BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1));
        assertThrows(IllegalArgumentException.class,
                () -> new DurableSinkCodecDescriptor("codec", 0, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new DurableSinkCodecDescriptor("codec", 1, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new DurableSinkAttemptBudget(0L, 1, 1, 1, 1L));
        assertThrows(IllegalArgumentException.class,
                () -> new DurableSinkAttemptBudget(1L, 0, 1, 1, 1L));
        assertThrows(IllegalArgumentException.class,
                () -> new DurableSinkCleanupContract(0L, 1, 1, true,
                        DurableBatchSinkProviderContract.OrphanOwnership.ATTEMPT_PRIVATE_V1,
                        DurableBatchSinkProviderContract.QuarantinePolicy.PRESERVE_ON_UNKNOWN_OR_CONFLICT,
                        DurableBatchSinkProviderContract.SafeDeleteProof
                                .ABORT_DECISION_OR_UNREFERENCED_RETENTION_V1));

        byte[] tooLarge = new byte[DurableBatchSinkProviderContractCodec.MAX_FRAME_BYTES + 1];
        assertThrows(IllegalArgumentException.class,
                () -> DurableBatchSinkProviderContractCodec.decode(tooLarge));
    }

    @Test
    void providerConfigCarrierIsExactFinalBoundedAndNeverActsAsLegacyOutput() {
        BatchSinkDescriptor descriptor = descriptor();
        DurableSinkCodecDescriptor configCodec =
                new DurableSinkCodecDescriptor("test.config", 1, 1024);
        ProviderBatchSink output = new ProviderBatchSink(
                META, descriptor, configCodec, new byte[]{1, 2, 3});

        assertTrue(java.lang.reflect.Modifier.isFinal(output.getClass().getModifiers()));
        assertEquals(META, output.getSchema());
        assertEquals(descriptor, output.batchSinkDescriptor());
        assertEquals(configCodec, output.configurationCodec());
        assertArrayEquals(new byte[]{1, 2, 3}, output.configurationBytes());
        byte[] copy = output.configurationBytes();
        copy[0] = 99;
        assertArrayEquals(new byte[]{1, 2, 3}, output.configurationBytes());
        assertFalse(output.supportsCoordinatedCommit());
        assertThrows(IllegalStateException.class,
                () -> output.collect(row(1)));
        assertThrows(IllegalStateException.class, output::prepareCommit);
        assertThrows(IllegalStateException.class, output::commitPrepared);
        assertThrows(IllegalStateException.class, output::rollbackCommit);
        assertThrows(IllegalStateException.class, output::finalizeCommit);
    }

    private static DurableBatchSinkProviderContract contract() {
        return new DurableBatchSinkProviderContract(
                DurableBatchSinkProviderContract.CURRENT_SPI_VERSION,
                new DurableSinkProviderKey("test.adversarial", 1,
                        BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1),
                new DurableSinkCodecDescriptor("test.config", 1, 1024),
                new DurableSinkCodecDescriptor("test.locator", 1, 4096),
                new DurableSinkCodecDescriptor("rowweave.batch-sink-outcome", 1, 1024),
                DurableBatchSinkProviderContract.IdempotencyScope
                        .ATTEMPT_AND_PUBLICATION_V1,
                new DurableSinkCleanupContract(60_000L, 16, 8, true,
                        DurableBatchSinkProviderContract.OrphanOwnership.ATTEMPT_PRIVATE_V1,
                        DurableBatchSinkProviderContract.QuarantinePolicy
                                .PRESERVE_ON_UNKNOWN_OR_CONFLICT,
                        DurableBatchSinkProviderContract.SafeDeleteProof
                                .ABORT_DECISION_OR_UNREFERENCED_RETENTION_V1),
                new DurableSinkAttemptBudget(1024L * 1024L, 8, 4096,
                        16 * 1024, 30_000L),
                DurableBatchSinkProviderContract.DeploymentCapability
                        .SIGNED_TEST_CONFORMANCE_V1);
    }

    private static BatchSinkDescriptor descriptor() {
        return new BatchSinkDescriptor(
                BatchSinkDescriptor.CURRENT_VERSION, "test.adversarial", 1,
                SchemaFingerprint.of(META), "test:destination", "test:domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
    }

    private static DataRow row(int id) {
        DataRow row = new DataRow(1);
        row.set(0, id);
        return row;
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
