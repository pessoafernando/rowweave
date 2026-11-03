package io.rowweave.cluster;

import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.incremental.CapturedTargetReceipt;
import io.rowweave.incremental.IncrementalCaptureRequest;
import io.rowweave.incremental.IncrementalFrontier;
import io.rowweave.incremental.IncrementalMaterializationResponse;
import io.rowweave.incremental.IncrementalMaterializedSplit;
import io.rowweave.incremental.IncrementalRunMode;
import io.rowweave.incremental.IncrementalSourceConfig;
import io.rowweave.incremental.IncrementalSourceProviderContract;
import io.rowweave.incremental.IncrementalSourceProviderContractCodec;
import io.rowweave.incremental.IncrementalSourceProviderKey;
import io.rowweave.deployment.IncrementalSourceProviderRegistryTest;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Master/Worker source actions need one canonical bounded wire. */
class IncrementalSourceActionWireTest {

    @Test
    void captureAndMaterializeCommandsRoundTripWithExactBindings() throws Exception {
        IncrementalSourceProviderContract contract = contract();
        IncrementalSourceConfig config = config(contract);
        IncrementalCaptureRequest capture = IncrementalCaptureRequest.normal(
                7L, digest("definition"), 0L, IncrementalFrontier.begin(),
                "trigger-1", IncrementalRunMode.SNAPSHOT_V1);
        IncrementalSourceActionCommand captureCommand =
                IncrementalSourceActionCommand.capture(contract, config, capture);
        byte[] captureBytes = IncrementalSourceActionCommandCodec.encode(
                captureCommand);
        assertEquals(captureCommand,
                IncrementalSourceActionCommandCodec.decode(captureBytes,
                        ignored -> { }));
        assertArrayEquals(captureBytes,
                IncrementalSourceActionCommandCodec.encode(
                        IncrementalSourceActionCommandCodec.decode(
                                captureBytes, ignored -> { })));

        CapturedTargetReceipt receipt = capture.bind(
                IncrementalSourceProviderContractCodec.digest(contract),
                new io.rowweave.incremental.IncrementalCaptureResponse(
                        IncrementalFrontier.int64Sequence(10L), bytes("snapshot")));
        IncrementalSourceActionCommand materialize =
                IncrementalSourceActionCommand.materialize(
                        contract, config, receipt);
        byte[] materializeBytes = IncrementalSourceActionCommandCodec.encode(
                materialize);
        assertEquals(materialize,
                IncrementalSourceActionCommandCodec.decode(
                        materializeBytes, ignored -> { }));

        IncrementalSourceConfig drift = new IncrementalSourceConfig(
                config.providerKey(), new byte[32], config.configurationBytes());
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalSourceActionCommand.capture(
                        contract, drift, capture));
    }

    @Test
    void actionResultsRoundTripAndChargeBeforeVariableAllocation() throws Exception {
        IncrementalSourceProviderContract contract = contract();
        IncrementalCaptureRequest capture = IncrementalCaptureRequest.normal(
                7L, digest("definition"), 0L, IncrementalFrontier.begin(),
                "trigger-1", IncrementalRunMode.SNAPSHOT_V1);
        CapturedTargetReceipt receipt = capture.bind(
                IncrementalSourceProviderContractCodec.digest(contract),
                new io.rowweave.incremental.IncrementalCaptureResponse(
                        IncrementalFrontier.int64Sequence(10L), bytes("snapshot")));

        IncrementalSourceActionResult captured =
                IncrementalSourceActionResult.captured(receipt);
        byte[] capturedBytes = IncrementalSourceActionResultCodec.encode(captured);
        AtomicLong captureReservation = new AtomicLong();
        assertEquals(captured, IncrementalSourceActionResultCodec.decode(
                capturedBytes, captureReservation::addAndGet));
        assertTrue(captureReservation.get() >= capturedBytes.length);

        IncrementalMaterializationResponse response = materialization(
                contract.providerKey(), receipt);
        IncrementalSourceActionResult materialized =
                IncrementalSourceActionResult.materialized(response);
        byte[] materializedBytes = IncrementalSourceActionResultCodec.encode(
                materialized);
        AtomicLong materializationReservation = new AtomicLong();
        assertEquals(materialized, IncrementalSourceActionResultCodec.decode(
                materializedBytes, materializationReservation::addAndGet));
        assertTrue(materializationReservation.get() >= materializedBytes.length);

        byte[] corrupt = materializedBytes.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalSourceActionResultCodec.decode(
                        corrupt, ignored -> { }));
        assertThrows(ReservationRejected.class,
                () -> IncrementalSourceActionResultCodec.decode(
                        materializedBytes, ignored -> {
                            throw new ReservationRejected();
                        }));
    }

    private static IncrementalSourceProviderContract contract() {
        return IncrementalSourceProviderRegistryTest.contractForProvider();
    }

    private static IncrementalSourceConfig config(
            IncrementalSourceProviderContract contract) {
        return new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract),
                bytes("config"));
    }

    private static IncrementalMaterializationResponse materialization(
            IncrementalSourceProviderKey key, CapturedTargetReceipt receipt)
            throws Exception {
        String snapshotId = hex(digest(receipt.snapshotIdentity()));
        BatchSourceSplitDescriptor descriptor = new BatchSourceSplitDescriptor(
                BatchSourceSplitDescriptor.CURRENT_VERSION,
                key.connectorType(), key.connectorVersion(), "p0",
                hex(digest("schema")), snapshotId,
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);
        return new IncrementalMaterializationResponse(
                key, receipt, digest("partitions"), Collections.singletonList(
                new IncrementalMaterializedSplit(descriptor, bytes("open"))));
    }

    private static byte[] digest(String value) throws Exception {
        return digest(bytes(value));
    }

    private static byte[] digest(byte[] value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(value);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String hex(byte[] value) {
        StringBuilder result = new StringBuilder(value.length * 2);
        for (byte item : value) {
            result.append(Character.forDigit((item >>> 4) & 0xf, 16));
            result.append(Character.forDigit(item & 0xf, 16));
        }
        return result.toString();
    }

    private static final class ReservationRejected extends RuntimeException { }
}
