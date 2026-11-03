package io.rowweave.incremental;

import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.BatchSourceSplitDescriptorCodec;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** materialization is bounded metadata bound to one capture. */
class IncrementalMaterializationResponseContractTest {

    @Test
    void twoSplitResponseRoundTripsCanonicallyAndDefensively() throws Exception {
        CapturedTargetReceipt receipt = receipt();
        IncrementalSourceProviderKey key =
                new IncrementalSourceProviderKey("test.embedded", 1);
        IncrementalMaterializationResponse response =
                new IncrementalMaterializationResponse(
                        key, receipt, digest("partitions"), Arrays.asList(
                        split(receipt, "p0", new byte[]{1, 2}),
                        split(receipt, "p1", new byte[]{3, 4})));

        byte[] encoded = IncrementalMaterializationResponseCodec.encode(response);
        assertTrue(encoded.length
                <= IncrementalMaterializationResponseCodec.MAX_FRAME_BYTES);
        assertEquals(response,
                IncrementalMaterializationResponseCodec.decode(encoded));
        assertArrayEquals(encoded, IncrementalMaterializationResponseCodec.encode(
                IncrementalMaterializationResponseCodec.decode(encoded)));
        assertArrayEquals(sha256(encoded),
                IncrementalMaterializationResponseCodec.digest(response));
        assertArrayEquals(CapturedTargetReceiptCodec.digest(receipt),
                response.captureDigest());

        byte[] request = response.splits().get(0).openRequest();
        request[0] = 99;
        assertArrayEquals(new byte[]{1, 2},
                response.splits().get(0).openRequest());
        assertThrows(UnsupportedOperationException.class,
                () -> response.splits().add(split(receipt, "p2", new byte[]{5})));

        byte[] corrupt = encoded.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalMaterializationResponseCodec.decode(corrupt));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalMaterializationResponseCodec.decode(
                        Arrays.copyOf(encoded, encoded.length + 1)));
    }

    @Test
    void splitSetRejectsEmptyDuplicateLegacyAndSnapshotDrift() throws Exception {
        CapturedTargetReceipt receipt = receipt();
        IncrementalSourceProviderKey key =
                new IncrementalSourceProviderKey("test.embedded", 1);
        byte[] partitions = digest("partitions");
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalMaterializationResponse(
                        key, receipt, partitions, Collections.emptyList()));
        IncrementalMaterializedSplit p0 =
                split(receipt, "p0", new byte[]{1});
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalMaterializationResponse(
                        key, receipt, partitions, Arrays.asList(p0, p0)));

        BatchSourceSplitDescriptor legacy = new BatchSourceSplitDescriptor(
                BatchSourceSplitDescriptor.CURRENT_VERSION,
                key.connectorType(), key.connectorVersion(), "legacy",
                hex(digest("schema")), snapshotId(receipt),
                BatchSourceSplitDescriptor.ReplayGuarantee
                        .LEGACY_SINGLE_ATTEMPT);
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalMaterializationResponse(
                        key, receipt, partitions,
                        Collections.singletonList(
                                new IncrementalMaterializedSplit(
                                        legacy, new byte[]{1}))));

        BatchSourceSplitDescriptor drift = new BatchSourceSplitDescriptor(
                BatchSourceSplitDescriptor.CURRENT_VERSION,
                key.connectorType(), key.connectorVersion(), "drift",
                hex(digest("schema")), hex(digest("wrong-snapshot")),
                BatchSourceSplitDescriptor.ReplayGuarantee
                        .EXACT_ORDERED_FROM_START);
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalMaterializationResponse(
                        key, receipt, partitions,
                        Collections.singletonList(
                                new IncrementalMaterializedSplit(
                                        drift, new byte[]{1}))));
    }

    @Test
    void providerIdentityAndAggregateMetadataAreBoundedBeforeAdmission()
            throws Exception {
        CapturedTargetReceipt receipt = receipt();
        IncrementalSourceProviderKey key =
                new IncrementalSourceProviderKey("test.embedded", 1);
        IncrementalMaterializedSplit wrongProvider =
                new IncrementalMaterializedSplit(
                        descriptor(receipt, "other", 1, "p0"),
                        new byte[]{1});
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalMaterializationResponse(
                        key, receipt, digest("partitions"),
                        Collections.singletonList(wrongProvider)));
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalMaterializedSplit(
                        descriptor(receipt, key.connectorType(),
                                key.connectorVersion(), "p0"),
                        new byte[IncrementalMaterializedSplit
                                .MAX_OPEN_REQUEST_BYTES + 1]));
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalMaterializationResponse(
                        key, receipt, new byte[31],
                        Collections.singletonList(
                                split(receipt, "p0", new byte[]{1}))));
    }

    @Test
    void largeResponseDecodeChargesResidentBytesBeforeAllocation()
            throws Exception {
        CapturedTargetReceipt receipt = receipt();
        IncrementalMaterializationResponse response =
                new IncrementalMaterializationResponse(
                        new IncrementalSourceProviderKey("test.embedded", 1),
                        receipt, digest("partitions"),
                        Collections.singletonList(
                                split(receipt, "p0", new byte[128 * 1024])));
        byte[] frame = IncrementalMaterializationResponseCodec.encode(response);
        AtomicLong charged = new AtomicLong();
        assertEquals(response, IncrementalMaterializationResponseCodec.decode(
                frame, charged::addAndGet));
        assertTrue(charged.get() >= frame.length,
                "decode must charge all variable-size allocations");

        assertThrows(ReservationRejected.class,
                () -> IncrementalMaterializationResponseCodec.decode(
                        frame, ignored -> {
                            throw new ReservationRejected();
                        }));
    }

    private static IncrementalMaterializedSplit split(
            CapturedTargetReceipt receipt, String splitId, byte[] request)
            throws Exception {
        return new IncrementalMaterializedSplit(
                descriptor(receipt, "test.embedded", 1, splitId), request);
    }

    private static BatchSourceSplitDescriptor descriptor(
            CapturedTargetReceipt receipt, String providerType,
            int providerVersion, String splitId) throws Exception {
        return new BatchSourceSplitDescriptor(
                BatchSourceSplitDescriptor.CURRENT_VERSION,
                providerType, providerVersion, splitId,
                hex(digest("schema")), snapshotId(receipt),
                BatchSourceSplitDescriptor.ReplayGuarantee
                        .EXACT_ORDERED_FROM_START);
    }

    private static String snapshotId(CapturedTargetReceipt receipt)
            throws Exception {
        return hex(sha256(receipt.snapshotIdentity()));
    }

    private static CapturedTargetReceipt receipt() throws Exception {
        return CapturedTargetReceipt.normal(
                1L, digest("definition"), digest("source-contract"),
                0L, IncrementalFrontier.begin(), "trigger",
                IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(10L),
                "snapshot-token".getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] digest(String value) throws Exception {
        return sha256(value.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] sha256(byte[] value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(value);
    }

    private static String hex(byte[] value) {
        StringBuilder result = new StringBuilder(value.length * 2);
        for (byte b : value) {
            result.append(Character.forDigit((b >>> 4) & 0xf, 16));
            result.append(Character.forDigit(b & 0xf, 16));
        }
        return result.toString();
    }

    private static final class ReservationRejected extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
