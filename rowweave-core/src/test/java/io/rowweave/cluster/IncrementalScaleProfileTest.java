package io.rowweave.cluster;

import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.BatchSourceSplitDescriptorCodec;
import io.rowweave.incremental.CapturedTargetReceipt;
import io.rowweave.incremental.IncrementalFrontier;
import io.rowweave.incremental.IncrementalMaterializationResponse;
import io.rowweave.incremental.IncrementalMaterializationResponseCodec;
import io.rowweave.incremental.IncrementalMaterializedSplit;
import io.rowweave.incremental.IncrementalRunMode;
import io.rowweave.incremental.IncrementalSourceActionBudget;
import io.rowweave.incremental.IncrementalSourceProviderKey;
import io.rowweave.cluster.rpc.RpcAdmissionConfig;
import io.rowweave.cluster.rpc.RpcCodec;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.internal.IncrementalSourceControlLedger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** maximum incremental metadata and RPC profile. */
@Tag("profile")
class IncrementalScaleProfileTest {

    @Test
    void maximumSplitCountAndAggregateMetadataFitLargeLaneExactly()
            throws Exception {
        CapturedTargetReceipt receipt = receipt();
        String snapshot = hex(sha256(receipt.snapshotIdentity()));
        List<BatchSourceSplitDescriptor> descriptors = new ArrayList<>(
                IncrementalMaterializationResponse.MAX_SPLITS);
        long fixedMetadataBytes = 0L;
        for (int index = 0;
             index < IncrementalMaterializationResponse.MAX_SPLITS; index++) {
            BatchSourceSplitDescriptor descriptor =
                    new BatchSourceSplitDescriptor(
                            BatchSourceSplitDescriptor.CURRENT_VERSION,
                            "test.incremental-profile", 1,
                            String.format("split-%04d", index),
                            hex(digest("schema")), snapshot,
                            BatchSourceSplitDescriptor.ReplayGuarantee
                                    .EXACT_ORDERED_FROM_START);
            descriptors.add(descriptor);
            fixedMetadataBytes += BatchSourceSplitDescriptorCodec
                    .encode(descriptor).length + 2L * Integer.BYTES;
        }
        long requestBytes = IncrementalMaterializationResponse
                .MAX_SPLIT_METADATA_BYTES - fixedMetadataBytes;
        assertTrue(requestBytes
                >= IncrementalMaterializationResponse.MAX_SPLITS);

        List<IncrementalMaterializedSplit> splits = new ArrayList<>(
                IncrementalMaterializationResponse.MAX_SPLITS);
        long distributed = 0L;
        for (int index = 0; index < descriptors.size(); index++) {
            int remainingSplits = descriptors.size() - index;
            int next = (int) ((requestBytes - distributed)
                    / remainingSplits);
            splits.add(new IncrementalMaterializedSplit(
                    descriptors.get(index), new byte[next]));
            distributed += next;
        }
        assertEquals(requestBytes, distributed);

        IncrementalMaterializationResponse response =
                new IncrementalMaterializationResponse(
                        new IncrementalSourceProviderKey(
                                "test.incremental-profile", 1),
                        receipt, digest("partitions"), splits);
        byte[] materialization =
                IncrementalMaterializationResponseCodec.encode(response);
        AtomicLong materializationResident = new AtomicLong();
        assertEquals(response,
                IncrementalMaterializationResponseCodec.decode(
                        materialization,
                        materializationResident::addAndGet));
        assertEquals(3L * materialization.length,
                materializationResident.get());
        assertTrue(materializationResident.get()
                <= IncrementalSourceActionBudget.MAX_LARGE_RESIDENT_BYTES);

        byte[] result = IncrementalSourceActionResultCodec.encode(
                IncrementalSourceActionResult.materialized(response));
        AtomicLong resultResident = new AtomicLong();
        assertEquals(IncrementalSourceActionResult.materialized(response),
                IncrementalSourceActionResultCodec.decode(
                        result, resultResident::addAndGet));
        assertEquals(3L * result.length, resultResident.get());
        assertTrue(result.length
                <= IncrementalSourceActionResultCodec.MAX_FRAME_BYTES);

        byte[] rpc = RpcCodec.encode(
                RpcMessage.incrementalSourceActionResult(result));
        assertTrue(rpc.length <= RpcAdmissionConfig.LARGE_FRAME_BYTES);
        assertEquals(64, IncrementalSourceControlLedger.MAX_NORMAL_ACTIONS);
        assertEquals(8, IncrementalSourceControlLedger.MAX_LARGE_ACTIONS);
        assertEquals(64L * 1024L * 1024L,
                IncrementalSourceControlLedger.MAX_NORMAL_RESIDENT_BYTES);
        assertEquals(192L * 1024L * 1024L,
                IncrementalSourceControlLedger.MAX_LARGE_RESIDENT_BYTES);

        System.out.printf("[CDC-SOURCE-PROFILE] splits=%d metadata=%dB "
                        + "materialization=%dB materializationResident=%dB "
                        + "result=%dB resultResident=%dB rpc=%dB "
                        + "normalActions=%d largeActions=%d%n",
                splits.size(),
                IncrementalMaterializationResponse.MAX_SPLIT_METADATA_BYTES,
                materialization.length, materializationResident.get(),
                result.length, resultResident.get(), rpc.length,
                IncrementalSourceControlLedger.MAX_NORMAL_ACTIONS,
                IncrementalSourceControlLedger.MAX_LARGE_ACTIONS);
    }

    private static CapturedTargetReceipt receipt() throws Exception {
        return CapturedTargetReceipt.normal(
                1L, digest("definition"), digest("source-contract"),
                0L, IncrementalFrontier.begin(), "profile-trigger",
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
        for (byte next : value) {
            result.append(Character.forDigit((next >>> 4) & 0xf, 16));
            result.append(Character.forDigit(next & 0xf, 16));
        }
        return result.toString();
    }
}
