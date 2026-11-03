package io.rowweave.incremental;

import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.BatchSourceSplitDescriptorCodec;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Canonical bounded metadata returned by materializing one exact interval. */
public final class IncrementalMaterializationResponse {
    public static final int DIGEST_BYTES = 32;
    public static final int MAX_SPLITS = 4_096;
    public static final int MAX_SPLIT_METADATA_BYTES = 4 * 1024 * 1024;

    private final IncrementalSourceProviderKey providerKey;
    private final CapturedTargetReceipt capturedTarget;
    private final byte[] captureDigest;
    private final byte[] partitionSetDigest;
    private final List<IncrementalMaterializedSplit> splits;

    public IncrementalMaterializationResponse(
            IncrementalSourceProviderKey providerKey,
            CapturedTargetReceipt capturedTarget,
            byte[] partitionSetDigest,
            List<IncrementalMaterializedSplit> splits) {
        if (providerKey == null || capturedTarget == null
                || partitionSetDigest == null
                || partitionSetDigest.length != DIGEST_BYTES
                || splits == null || splits.isEmpty()
                || splits.size() > MAX_SPLITS) {
            throw new IllegalArgumentException(
                    "incremental materialization fields are invalid");
        }
        String snapshotId = hex(sha256(capturedTarget.snapshotIdentity()));
        String schema = null;
        long metadataBytes = 0L;
        Set<String> splitIds = new HashSet<>();
        List<IncrementalMaterializedSplit> copied =
                new ArrayList<>(splits.size());
        for (IncrementalMaterializedSplit split : splits) {
            if (split == null) {
                throw new IllegalArgumentException(
                        "incremental materialization split is required");
            }
            BatchSourceSplitDescriptor descriptor = split.descriptor();
            if (!providerKey.connectorType().equals(
                    descriptor.connectorType())
                    || providerKey.connectorVersion()
                    != descriptor.connectorVersion()) {
                throw new IllegalArgumentException(
                        "incremental split provider identity differs");
            }
            if (!snapshotId.equals(descriptor.snapshotId())) {
                throw new IllegalArgumentException(
                        "incremental split snapshot identity differs");
            }
            if (!splitIds.add(descriptor.splitId())) {
                throw new IllegalArgumentException(
                        "incremental split identity is duplicated");
            }
            if (schema == null) {
                schema = descriptor.schemaFingerprint();
            } else if (!schema.equals(descriptor.schemaFingerprint())) {
                throw new IllegalArgumentException(
                        "incremental split schemas differ");
            }
            metadataBytes = addExact(metadataBytes,
                    BatchSourceSplitDescriptorCodec.encode(descriptor).length);
            metadataBytes = addExact(metadataBytes, split.openRequestSize());
            metadataBytes = addExact(metadataBytes, 2L * Integer.BYTES);
            if (metadataBytes > MAX_SPLIT_METADATA_BYTES) {
                throw new IllegalArgumentException(
                        "incremental split metadata exceeds aggregate bound");
            }
            copied.add(split);
        }
        this.providerKey = providerKey;
        this.capturedTarget = capturedTarget;
        this.captureDigest = CapturedTargetReceiptCodec.digest(capturedTarget);
        this.partitionSetDigest = partitionSetDigest.clone();
        this.splits = Collections.unmodifiableList(copied);
    }

    public IncrementalSourceProviderKey providerKey() { return providerKey; }
    public CapturedTargetReceipt capturedTarget() { return capturedTarget; }
    public byte[] captureDigest() { return captureDigest.clone(); }
    public byte[] partitionSetDigest() { return partitionSetDigest.clone(); }
    public List<IncrementalMaterializedSplit> splits() { return splits; }

    private static long addExact(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "incremental split metadata length overflows", overflow);
        }
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static String hex(byte[] value) {
        StringBuilder result = new StringBuilder(value.length * 2);
        for (byte b : value) {
            result.append(Character.forDigit((b >>> 4) & 0xf, 16));
            result.append(Character.forDigit(b & 0xf, 16));
        }
        return result.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalMaterializationResponse)) return false;
        IncrementalMaterializationResponse that =
                (IncrementalMaterializationResponse) other;
        return providerKey.equals(that.providerKey)
                && capturedTarget.equals(that.capturedTarget)
                && Arrays.equals(partitionSetDigest, that.partitionSetDigest)
                && splits.equals(that.splits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(providerKey, capturedTarget,
                Arrays.hashCode(partitionSetDigest), splits);
    }
}
