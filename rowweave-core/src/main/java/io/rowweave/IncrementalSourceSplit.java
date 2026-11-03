package io.rowweave;

import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.incremental.CapturedTargetReceipt;
import io.rowweave.incremental.CapturedTargetReceiptCodec;
import io.rowweave.incremental.IncrementalMaterializedSplit;
import io.rowweave.incremental.IncrementalPlanDescriptor;
import io.rowweave.incremental.IncrementalPlanDescriptorCodec;
import io.rowweave.incremental.IncrementalSourceConfig;

import java.io.ObjectStreamException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;

/**
 * Inert durable carrier for one exact incremental source split. Provider code is
 * deliberately absent; a Worker must verify and replace this proxy before open.
 */
public final class IncrementalSourceSplit implements BatchSource {
    private static final long serialVersionUID = 1L;

    public static final String RUNTIME_REQUIRED =
            "INCREMENTAL_SOURCE_RUNTIME_REQUIRED";

    private final DataSchema dataSchema;
    private final IncrementalSourceConfig configuration;
    private final byte[] capturedTargetFrame;
    private final byte[] partitionSetDigest;
    private final int partitionOrdinal;
    private final byte[] planDescriptorFrame;
    private final BatchSourceSplitDescriptor descriptor;
    private final byte[] openRequest;

    public IncrementalSourceSplit(
            DataSchema dataSchema, IncrementalSourceConfig configuration,
            CapturedTargetReceipt capturedTarget, byte[] partitionSetDigest,
            IncrementalMaterializedSplit split) {
        this(dataSchema, configuration, capturedTarget, partitionSetDigest,
                split, null, 0);
    }

    public IncrementalSourceSplit(
            DataSchema dataSchema, IncrementalSourceConfig configuration,
            CapturedTargetReceipt capturedTarget, byte[] partitionSetDigest,
            IncrementalMaterializedSplit split,
            IncrementalPlanDescriptor planDescriptor) {
        this(dataSchema, configuration, capturedTarget, partitionSetDigest,
                split, planDescriptor, 0);
    }

    public IncrementalSourceSplit(
            DataSchema dataSchema, IncrementalSourceConfig configuration,
            CapturedTargetReceipt capturedTarget, byte[] partitionSetDigest,
            IncrementalMaterializedSplit split,
            IncrementalPlanDescriptor planDescriptor,
            int partitionOrdinal) {
        if (dataSchema == null || configuration == null || capturedTarget == null
                || partitionSetDigest == null || partitionSetDigest.length != 32
                || split == null || partitionOrdinal < 0
                || partitionOrdinal
                > io.rowweave.incremental.IncrementalRecordPosition
                .MAX_PARTITION_ORDINAL) {
            throw new IllegalArgumentException(
                    "incremental source split binding fields are invalid");
        }
        BatchSourceSplitDescriptor frozen = split.descriptor();
        if (!configuration.providerKey().connectorType().equals(
                frozen.connectorType())
                || configuration.providerKey().connectorVersion()
                != frozen.connectorVersion()) {
            throw new IllegalArgumentException(
                    "incremental source split provider differs from configuration");
        }
        if (!SchemaFingerprint.of(dataSchema).equals(
                frozen.schemaFingerprint())) {
            throw new IllegalArgumentException(
                    "incremental source split schema differs from DataSchema");
        }
        if (!snapshotId(capturedTarget.snapshotIdentity()).equals(
                frozen.snapshotId())) {
            throw new IllegalArgumentException(
                    "incremental source split snapshot differs from capture");
        }
        if (!java.security.MessageDigest.isEqual(
                configuration.contractDigest(),
                capturedTarget.sourceContractDigest())) {
            throw new IllegalArgumentException(
                    "incremental source split contract differs from capture");
        }
        this.dataSchema = dataSchema;
        this.configuration = configuration;
        this.capturedTargetFrame =
                CapturedTargetReceiptCodec.encode(capturedTarget);
        this.partitionSetDigest = partitionSetDigest.clone();
        this.partitionOrdinal = partitionOrdinal;
        this.planDescriptorFrame = planDescriptor == null ? null
                : IncrementalPlanDescriptorCodec.encode(planDescriptor);
        this.descriptor = frozen;
        this.openRequest = split.openRequest();
    }

    @Override
    public DataSchema getSchema() { return dataSchema; }

    @Override
    public BatchSourceSplitDescriptor batchSourceSplit() { return descriptor; }

    public IncrementalSourceConfig configuration() { return configuration; }

    public CapturedTargetReceipt capturedTarget() {
        return CapturedTargetReceiptCodec.decode(capturedTargetFrame);
    }

    public byte[] capturedTargetFrame() { return capturedTargetFrame.clone(); }

    public byte[] partitionSetDigest() { return partitionSetDigest.clone(); }

    public int partitionOrdinal() { return partitionOrdinal; }

    public IncrementalPlanDescriptor planDescriptor() {
        return planDescriptorFrame == null ? null
                : IncrementalPlanDescriptorCodec.decode(planDescriptorFrame);
    }

    public byte[] openRequest() { return openRequest.clone(); }

    public int openRequestSize() { return openRequest.length; }

    @Override
    public Iterator<DataRow> iterator() {
        throw new IllegalStateException(RUNTIME_REQUIRED);
    }

    private Object readResolve() throws ObjectStreamException {
        return new IncrementalSourceSplit(
                dataSchema, configuration,
                CapturedTargetReceiptCodec.decode(capturedTargetFrame),
                partitionSetDigest,
                new IncrementalMaterializedSplit(descriptor, openRequest),
                planDescriptor(), partitionOrdinal);
    }

    private static String snapshotId(byte[] identity) {
        final byte[] digest;
        try {
            digest = MessageDigest.getInstance("SHA-256").digest(identity);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
        char[] encoded = new char[digest.length * 2];
        char[] digits = "0123456789abcdef".toCharArray();
        for (int index = 0; index < digest.length; index++) {
            int value = digest[index] & 0xff;
            encoded[index * 2] = digits[value >>> 4];
            encoded[index * 2 + 1] = digits[value & 0xf];
        }
        return new String(encoded);
    }
}
