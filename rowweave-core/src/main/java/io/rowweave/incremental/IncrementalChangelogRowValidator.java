package io.rowweave.incremental;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.mem.RowSerializer;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/** O(1) per-split validator for the first bounded UPSERT changelog profile. */
public final class IncrementalChangelogRowValidator {
    private final DataSchema dataSchema;
    private final IncrementalPlanDescriptor descriptor;
    private final CapturedTargetReceipt capturedTarget;
    private final int[] keyIndexes;
    private final RowSerializer keySerializer;
    private final byte[] intervalDigest;
    private final int partitionOrdinal;
    private IncrementalRecordPosition previous;

    public IncrementalChangelogRowValidator(
            DataSchema dataSchema, IncrementalPlanDescriptor descriptor,
            CapturedTargetReceipt capturedTarget, byte[] partitionSetDigest,
            int partitionOrdinal) {
        if (dataSchema == null || descriptor == null || capturedTarget == null
                || partitionSetDigest == null
                || partitionOrdinal < 0
                || partitionOrdinal
                > IncrementalRecordPosition.MAX_PARTITION_ORDINAL) {
            throw invalid("incremental changelog binding is invalid");
        }
        if (!SchemaFingerprint.of(dataSchema).equals(
                hex(descriptor.outputSchemaFingerprint()))
                || !MessageDigest.isEqual(
                descriptor.partitionSetDigest(), partitionSetDigest)) {
            throw invalid("incremental changelog schema or partition differs");
        }
        requireField(dataSchema, descriptor.operationFieldIndex(),
                DataType.StringType, "operation");
        List<Integer> positions = descriptor.recordPositionFieldIndexes();
        if (positions.size() != 1) {
            throw invalid(
                    "INT64/snapshot changelog requires one position field");
        }
        requireField(dataSchema, positions.get(0), DataType.LongType,
                "record position");
        if (capturedTarget.target().kind()
                != IncrementalFrontier.Kind.INT64_SEQUENCE_V1
                || (capturedTarget.mode() == IncrementalRunMode.SNAPSHOT_V1
                && capturedTarget.from().kind()
                != IncrementalFrontier.Kind.BEGIN_V1)
                || (capturedTarget.mode() != IncrementalRunMode.SNAPSHOT_V1
                && capturedTarget.from().kind()
                != IncrementalFrontier.Kind.INT64_SEQUENCE_V1)) {
            throw invalid(
                    "local changelog validator only accepts INT64 intervals");
        }
        this.dataSchema = dataSchema;
        this.descriptor = descriptor;
        this.capturedTarget = capturedTarget;
        this.partitionOrdinal = partitionOrdinal;
        this.intervalDigest = decodeDigest(
                IncrementalIntervalIdentity.from(capturedTarget).intervalId());
        this.keyIndexes = indexes(descriptor.primaryKeyFieldIndexes());
        Field[] keyFields = new Field[keyIndexes.length];
        for (int index = 0; index < keyIndexes.length; index++) {
            keyFields[index] = dataSchema.getField(keyIndexes[index]);
        }
        this.keySerializer = new RowSerializer(new DataSchema(keyFields));
    }

    /** Validates and returns the same immutable-after-emit row reference. */
    public DataRow validate(DataRow row) {
        if (row == null || row.size() != dataSchema.getFieldCount()) {
            throw invalid("incremental changelog row width differs");
        }
        for (int keyIndex : keyIndexes) {
            if (row.get(keyIndex) == null) {
                throw invalid("incremental changelog primary key is null");
            }
        }
        Object operationValue = row.get(descriptor.operationFieldIndex());
        if (!(operationValue instanceof String)) {
            throw invalid("incremental changelog operation is invalid");
        }
        String operation = (String) operationValue;
        if (!"UPSERT".equals(operation) && !"DELETE".equals(operation)) {
            throw invalid("incremental changelog operation is unsupported");
        }
        if ("DELETE".equals(operation)
                && (capturedTarget.mode() == IncrementalRunMode.SNAPSHOT_V1
                || !descriptor.deleteCapable())) {
            throw invalid("incremental changelog DELETE is not admitted");
        }

        Object positionValue = row.get(
                descriptor.recordPositionFieldIndexes().get(0));
        if (!(positionValue instanceof Long) || (Long) positionValue < 0L) {
            throw invalid("incremental changelog position is invalid");
        }
        long scalar = (Long) positionValue;
        byte[] keyDigest = sha256(
                keySerializer.serializeProjected(row, keyIndexes));
        IncrementalRecordPosition position;
        if (capturedTarget.mode() == IncrementalRunMode.SNAPSHOT_V1) {
            position = IncrementalRecordPosition.snapshotRow(
                    intervalDigest, partitionOrdinal, scalar, keyDigest);
        } else {
            long from = capturedTarget.from().sequence();
            long target = capturedTarget.target().sequence();
            if (scalar <= from || scalar > target) {
                throw invalid(
                        "incremental changelog position is outside the interval");
            }
            position = IncrementalRecordPosition.sequence(
                    intervalDigest, partitionOrdinal, scalar, keyDigest);
        }
        if (previous != null
                && IncrementalRecordPositionOrder.compareWithinSplit(
                previous, position) >= 0) {
            throw invalid(
                    "incremental changelog position is not strictly increasing");
        }
        previous = position;
        return row;
    }

    private static void requireField(
            DataSchema dataSchema, int index, DataType type, String field) {
        if (index < 0 || index >= dataSchema.getFieldCount()
                || dataSchema.getField(index).getDataType() != type) {
            throw invalid(field + " field type is invalid");
        }
    }

    private static int[] indexes(List<Integer> values) {
        int[] indexes = new int[values.size()];
        for (int index = 0; index < indexes.length; index++) {
            indexes[index] = values.get(index);
        }
        return indexes;
    }

    private static byte[] decodeDigest(String value) {
        if (value == null || value.length() != 64) {
            throw invalid("interval identity is not SHA-256");
        }
        byte[] decoded = new byte[32];
        for (int index = 0; index < decoded.length; index++) {
            int high = Character.digit(value.charAt(index * 2), 16);
            int low = Character.digit(value.charAt(index * 2 + 1), 16);
            if (high < 0 || low < 0) {
                throw invalid("interval identity is not canonical hex");
            }
            decoded[index] = (byte) ((high << 4) | low);
        }
        if (!hex(decoded).equals(value)) {
            throw invalid("interval identity is not canonical hex");
        }
        return decoded;
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static String hex(byte[] value) {
        char[] chars = new char[value.length * 2];
        char[] digits = "0123456789abcdef".toCharArray();
        for (int index = 0; index < value.length; index++) {
            int unsigned = value[index] & 0xff;
            chars[index * 2] = digits[unsigned >>> 4];
            chars[index * 2 + 1] = digits[unsigned & 0xf];
        }
        return new String(chars);
    }

    private static IllegalArgumentException invalid(String detail) {
        return new IllegalArgumentException(detail);
    }
}
