package io.rowweave;

import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.sched.stream.SourceRecordPosition;

import java.io.InvalidObjectException;
import java.io.ObjectStreamException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;

/**
 * Engine-owned description for one Object Storage row-quality split.
 * Under-limit record decoding is available; oversize resynchronization remains closed until OR3.
 */
public final class ObjectStorageQualitySource implements BatchExecutableSource {

    private static final long serialVersionUID = 1L;

    static final String CARRIER_VERSION = "1";
    static final String VALID_ROUTE = "VALID";
    static final String REJECTED_ROUTE = "REJECTED";
    static final int MAX_REASON_BUCKETS = 4;
    private static final int QUALITY_CONNECTOR_VERSION = 1;
    private static final String CONNECTOR_PREFIX =
            "rowweave.object-storage-row-quality/";

    private final ObjectStorageSnapshotSource originalSource;
    private final String sourceOperatorUid;
    private final String originalSplitIdentitySha256;
    private final DataSchema dataSchema;
    private final BatchSourceSplitDescriptor batchSourceSplit;

    ObjectStorageQualitySource(
            String sourceOperatorUid, ObjectStorageSnapshotSource originalSource) {
        if (originalSource == null) {
            throw invalid("Object Storage input is required");
        }
        BatchSourceSplitDescriptor originalDescriptor = originalSource.batchSourceSplit();
        SourceRecordPosition.forSplit(sourceOperatorUid, originalDescriptor);
        DataSchema carrier = carrierSchema(originalSource.getSchema());
        this.originalSource = originalSource;
        this.sourceOperatorUid = sourceOperatorUid;
        this.originalSplitIdentitySha256 = lowercaseHex(
                BatchSourceSplitDescriptorCodec.digest(originalDescriptor));
        this.dataSchema = carrier;
        this.batchSourceSplit = new BatchSourceSplitDescriptor(
                BatchSourceSplitDescriptor.CURRENT_VERSION,
                CONNECTOR_PREFIX + originalSource.providerType(),
                QUALITY_CONNECTOR_VERSION,
                digest("quality-split", originalDescriptor),
                SchemaFingerprint.of(carrier),
                digest("quality-snapshot", originalDescriptor),
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);
    }

    @Override
    public DataSchema getSchema() {
        return dataSchema;
    }

    @Override
    public BatchSourceSplitDescriptor batchSourceSplit() {
        BatchSourceSplitDescriptor current = originalSource.batchSourceSplit();
        String currentIdentity = lowercaseHex(
                BatchSourceSplitDescriptorCodec.digest(current));
        if (!originalSplitIdentitySha256.equals(currentIdentity)) {
            throw invalid("original Object Storage split identity changed");
        }
        return batchSourceSplit;
    }

    @Override
    public SourceExecutor<?> createSourceExecutor() {
        batchSourceSplit();
        return new ObjectStorageQualityInputExecutor(this);
    }

    @Override
    public Iterator<DataRow> iterator() {
        throw new UnsupportedOperationException(
                "ObjectStorageQualitySource requires its managed executor");
    }

    /** Engine admission view; contains no object locator or credential material. */
    public String sourceOperatorUid() {
        return sourceOperatorUid;
    }

    String originalSplitIdentitySha256() {
        return originalSplitIdentitySha256;
    }

    /** Engine admission view for re-proving the frozen built-in wrapper contract. */
    public ObjectStorageSnapshotSource originalSource() {
        return originalSource;
    }

    static DataSchema carrierSchema(DataSchema inputMeta) {
        if (inputMeta == null) {
            throw invalid("input schema is required");
        }
        if (inputMeta.getFieldCount() + 8 > io.rowweave.ops.RowValidation.MAX_FIELDS) {
            throw invalid("carrier field count exceeds the bounded field limit");
        }
        Field[] fields = new Field[inputMeta.getFieldCount() + 8];
        System.arraycopy(inputMeta.getFields(), 0, fields, 0, inputMeta.getFieldCount());
        int offset = inputMeta.getFieldCount();
        fields[offset] = new Field("quality_route_tag", DataType.IntegerType);
        System.arraycopy(rejectedSchema().getFields(), 0, fields, offset + 1, 7);
        return new DataSchema(fields);
    }

    static DataSchema rejectedSchema() {
        return new DataSchema(
                new Field("source_operator_uid", DataType.StringType),
                new Field("split_identity_sha256", DataType.StringType),
                new Field("split_local_ordinal", DataType.LongType),
                new Field("quality_error_code", DataType.StringType),
                new Field("quality_rule_id", DataType.StringType),
                new Field("quality_field_index", DataType.IntegerType),
                new Field("quality_detail", DataType.StringType));
    }

    private Object readResolve() throws ObjectStreamException {
        try {
            ObjectStorageQualitySource validated =
                    new ObjectStorageQualitySource(sourceOperatorUid, originalSource);
            if (!originalSplitIdentitySha256.equals(
                    validated.originalSplitIdentitySha256)
                    || !batchSourceSplit.equals(validated.batchSourceSplit)) {
                throw invalid("derived quality split identity mismatch");
            }
            return validated;
        } catch (RuntimeException failure) {
            InvalidObjectException rejected = new InvalidObjectException(
                    "invalid serialized ObjectStorageQualitySource");
            rejected.initCause(failure);
            throw rejected;
        }
    }

    @Override
    public String toString() {
        return "ObjectStorageQualitySource{split="
                + originalSplitIdentitySha256.substring(0, 16) + '}';
    }

    private static String digest(
            String domain, BatchSourceSplitDescriptor descriptor) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(domain.getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
            digest.update(BatchSourceSplitDescriptorCodec.encode(descriptor));
            return lowercaseHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static String lowercaseHex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(Character.forDigit((value >>> 4) & 0xf, 16));
            result.append(Character.forDigit(value & 0xf, 16));
        }
        return result.toString();
    }

    static IllegalArgumentException invalid(String detail) {
        return new IllegalArgumentException("OBJECT_QUALITY_INVALID: " + detail);
    }

}
