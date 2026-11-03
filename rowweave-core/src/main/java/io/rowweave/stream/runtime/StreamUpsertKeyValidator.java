package io.rowweave.stream.runtime;

import io.rowweave.DataRow;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Exact edge-level validation for canonical UPSERT identity frames. */
public final class StreamUpsertKeyValidator {
    private final RowSerializer rowSerializer;
    private final RowSerializer keySerializer;
    private final int[] keyIndices;
    private final int maximumFrameBytes;

    public StreamUpsertKeyValidator(PortableRowSchema schema,
            List<String> stableKeyFields, int maximumFrameBytes) {
        if (schema == null || stableKeyFields == null
                || stableKeyFields.isEmpty() || maximumFrameBytes < 1) {
            throw new IllegalArgumentException(
                    "UPSERT edge key contract is incomplete");
        }
        this.keyIndices = indices(schema, stableKeyFields);
        this.rowSerializer = new RowSerializer(
                PortablePlanCompiler.toDataSchema(schema));
        this.keySerializer = new RowSerializer(
                PortablePlanCompiler.toDataSchema(keySchema(
                        schema, keyIndices)));
        this.maximumFrameBytes = maximumFrameBytes;
    }

    public StreamUpsertRecord validate(byte[] frame) {
        StreamUpsertRecord record = StreamUpsertRecordCodec.decode(
                frame, maximumFrameBytes, maximumFrameBytes);
        byte[] stableKey = record.stableKeyFrame();
        keySerializer.deserialize(stableKey);
        if (record.hasAfterImage()) {
            DataRow row = rowSerializer.deserialize(record.afterImageFrame());
            byte[] projected = keySerializer.serializeProjected(
                    row, keyIndices);
            if (!MessageDigest.isEqual(stableKey, projected)) {
                throw new IllegalArgumentException(
                        "UPSERT stable key differs from its after-image");
            }
        }
        return record;
    }

    private static int[] indices(PortableRowSchema schema,
            List<String> fields) {
        int[] result = new int[fields.size()];
        Set<String> seen = new HashSet<>();
        for (int keyIndex = 0; keyIndex < fields.size(); keyIndex++) {
            String fieldId = fields.get(keyIndex);
            if (!seen.add(fieldId)) {
                throw new IllegalArgumentException(
                        "UPSERT edge key fields must be unique");
            }
            int match = -1;
            for (int schemaIndex = 0;
                    schemaIndex < schema.fields().size(); schemaIndex++) {
                if (schema.fields().get(schemaIndex).fieldId()
                        .equals(fieldId)) {
                    match = schemaIndex;
                    break;
                }
            }
            if (match < 0) {
                throw new IllegalArgumentException(
                        "UPSERT edge key is absent from its schema");
            }
            result[keyIndex] = match;
        }
        return result;
    }

    private static PortableRowSchema keySchema(PortableRowSchema schema,
            int[] indices) {
        List<PortableField> fields = new ArrayList<>(indices.length);
        for (int index : indices) fields.add(schema.fields().get(index));
        return new PortableRowSchema(fields);
    }
}
