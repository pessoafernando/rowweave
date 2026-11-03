package io.rowweave.stream.api;

import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;

import java.security.MessageDigest;

/** Public bounded decoder for canonical Stream UPSERT records. */
public final class StreamUpsertCodec {
    private static final int MAX_FRAME_BYTES = 4 * 1024 * 1024;
    private final StreamDataRowCodec keyCodec;
    private final StreamDataRowCodec valueCodec;
    private final int[] valueKeyIndices;

    public StreamUpsertCodec(StreamSchema keySchema, StreamSchema valueSchema) {
        this.keyCodec = new StreamDataRowCodec(keySchema);
        this.valueCodec = new StreamDataRowCodec(valueSchema);
        this.valueKeyIndices = keyIndices(keySchema, valueSchema);
    }

    public StreamUpsertValue decode(byte[] frame) {
        StreamUpsertRecord record = StreamUpsertRecordCodec.decode(
                frame, MAX_FRAME_BYTES, MAX_FRAME_BYTES);
        if (!record.hasAfterImage()) {
            throw new IllegalArgumentException(
                    "UPSERT tombstone has no after-image value");
        }
        io.rowweave.DataRow stableKey = keyCodec.decode(
                record.stableKeyFrame());
        io.rowweave.DataRow afterImage = valueCodec.decode(
                record.afterImageFrame());
        requireMatchingKey(stableKey, afterImage);
        return new StreamUpsertValue(stableKey, afterImage);
    }

    public byte[] encodeAfterImage(
            io.rowweave.DataRow stableKey, io.rowweave.DataRow afterImage) {
        if (stableKey == null || afterImage == null) {
            throw new IllegalArgumentException(
                    "UPSERT key and after-image are required");
        }
        requireMatchingKey(stableKey, afterImage);
        return StreamUpsertRecordCodec.encode(StreamUpsertRecord.afterImage(
                keyCodec.encode(stableKey), valueCodec.encode(afterImage)));
    }

    public byte[] encodeTombstone(io.rowweave.DataRow stableKey) {
        if (stableKey == null) {
            throw new IllegalArgumentException("UPSERT key is required");
        }
        return StreamUpsertRecordCodec.encode(StreamUpsertRecord.tombstone(
                keyCodec.encode(stableKey)));
    }

    private void requireMatchingKey(io.rowweave.DataRow stableKey,
            io.rowweave.DataRow afterImage) {
        io.rowweave.DataRow projected = new io.rowweave.DataRow(
                valueKeyIndices.length);
        for (int index = 0; index < valueKeyIndices.length; index++) {
            projected.set(index, afterImage.get(valueKeyIndices[index]));
        }
        if (!MessageDigest.isEqual(keyCodec.encode(stableKey),
                keyCodec.encode(projected))) {
            throw new IllegalArgumentException(
                    "UPSERT stable key differs from its after-image");
        }
    }

    private static int[] keyIndices(StreamSchema keySchema,
            StreamSchema valueSchema) {
        int[] result = new int[keySchema.fieldCount()];
        for (int keyIndex = 0; keyIndex < keySchema.fieldCount(); keyIndex++) {
            StreamField key = keySchema.field(keyIndex);
            int match = -1;
            for (int valueIndex = 0;
                    valueIndex < valueSchema.fieldCount(); valueIndex++) {
                if (key.fieldId().equals(
                        valueSchema.field(valueIndex).fieldId())) {
                    match = valueIndex;
                    if (!key.equals(valueSchema.field(valueIndex))) {
                        throw new IllegalArgumentException(
                                "UPSERT stable key codec differs from its after-image field");
                    }
                    break;
                }
            }
            if (match < 0) {
                throw new IllegalArgumentException(
                        "UPSERT stable key is absent from its after-image schema");
            }
            result[keyIndex] = match;
        }
        return result;
    }
}
