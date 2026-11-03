package io.rowweave.stream.api;

import io.rowweave.DataRow;
import io.rowweave.mem.RowSerializer;

/** Public deterministic DataRow codec for connector records and state payloads. */
public final class StreamDataRowCodec {
    private static final int MAX_FRAME_BYTES = 4 * 1024 * 1024;
    private final RowSerializer serializer;

    public StreamDataRowCodec(StreamSchema schema) {
        if (schema == null) throw new IllegalArgumentException("schema is required");
        this.serializer = new RowSerializer(schema.toDataSchema());
    }

    public byte[] encode(DataRow row) {
        if (row == null) throw new IllegalArgumentException("row is required");
        byte[] frame = serializer.serialize(row);
        if (frame.length == 0 || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "encoded Stream row exceeds its bound");
        }
        return frame;
    }

    public DataRow decode(byte[] frame) {
        if (frame == null || frame.length == 0
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("row frame is invalid");
        }
        return serializer.deserialize(frame);
    }
}
