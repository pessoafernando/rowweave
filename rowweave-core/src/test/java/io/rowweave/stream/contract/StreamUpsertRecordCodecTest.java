package io.rowweave.stream.contract;

import io.rowweave.DataType;
import io.rowweave.DataRow;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamDataRowCodec;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamUpsertCodec;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** canonical after-image/tombstone UPSERT data frame. */
class StreamUpsertRecordCodecTest {

    @Test
    void afterImageAndTombstoneRoundTripCanonically() {
        StreamUpsertRecord afterImage = StreamUpsertRecord.afterImage(
                bytes("adaptor-7"), bytes("readout=12.3400"));
        byte[] encoded = StreamUpsertRecordCodec.encode(afterImage);
        StreamUpsertRecord decoded = StreamUpsertRecordCodec.decode(
                encoded, 128, 1024);
        assertEquals(StreamUpsertRecord.Kind.AFTER_IMAGE, decoded.kind());
        assertArrayEquals(bytes("adaptor-7"), decoded.stableKeyFrame());
        assertArrayEquals(bytes("readout=12.3400"), decoded.afterImageFrame());
        assertArrayEquals(encoded, StreamUpsertRecordCodec.encode(decoded));

        StreamUpsertRecord tombstone = StreamUpsertRecord.tombstone(
                bytes("adaptor-7"));
        StreamUpsertRecord decodedTombstone = StreamUpsertRecordCodec.decode(
                StreamUpsertRecordCodec.encode(tombstone), 128, 1024);
        assertEquals(StreamUpsertRecord.Kind.TOMBSTONE,
                decodedTombstone.kind());
        assertFalse(decodedTombstone.hasAfterImage());
    }

    @Test
    void checksumBoundsAndRecordShapeFailClosed() {
        byte[] encoded = StreamUpsertRecordCodec.encode(
                StreamUpsertRecord.afterImage(bytes("k"), bytes("value")));
        encoded[encoded.length - 1] ^= 1;
        assertCode(StreamUpsertException.Code.CHECKSUM_MISMATCH,
                () -> StreamUpsertRecordCodec.decode(encoded, 128, 1024));

        byte[] oversizedKey = new byte[129];
        Arrays.fill(oversizedKey, (byte) 1);
        byte[] oversized = StreamUpsertRecordCodec.encode(
                StreamUpsertRecord.afterImage(oversizedKey, bytes("value")));
        assertCode(StreamUpsertException.Code.FRAME_LIMIT,
                () -> StreamUpsertRecordCodec.decode(oversized, 128, 1024));

        assertCode(StreamUpsertException.Code.INVALID_RECORD,
                () -> StreamUpsertRecord.afterImage(new byte[0], bytes("value")));
        assertCode(StreamUpsertException.Code.INVALID_RECORD,
                () -> StreamUpsertRecord.afterImage(bytes("k"), new byte[0]));
        assertCode(StreamUpsertException.Code.MALFORMED_FRAME,
                () -> StreamUpsertRecordCodec.decode(
                        ByteBuffer.allocate(16).putLong(1L).putLong(2L).array(),
                        128, 1024));
    }

    @Test
    void typedCodecRejectsWrapperKeyDifferentFromAfterImage() {
        StreamSchema keySchema = new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id",
                        DataType.StringType, false)));
        StreamSchema rowSchema = new StreamSchema(Arrays.asList(
                new StreamField("id", "id", "id",
                        DataType.StringType, false),
                new StreamField("value", "value", "value",
                        DataType.StringType, false)));
        StreamUpsertCodec codec = new StreamUpsertCodec(keySchema, rowSchema);
        DataRow wrapperKey = row("K1");
        DataRow afterImage = row("K2", "value");
        byte[] forged = StreamUpsertRecordCodec.encode(
                StreamUpsertRecord.afterImage(
                        new StreamDataRowCodec(keySchema).encode(wrapperKey),
                        new StreamDataRowCodec(rowSchema).encode(afterImage)));

        assertThrows(IllegalArgumentException.class,
                () -> codec.encodeAfterImage(wrapperKey, afterImage));
        assertThrows(IllegalArgumentException.class,
                () -> codec.decode(forged));
    }

    private static void assertCode(
            StreamUpsertException.Code code,
            org.junit.jupiter.api.function.Executable action) {
        StreamUpsertException failure = assertThrows(
                StreamUpsertException.class, action);
        assertEquals(code, failure.code(), failure.getMessage());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static DataRow row(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }
}
