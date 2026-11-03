package io.rowweave.stream.contract;

import io.rowweave.stream.api.StreamChangelogMode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** canonical independently-versioned RETRACT record wire. */
class StreamChangeRecordCodecTest {

    @Test
    void allFourChangeKindsRoundTripCanonically() {
        StreamChangeRecord[] records = new StreamChangeRecord[] {
                StreamChangeRecord.insert(bytes("row-1")),
                StreamChangeRecord.updateBefore(bytes("key-1"),
                        bytes("row-1")),
                StreamChangeRecord.updateAfter(bytes("key-1"),
                        bytes("row-2")),
                StreamChangeRecord.delete(bytes("key-1"),
                        bytes("row-2")),
                StreamChangeRecord.keyOnlyDelete(bytes("key-1"))};

        for (StreamChangeRecord record : records) {
            byte[] encoded = StreamChangeRecordCodec.encode(record);
            StreamChangeRecord decoded = StreamChangeRecordCodec.decode(
                    encoded, 128, 1_024, 1_024);
            assertEquals(record, decoded);
            assertArrayEquals(encoded, StreamChangeRecordCodec.encode(decoded));
        }
        assertEquals(StreamChangelogMode.RETRACT,
                StreamChangelogMode.valueOf("RETRACT"));
        assertFalse(Arrays.equals(StreamChangeRecordCodec.magic(),
                StreamUpsertRecordCodec.magic()));
    }

    @Test
    void imageAndStableKeyPresenceAreKindSpecific() {
        StreamChangeRecord insert = StreamChangeRecord.insert(bytes("after"));
        assertFalse(insert.hasStableKey());
        assertFalse(insert.hasBeforeImage());
        assertTrue(insert.hasAfterImage());

        StreamChangeRecord before = StreamChangeRecord.updateBefore(
                bytes("key"), bytes("before"));
        assertTrue(before.hasStableKey());
        assertTrue(before.hasBeforeImage());
        assertFalse(before.hasAfterImage());

        StreamChangeRecord delete = StreamChangeRecord.keyOnlyDelete(
                bytes("key"));
        assertTrue(delete.hasStableKey());
        assertFalse(delete.hasBeforeImage());
        assertFalse(delete.hasAfterImage());
        assertThrows(IllegalStateException.class, delete::beforeImageFrame);
        assertThrows(IllegalStateException.class, delete::afterImageFrame);
        assertThrows(IllegalStateException.class, insert::stableKeyFrame);
        assertNotEquals(insert, before);
    }

    @Test
    void checksumShapeAndDecodeBoundsFailClosed() {
        byte[] encoded = StreamChangeRecordCodec.encode(
                StreamChangeRecord.updateAfter(
                        bytes("key"), bytes("after")));
        encoded[encoded.length - 1] ^= 1;
        assertCode(StreamChangeException.Code.CHECKSUM_MISMATCH,
                () -> StreamChangeRecordCodec.decode(
                        encoded, 128, 1_024, 1_024));

        byte[] largeKey = new byte[129];
        Arrays.fill(largeKey, (byte) 1);
        byte[] oversized = StreamChangeRecordCodec.encode(
                StreamChangeRecord.keyOnlyDelete(largeKey));
        assertCode(StreamChangeException.Code.FRAME_LIMIT,
                () -> StreamChangeRecordCodec.decode(
                        oversized, 128, 1_024, 1_024));

        assertCode(StreamChangeException.Code.INVALID_RECORD,
                () -> StreamChangeRecord.insert(new byte[0]));
        assertCode(StreamChangeException.Code.INVALID_RECORD,
                () -> StreamChangeRecord.updateBefore(
                        new byte[0], bytes("before")));
        assertCode(StreamChangeException.Code.INVALID_RECORD,
                () -> StreamChangeRecord.delete(bytes("key"), new byte[0]));
        assertCode(StreamChangeException.Code.MALFORMED_FRAME,
                () -> StreamChangeRecordCodec.decode(
                        StreamUpsertRecordCodec.encode(
                                StreamUpsertRecord.tombstone(bytes("key"))),
                        128, 1_024, 1_024));
    }

    private static void assertCode(StreamChangeException.Code code,
            org.junit.jupiter.api.function.Executable action) {
        StreamChangeException failure = assertThrows(
                StreamChangeException.class, action);
        assertEquals(code, failure.code(), failure.getMessage());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
