package io.rowweave.incremental;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** record position orders rows inside a cut; it is not the cut frontier. */
class IncrementalRecordPositionContractTest {

    @Test
    void closedPositionKindsRoundTripCanonically() throws Exception {
        byte[] interval = sha256("interval-1");
        byte[] key = sha256("key-1");
        IncrementalRecordPosition[] positions = {
                IncrementalRecordPosition.sequence(interval, 0, 11L, key),
                IncrementalRecordPosition.timestampKey(interval, 1, 100L,
                        OrderedFrontierKey.of(OrderedFrontierKey.utf8("k")), key),
                IncrementalRecordPosition.manifestEntry(interval, 2, 7L, 3L, key),
                IncrementalRecordPosition.snapshotRow(interval, 3, 9L, key)
        };

        for (IncrementalRecordPosition position : positions) {
            byte[] encoded = IncrementalRecordPositionCodec.encode(position);
            assertTrue(encoded.length <= IncrementalRecordPositionCodec.MAX_FRAME_BYTES);
            IncrementalRecordPosition decoded = IncrementalRecordPositionCodec.decode(encoded);
            assertEquals(position, decoded);
            assertArrayEquals(encoded, IncrementalRecordPositionCodec.encode(decoded));
        }
    }

    @Test
    void snapshotRowsHaveStableDistinctPositionsUnderOneTargetCut() throws Exception {
        IncrementalFrontier target = IncrementalFrontier.int64Sequence(100L);
        byte[] interval = sha256(IncrementalFrontierCodec.encode(target));
        IncrementalRecordPosition first = IncrementalRecordPosition.snapshotRow(
                interval, 4, 0L, sha256("customer-1"));
        IncrementalRecordPosition second = IncrementalRecordPosition.snapshotRow(
                interval, 4, 1L, sha256("customer-2"));

        assertEquals(IncrementalRecordPosition.Kind.SNAPSHOT_ROW_V1, first.kind());
        assertTrue(IncrementalRecordPositionOrder.compareWithinSplit(first, second) < 0);
        assertEquals(target, IncrementalFrontierCodec.decode(
                IncrementalFrontierCodec.encode(target)));
    }

    @Test
    void manifestEntriesOrderByGenerationThenManifestOrdinalNotDigest() throws Exception {
        byte[] interval = sha256("manifest-interval");
        IncrementalRecordPosition first = IncrementalRecordPosition.manifestEntry(
                interval, 0, 8L, 0L, sha256("z-key"));
        IncrementalRecordPosition second = IncrementalRecordPosition.manifestEntry(
                interval, 0, 8L, 1L, sha256("a-key"));
        IncrementalRecordPosition laterGeneration = IncrementalRecordPosition.manifestEntry(
                interval, 0, 9L, 0L, sha256("a-key"));

        assertTrue(IncrementalRecordPositionOrder.compareWithinSplit(first, second) < 0);
        assertTrue(IncrementalRecordPositionOrder.compareWithinSplit(second, laterGeneration) < 0);
    }

    @Test
    void sequenceAndTimestampUseExactSourceOrderWithKeyDigestTieBreak() throws Exception {
        byte[] interval = sha256("change-interval");
        IncrementalRecordPosition seq1 = IncrementalRecordPosition.sequence(
                interval, 0, 5L, sha256("a"));
        IncrementalRecordPosition seq2 = IncrementalRecordPosition.sequence(
                interval, 0, 6L, sha256("a"));
        assertTrue(IncrementalRecordPositionOrder.compareWithinSplit(seq1, seq2) < 0);

        byte[] lowDigest = new byte[32];
        byte[] highDigest = new byte[32];
        highDigest[31] = 1;
        IncrementalRecordPosition tieLow = IncrementalRecordPosition.sequence(
                interval, 0, 7L, lowDigest);
        IncrementalRecordPosition tieHigh = IncrementalRecordPosition.sequence(
                interval, 0, 7L, highDigest);
        assertTrue(IncrementalRecordPositionOrder.compareWithinSplit(tieLow, tieHigh) < 0);
        assertTrue(IncrementalRecordPositionOrder.compareWithinSplit(tieHigh, tieLow) > 0);
        assertEquals(0, IncrementalRecordPositionOrder.compareWithinSplit(tieLow, tieLow));

        IncrementalRecordPosition ts1 = IncrementalRecordPosition.timestampKey(
                interval, 0, 5L,
                OrderedFrontierKey.of(OrderedFrontierKey.int64(-1L)), sha256("a"));
        IncrementalRecordPosition ts2 = IncrementalRecordPosition.timestampKey(
                interval, 0, 5L,
                OrderedFrontierKey.of(OrderedFrontierKey.int64(0L)), sha256("a"));
        assertTrue(IncrementalRecordPositionOrder.compareWithinSplit(ts1, ts2) < 0);
    }

    @Test
    void crossIntervalPartitionOrKindComparisonFailsClosed() throws Exception {
        IncrementalRecordPosition base = IncrementalRecordPosition.sequence(
                sha256("i1"), 0, 1L, sha256("key"));
        IncrementalRecordPosition otherInterval = IncrementalRecordPosition.sequence(
                sha256("i2"), 0, 2L, sha256("key"));
        IncrementalRecordPosition otherPartition = IncrementalRecordPosition.sequence(
                sha256("i1"), 1, 2L, sha256("key"));
        IncrementalRecordPosition otherKind = IncrementalRecordPosition.snapshotRow(
                sha256("i1"), 0, 2L, sha256("key"));

        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRecordPositionOrder.compareWithinSplit(base, otherInterval));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRecordPositionOrder.compareWithinSplit(base, otherPartition));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRecordPositionOrder.compareWithinSplit(base, otherKind));
    }

    @Test
    void factoriesDefensivelyCopyAndRejectInvalidIdentityOrOrdinals() throws Exception {
        byte[] interval = sha256("immutable-interval");
        byte[] key = sha256("immutable-key");
        IncrementalRecordPosition position = IncrementalRecordPosition.sequence(
                interval, 0, 1L, key);
        interval[0] ^= 1;
        key[0] ^= 1;
        assertArrayEquals(sha256("immutable-interval"), position.intervalDigest());
        assertArrayEquals(sha256("immutable-key"), position.keyDigest());

        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRecordPosition.sequence(new byte[31], 0, 1L, new byte[32]));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRecordPosition.sequence(new byte[32], -1, 1L, new byte[32]));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRecordPosition.sequence(new byte[32], 0, -1L, new byte[32]));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRecordPosition.sequence(new byte[32], 0, 1L, new byte[31]));
        IncrementalRecordPosition.sequence(
                new byte[32], IncrementalRecordPosition.MAX_PARTITION_ORDINAL, 1L, new byte[32]);
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRecordPosition.sequence(
                        new byte[32], IncrementalRecordPosition.MAX_PARTITION_ORDINAL + 1,
                        1L, new byte[32]));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRecordPosition.manifestEntry(
                        new byte[32], 0, -1L, 0L, new byte[32]));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRecordPosition.manifestEntry(
                        new byte[32], 0, 1L, -1L, new byte[32]));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRecordPosition.snapshotRow(
                        new byte[32], 0, -1L, new byte[32]));

        byte[] full = new byte[OrderedFrontierKey.MAX_COMPONENT_BYTES];
        Arrays.fill(full, (byte) 1);
        byte[] exactRemainder = new byte[885];
        Arrays.fill(exactRemainder, (byte) 1);
        OrderedFrontierKey exact = OrderedFrontierKey.of(
                OrderedFrontierKey.bytes(full), OrderedFrontierKey.bytes(full),
                OrderedFrontierKey.bytes(full), OrderedFrontierKey.bytes(exactRemainder));
        IncrementalRecordPosition exactPosition = IncrementalRecordPosition.timestampKey(
                new byte[32], 0, 0L, exact, new byte[32]);
        assertEquals(IncrementalRecordPositionCodec.MAX_FRAME_BYTES,
                IncrementalRecordPositionCodec.encode(exactPosition).length);
        byte[] overRemainder = new byte[886];
        Arrays.fill(overRemainder, (byte) 1);
        OrderedFrontierKey over = OrderedFrontierKey.of(
                OrderedFrontierKey.bytes(full), OrderedFrontierKey.bytes(full),
                OrderedFrontierKey.bytes(full), OrderedFrontierKey.bytes(overRemainder));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRecordPosition.timestampKey(
                        new byte[32], 0, 0L, over, new byte[32]));
    }

    @Test
    void codecRejectsUnknownCorruptMalformedAndOversizedFrames() throws Exception {
        byte[] valid = IncrementalRecordPositionCodec.encode(
                IncrementalRecordPosition.snapshotRow(
                        sha256("interval"), 0, 1L, sha256("key")));

        byte[] unknownVersion = valid.clone();
        unknownVersion[4] = 99;
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRecordPositionCodec.decode(unknownVersion));
        byte[] unknownKind = valid.clone();
        unknownKind[5] = 99;
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRecordPositionCodec.decode(unknownKind));
        byte[] unknownCodec = valid.clone();
        unknownCodec[6] = 99;
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRecordPositionCodec.decode(unknownCodec));
        byte[] negativePayload = valid.clone();
        Arrays.fill(negativePayload, 7, 11, (byte) 0xff);
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRecordPositionCodec.decode(negativePayload));
        byte[] mismatchedPayload = valid.clone();
        mismatchedPayload[10]--;
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRecordPositionCodec.decode(mismatchedPayload));
        byte[] corrupt = valid.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRecordPositionCodec.decode(corrupt));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRecordPositionCodec.decode(
                        Arrays.copyOf(valid, valid.length + 1)));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRecordPositionCodec.decode(
                        new byte[IncrementalRecordPositionCodec.MAX_FRAME_BYTES + 1]));
    }

    private static byte[] sha256(String value) throws Exception {
        return sha256(value.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] sha256(byte[] value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(value);
    }
}
