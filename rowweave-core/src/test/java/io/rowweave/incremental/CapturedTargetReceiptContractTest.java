package io.rowweave.incremental;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** exact capture receipt and mode-specific stable identities. */
class CapturedTargetReceiptContractTest {

    @Test
    void snapshotIncrementalAndBackfillReceiptsRoundTripCanonically() throws Exception {
        CapturedTargetReceipt[] receipts = {
                CapturedTargetReceipt.normal(
                        3L, sha256("definition"), sha256("contract"), 0L,
                        IncrementalFrontier.begin(), "snapshot-trigger",
                        IncrementalRunMode.SNAPSHOT_V1,
                        IncrementalFrontier.int64Sequence(10L), bytes("snap-1")),
                CapturedTargetReceipt.normal(
                        3L, sha256("definition"), sha256("contract"), 9L,
                        IncrementalFrontier.int64Sequence(10L), "incremental-trigger",
                        IncrementalRunMode.INCREMENTAL_V1,
                        IncrementalFrontier.int64Sequence(20L), bytes("snap-2")),
                CapturedTargetReceipt.backfill(
                        3L, sha256("definition"), sha256("contract"), 7L,
                        IncrementalFrontier.int64Sequence(2L),
                        IncrementalFrontier.int64Sequence(5L),
                        "historical-correction-7", bytes("snap-3"))
        };

        for (CapturedTargetReceipt receipt : receipts) {
            byte[] frame = CapturedTargetReceiptCodec.encode(receipt);
            assertTrue(frame.length <= CapturedTargetReceiptCodec.MAX_FRAME_BYTES);
            CapturedTargetReceipt decoded = CapturedTargetReceiptCodec.decode(frame);
            assertEquals(receipt, decoded);
            assertArrayEquals(frame, CapturedTargetReceiptCodec.encode(decoded));
            assertArrayEquals(CapturedTargetReceiptCodec.digest(receipt),
                    CapturedTargetReceiptCodec.digest(decoded));
        }
    }

    @Test
    void sameNormalCandidateWithDifferentSnapshotIdentityCollidesBeforeExecution() throws Exception {
        CapturedTargetReceipt first = normal("token-a", 10L);
        CapturedTargetReceipt drifted = normal("token-b", 10L);
        IncrementalIntervalIdentity firstIdentity = IncrementalIntervalIdentity.from(first);
        IncrementalIntervalIdentity driftedIdentity = IncrementalIntervalIdentity.from(drifted);

        assertEquals(firstIdentity.candidateRunId(), driftedIdentity.candidateRunId());
        assertNotEquals(firstIdentity.intervalId(), driftedIdentity.intervalId());
        assertNotEquals(firstIdentity.capturedTargetReceiptDigest(),
                driftedIdentity.capturedTargetReceiptDigest());
    }

    @Test
    void sameTriggerWithDifferentTargetAlsoKeepsCandidateAndChangesInterval() throws Exception {
        IncrementalIntervalIdentity first = IncrementalIntervalIdentity.from(normal("token", 10L));
        IncrementalIntervalIdentity drifted = IncrementalIntervalIdentity.from(normal("token", 11L));

        assertEquals(first.candidateRunId(), drifted.candidateRunId());
        assertNotEquals(first.intervalId(), drifted.intervalId());
    }

    @Test
    void backfillIdentityUsesOrdinalRequestAndBoundsNotMainFrontierRevision() throws Exception {
        CapturedTargetReceipt first = backfill(4L, "request-4", 2L, 5L, "token-a");
        CapturedTargetReceipt replay = backfill(4L, "request-4", 2L, 5L, "token-a");
        CapturedTargetReceipt recaptured = backfill(4L, "request-4", 2L, 5L, "token-b");
        IncrementalIntervalIdentity firstIdentity = IncrementalIntervalIdentity.from(first);
        IncrementalIntervalIdentity replayIdentity = IncrementalIntervalIdentity.from(replay);
        IncrementalIntervalIdentity recapturedIdentity = IncrementalIntervalIdentity.from(recaptured);

        assertEquals(firstIdentity.backfillId(), replayIdentity.backfillId());
        assertEquals(firstIdentity.candidateRunId(), replayIdentity.candidateRunId());
        assertEquals(firstIdentity.intervalId(), replayIdentity.intervalId());
        assertEquals(firstIdentity.backfillId(), recapturedIdentity.backfillId());
        assertEquals(firstIdentity.candidateRunId(), recapturedIdentity.candidateRunId());
        assertEquals(firstIdentity.intervalId(), recapturedIdentity.intervalId());
        assertNotEquals(firstIdentity.capturedTargetReceiptDigest(),
                recapturedIdentity.capturedTargetReceiptDigest());

        assertNotEquals(firstIdentity.backfillId(),
                IncrementalIntervalIdentity.from(
                        backfill(5L, "request-4", 2L, 5L, "token-a")).backfillId());
        assertNotEquals(firstIdentity.backfillId(),
                IncrementalIntervalIdentity.from(
                        backfill(4L, "request-other", 2L, 5L, "token-a")).backfillId());
        assertNotEquals(firstIdentity.backfillId(),
                IncrementalIntervalIdentity.from(
                        backfill(4L, "request-4", 1L, 5L, "token-a")).backfillId());
    }

    @Test
    void receiptAndIdentityHashesAreAcyclicAndRecomputedAfterDecode() throws Exception {
        CapturedTargetReceipt receipt = normal("token", 10L);
        byte[] frame = CapturedTargetReceiptCodec.encode(receipt);
        byte[] digest = CapturedTargetReceiptCodec.digest(receipt);
        IncrementalIntervalIdentity identity = IncrementalIntervalIdentity.from(receipt);

        assertEquals(32, digest.length);
        assertEquals(64, identity.candidateRunId().length());
        assertEquals(64, identity.intervalId().length());
        assertEquals(hex(digest), identity.capturedTargetReceiptDigest());
        assertEquals(identity, IncrementalIntervalIdentity.from(
                CapturedTargetReceiptCodec.decode(frame)));

        byte[] corrupt = frame.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> CapturedTargetReceiptCodec.decode(corrupt));
    }

    @Test
    void modeAndFrontierBoundariesFailClosed() throws Exception {
        assertThrows(IllegalArgumentException.class,
                () -> CapturedTargetReceipt.normal(
                        1L, sha256("d"), sha256("c"), 0L,
                        IncrementalFrontier.int64Sequence(0L), "t",
                        IncrementalRunMode.SNAPSHOT_V1,
                        IncrementalFrontier.int64Sequence(1L), bytes("s")));
        assertThrows(IllegalArgumentException.class,
                () -> CapturedTargetReceipt.normal(
                        1L, sha256("d"), sha256("c"), 0L,
                        IncrementalFrontier.begin(), "t",
                        IncrementalRunMode.INCREMENTAL_V1,
                        IncrementalFrontier.int64Sequence(1L), bytes("s")));
        assertThrows(IllegalArgumentException.class,
                () -> CapturedTargetReceipt.normal(
                        1L, sha256("d"), sha256("c"), 0L,
                        IncrementalFrontier.int64Sequence(2L), "t",
                        IncrementalRunMode.INCREMENTAL_V1,
                        IncrementalFrontier.int64Sequence(1L), bytes("s")));
        assertThrows(IllegalArgumentException.class,
                () -> CapturedTargetReceipt.backfill(
                        1L, sha256("d"), sha256("c"), 1L,
                        IncrementalFrontier.int64Sequence(2L),
                        IncrementalFrontier.int64Sequence(2L), "b", bytes("s")));
        assertThrows(IllegalArgumentException.class,
                () -> CapturedTargetReceipt.normal(
                        1L, sha256("d"), sha256("c"), 0L,
                        IncrementalFrontier.begin(), "t",
                        IncrementalRunMode.BACKFILL_V1,
                        IncrementalFrontier.int64Sequence(1L), bytes("s")));
    }

    @Test
    void receiptDefensivelyCopiesAndEnforcesTokenTextAndFrameLimits() throws Exception {
        byte[] definition = sha256("definition");
        byte[] contract = sha256("contract");
        byte[] token = bytes("snapshot");
        CapturedTargetReceipt receipt = CapturedTargetReceipt.normal(
                1L, definition, contract, 0L, IncrementalFrontier.begin(), "trigger",
                IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(1L), token);
        definition[0] ^= 1;
        contract[0] ^= 1;
        token[0] ^= 1;
        assertArrayEquals(sha256("definition"), receipt.definitionDigest());
        assertArrayEquals(sha256("contract"), receipt.sourceContractDigest());
        assertArrayEquals(bytes("snapshot"), receipt.snapshotIdentity());

        assertThrows(IllegalArgumentException.class,
                () -> CapturedTargetReceipt.normal(
                        0L, new byte[32], new byte[32], 0L,
                        IncrementalFrontier.begin(), "t", IncrementalRunMode.SNAPSHOT_V1,
                        IncrementalFrontier.int64Sequence(1L), bytes("s")));
        char[] tooLong = new char[CapturedTargetReceipt.MAX_REQUEST_CODE_UNITS + 1];
        Arrays.fill(tooLong, 'a');
        assertThrows(IllegalArgumentException.class,
                () -> CapturedTargetReceipt.normal(
                        1L, new byte[32], new byte[32], 0L,
                        IncrementalFrontier.begin(), new String(tooLong),
                        IncrementalRunMode.SNAPSHOT_V1,
                        IncrementalFrontier.int64Sequence(1L), bytes("s")));
        assertThrows(IllegalArgumentException.class,
                () -> CapturedTargetReceipt.normal(
                        1L, new byte[32], new byte[32], 0L,
                        IncrementalFrontier.begin(), "\ud800",
                        IncrementalRunMode.SNAPSHOT_V1,
                        IncrementalFrontier.int64Sequence(1L), bytes("s")));
        assertThrows(IllegalArgumentException.class,
                () -> CapturedTargetReceipt.normal(
                        1L, new byte[32], new byte[32], 0L,
                        IncrementalFrontier.begin(), "t",
                        IncrementalRunMode.SNAPSHOT_V1,
                        IncrementalFrontier.int64Sequence(1L),
                        new byte[CapturedTargetReceipt.MAX_SNAPSHOT_IDENTITY_BYTES + 1]));
        assertThrows(IllegalArgumentException.class,
                () -> CapturedTargetReceiptCodec.decode(
                        new byte[CapturedTargetReceiptCodec.MAX_FRAME_BYTES + 1]));
    }

    private static CapturedTargetReceipt normal(String token, long target) throws Exception {
        return CapturedTargetReceipt.normal(
                3L, sha256("definition"), sha256("contract"), 9L,
                IncrementalFrontier.int64Sequence(5L), "trigger-9",
                IncrementalRunMode.INCREMENTAL_V1,
                IncrementalFrontier.int64Sequence(target), bytes(token));
    }

    private static CapturedTargetReceipt backfill(
            long ordinal, String request, long from, long target, String token) throws Exception {
        return CapturedTargetReceipt.backfill(
                3L, sha256("definition"), sha256("contract"), ordinal,
                IncrementalFrontier.int64Sequence(from),
                IncrementalFrontier.int64Sequence(target), request, bytes(token));
    }

    private static byte[] sha256(String value) throws Exception {
        return sha256(bytes(value));
    }

    private static byte[] sha256(byte[] value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(value);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String hex(byte[] value) {
        StringBuilder out = new StringBuilder(value.length * 2);
        for (byte b : value) out.append(String.format("%02x", b & 0xff));
        return out.toString();
    }
}
