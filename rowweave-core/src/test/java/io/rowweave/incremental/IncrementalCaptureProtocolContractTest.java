package io.rowweave.incremental;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** capture request/response are core-owned exact metadata. */
class IncrementalCaptureProtocolContractTest {

    @Test
    void normalCaptureRoundTripsAndBindsAReceiptWithoutProviderAuthorship()
            throws Exception {
        IncrementalCaptureRequest request = IncrementalCaptureRequest.normal(
                7L, digest("definition"), 12L,
                IncrementalFrontier.int64Sequence(20L), "trigger",
                IncrementalRunMode.INCREMENTAL_V1);
        IncrementalCaptureResponse response = new IncrementalCaptureResponse(
                IncrementalFrontier.int64Sequence(30L), bytes("snapshot"));

        byte[] requestFrame = IncrementalCaptureRequestCodec.encode(request);
        byte[] responseFrame = IncrementalCaptureResponseCodec.encode(response);
        assertEquals(request,
                IncrementalCaptureRequestCodec.decode(requestFrame));
        assertEquals(response,
                IncrementalCaptureResponseCodec.decode(responseFrame));
        assertArrayEquals(requestFrame, IncrementalCaptureRequestCodec.encode(
                IncrementalCaptureRequestCodec.decode(requestFrame)));
        assertArrayEquals(responseFrame, IncrementalCaptureResponseCodec.encode(
                IncrementalCaptureResponseCodec.decode(responseFrame)));
        assertTrue(requestFrame.length
                <= IncrementalCaptureRequestCodec.MAX_FRAME_BYTES);
        assertTrue(responseFrame.length
                <= IncrementalCaptureResponseCodec.MAX_FRAME_BYTES);
        assertNull(request.requestedTarget());

        byte[] contract = digest("source-contract");
        CapturedTargetReceipt receipt = request.bind(contract, response);
        assertEquals(request.feedIncarnation(), receipt.feedIncarnation());
        assertArrayEquals(request.definitionDigest(), receipt.definitionDigest());
        assertArrayEquals(contract, receipt.sourceContractDigest());
        assertEquals(request.expectedSequence(), receipt.expectedSequence());
        assertEquals(request.from(), receipt.from());
        assertEquals(request.requestIdentity(), receipt.requestIdentity());
        assertEquals(request.mode(), receipt.mode());
        assertEquals(response.target(), receipt.target());
        assertArrayEquals(response.snapshotIdentity(), receipt.snapshotIdentity());
    }

    @Test
    void backfillCaptureRequiresAndPreservesTheExplicitTarget()
            throws Exception {
        IncrementalCaptureRequest request = IncrementalCaptureRequest.backfill(
                9L, digest("definition"), 4L,
                IncrementalFrontier.int64Sequence(3L),
                IncrementalFrontier.int64Sequence(8L), "backfill-key");
        IncrementalCaptureResponse exact = new IncrementalCaptureResponse(
                IncrementalFrontier.int64Sequence(8L), bytes("snapshot"));
        assertEquals(request, IncrementalCaptureRequestCodec.decode(
                IncrementalCaptureRequestCodec.encode(request)));
        assertEquals(request.requestedTarget(), exact.target());
        assertEquals(IncrementalRunMode.BACKFILL_V1,
                request.bind(digest("source-contract"), exact).mode());

        IncrementalCaptureResponse drift = new IncrementalCaptureResponse(
                IncrementalFrontier.int64Sequence(9L), bytes("snapshot"));
        assertThrows(IllegalArgumentException.class,
                () -> request.bind(digest("source-contract"), drift));
    }

    @Test
    void modeFrontierAndSnapshotBoundsFailBeforeProviderAction()
            throws Exception {
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalCaptureRequest.normal(
                        1L, digest("definition"), 0L,
                        IncrementalFrontier.begin(), "trigger",
                        IncrementalRunMode.INCREMENTAL_V1));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalCaptureRequest.normal(
                        1L, digest("definition"), 0L,
                        IncrementalFrontier.int64Sequence(1L), "trigger",
                        IncrementalRunMode.SNAPSHOT_V1));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalCaptureRequest.backfill(
                        1L, digest("definition"), 0L,
                        IncrementalFrontier.int64Sequence(1L),
                        IncrementalFrontier.int64Sequence(2L), "key"));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalCaptureRequest.backfill(
                        1L, digest("definition"), 1L,
                        IncrementalFrontier.int64Sequence(2L),
                        IncrementalFrontier.int64Sequence(2L), "key"));
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalCaptureResponse(
                        IncrementalFrontier.begin(), bytes("snapshot")));
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalCaptureResponse(
                        IncrementalFrontier.int64Sequence(2L),
                        new byte[CapturedTargetReceipt
                                .MAX_SNAPSHOT_IDENTITY_BYTES + 1]));
    }

    @Test
    void checksumTrailingAndResignedUnknownModeAreRejected() throws Exception {
        IncrementalCaptureRequest request = IncrementalCaptureRequest.normal(
                1L, digest("definition"), 0L, IncrementalFrontier.begin(),
                "trigger", IncrementalRunMode.SNAPSHOT_V1);
        byte[] encoded = IncrementalCaptureRequestCodec.encode(request);
        byte[] corrupt = encoded.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalCaptureRequestCodec.decode(corrupt));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalCaptureRequestCodec.decode(
                        Arrays.copyOf(encoded, encoded.length + 1)));

        byte[] unknownMode = encoded.clone();
        unknownMode[8] = 99;
        resign(unknownMode);
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalCaptureRequestCodec.decode(unknownMode));
    }

    private static void resign(byte[] frame) throws Exception {
        byte[] digest = sha256(Arrays.copyOf(frame, frame.length - 32));
        System.arraycopy(digest, 0, frame, frame.length - 32, digest.length);
    }

    private static byte[] digest(String value) throws Exception {
        return sha256(bytes(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] sha256(byte[] value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(value);
    }
}
