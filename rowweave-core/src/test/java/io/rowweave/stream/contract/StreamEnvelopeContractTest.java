package io.rowweave.stream.contract;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** ordered Stream data-lane envelope and checkpoint identity. */
class StreamEnvelopeContractTest {
    private static final String A = repeat('a');
    private static final String B = repeat('b');

    @Test
    void recordWatermarkAndBarrierUseCanonicalIndependentWire() {
        StreamChannelIdentity channel = new StreamChannelIdentity(
                "run-1", "attempt-1", 1L, 2L, "source-to-sink", 3L);
        CheckpointIdentity checkpoint = new CheckpointIdentity(
                "definition-1", A, "run-1", "attempt-1", 1L,
                4L, "coordinator-1", 7L, A, B, 1, 1);
        StreamEnvelope[] events = {
                StreamEnvelope.record(channel, 1L, 100L, new byte[]{1, 2}),
                StreamEnvelope.watermark(channel, 2L, 100L),
                StreamEnvelope.checkpointBarrier(channel, 3L, checkpoint)
        };

        StreamEnvelopeSequenceGuard guard = new StreamEnvelopeSequenceGuard(channel);
        for (StreamEnvelope event : events) {
            byte[] encoded = StreamEnvelopeCodec.encode(event);
            assertArrayEquals("RWXXSEV1".getBytes(StandardCharsets.US_ASCII),
                    StreamEnvelopeCodec.magic());
            assertEquals(event, StreamEnvelopeCodec.decode(encoded));
            assertArrayEquals(encoded, StreamEnvelopeCodec.encode(
                    StreamEnvelopeCodec.decode(encoded)));
            guard.accept(event);
        }
        assertEquals(3L, guard.lastSequence());
        assertEquals(100L, guard.lastWatermark());
    }

    @Test
    void completeEnvelopeKindSetHasCanonicalTypedPayloads() {
        StreamChannelIdentity channel = new StreamChannelIdentity(
                "run-1", "attempt-1", 1L, 2L, "source-to-sink", 3L);
        StreamEnvelope[] events = {
                StreamEnvelope.record(channel, 1L, new byte[]{1}),
                StreamEnvelope.watermarkStatus(channel, 2L,
                        StreamEnvelope.WatermarkStatus.IDLE, 1L),
                StreamEnvelope.watermarkStatus(channel, 3L,
                        StreamEnvelope.WatermarkStatus.ACTIVE, 2L),
                StreamEnvelope.asyncCompletion(channel, 4L, "snapshot-upload-1"),
                StreamEnvelope.endOfInput(channel, 5L),
                StreamEnvelope.cancel(channel, 6L),
                StreamEnvelope.error(channel, 7L, "USER_FUNCTION_FAILED")
        };

        for (StreamEnvelope event : events) {
            byte[] encoded = StreamEnvelopeCodec.encode(event);
            assertEquals(event, StreamEnvelopeCodec.decode(encoded));
            assertArrayEquals(encoded, StreamEnvelopeCodec.encode(
                    StreamEnvelopeCodec.decode(encoded)));
        }
        assertEquals(false, events[0].hasEventTimestamp());
        assertEquals(StreamEnvelope.WatermarkStatus.IDLE,
                events[1].watermarkStatus());
        assertEquals(1L, events[1].watermarkStatusGeneration());
        assertEquals("snapshot-upload-1", events[3].operationIdentity());
        assertEquals("USER_FUNCTION_FAILED", events[6].errorCode());
    }

    @Test
    void statusGenerationAndTerminalEventsFailClosedBeforeGuardMutation() {
        StreamChannelIdentity channel = new StreamChannelIdentity(
                "run-1", "attempt-1", 1L, 2L, "edge-1", 3L);
        StreamEnvelopeSequenceGuard guard = new StreamEnvelopeSequenceGuard(channel);
        guard.accept(StreamEnvelope.watermarkStatus(channel, 1L,
                StreamEnvelope.WatermarkStatus.IDLE, 2L));

        assertCode("WATERMARK_STATUS_GENERATION_REPLAY", () -> guard.accept(
                StreamEnvelope.watermarkStatus(channel, 2L,
                        StreamEnvelope.WatermarkStatus.ACTIVE, 2L)));
        assertEquals(1L, guard.lastSequence());
        assertEquals(2L, guard.lastWatermarkStatusGeneration());

        guard.accept(StreamEnvelope.endOfInput(channel, 2L));
        assertCode("CHANNEL_ALREADY_TERMINATED", () -> guard.accept(
                StreamEnvelope.record(channel, 3L, new byte[]{1})));
        assertEquals(2L, guard.lastSequence());

        assertCode("INVALID_WATERMARK_STATUS", () ->
                StreamEnvelope.watermarkStatus(channel, 3L,
                        StreamEnvelope.WatermarkStatus.ACTIVE, 0L));
        assertCode("INVALID_ASYNC_COMPLETION", () ->
                StreamEnvelope.asyncCompletion(channel, 3L, ""));
        assertCode("INVALID_ERROR", () ->
                StreamEnvelope.error(channel, 3L, "bad error code"));
    }

    @Test
    void sequenceWatermarkAndEpochDriftFailBeforeAcceptance() {
        StreamChannelIdentity channel = new StreamChannelIdentity(
                "run-1", "attempt-1", 1L, 2L, "edge-1", 3L);
        StreamEnvelopeSequenceGuard guard = new StreamEnvelopeSequenceGuard(channel);
        guard.accept(StreamEnvelope.watermark(channel, 1L, 10L));

        assertCode("CHANNEL_SEQUENCE_GAP", () -> guard.accept(
                StreamEnvelope.record(channel, 3L, 11L, new byte[]{1})));
        assertCode("CHANNEL_SEQUENCE_REPLAY", () -> guard.accept(
                StreamEnvelope.record(channel, 1L, 11L, new byte[]{1})));
        assertCode("WATERMARK_REGRESSION", () -> guard.accept(
                StreamEnvelope.watermark(channel, 2L, 9L)));

        StreamChannelIdentity oldEpoch = new StreamChannelIdentity(
                "run-1", "attempt-1", 1L, 2L, "edge-1", 2L);
        assertCode("STALE_CHANNEL", () -> guard.accept(
                StreamEnvelope.record(oldEpoch, 2L, 11L, new byte[]{1})));
    }

    @Test
    void checkpointCannotCrossRunAttemptOrFence() {
        StreamChannelIdentity channel = new StreamChannelIdentity(
                "run-1", "attempt-1", 1L, 2L, "edge-1", 3L);
        CheckpointIdentity otherAttempt = new CheckpointIdentity(
                "definition-1", A, "run-1", "attempt-2", 2L,
                4L, "coordinator-1", 7L, A, B, 1, 1);
        assertCode("CHECKPOINT_SCOPE_MISMATCH", () ->
                StreamEnvelope.checkpointBarrier(channel, 1L, otherAttempt));
    }

    @Test
    void everyTruncationAndChecksumDriftFailClosed() {
        StreamChannelIdentity channel = new StreamChannelIdentity(
                "run-1", "attempt-1", 1L, 2L, "edge-1", 3L);
        byte[] encoded = StreamEnvelopeCodec.encode(
                StreamEnvelope.record(channel, 1L, 10L, new byte[]{1, 2, 3}));
        for (int length = 0; length < encoded.length; length++) {
            byte[] truncated = Arrays.copyOf(encoded, length);
            assertCode("FRAME_LENGTH_MISMATCH",
                    () -> StreamEnvelopeCodec.decode(truncated));
        }
        byte[] corrupt = encoded.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertCode("CHECKSUM_MISMATCH", () -> StreamEnvelopeCodec.decode(corrupt));
    }

    private static void assertCode(String code,
                                   org.junit.jupiter.api.function.Executable action) {
        StreamContractException failure = assertThrows(StreamContractException.class, action);
        assertEquals(code, failure.code(), failure.getMessage());
    }

    private static String repeat(char value) {
        char[] chars = new char[64];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }
}
