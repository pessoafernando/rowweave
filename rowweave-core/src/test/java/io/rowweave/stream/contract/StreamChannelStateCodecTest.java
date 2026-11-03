package io.rowweave.stream.contract;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** bounded, canonical unaligned input-lane state. */
class StreamChannelStateCodecTest {

    @Test
    void canonicalStateBindsCutLaneEpochCreditAndOrderedFrames() {
        StreamChannelIdentity channel = channel();
        CheckpointIdentity checkpoint = checkpoint();
        StreamEnvelope record = StreamEnvelope.record(
                channel, 3L, 17L, new byte[] {1, 2, 3});
        StreamEnvelope watermark = StreamEnvelope.watermark(
                channel, 4L, 19L);
        int retainedBytes = StreamEnvelopeCodec.encode(record).length
                + StreamEnvelopeCodec.encode(watermark).length;
        StreamChannelState state = StreamChannelState.capture(
                checkpoint, channel, 2L, 5L,
                Arrays.asList(record, watermark), 2, retainedBytes,
                StreamEnvelopeCodec.MAX_FRAME_BYTES);

        assertEquals(checkpoint, state.checkpointIdentity());
        assertEquals(channel, state.channelIdentity());
        assertEquals(2L, state.lastConsumedSequence());
        assertEquals(5L, state.barrierSequence());
        assertEquals(2, state.retainedEventCredits());
        assertEquals(retainedBytes, state.retainedByteCredits());
        assertEquals(Arrays.asList(record, watermark), state.envelopes());

        byte[] encoded = StreamChannelStateCodec.encode(state);
        StreamChannelState decoded = StreamChannelStateCodec.decode(
                encoded, 2, retainedBytes,
                StreamEnvelopeCodec.MAX_FRAME_BYTES);
        assertEquals(state, decoded);
        assertArrayEquals(encoded, StreamChannelStateCodec.encode(decoded));
        assertArrayEquals("RWXXUCS1".getBytes(StandardCharsets.US_ASCII),
                Arrays.copyOf(encoded, 8));
    }

    @Test
    void emptyLaneCutIsCanonicalAndResourceLimitsFailBeforeAcceptance() {
        StreamChannelState empty = StreamChannelState.capture(
                checkpoint(), channel(), 8L, 9L,
                Collections.<StreamEnvelope>emptyList(), 1, 1L,
                StreamEnvelopeCodec.MAX_FRAME_BYTES);
        assertEquals(0, empty.retainedEventCredits());
        assertEquals(0L, empty.retainedByteCredits());

        StreamEnvelope record = StreamEnvelope.record(
                channel(), 3L, new byte[] {1});
        int bytes = StreamEnvelopeCodec.encode(record).length;
        assertCode("CHANNEL_STATE_EVENT_LIMIT", () ->
                StreamChannelState.capture(checkpoint(), channel(), 2L, 4L,
                        Collections.singletonList(record), 0, bytes,
                        StreamEnvelopeCodec.MAX_FRAME_BYTES));
        assertCode("CHANNEL_STATE_BYTE_LIMIT", () ->
                StreamChannelState.capture(checkpoint(), channel(), 2L, 4L,
                        Collections.singletonList(record), 1, bytes - 1L,
                        StreamEnvelopeCodec.MAX_FRAME_BYTES));
        assertCode("CHANNEL_STATE_FRAME_LIMIT", () ->
                StreamChannelState.capture(checkpoint(), channel(), 2L, 4L,
                        Collections.singletonList(record), 1, bytes,
                        bytes - 1));
    }

    @Test
    void staleScopeGapsBarriersAndCorruptPayloadFailClosed() {
        StreamChannelIdentity channel = channel();
        StreamEnvelope sequenceFour = StreamEnvelope.record(
                channel, 4L, new byte[] {1});
        assertCode("CHANNEL_STATE_SEQUENCE_GAP", () ->
                StreamChannelState.capture(checkpoint(), channel, 2L, 5L,
                        Collections.singletonList(sequenceFour), 1, 1_024L,
                        StreamEnvelopeCodec.MAX_FRAME_BYTES));
        assertCode("CHANNEL_STATE_BARRIER_INCLUDED", () ->
                StreamChannelState.capture(checkpoint(), channel, 2L, 4L,
                        Collections.singletonList(
                                StreamEnvelope.checkpointBarrier(
                                        channel, 3L, checkpoint())),
                        1, 4_096L, StreamEnvelopeCodec.MAX_FRAME_BYTES));

        StreamChannelIdentity other = new StreamChannelIdentity(
                "other-run", "attempt", 7L, 3L, "edge#0", 11L);
        assertCode("CHANNEL_STATE_SCOPE_MISMATCH", () ->
                StreamChannelState.capture(checkpoint(), other, 0L, 1L,
                        Collections.<StreamEnvelope>emptyList(), 1, 1L,
                        StreamEnvelopeCodec.MAX_FRAME_BYTES));

        StreamChannelState valid = StreamChannelState.capture(
                checkpoint(), channel, 2L, 3L,
                Collections.<StreamEnvelope>emptyList(), 1, 1L,
                StreamEnvelopeCodec.MAX_FRAME_BYTES);
        byte[] corrupt = StreamChannelStateCodec.encode(valid);
        corrupt[corrupt.length - 1] ^= 1;
        assertCode("CHECKSUM_MISMATCH", () ->
                StreamChannelStateCodec.decode(corrupt, 1, 1L,
                        StreamEnvelopeCodec.MAX_FRAME_BYTES));
    }

    private static StreamChannelIdentity channel() {
        return new StreamChannelIdentity(
                "run", "attempt", 7L, 3L, "edge#0", 11L);
    }

    private static CheckpointIdentity checkpoint() {
        return new CheckpointIdentity(
                "definition", digest('a'), "run", "attempt", 7L, 5L,
                "coordinator", 13L, digest('b'), digest('c'), 2, 2);
    }

    private static void assertCode(String code,
            org.junit.jupiter.api.function.Executable action) {
        StreamContractException failure = assertThrows(
                StreamContractException.class, action);
        assertEquals(code, failure.code(), failure.getMessage());
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
