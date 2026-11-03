package io.rowweave.stream.contract;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** One bounded input lane captured by an unaligned checkpoint. */
public final class StreamChannelState {
    private final CheckpointIdentity checkpointIdentity;
    private final StreamChannelIdentity channelIdentity;
    private final long lastConsumedSequence;
    private final long barrierSequence;
    private final List<StreamEnvelope> envelopes;
    private final long retainedByteCredits;

    private StreamChannelState(CheckpointIdentity checkpointIdentity,
            StreamChannelIdentity channelIdentity,
            long lastConsumedSequence, long barrierSequence,
            List<StreamEnvelope> envelopes, long retainedByteCredits) {
        this.checkpointIdentity = checkpointIdentity;
        this.channelIdentity = channelIdentity;
        this.lastConsumedSequence = lastConsumedSequence;
        this.barrierSequence = barrierSequence;
        this.envelopes = Collections.unmodifiableList(
                new ArrayList<>(envelopes));
        this.retainedByteCredits = retainedByteCredits;
    }

    public static StreamChannelState capture(
            CheckpointIdentity checkpointIdentity,
            StreamChannelIdentity channelIdentity,
            long lastConsumedSequence, long barrierSequence,
            List<StreamEnvelope> envelopes, int maximumEvents,
            long maximumBytes, int maximumFrameBytes) {
        if (checkpointIdentity == null || channelIdentity == null
                || envelopes == null || lastConsumedSequence < 0L
                || barrierSequence <= lastConsumedSequence
                || maximumEvents < 0 || maximumBytes < 0L
                || maximumFrameBytes <= 0) {
            throw failure("INVALID_CHANNEL_STATE",
                    "channelState",
                    "channel state cut and finite resource limits are required");
        }
        if (!checkpointIdentity.runId().equals(channelIdentity.runId())
                || !checkpointIdentity.attemptId().equals(
                channelIdentity.attemptId())
                || checkpointIdentity.fenceToken()
                != channelIdentity.fenceToken()) {
            throw failure("CHANNEL_STATE_SCOPE_MISMATCH",
                    "channelState.channel",
                    "channel state does not belong to its checkpoint attempt");
        }
        if (envelopes.size() > maximumEvents) {
            throw failure("CHANNEL_STATE_EVENT_LIMIT",
                    "channelState.envelopes",
                    "channel state event count exceeds its lease");
        }
        long expectedSequence = Math.addExact(lastConsumedSequence, 1L);
        long encodedBytes = 0L;
        for (StreamEnvelope envelope : envelopes) {
            if (envelope == null
                    || !channelIdentity.equals(envelope.channelIdentity())) {
                throw failure("CHANNEL_STATE_SCOPE_MISMATCH",
                        "channelState.envelopes",
                        "captured envelope belongs to another channel");
            }
            if (envelope.sequence() != expectedSequence) {
                throw failure("CHANNEL_STATE_SEQUENCE_GAP",
                        "channelState.envelopes",
                        "captured envelope sequence is not contiguous");
            }
            if (envelope.kind() == StreamEnvelope.Kind.CHECKPOINT_BARRIER) {
                throw failure("CHANNEL_STATE_BARRIER_INCLUDED",
                        "channelState.envelopes",
                        "checkpoint barriers are cut markers, not replay data");
            }
            if (envelope.kind() == StreamEnvelope.Kind.CANCEL
                    || envelope.kind() == StreamEnvelope.Kind.ERROR
                    || envelope.kind() == StreamEnvelope.Kind.END_OF_INPUT) {
                throw failure("CHANNEL_STATE_TERMINAL_INCLUDED",
                        "channelState.envelopes",
                        "terminal lane events cannot precede a checkpoint barrier");
            }
            byte[] frame = StreamEnvelopeCodec.encode(envelope);
            if (frame.length > maximumFrameBytes) {
                throw failure("CHANNEL_STATE_FRAME_LIMIT",
                        "channelState.envelopes",
                        "captured envelope exceeds its frame lease");
            }
            encodedBytes = addBytes(encodedBytes, frame.length);
            if (encodedBytes > maximumBytes) {
                throw failure("CHANNEL_STATE_BYTE_LIMIT",
                        "channelState.envelopes",
                        "channel state bytes exceed their lease");
            }
            expectedSequence = Math.addExact(expectedSequence, 1L);
        }
        if (expectedSequence != barrierSequence) {
            throw failure("CHANNEL_STATE_SEQUENCE_GAP",
                    "channelState.barrierSequence",
                    "barrier does not immediately follow captured lane data");
        }
        return new StreamChannelState(checkpointIdentity, channelIdentity,
                lastConsumedSequence, barrierSequence, envelopes,
                encodedBytes);
    }

    public CheckpointIdentity checkpointIdentity() {
        return checkpointIdentity;
    }

    public StreamChannelIdentity channelIdentity() { return channelIdentity; }

    public long lastConsumedSequence() { return lastConsumedSequence; }

    public long barrierSequence() { return barrierSequence; }

    public int retainedEventCredits() { return envelopes.size(); }

    public long retainedByteCredits() { return retainedByteCredits; }

    public List<StreamEnvelope> envelopes() { return envelopes; }

    private static long addBytes(long current, int additional) {
        try {
            return Math.addExact(current, (long) additional);
        } catch (ArithmeticException overflow) {
            throw failure("CHANNEL_STATE_BYTE_LIMIT",
                    "channelState.envelopes",
                    "channel state byte accounting overflowed");
        }
    }

    private static StreamContractException failure(
            String code, String path, String message) {
        return StreamContractSupport.failure(code, path, message);
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamChannelState)) return false;
        StreamChannelState that = (StreamChannelState) other;
        return lastConsumedSequence == that.lastConsumedSequence
                && barrierSequence == that.barrierSequence
                && retainedByteCredits == that.retainedByteCredits
                && checkpointIdentity.equals(that.checkpointIdentity)
                && channelIdentity.equals(that.channelIdentity)
                && envelopes.equals(that.envelopes);
    }

    @Override public int hashCode() {
        return Objects.hash(checkpointIdentity, channelIdentity,
                lastConsumedSequence, barrierSequence, envelopes,
                retainedByteCredits);
    }
}
