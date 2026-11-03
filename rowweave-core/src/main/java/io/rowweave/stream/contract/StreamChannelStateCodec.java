package io.rowweave.stream.contract;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Canonical, checksummed wire for one bounded unaligned input lane. */
public final class StreamChannelStateCodec {
    public static final String CODEC_ID = "rowweave.stream.channel-state-v1";
    private static final byte[] MAGIC =
            "RWXXUCS1".getBytes(StandardCharsets.US_ASCII);
    private static final int MAX_IDENTITY_OVERHEAD_BYTES = 16 * 1024;
    private static final int HARD_MAX_BODY_BYTES = 1024 * 1024 * 1024;

    private StreamChannelStateCodec() { }

    public static byte[] encode(StreamChannelState state) {
        if (state == null) {
            throw new IllegalArgumentException("channel state is required");
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                CheckpointManifestCodec.writeIdentity(
                        output, state.checkpointIdentity());
                writeChannel(output, state.channelIdentity());
                output.writeLong(state.lastConsumedSequence());
                output.writeLong(state.barrierSequence());
                output.writeInt(state.retainedEventCredits());
                output.writeLong(state.retainedByteCredits());
                for (StreamEnvelope envelope : state.envelopes()) {
                    byte[] frame = StreamEnvelopeCodec.encode(envelope);
                    output.writeInt(frame.length);
                    output.write(frame);
                }
            }
            byte[] body = bytes.toByteArray();
            if (body.length > HARD_MAX_BODY_BYTES) {
                throw failure("CHANNEL_STATE_BYTE_LIMIT",
                        "channelState.body",
                        "channel state body exceeds its hard limit");
            }
            return CheckpointManifestCodec.frame(MAGIC, body);
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory channel state encode failed", impossible);
        }
    }

    public static StreamChannelState decode(byte[] frame, int maximumEvents,
            long maximumBytes, int maximumFrameBytes) {
        int maximumBodyBytes = maximumBodyBytes(
                maximumEvents, maximumBytes, maximumFrameBytes);
        byte[] body = CheckpointManifestCodec.bodyFromFrame(
                frame, MAGIC, maximumBodyBytes);
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(body))) {
            CheckpointIdentity checkpoint =
                    CheckpointManifestCodec.readIdentity(input);
            StreamChannelIdentity channel = readChannel(input);
            long lastConsumed = input.readLong();
            long barrier = input.readLong();
            int count = input.readInt();
            long declaredBytes = input.readLong();
            if (count < 0 || count > maximumEvents) {
                throw failure("CHANNEL_STATE_EVENT_LIMIT",
                        "channelState.envelopes",
                        "channel state event count exceeds its lease");
            }
            if (declaredBytes < 0L || declaredBytes > maximumBytes) {
                throw failure("CHANNEL_STATE_BYTE_LIMIT",
                        "channelState.envelopes",
                        "channel state bytes exceed their lease");
            }
            List<StreamEnvelope> envelopes = new ArrayList<>(count);
            long actualBytes = 0L;
            for (int index = 0; index < count; index++) {
                int length = input.readInt();
                if (length <= 0 || length > maximumFrameBytes
                        || length > input.available()) {
                    throw failure("CHANNEL_STATE_FRAME_LIMIT",
                            "channelState.envelopes",
                            "captured envelope frame exceeds its lease");
                }
                actualBytes = Math.addExact(actualBytes, (long) length);
                if (actualBytes > maximumBytes) {
                    throw failure("CHANNEL_STATE_BYTE_LIMIT",
                            "channelState.envelopes",
                            "channel state bytes exceed their lease");
                }
                byte[] encoded = new byte[length];
                input.readFully(encoded);
                envelopes.add(StreamEnvelopeCodec.decode(encoded));
            }
            if (input.available() != 0 || actualBytes != declaredBytes) {
                throw failure("FRAME_LENGTH_MISMATCH",
                        "channelState.body",
                        "channel state byte accounting is not canonical");
            }
            return StreamChannelState.capture(checkpoint, channel,
                    lastConsumed, barrier, envelopes, maximumEvents,
                    maximumBytes, maximumFrameBytes);
        } catch (StreamContractException failure) {
            throw failure;
        } catch (EOFException truncated) {
            throw failure("FRAME_LENGTH_MISMATCH", "channelState.body",
                    "channel state body is truncated");
        } catch (ArithmeticException overflow) {
            throw failure("CHANNEL_STATE_BYTE_LIMIT",
                    "channelState.envelopes",
                    "channel state byte accounting overflowed");
        } catch (IOException malformed) {
            throw failure("FRAME_LENGTH_MISMATCH", "channelState.body",
                    "channel state body is malformed");
        }
    }

    private static int maximumBodyBytes(int maximumEvents,
            long maximumBytes, int maximumFrameBytes) {
        if (maximumEvents < 0 || maximumBytes < 0L
                || maximumFrameBytes <= 0) {
            throw new IllegalArgumentException(
                    "finite channel state decode limits are required");
        }
        long framed = maximumBytes + 4L * maximumEvents
                + MAX_IDENTITY_OVERHEAD_BYTES;
        if (framed < 0L) framed = Long.MAX_VALUE;
        return (int) Math.min(HARD_MAX_BODY_BYTES, framed);
    }

    private static void writeChannel(DataOutputStream output,
            StreamChannelIdentity channel) throws IOException {
        CheckpointManifestCodec.writeText(output, channel.runId());
        CheckpointManifestCodec.writeText(output, channel.attemptId());
        output.writeLong(channel.fenceToken());
        output.writeLong(channel.assignmentGeneration());
        CheckpointManifestCodec.writeText(output, channel.channelId());
        output.writeLong(channel.channelEpoch());
    }

    private static StreamChannelIdentity readChannel(DataInputStream input)
            throws IOException {
        return new StreamChannelIdentity(
                CheckpointManifestCodec.readText(
                        input, 256, "channelState.channel.runId"),
                CheckpointManifestCodec.readText(
                        input, 256, "channelState.channel.attemptId"),
                input.readLong(), input.readLong(),
                CheckpointManifestCodec.readText(
                        input, 256, "channelState.channel.channelId"),
                input.readLong());
    }

    private static StreamContractException failure(
            String code, String path, String message) {
        return StreamContractSupport.failure(code, path, message);
    }
}
