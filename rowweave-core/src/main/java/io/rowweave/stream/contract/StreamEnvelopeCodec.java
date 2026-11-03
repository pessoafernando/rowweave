package io.rowweave.stream.contract;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Canonical, checksummed and bounded Stream data-lane envelope codec. */
public final class StreamEnvelopeCodec {
    private static final byte[] MAGIC =
            "RWXXSEV1".getBytes(StandardCharsets.US_ASCII);
    private static final int HEADER_BYTES = 16;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_BODY_BYTES =
            StreamContractSupport.MAX_FRAME_BYTES + 16 * 1024;
    public static final int MAX_FRAME_BYTES =
            HEADER_BYTES + MAX_BODY_BYTES + DIGEST_BYTES;

    private StreamEnvelopeCodec() { }

    public static byte[] magic() { return MAGIC.clone(); }

    public static byte[] encode(StreamEnvelope event) {
        if (event == null) throw new IllegalArgumentException("event is required");
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bodyBytes)) {
                writeChannel(output, event.channelIdentity());
                output.writeLong(event.sequence());
                output.writeInt(event.kind().wireId());
                switch (event.kind()) {
                    case RECORD:
                        output.writeBoolean(event.hasEventTimestamp());
                        if (event.hasEventTimestamp()) output.writeLong(event.timestamp());
                        writeFrame(output, event.recordFrameUnsafe());
                        break;
                    case WATERMARK:
                        output.writeLong(event.timestamp());
                        break;
                    case CHECKPOINT_BARRIER:
                        writeCheckpoint(output, event.checkpointIdentity());
                        break;
                    case WATERMARK_STATUS:
                        output.writeInt(event.watermarkStatus().wireId());
                        output.writeLong(event.watermarkStatusGeneration());
                        break;
                    case ASYNC_COMPLETION:
                        writeText(output, event.operationIdentity());
                        break;
                    case ERROR:
                        writeText(output, event.errorCode());
                        break;
                    case END_OF_INPUT:
                    case CANCEL:
                        break;
                    default:
                        throw new IllegalStateException("unhandled Stream event kind");
                }
            }
            byte[] body = bodyBytes.toByteArray();
            if (body.length > MAX_BODY_BYTES) {
                throw failure("LIMIT_EXCEEDED", "envelope.body",
                        "envelope exceeds its limit");
            }
            ByteArrayOutputStream frame = new ByteArrayOutputStream(
                    HEADER_BYTES + body.length + DIGEST_BYTES);
            try (DataOutputStream output = new DataOutputStream(frame)) {
                output.write(MAGIC);
                output.writeInt(1);
                output.writeInt(body.length);
                output.write(body);
                output.write(sha256(body, 0, body.length));
            }
            return frame.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory Stream envelope encode failed", impossible);
        }
    }

    public static StreamEnvelope decode(byte[] frame) {
        if (frame == null || frame.length < HEADER_BYTES + DIGEST_BYTES) {
            throw failure("FRAME_LENGTH_MISMATCH", "envelope", "frame is too short");
        }
        if (frame.length > MAX_FRAME_BYTES) {
            throw failure("LIMIT_EXCEEDED", "envelope", "frame exceeds its limit");
        }
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            if (!MessageDigest.isEqual(MAGIC, magic)) {
                throw failure("INVALID_MAGIC", "envelope.magic", "invalid magic");
            }
            if (input.readInt() != 1) {
                throw failure("UNSUPPORTED_ENVELOPE_VERSION", "envelope.version",
                        "unsupported Stream envelope version");
            }
            int bodyLength = input.readInt();
            if (bodyLength < 0 || bodyLength > MAX_BODY_BYTES
                    || HEADER_BYTES + (long) bodyLength + DIGEST_BYTES != frame.length) {
                throw failure("FRAME_LENGTH_MISMATCH", "envelope.body",
                        "frame length does not match body length");
            }
            byte[] actual = sha256(frame, HEADER_BYTES, bodyLength);
            byte[] expected = new byte[DIGEST_BYTES];
            System.arraycopy(frame, HEADER_BYTES + bodyLength,
                    expected, 0, DIGEST_BYTES);
            if (!MessageDigest.isEqual(actual, expected)) {
                throw failure("CHECKSUM_MISMATCH", "envelope.digest",
                        "Stream envelope checksum mismatch");
            }
            try (DataInputStream body = new DataInputStream(
                    new ByteArrayInputStream(frame, HEADER_BYTES, bodyLength))) {
                StreamChannelIdentity channel = readChannel(body);
                long sequence = body.readLong();
                StreamEnvelope.Kind kind = StreamEnvelope.Kind.fromWire(body.readInt());
                StreamEnvelope event;
                switch (kind) {
                    case RECORD:
                        boolean hasTimestamp = body.readBoolean();
                        Long timestamp = hasTimestamp ? body.readLong() : null;
                        byte[] record = readFrame(body, "envelope.recordFrame");
                        event = timestamp == null
                                ? StreamEnvelope.record(channel, sequence, record)
                                : StreamEnvelope.record(channel, sequence, timestamp, record);
                        break;
                    case WATERMARK:
                        event = StreamEnvelope.watermark(
                                channel, sequence, body.readLong());
                        break;
                    case CHECKPOINT_BARRIER:
                        event = StreamEnvelope.checkpointBarrier(
                                channel, sequence, readCheckpoint(body));
                        break;
                    case WATERMARK_STATUS:
                        event = StreamEnvelope.watermarkStatus(channel, sequence,
                                StreamEnvelope.WatermarkStatus.fromWire(body.readInt()),
                                body.readLong());
                        break;
                    case END_OF_INPUT:
                        event = StreamEnvelope.endOfInput(channel, sequence);
                        break;
                    case ASYNC_COMPLETION:
                        event = StreamEnvelope.asyncCompletion(channel, sequence,
                                readText(body, "envelope.operationIdentity"));
                        break;
                    case CANCEL:
                        event = StreamEnvelope.cancel(channel, sequence);
                        break;
                    case ERROR:
                        event = StreamEnvelope.error(channel, sequence,
                                readText(body, "envelope.errorCode"));
                        break;
                    default:
                        throw new IllegalStateException("unhandled Stream event kind");
                }
                if (body.available() != 0) {
                    throw failure("FRAME_LENGTH_MISMATCH", "envelope.body",
                            "Stream envelope body has trailing bytes");
                }
                return event;
            }
        } catch (StreamContractException failure) {
            throw failure;
        } catch (EOFException truncated) {
            throw failure("FRAME_LENGTH_MISMATCH", "envelope.body",
                    "Stream envelope body is truncated");
        } catch (IOException malformed) {
            throw failure("FRAME_LENGTH_MISMATCH", "envelope.body",
                    "Stream envelope body is malformed");
        }
    }

    private static void writeChannel(
            DataOutputStream output, StreamChannelIdentity channel) throws IOException {
        writeText(output, channel.runId());
        writeText(output, channel.attemptId());
        output.writeLong(channel.fenceToken());
        output.writeLong(channel.assignmentGeneration());
        writeText(output, channel.channelId());
        output.writeLong(channel.channelEpoch());
    }

    private static StreamChannelIdentity readChannel(DataInputStream input)
            throws IOException {
        return new StreamChannelIdentity(
                readText(input, "channel.runId"),
                readText(input, "channel.attemptId"),
                input.readLong(), input.readLong(),
                readText(input, "channel.channelId"), input.readLong());
    }

    private static void writeCheckpoint(
            DataOutputStream output, CheckpointIdentity checkpoint) throws IOException {
        writeText(output, checkpoint.definitionId());
        writeText(output, checkpoint.artifactId());
        writeText(output, checkpoint.runId());
        writeText(output, checkpoint.attemptId());
        output.writeLong(checkpoint.fenceToken());
        output.writeLong(checkpoint.coordinatorTerm());
        writeText(output, checkpoint.coordinatorIncarnation());
        output.writeLong(checkpoint.checkpointSequence());
        writeText(output, checkpoint.planDigest());
        writeText(output, checkpoint.deploymentDigest());
        output.writeInt(checkpoint.dataProtocolVersion());
        output.writeInt(checkpoint.stateFormatVersion());
    }

    private static CheckpointIdentity readCheckpoint(DataInputStream input)
            throws IOException {
        return new CheckpointIdentity(
                readText(input, "checkpoint.definitionId"),
                readText(input, "checkpoint.artifactId"),
                readText(input, "checkpoint.runId"),
                readText(input, "checkpoint.attemptId"),
                input.readLong(), input.readLong(),
                readText(input, "checkpoint.coordinatorIncarnation"),
                input.readLong(), readText(input, "checkpoint.planDigest"),
                readText(input, "checkpoint.deploymentDigest"),
                input.readInt(), input.readInt());
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readText(DataInputStream input, String path)
            throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > 256 || length > input.available()) {
            throw failure("LIMIT_EXCEEDED", path, "text length exceeds its limit");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        try {
            return StreamContractSupport.text(StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(java.nio.ByteBuffer.wrap(bytes)).toString(), path, 256);
        } catch (CharacterCodingException malformed) {
            throw failure("MALFORMED_UTF8", path, "text is not valid UTF-8");
        }
    }

    private static void writeFrame(DataOutputStream output, byte[] value)
            throws IOException {
        output.writeInt(value.length);
        output.write(value);
    }

    private static byte[] readFrame(DataInputStream input, String path)
            throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > StreamContractSupport.MAX_FRAME_BYTES
                || length > input.available()) {
            throw failure("LIMIT_EXCEEDED", path, "record frame length exceeds its limit");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return bytes;
    }

    private static StreamContractException failure(
            String code, String path, String message) {
        return StreamContractSupport.failure(code, path, message);
    }

    private static byte[] sha256(byte[] bytes, int offset, int length) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes, offset, length);
            return digest.digest();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
