package io.rowweave.stream.cluster;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Canonical checksummed frame for one bounded Stream task progress sample. */
final class StreamTaskProgressCodec {
    private static final byte[] MAGIC =
            "RWXXTPS1".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 1;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_TEXT_BYTES = 256;
    static final int MAX_FRAME_BYTES = 4 * 1024;

    private StreamTaskProgressCodec() { }

    static byte[] encode(StreamTaskProgress progress) {
        if (progress == null) {
            throw new IllegalArgumentException(
                    "Stream task progress is required");
        }
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            try (DataOutputStream body = new DataOutputStream(bodyBytes)) {
                writeText(body, progress.runId());
                writeText(body, progress.attemptId());
                body.writeLong(progress.fenceToken());
                body.writeLong(progress.coordinatorTerm());
                body.writeLong(progress.assignmentGeneration());
                writeText(body, progress.taskIdentity().operatorUid());
                body.writeInt(progress.taskIdentity().subtaskIndex());
                body.writeLong(progress.sampleSequence());
                body.writeBoolean(progress.complete());
                body.writeLong(progress.recordsRead());
                body.writeLong(progress.recordsWritten());
                body.writeLong(progress.bytesRead());
                body.writeLong(progress.bytesWritten());
                body.writeLong(progress.operatorWatermark());
                body.writeBoolean(progress.watermarkIdle());
                body.writeLong(progress.backlogEvents());
                body.writeLong(progress.backlogBytes());
                body.writeInt(progress.mailboxHighWaterEvents());
                body.writeLong(progress.channelHighWaterBytes());
                body.writeLong(progress.managedStateBytes());
                body.writeInt(progress.timerHighWaterCount());
                body.writeInt(progress.windowHighWaterCount());
                body.writeLong(progress.backpressuredNanos());
            }
            byte[] body = bodyBytes.toByteArray();
            ByteArrayOutputStream frameBytes = new ByteArrayOutputStream();
            try (DataOutputStream frame = new DataOutputStream(frameBytes)) {
                frame.write(MAGIC);
                frame.writeInt(VERSION);
                frame.writeInt(body.length);
                frame.write(body);
                frame.write(sha256(body));
            }
            byte[] frame = frameBytes.toByteArray();
            if (frame.length > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException(
                        "Stream task progress frame exceeds its bound");
            }
            return frame;
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    static StreamTaskProgress decode(byte[] frame) {
        if (frame == null || frame.length > MAX_FRAME_BYTES) {
            throw invalid("Stream task progress frame exceeds its bound");
        }
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            int version = input.readInt();
            int bodyLength = input.readInt();
            if (!MessageDigest.isEqual(MAGIC, magic) || version != VERSION
                    || bodyLength < 0 || MAGIC.length + 8L + bodyLength
                    + DIGEST_BYTES != frame.length) {
                throw invalid("Stream task progress frame header is invalid");
            }
            byte[] body = new byte[bodyLength];
            input.readFully(body);
            byte[] expected = new byte[DIGEST_BYTES];
            input.readFully(expected);
            if (!MessageDigest.isEqual(expected, sha256(body))) {
                throw invalid("Stream task progress checksum differs");
            }
            try (DataInputStream value = new DataInputStream(
                    new ByteArrayInputStream(body))) {
                StreamTaskProgress result = new StreamTaskProgress(
                        readText(value), readText(value), value.readLong(),
                        value.readLong(), value.readLong(),
                        new StreamTaskIdentity(
                                readText(value), value.readInt()),
                        value.readLong(), value.readBoolean(),
                        value.readLong(), value.readLong(), value.readLong(),
                        value.readLong(), value.readLong(),
                        value.readBoolean(), value.readLong(), value.readLong(),
                        value.readInt(), value.readLong(), value.readLong(),
                        value.readInt(), value.readInt(), value.readLong());
                if (value.available() != 0) {
                    throw invalid(
                            "Stream task progress frame has trailing bytes");
                }
                return result;
            }
        } catch (EOFException truncated) {
            throw invalid("Stream task progress frame is truncated");
        } catch (IOException | RuntimeException malformed) {
            if (malformed instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) malformed;
            }
            throw invalid("Stream task progress frame is malformed");
        }
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0 || bytes.length > MAX_TEXT_BYTES) {
            throw invalid("Stream task progress text exceeds its bound");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readText(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > MAX_TEXT_BYTES) {
            throw invalid("Stream task progress text exceeds its bound");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        try {
            String value = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
            if (!MessageDigest.isEqual(
                    bytes, value.getBytes(StandardCharsets.UTF_8))) {
                throw invalid("Stream task progress text is noncanonical");
            }
            return value;
        } catch (java.nio.charset.CharacterCodingException malformed) {
            throw invalid("Stream task progress text is invalid UTF-8");
        }
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException(message);
    }
}
