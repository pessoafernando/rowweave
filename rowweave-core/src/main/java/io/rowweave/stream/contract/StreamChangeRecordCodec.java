package io.rowweave.stream.contract;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** Canonical, checksummed and independently versioned Stream RETRACT frame. */
public final class StreamChangeRecordCodec {
    private static final byte[] MAGIC =
            "RWXXRTV1".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 1;
    private static final int CHECKSUM_BYTES = 32;
    private static final int MAX_FRAME_BYTES = StreamEdge.MAX_FRAME_BYTES;

    private StreamChangeRecordCodec() { }

    public static byte[] encode(StreamChangeRecord record) {
        if (record == null) {
            throw failure(StreamChangeException.Code.INVALID_RECORD,
                    "Stream RETRACT record is required");
        }
        byte[] key = record.stableKeyFrameUnsafe();
        byte[] before = record.beforeImageFrameUnsafe();
        byte[] after = record.afterImageFrameUnsafe();
        long bodySize = 16L + length(key) + length(before) + length(after);
        long frameSize = MAGIC.length + 8L + bodySize + CHECKSUM_BYTES;
        if (bodySize > Integer.MAX_VALUE || frameSize > MAX_FRAME_BYTES) {
            throw failure(StreamChangeException.Code.FRAME_LIMIT,
                    "Stream RETRACT frame exceeds its hard byte bound");
        }
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream(
                    (int) bodySize);
            try (DataOutputStream body = new DataOutputStream(bodyBytes)) {
                body.writeInt(record.kind().wireId());
                writeFrame(body, key);
                writeFrame(body, before);
                writeFrame(body, after);
            }
            byte[] payload = bodyBytes.toByteArray();
            ByteArrayOutputStream frame = new ByteArrayOutputStream(
                    (int) frameSize);
            try (DataOutputStream output = new DataOutputStream(frame)) {
                output.write(MAGIC);
                output.writeInt(VERSION);
                output.writeInt(payload.length);
                output.write(payload);
                output.write(sha256(payload));
            }
            return frame.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory Stream RETRACT encoding failed", impossible);
        }
    }

    public static StreamChangeRecord decode(byte[] frame,
            int maximumKeyBytes, int maximumBeforeImageBytes,
            int maximumAfterImageBytes) {
        validateBound(maximumKeyBytes);
        validateBound(maximumBeforeImageBytes);
        validateBound(maximumAfterImageBytes);
        if (frame == null || frame.length < MAGIC.length + 8 + 17
                + CHECKSUM_BYTES || frame.length > MAX_FRAME_BYTES) {
            throw failure(StreamChangeException.Code.MALFORMED_FRAME,
                    "Stream RETRACT frame length is invalid");
        }
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            if (!Arrays.equals(MAGIC, magic) || input.readInt() != VERSION) {
                malformed("Stream RETRACT magic or version is unsupported");
            }
            int bodyLength = input.readInt();
            if (bodyLength < 17
                    || bodyLength != frame.length - MAGIC.length - 8
                    - CHECKSUM_BYTES) {
                malformed("Stream RETRACT body length is invalid");
            }
            byte[] body = new byte[bodyLength];
            input.readFully(body);
            byte[] checksum = new byte[CHECKSUM_BYTES];
            input.readFully(checksum);
            if (input.read() != -1) {
                malformed("Stream RETRACT frame has trailing bytes");
            }
            if (!MessageDigest.isEqual(checksum, sha256(body))) {
                throw failure(StreamChangeException.Code.CHECKSUM_MISMATCH,
                        "Stream RETRACT checksum mismatch");
            }
            return decodeBody(body, maximumKeyBytes,
                    maximumBeforeImageBytes, maximumAfterImageBytes);
        } catch (StreamChangeException failure) {
            throw failure;
        } catch (EOFException truncated) {
            throw failure(StreamChangeException.Code.MALFORMED_FRAME,
                    "Stream RETRACT frame is truncated", truncated);
        } catch (IOException malformed) {
            throw failure(StreamChangeException.Code.MALFORMED_FRAME,
                    "Stream RETRACT frame cannot be decoded", malformed);
        }
    }

    public static byte[] magic() { return MAGIC.clone(); }

    private static StreamChangeRecord decodeBody(byte[] body,
            int maximumKeyBytes, int maximumBeforeImageBytes,
            int maximumAfterImageBytes) throws IOException {
        try (DataInputStream fields = new DataInputStream(
                new ByteArrayInputStream(body))) {
            StreamChangeRecord.Kind kind =
                    StreamChangeRecord.Kind.fromWire(fields.readInt());
            byte[] key = readFrame(fields, maximumKeyBytes);
            byte[] before = readFrame(fields, maximumBeforeImageBytes);
            byte[] after = readFrame(fields, maximumAfterImageBytes);
            if (fields.read() != -1) {
                malformed("Stream RETRACT record has trailing fields");
            }
            switch (kind) {
                case INSERT:
                    return StreamChangeRecord.insert(after);
                case UPDATE_BEFORE:
                    return StreamChangeRecord.updateBefore(key, before);
                case UPDATE_AFTER:
                    return StreamChangeRecord.updateAfter(key, after);
                case DELETE:
                    return before == null
                            ? StreamChangeRecord.keyOnlyDelete(key)
                            : StreamChangeRecord.delete(key, before);
                default:
                    throw new IllegalStateException("unreachable change kind");
            }
        }
    }

    private static byte[] readFrame(DataInputStream input, int maximum)
            throws IOException {
        int valueLength = input.readInt();
        if (valueLength == -1) return null;
        if (valueLength < 1) {
            malformed("Stream RETRACT component is empty");
        }
        if (valueLength > maximum) {
            throw failure(StreamChangeException.Code.FRAME_LIMIT,
                    "Stream RETRACT component exceeds its configured bound");
        }
        byte[] value = new byte[valueLength];
        input.readFully(value);
        return value;
    }

    private static void writeFrame(DataOutputStream output, byte[] value)
            throws IOException {
        if (value == null) {
            output.writeInt(-1);
        } else {
            output.writeInt(value.length);
            output.write(value);
        }
    }

    private static int length(byte[] value) {
        return value == null ? 0 : value.length;
    }

    private static void validateBound(int value) {
        if (value < 1 || value > MAX_FRAME_BYTES) {
            throw failure(StreamChangeException.Code.FRAME_LIMIT,
                    "invalid Stream RETRACT decode bound");
        }
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static void malformed(String message) {
        throw failure(StreamChangeException.Code.MALFORMED_FRAME, message);
    }

    private static StreamChangeException failure(
            StreamChangeException.Code code, String message) {
        return new StreamChangeException(code, message);
    }

    private static StreamChangeException failure(
            StreamChangeException.Code code, String message,
            Throwable cause) {
        return new StreamChangeException(code, message, cause);
    }
}
