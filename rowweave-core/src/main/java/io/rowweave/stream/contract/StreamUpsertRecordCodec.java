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

/** Canonical, checksummed and independently versioned Stream UPSERT frame. */
public final class StreamUpsertRecordCodec {
    private static final byte[] MAGIC =
            "RWXXUPV1".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 1;
    private static final int CHECKSUM_BYTES = 32;
    private static final int MAX_FRAME_BYTES = StreamEdge.MAX_FRAME_BYTES;

    private StreamUpsertRecordCodec() { }

    public static byte[] encode(StreamUpsertRecord record) {
        if (record == null) {
            throw failure(StreamUpsertException.Code.INVALID_RECORD,
                    "Stream UPSERT record is required");
        }
        byte[] key = record.stableKeyFrameUnsafe();
        byte[] value = record.afterImageFrameUnsafe();
        long bodySize = 12L + key.length + (value == null ? 0L : value.length);
        long frameSize = MAGIC.length + 8L + bodySize + CHECKSUM_BYTES;
        if (bodySize > Integer.MAX_VALUE || frameSize > MAX_FRAME_BYTES) {
            throw failure(StreamUpsertException.Code.FRAME_LIMIT,
                    "Stream UPSERT frame exceeds its hard byte bound");
        }
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream(
                    (int) bodySize);
            try (DataOutputStream body = new DataOutputStream(bodyBytes)) {
                body.writeInt(record.kind().wireId());
                writeFrame(body, key);
                if (value == null) {
                    body.writeInt(-1);
                } else {
                    writeFrame(body, value);
                }
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
                    "in-memory Stream UPSERT encoding failed", impossible);
        }
    }

    public static StreamUpsertRecord decode(
            byte[] frame, int maximumKeyBytes, int maximumAfterImageBytes) {
        if (maximumKeyBytes < 1 || maximumAfterImageBytes < 1
                || maximumKeyBytes > MAX_FRAME_BYTES
                || maximumAfterImageBytes > MAX_FRAME_BYTES) {
            throw failure(StreamUpsertException.Code.FRAME_LIMIT,
                    "invalid Stream UPSERT decode bounds");
        }
        if (frame == null || frame.length < MAGIC.length + 8 + 13
                + CHECKSUM_BYTES || frame.length > MAX_FRAME_BYTES) {
            throw failure(StreamUpsertException.Code.MALFORMED_FRAME,
                    "Stream UPSERT frame length is invalid");
        }
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            if (!Arrays.equals(MAGIC, magic) || input.readInt() != VERSION) {
                malformed("Stream UPSERT magic or version is unsupported");
            }
            int bodyLength = input.readInt();
            if (bodyLength < 13
                    || bodyLength != frame.length - MAGIC.length - 8
                    - CHECKSUM_BYTES) {
                malformed("Stream UPSERT body length is invalid");
            }
            byte[] body = new byte[bodyLength];
            input.readFully(body);
            byte[] checksum = new byte[CHECKSUM_BYTES];
            input.readFully(checksum);
            if (input.read() != -1) malformed("Stream UPSERT frame has trailing bytes");
            if (!MessageDigest.isEqual(checksum, sha256(body))) {
                throw failure(StreamUpsertException.Code.CHECKSUM_MISMATCH,
                        "Stream UPSERT checksum mismatch");
            }
            try (DataInputStream fields = new DataInputStream(
                    new ByteArrayInputStream(body))) {
                StreamUpsertRecord.Kind kind =
                        StreamUpsertRecord.Kind.fromWire(fields.readInt());
                byte[] key = readFrame(fields, maximumKeyBytes, false);
                int valueLength = fields.readInt();
                byte[] value = null;
                if (valueLength >= 0) {
                    if (valueLength < 1 || valueLength > maximumAfterImageBytes) {
                        throw failure(StreamUpsertException.Code.FRAME_LIMIT,
                                "Stream UPSERT after-image exceeds its bound");
                    }
                    value = new byte[valueLength];
                    fields.readFully(value);
                }
                if (fields.read() != -1
                        || (kind == StreamUpsertRecord.Kind.AFTER_IMAGE
                        && value == null)
                        || (kind == StreamUpsertRecord.Kind.TOMBSTONE
                        && value != null)) {
                    malformed("Stream UPSERT record shape is invalid");
                }
                return kind == StreamUpsertRecord.Kind.AFTER_IMAGE
                        ? StreamUpsertRecord.afterImage(key, value)
                        : StreamUpsertRecord.tombstone(key);
            }
        } catch (StreamUpsertException failure) {
            throw failure;
        } catch (EOFException truncated) {
            throw failure(StreamUpsertException.Code.MALFORMED_FRAME,
                    "Stream UPSERT frame is truncated", truncated);
        } catch (IOException malformed) {
            throw failure(StreamUpsertException.Code.MALFORMED_FRAME,
                    "Stream UPSERT frame cannot be decoded", malformed);
        }
    }

    public static byte[] magic() { return MAGIC.clone(); }

    private static byte[] readFrame(
            DataInputStream input, int maximum, boolean nullable)
            throws IOException {
        int length = input.readInt();
        if (nullable && length == -1) return null;
        if (length < 1) malformed("Stream UPSERT component is empty");
        if (length > maximum) {
            throw failure(StreamUpsertException.Code.FRAME_LIMIT,
                    "Stream UPSERT component exceeds its configured bound");
        }
        byte[] value = new byte[length];
        input.readFully(value);
        return value;
    }

    private static void writeFrame(DataOutputStream output, byte[] frame)
            throws IOException {
        output.writeInt(frame.length);
        output.write(frame);
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static void malformed(String message) {
        throw failure(StreamUpsertException.Code.MALFORMED_FRAME, message);
    }

    private static StreamUpsertException failure(
            StreamUpsertException.Code code, String message) {
        return new StreamUpsertException(code, message);
    }

    private static StreamUpsertException failure(
            StreamUpsertException.Code code, String message, Throwable cause) {
        return new StreamUpsertException(code, message, cause);
    }
}
