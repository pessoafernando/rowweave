package io.rowweave.incremental;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** Canonical bounded wire for one provider target-capture response. */
public final class IncrementalCaptureResponseCodec {
    public static final int MAX_FRAME_BYTES = 64 * 1024;
    private static final int MAGIC = 0x52574952; // RWIR
    private static final int FRAME_VERSION = 1;
    private static final int HEADER_BYTES = 12;
    private static final int DIGEST_BYTES = 32;

    private IncrementalCaptureResponseCodec() { }

    public static byte[] encode(IncrementalCaptureResponse response) {
        if (response == null) {
            throw new IllegalArgumentException("capture response is required");
        }
        try {
            ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream();
            DataOutputStream payload = new DataOutputStream(payloadBytes);
            writeBytes(payload, IncrementalFrontierCodec.encode(
                    response.target()));
            writeBytes(payload, response.snapshotIdentity());
            payload.flush();
            byte[] value = payloadBytes.toByteArray();
            long total = (long) HEADER_BYTES + value.length + DIGEST_BYTES;
            if (total > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException(
                        "capture response frame exceeds bound");
            }
            ByteArrayOutputStream bodyBytes =
                    new ByteArrayOutputStream((int) total);
            DataOutputStream body = new DataOutputStream(bodyBytes);
            body.writeInt(MAGIC);
            body.writeInt(FRAME_VERSION);
            body.writeInt(value.length);
            body.write(value);
            body.flush();
            byte[] canonical = bodyBytes.toByteArray();
            body.write(sha256(canonical));
            body.flush();
            return bodyBytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory capture response encoding failed", impossible);
        }
    }

    public static IncrementalCaptureResponse decode(byte[] frame) {
        if (frame == null || frame.length < HEADER_BYTES + DIGEST_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "capture response frame length is invalid");
        }
        int bodyLength = frame.length - DIGEST_BYTES;
        if (!MessageDigest.isEqual(
                Arrays.copyOfRange(frame, bodyLength, frame.length),
                sha256(Arrays.copyOf(frame, bodyLength)))) {
            throw new IllegalArgumentException(
                    "capture response checksum is invalid");
        }
        try {
            DataInputStream input = new DataInputStream(
                    new ByteArrayInputStream(frame, 0, bodyLength));
            if (input.readInt() != MAGIC
                    || input.readInt() != FRAME_VERSION) {
                throw new IllegalArgumentException(
                        "capture response header is invalid");
            }
            int length = input.readInt();
            if (length < 0 || length != input.available()) {
                throw new IllegalArgumentException(
                        "capture response payload length is invalid");
            }
            IncrementalFrontier target = IncrementalFrontierCodec.decode(
                    readBytes(input, IncrementalFrontierCodec.MAX_FRAME_BYTES,
                            "target frontier"));
            byte[] snapshot = readBytes(input,
                    CapturedTargetReceipt.MAX_SNAPSHOT_IDENTITY_BYTES,
                    "snapshot identity");
            if (input.read() != -1) {
                throw new IllegalArgumentException(
                        "capture response payload has trailing bytes");
            }
            IncrementalCaptureResponse decoded =
                    new IncrementalCaptureResponse(target, snapshot);
            if (!MessageDigest.isEqual(frame, encode(decoded))) {
                throw new IllegalArgumentException(
                        "capture response frame is not canonical");
            }
            return decoded;
        } catch (EOFException truncated) {
            throw new IllegalArgumentException(
                    "capture response frame is truncated", truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException(
                    "capture response frame is malformed", malformed);
        }
    }

    private static void writeBytes(DataOutputStream out, byte[] value)
            throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] readBytes(
            DataInputStream in, int limit, String field) throws IOException {
        if (in.available() < Integer.BYTES) {
            throw new IllegalArgumentException(field + " length is missing");
        }
        int length = in.readInt();
        if (length <= 0 || length > limit || length > in.available()) {
            throw new IllegalArgumentException(field + " length is invalid");
        }
        byte[] value = new byte[length];
        in.readFully(value);
        return value;
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
