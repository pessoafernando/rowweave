package io.rowweave.incremental;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** Canonical bounded wire for one incremental target-capture request. */
public final class IncrementalCaptureRequestCodec {
    public static final int MAX_FRAME_BYTES = 64 * 1024;
    private static final int MAGIC = 0x52574951; // RWIQ
    private static final int FRAME_VERSION = 1;
    private static final int HEADER_BYTES = 4 + 4 + 1 + 4;
    private static final int DIGEST_BYTES = 32;

    private IncrementalCaptureRequestCodec() { }

    public static byte[] encode(IncrementalCaptureRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("capture request is required");
        }
        try {
            ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream();
            DataOutputStream payload = new DataOutputStream(payloadBytes);
            payload.writeLong(request.feedIncarnation());
            payload.write(request.definitionDigest());
            payload.writeLong(request.expectedSequence());
            writeBytes(payload, request.requestIdentityBytes());
            writeBytes(payload, IncrementalFrontierCodec.encode(request.from()));
            payload.writeBoolean(request.requestedTarget() != null);
            if (request.requestedTarget() != null) {
                writeBytes(payload, IncrementalFrontierCodec.encode(
                        request.requestedTarget()));
            }
            payload.flush();
            byte[] value = payloadBytes.toByteArray();
            long total = (long) HEADER_BYTES + value.length + DIGEST_BYTES;
            if (total > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException(
                        "capture request frame exceeds bound");
            }
            ByteArrayOutputStream bodyBytes =
                    new ByteArrayOutputStream((int) total);
            DataOutputStream body = new DataOutputStream(bodyBytes);
            body.writeInt(MAGIC);
            body.writeInt(FRAME_VERSION);
            body.writeByte(request.mode().wireId());
            body.writeInt(value.length);
            body.write(value);
            body.flush();
            byte[] canonical = bodyBytes.toByteArray();
            body.write(sha256(canonical));
            body.flush();
            return bodyBytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory capture request encoding failed", impossible);
        }
    }

    public static IncrementalCaptureRequest decode(byte[] frame) {
        if (frame == null || frame.length < HEADER_BYTES + DIGEST_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "capture request frame length is invalid");
        }
        int bodyLength = frame.length - DIGEST_BYTES;
        if (!MessageDigest.isEqual(
                Arrays.copyOfRange(frame, bodyLength, frame.length),
                sha256(Arrays.copyOf(frame, bodyLength)))) {
            throw new IllegalArgumentException(
                    "capture request checksum is invalid");
        }
        try {
            DataInputStream input = new DataInputStream(
                    new ByteArrayInputStream(frame, 0, bodyLength));
            if (input.readInt() != MAGIC
                    || input.readInt() != FRAME_VERSION) {
                throw new IllegalArgumentException(
                        "capture request header is invalid");
            }
            IncrementalRunMode mode =
                    IncrementalRunMode.fromWire(input.readUnsignedByte());
            int length = input.readInt();
            if (length < 0 || length != input.available()) {
                throw new IllegalArgumentException(
                        "capture request payload length is invalid");
            }
            long incarnation = input.readLong();
            byte[] definition = readExact(input, 32, "definition digest");
            long expected = input.readLong();
            String identity = strictUtf8(readBytes(
                    input, CapturedTargetReceipt.MAX_REQUEST_BYTES,
                    "request identity"));
            IncrementalFrontier from = IncrementalFrontierCodec.decode(
                    readBytes(input, IncrementalFrontierCodec.MAX_FRAME_BYTES,
                            "from frontier"));
            IncrementalFrontier requested = input.readBoolean()
                    ? IncrementalFrontierCodec.decode(readBytes(
                    input, IncrementalFrontierCodec.MAX_FRAME_BYTES,
                    "requested target")) : null;
            if (input.read() != -1) {
                throw new IllegalArgumentException(
                        "capture request payload has trailing bytes");
            }
            IncrementalCaptureRequest decoded =
                    mode == IncrementalRunMode.BACKFILL_V1
                            ? IncrementalCaptureRequest.backfill(
                            incarnation, definition, expected, from,
                            requested, identity)
                            : IncrementalCaptureRequest.normal(
                            incarnation, definition, expected, from,
                            identity, mode);
            if (!MessageDigest.isEqual(frame, encode(decoded))) {
                throw new IllegalArgumentException(
                        "capture request frame is not canonical");
            }
            return decoded;
        } catch (EOFException truncated) {
            throw new IllegalArgumentException(
                    "capture request frame is truncated", truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException(
                    "capture request frame is malformed", malformed);
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
        return readExact(in, length, field);
    }

    private static byte[] readExact(
            DataInputStream in, int length, String field) throws IOException {
        if (length < 0 || length > in.available()) {
            throw new IllegalArgumentException(field + " is truncated");
        }
        byte[] value = new byte[length];
        in.readFully(value);
        return value;
    }

    private static String strictUtf8(byte[] encoded) {
        try {
            CharBuffer value = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(encoded));
            return value.toString();
        } catch (CharacterCodingException invalid) {
            throw new IllegalArgumentException(
                    "capture request identity is invalid UTF-8", invalid);
        }
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
