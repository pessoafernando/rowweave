package io.rowweave.incremental;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** Canonical bounded wire for an exact captured target receipt. */
public final class CapturedTargetReceiptCodec {
    public static final int MAX_FRAME_BYTES = 64 * 1024;

    private static final byte[] MAGIC = {'A', 'X', 'C', 'T'};
    private static final int SCHEMA_VERSION = 1;
    private static final int CODEC_VERSION = 1;
    private static final int DIGEST_BYTES = 32;
    private static final int HEADER_BYTES = 4 + 1 + 1 + 1 + 4;
    private static final int MAX_PAYLOAD_BYTES = MAX_FRAME_BYTES - HEADER_BYTES - DIGEST_BYTES;

    private CapturedTargetReceiptCodec() {
    }

    public static byte[] encode(CapturedTargetReceipt receipt) {
        if (receipt == null) throw new IllegalArgumentException("captured target receipt is required");
        try {
            byte[] payload = payload(receipt);
            if (payload.length > MAX_PAYLOAD_BYTES) {
                throw new IllegalArgumentException("captured target payload exceeds limit");
            }
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream(HEADER_BYTES + payload.length);
            DataOutputStream body = new DataOutputStream(bodyBytes);
            body.write(MAGIC);
            body.writeByte(SCHEMA_VERSION);
            body.writeByte(receipt.mode().wireId());
            body.writeByte(CODEC_VERSION);
            body.writeInt(payload.length);
            body.write(payload);
            body.flush();
            byte[] canonicalBody = bodyBytes.toByteArray();
            ByteArrayOutputStream frameBytes = new ByteArrayOutputStream(canonicalBody.length + DIGEST_BYTES);
            frameBytes.write(canonicalBody);
            frameBytes.write(sha256(canonicalBody));
            return frameBytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory capture receipt encoding failed", impossible);
        }
    }

    public static CapturedTargetReceipt decode(byte[] frame) {
        if (frame == null || frame.length < HEADER_BYTES + DIGEST_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("captured target frame length is invalid");
        }
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(frame));
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            if (!Arrays.equals(MAGIC, magic)) {
                throw new IllegalArgumentException("captured target magic is invalid");
            }
            if (input.readUnsignedByte() != SCHEMA_VERSION) {
                throw new IllegalArgumentException("unknown captured target schema version");
            }
            IncrementalRunMode mode = IncrementalRunMode.fromWire(input.readUnsignedByte());
            if (input.readUnsignedByte() != CODEC_VERSION) {
                throw new IllegalArgumentException("unknown captured target codec version");
            }
            int payloadLength = input.readInt();
            if (payloadLength < 0 || payloadLength > MAX_PAYLOAD_BYTES
                    || payloadLength != frame.length - HEADER_BYTES - DIGEST_BYTES) {
                throw new IllegalArgumentException("captured target payload length is invalid");
            }
            int bodyLength = HEADER_BYTES + payloadLength;
            byte[] expected = Arrays.copyOfRange(frame, bodyLength, frame.length);
            byte[] actual = sha256(Arrays.copyOf(frame, bodyLength));
            if (!MessageDigest.isEqual(expected, actual)) {
                throw new IllegalArgumentException("captured target digest mismatch");
            }
            byte[] payload = new byte[payloadLength];
            input.readFully(payload);
            byte[] checksum = new byte[DIGEST_BYTES];
            input.readFully(checksum);
            if (input.read() != -1) throw new IllegalArgumentException("captured target trailing bytes");
            CapturedTargetReceipt decoded = decodePayload(mode, payload);
            if (!Arrays.equals(frame, encode(decoded))) {
                throw new IllegalArgumentException("captured target frame is not canonical");
            }
            return decoded;
        } catch (IOException malformed) {
            throw new IllegalArgumentException("captured target frame is malformed", malformed);
        }
    }

    public static byte[] digest(CapturedTargetReceipt receipt) {
        return sha256(encode(receipt));
    }

    private static byte[] payload(CapturedTargetReceipt receipt) throws IOException {
        byte[] request = receipt.requestIdentityBytes();
        byte[] from = IncrementalFrontierCodec.encode(receipt.from());
        byte[] target = IncrementalFrontierCodec.encode(receipt.target());
        byte[] snapshot = receipt.snapshotIdentity();
        int payloadLength = checkedPayloadLength(request, from, target, snapshot);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(payloadLength);
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeLong(receipt.feedIncarnation());
        out.write(receipt.definitionDigest());
        out.write(receipt.sourceContractDigest());
        out.writeLong(receipt.expectedSequence());
        writeBytes(out, request);
        writeBytes(out, from);
        writeBytes(out, target);
        writeBytes(out, snapshot);
        out.flush();
        return bytes.toByteArray();
    }

    private static int checkedPayloadLength(byte[]... fields) {
        int length = 8 + 32 + 32 + 8;
        try {
            for (byte[] field : fields) {
                length = Math.addExact(length, 4);
                length = Math.addExact(length, field.length);
            }
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("captured target payload length overflows", overflow);
        }
        if (length > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("captured target payload exceeds limit");
        }
        return length;
    }

    private static CapturedTargetReceipt decodePayload(
            IncrementalRunMode mode, byte[] payload) throws IOException {
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload));
        if (payload.length < 8 + 32 + 32 + 8 + 4 * 4) {
            throw new IllegalArgumentException("captured target payload is truncated");
        }
        long feedIncarnation = input.readLong();
        byte[] definition = readExact(input, 32, 32, "definition digest");
        byte[] contract = readExact(input, 32, 32, "source contract digest");
        long expectedSequence = input.readLong();
        byte[] request = readBounded(input, CapturedTargetReceipt.MAX_REQUEST_BYTES, "request identity");
        byte[] fromFrame = readBounded(input, IncrementalFrontierCodec.MAX_FRAME_BYTES, "from frontier");
        byte[] targetFrame = readBounded(input, IncrementalFrontierCodec.MAX_FRAME_BYTES, "target frontier");
        byte[] snapshot = readBounded(
                input, CapturedTargetReceipt.MAX_SNAPSHOT_IDENTITY_BYTES, "snapshot identity");
        if (input.read() != -1) throw new IllegalArgumentException("captured target payload trailing bytes");
        String requestIdentity = strictUtf8(request);
        IncrementalFrontier from = IncrementalFrontierCodec.decode(fromFrame);
        IncrementalFrontier target = IncrementalFrontierCodec.decode(targetFrame);
        if (mode == IncrementalRunMode.BACKFILL_V1) {
            return CapturedTargetReceipt.backfill(
                    feedIncarnation, definition, contract, expectedSequence,
                    from, target, requestIdentity, snapshot);
        }
        return CapturedTargetReceipt.normal(
                feedIncarnation, definition, contract, expectedSequence,
                from, requestIdentity, mode, target, snapshot);
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] readExact(
            DataInputStream input, int expected, int limit, String field) throws IOException {
        byte[] value = new byte[expected];
        if (expected > limit || input.available() < expected) {
            throw new IllegalArgumentException(field + " length is invalid");
        }
        input.readFully(value);
        return value;
    }

    private static byte[] readBounded(
            DataInputStream input, int limit, String field) throws IOException {
        if (input.available() < 4) throw new IllegalArgumentException(field + " length is missing");
        int length = input.readInt();
        if (length <= 0 || length > limit || input.available() < length) {
            throw new IllegalArgumentException(field + " length is invalid");
        }
        byte[] value = new byte[length];
        input.readFully(value);
        return value;
    }

    private static String strictUtf8(byte[] value) {
        try {
            return java.nio.charset.StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                    .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
                    .decode(java.nio.ByteBuffer.wrap(value)).toString();
        } catch (java.nio.charset.CharacterCodingException invalid) {
            throw new IllegalArgumentException("capture request identity is invalid UTF-8", invalid);
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
