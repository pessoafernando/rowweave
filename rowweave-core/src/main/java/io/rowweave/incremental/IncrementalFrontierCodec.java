package io.rowweave.incremental;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** Canonical bounded wire codec for IncrementalFrontier. */
public final class IncrementalFrontierCodec {
    public static final int MAX_PAYLOAD_BYTES = 4096;
    public static final int MAX_FRAME_BYTES = 8192;

    private static final byte[] MAGIC = {'A', 'X', 'I', 'F'};
    private static final int SCHEMA_VERSION = 1;
    private static final int CODEC_VERSION = 1;
    private static final int DIGEST_BYTES = 32;
    private static final int HEADER_BYTES = 4 + 1 + 1 + 1 + 4;

    private IncrementalFrontierCodec() {
    }

    public static byte[] encode(IncrementalFrontier frontier) {
        if (frontier == null) throw new IllegalArgumentException("frontier is required");
        try {
            byte[] payload = payload(frontier);
            if (payload.length > MAX_PAYLOAD_BYTES) {
                throw new IllegalArgumentException("frontier payload exceeds limit");
            }
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream(HEADER_BYTES + payload.length);
            DataOutputStream body = new DataOutputStream(bodyBytes);
            body.write(MAGIC);
            body.writeByte(SCHEMA_VERSION);
            body.writeByte(frontier.kind().wireId());
            body.writeByte(CODEC_VERSION);
            body.writeInt(payload.length);
            body.write(payload);
            body.flush();
            byte[] canonicalBody = bodyBytes.toByteArray();
            ByteArrayOutputStream frameBytes = new ByteArrayOutputStream(canonicalBody.length + DIGEST_BYTES);
            frameBytes.write(canonicalBody);
            frameBytes.write(sha256(canonicalBody));
            byte[] frame = frameBytes.toByteArray();
            if (frame.length > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException("frontier frame exceeds limit");
            }
            return frame;
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory frontier encoding failed", impossible);
        }
    }

    public static IncrementalFrontier decode(byte[] frame) {
        if (frame == null || frame.length < HEADER_BYTES + DIGEST_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("frontier frame length is invalid");
        }
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(frame));
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            if (!Arrays.equals(MAGIC, magic)) throw new IllegalArgumentException("frontier magic is invalid");
            int schemaVersion = input.readUnsignedByte();
            if (schemaVersion != SCHEMA_VERSION) {
                throw new IllegalArgumentException("unknown frontier schema version");
            }
            IncrementalFrontier.Kind kind = IncrementalFrontier.Kind.fromWire(input.readUnsignedByte());
            int codecVersion = input.readUnsignedByte();
            if (codecVersion != CODEC_VERSION) {
                throw new IllegalArgumentException("unknown frontier codec version");
            }
            int payloadLength = input.readInt();
            if (payloadLength < 0 || payloadLength > MAX_PAYLOAD_BYTES
                    || payloadLength != frame.length - HEADER_BYTES - DIGEST_BYTES) {
                throw new IllegalArgumentException("frontier payload length is invalid");
            }
            int bodyLength = HEADER_BYTES + payloadLength;
            byte[] expected = Arrays.copyOfRange(frame, bodyLength, frame.length);
            byte[] actual = sha256(Arrays.copyOf(frame, bodyLength));
            if (!MessageDigest.isEqual(expected, actual)) {
                throw new IllegalArgumentException("frontier digest mismatch");
            }
            byte[] payload = new byte[payloadLength];
            input.readFully(payload);
            byte[] digest = new byte[DIGEST_BYTES];
            input.readFully(digest);
            if (input.read() != -1) throw new IllegalArgumentException("frontier trailing bytes");
            IncrementalFrontier decoded = decodePayload(kind, payload);
            if (!Arrays.equals(frame, encode(decoded))) {
                throw new IllegalArgumentException("frontier frame is not canonical");
            }
            return decoded;
        } catch (IOException malformed) {
            throw new IllegalArgumentException("frontier frame is malformed", malformed);
        }
    }

    private static byte[] payload(IncrementalFrontier frontier) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        switch (frontier.kind()) {
            case BEGIN_V1:
                break;
            case INT64_SEQUENCE_V1:
                out.writeLong(frontier.sequence());
                break;
            case TIMESTAMP_KEY_V1:
                byte[] key = frontier.orderedKey().canonicalOrderBytes();
                out.writeLong(frontier.epochMicros());
                out.writeInt(key.length);
                out.write(key);
                break;
            case MANIFEST_GENERATION_V1:
                out.writeLong(frontier.generation());
                out.write(frontier.manifestDigest());
                break;
            default:
                throw new IllegalArgumentException("unsupported frontier kind");
        }
        out.flush();
        return bytes.toByteArray();
    }

    private static IncrementalFrontier decodePayload(
            IncrementalFrontier.Kind kind, byte[] payload) throws IOException {
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload));
        IncrementalFrontier frontier;
        switch (kind) {
            case BEGIN_V1:
                if (payload.length != 0) throw new IllegalArgumentException("BEGIN payload must be empty");
                frontier = IncrementalFrontier.begin();
                break;
            case INT64_SEQUENCE_V1:
                requirePayload(payload, 8);
                frontier = IncrementalFrontier.int64Sequence(input.readLong());
                break;
            case TIMESTAMP_KEY_V1:
                if (payload.length < 8 + 4 + 5) {
                    throw new IllegalArgumentException("timestamp-key payload is truncated");
                }
                long epochMicros = input.readLong();
                int keyLength = input.readInt();
                if (keyLength < 0 || keyLength > OrderedFrontierKey.MAX_ORDER_BYTES
                        || keyLength != payload.length - 12) {
                    throw new IllegalArgumentException("ordered key length is invalid");
                }
                byte[] key = new byte[keyLength];
                input.readFully(key);
                frontier = IncrementalFrontier.timestampKey(
                        epochMicros, OrderedFrontierKey.decode(key));
                break;
            case MANIFEST_GENERATION_V1:
                requirePayload(payload, 8 + 32);
                long generation = input.readLong();
                byte[] digest = new byte[32];
                input.readFully(digest);
                frontier = IncrementalFrontier.manifestGeneration(generation, digest);
                break;
            default:
                throw new IllegalArgumentException("unsupported frontier kind");
        }
        if (input.read() != -1) throw new IllegalArgumentException("frontier payload has trailing bytes");
        return frontier;
    }

    private static void requirePayload(byte[] payload, int expected) {
        if (payload.length != expected) throw new IllegalArgumentException("frontier payload length is invalid");
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
