package io.rowweave.incremental;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** Canonical bounded wire codec for record positions. */
public final class IncrementalRecordPositionCodec {
    public static final int MAX_FRAME_BYTES = 4096;

    private static final byte[] MAGIC = {'A', 'X', 'I', 'P'};
    private static final int SCHEMA_VERSION = 1;
    private static final int CODEC_VERSION = 1;
    private static final int DIGEST_BYTES = 32;
    private static final int HEADER_BYTES = 4 + 1 + 1 + 1 + 4;
    public static final int MAX_PAYLOAD_BYTES = MAX_FRAME_BYTES - HEADER_BYTES - DIGEST_BYTES;

    private IncrementalRecordPositionCodec() {
    }

    public static byte[] encode(IncrementalRecordPosition position) {
        if (position == null) throw new IllegalArgumentException("record position is required");
        try {
            byte[] payload = payload(position);
            if (payload.length > MAX_PAYLOAD_BYTES) {
                throw new IllegalArgumentException("record position payload exceeds limit");
            }
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream(HEADER_BYTES + payload.length);
            DataOutputStream body = new DataOutputStream(bodyBytes);
            body.write(MAGIC);
            body.writeByte(SCHEMA_VERSION);
            body.writeByte(position.kind().wireId());
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
            throw new IllegalStateException("in-memory record position encoding failed", impossible);
        }
    }

    public static IncrementalRecordPosition decode(byte[] frame) {
        if (frame == null || frame.length < HEADER_BYTES + DIGEST_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("record position frame length is invalid");
        }
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(frame));
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            if (!Arrays.equals(MAGIC, magic)) {
                throw new IllegalArgumentException("record position magic is invalid");
            }
            int schemaVersion = input.readUnsignedByte();
            if (schemaVersion != SCHEMA_VERSION) {
                throw new IllegalArgumentException("unknown record position schema version");
            }
            IncrementalRecordPosition.Kind kind = IncrementalRecordPosition.Kind.fromWire(
                    input.readUnsignedByte());
            if (input.readUnsignedByte() != CODEC_VERSION) {
                throw new IllegalArgumentException("unknown record position codec version");
            }
            int payloadLength = input.readInt();
            if (payloadLength < 0 || payloadLength > MAX_PAYLOAD_BYTES
                    || payloadLength != frame.length - HEADER_BYTES - DIGEST_BYTES) {
                throw new IllegalArgumentException("record position payload length is invalid");
            }
            int bodyLength = HEADER_BYTES + payloadLength;
            byte[] expected = Arrays.copyOfRange(frame, bodyLength, frame.length);
            byte[] actual = sha256(Arrays.copyOf(frame, bodyLength));
            if (!MessageDigest.isEqual(expected, actual)) {
                throw new IllegalArgumentException("record position digest mismatch");
            }
            byte[] payload = new byte[payloadLength];
            input.readFully(payload);
            byte[] digest = new byte[DIGEST_BYTES];
            input.readFully(digest);
            if (input.read() != -1) throw new IllegalArgumentException("record position trailing bytes");
            IncrementalRecordPosition decoded = decodePayload(kind, payload);
            if (!Arrays.equals(frame, encode(decoded))) {
                throw new IllegalArgumentException("record position frame is not canonical");
            }
            return decoded;
        } catch (IOException malformed) {
            throw new IllegalArgumentException("record position frame is malformed", malformed);
        }
    }

    private static byte[] payload(IncrementalRecordPosition position) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.write(position.intervalDigest());
        out.writeInt(position.partitionOrdinal());
        switch (position.kind()) {
            case SEQUENCE_V1:
                out.writeLong(position.sequence());
                break;
            case TIMESTAMP_KEY_V1:
                byte[] key = position.orderedKey().canonicalOrderBytes();
                out.writeLong(position.epochMicros());
                out.writeInt(key.length);
                out.write(key);
                break;
            case MANIFEST_ENTRY_V1:
                out.writeLong(position.generation());
                out.writeLong(position.entryOrdinal());
                break;
            case SNAPSHOT_ROW_V1:
                out.writeLong(position.rowOrdinal());
                break;
            default:
                throw new IllegalArgumentException("unsupported record position kind");
        }
        out.write(position.keyDigest());
        out.flush();
        return bytes.toByteArray();
    }

    private static IncrementalRecordPosition decodePayload(
            IncrementalRecordPosition.Kind kind, byte[] payload) throws IOException {
        int minimum = IncrementalRecordPosition.DIGEST_BYTES + 4 + 8
                + IncrementalRecordPosition.DIGEST_BYTES;
        if (payload.length < minimum) {
            throw new IllegalArgumentException("record position payload is truncated");
        }
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload));
        byte[] interval = new byte[IncrementalRecordPosition.DIGEST_BYTES];
        input.readFully(interval);
        int partition = input.readInt();
        IncrementalRecordPosition position;
        switch (kind) {
            case SEQUENCE_V1:
                requirePayload(payload, minimum);
                position = IncrementalRecordPosition.sequence(
                        interval, partition, input.readLong(), readDigest(input));
                break;
            case TIMESTAMP_KEY_V1:
                long epochMicros = input.readLong();
                int keyLength = input.readInt();
                int fixed = IncrementalRecordPosition.DIGEST_BYTES + 4 + 8 + 4
                        + IncrementalRecordPosition.DIGEST_BYTES;
                if (keyLength < 0 || keyLength > OrderedFrontierKey.MAX_ORDER_BYTES
                        || keyLength != payload.length - fixed) {
                    throw new IllegalArgumentException("record position ordered key length is invalid");
                }
                byte[] key = new byte[keyLength];
                input.readFully(key);
                position = IncrementalRecordPosition.timestampKey(
                        interval, partition, epochMicros,
                        OrderedFrontierKey.decode(key), readDigest(input));
                break;
            case MANIFEST_ENTRY_V1:
                requirePayload(payload, minimum + 8);
                position = IncrementalRecordPosition.manifestEntry(
                        interval, partition, input.readLong(), input.readLong(), readDigest(input));
                break;
            case SNAPSHOT_ROW_V1:
                requirePayload(payload, minimum);
                position = IncrementalRecordPosition.snapshotRow(
                        interval, partition, input.readLong(), readDigest(input));
                break;
            default:
                throw new IllegalArgumentException("unsupported record position kind");
        }
        if (input.read() != -1) throw new IllegalArgumentException("record position payload trailing bytes");
        return position;
    }

    private static byte[] readDigest(DataInputStream input) throws IOException {
        byte[] digest = new byte[IncrementalRecordPosition.DIGEST_BYTES];
        input.readFully(digest);
        return digest;
    }

    private static void requirePayload(byte[] payload, int expected) {
        if (payload.length != expected) {
            throw new IllegalArgumentException("record position payload length is invalid");
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
