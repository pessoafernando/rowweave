package io.rowweave.incremental;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Canonical bounded wire for the SOURCE incremental changelog contract. */
public final class IncrementalPlanDescriptorCodec {
    public static final int MAX_FRAME_BYTES = 16 * 1024;

    private static final byte[] MAGIC = {'A', 'X', 'I', 'P'};
    private static final int SCHEMA_VERSION = 1;
    private static final int CODEC_VERSION = 1;
    private static final int DIGEST_BYTES = 32;
    private static final int HEADER_BYTES = 4 + 1 + 1 + 4;
    private static final int MAX_PAYLOAD_BYTES = MAX_FRAME_BYTES - HEADER_BYTES - DIGEST_BYTES;

    private IncrementalPlanDescriptorCodec() {
    }

    public static byte[] encode(IncrementalPlanDescriptor descriptor) {
        if (descriptor == null) throw new IllegalArgumentException("incremental plan descriptor is required");
        try {
            byte[] uid = descriptor.sourceOperatorUidBytes();
            int payloadLength = checkedPayloadLength(
                    uid.length, descriptor.primaryKeyFieldIndexes().size(),
                    descriptor.recordPositionFieldIndexes().size());
            ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream(payloadLength);
            DataOutputStream payload = new DataOutputStream(payloadBytes);
            writeBytes(payload, uid);
            payload.write(descriptor.frontierContractDigest());
            writeIndexes(payload, descriptor.primaryKeyFieldIndexes());
            payload.writeInt(descriptor.operationFieldIndex());
            writeIndexes(payload, descriptor.recordPositionFieldIndexes());
            payload.write(descriptor.outputSchemaFingerprint());
            payload.writeBoolean(descriptor.deleteCapable());
            payload.write(descriptor.partitionSetDigest());
            payload.writeByte(descriptor.sinkCommitProfile().wireId());
            payload.flush();
            byte[] canonicalPayload = payloadBytes.toByteArray();

            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream(HEADER_BYTES + payloadLength);
            DataOutputStream body = new DataOutputStream(bodyBytes);
            body.write(MAGIC);
            body.writeByte(SCHEMA_VERSION);
            body.writeByte(CODEC_VERSION);
            body.writeInt(payloadLength);
            body.write(canonicalPayload);
            body.flush();
            byte[] canonicalBody = bodyBytes.toByteArray();

            ByteArrayOutputStream frame = new ByteArrayOutputStream(canonicalBody.length + DIGEST_BYTES);
            frame.write(canonicalBody);
            frame.write(sha256(canonicalBody));
            return frame.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory incremental descriptor encoding failed", impossible);
        }
    }

    public static IncrementalPlanDescriptor decode(byte[] frame) {
        if (frame == null || frame.length < HEADER_BYTES + DIGEST_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("incremental descriptor frame length is invalid");
        }
        ByteBuffer header = ByteBuffer.wrap(frame);
        byte[] magic = new byte[MAGIC.length];
        header.get(magic);
        if (!Arrays.equals(MAGIC, magic)) {
            throw new IllegalArgumentException("incremental descriptor magic is invalid");
        }
        if ((header.get() & 0xff) != SCHEMA_VERSION) {
            throw new IllegalArgumentException("unknown incremental descriptor schema version");
        }
        if ((header.get() & 0xff) != CODEC_VERSION) {
            throw new IllegalArgumentException("unknown incremental descriptor codec version");
        }
        int payloadLength = header.getInt();
        if (payloadLength < 0 || payloadLength > MAX_PAYLOAD_BYTES
                || payloadLength != frame.length - HEADER_BYTES - DIGEST_BYTES) {
            throw new IllegalArgumentException("incremental descriptor payload length is invalid");
        }
        int bodyLength = HEADER_BYTES + payloadLength;
        if (!MessageDigest.isEqual(
                Arrays.copyOfRange(frame, bodyLength, frame.length),
                sha256(Arrays.copyOf(frame, bodyLength)))) {
            throw new IllegalArgumentException("incremental descriptor digest mismatch");
        }
        byte[] payload = Arrays.copyOfRange(frame, HEADER_BYTES, bodyLength);
        IncrementalPlanDescriptor decoded = decodePayload(payload);
        if (!Arrays.equals(frame, encode(decoded))) {
            throw new IllegalArgumentException("incremental descriptor frame is not canonical");
        }
        return decoded;
    }

    public static byte[] digest(IncrementalPlanDescriptor descriptor) {
        return sha256(encode(descriptor));
    }

    private static IncrementalPlanDescriptor decodePayload(byte[] payload) {
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload));
            byte[] uid = readBytes(input, IncrementalPlanDescriptor.MAX_OPERATOR_UID_BYTES, "source UID");
            byte[] frontier = readExact(input, 32, "frontier contract digest");
            List<Integer> keys = readIndexes(input, "primary key indexes");
            if (input.available() < 4) throw new IllegalArgumentException("operation field is truncated");
            int operation = input.readInt();
            List<Integer> positions = readIndexes(input, "record position indexes");
            byte[] schema = readExact(input, 32, "output schema fingerprint");
            if (input.available() < 34) throw new IllegalArgumentException("incremental descriptor tail is truncated");
            boolean deleteCapable = input.readBoolean();
            byte[] partitions = readExact(input, 32, "partition set digest");
            IncrementalPlanDescriptor.SinkCommitProfile profile =
                    IncrementalPlanDescriptor.SinkCommitProfile.fromWire(input.readUnsignedByte());
            if (input.read() != -1) throw new IllegalArgumentException("incremental descriptor trailing bytes");
            return new IncrementalPlanDescriptor(
                    strictUtf8(uid), frontier, keys, operation, positions, schema,
                    deleteCapable, partitions, profile);
        } catch (IOException malformed) {
            throw new IllegalArgumentException("incremental descriptor payload is malformed", malformed);
        }
    }

    private static int checkedPayloadLength(int uidBytes, int keyCount, int positionCount) {
        long length = 4L + uidBytes + 32L + 4L + 4L * keyCount + 4L
                + 4L + 4L * positionCount + 32L + 1L + 32L + 1L;
        if (length > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("incremental descriptor payload exceeds limit");
        }
        return (int) length;
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    private static void writeIndexes(DataOutputStream out, List<Integer> indexes) throws IOException {
        out.writeInt(indexes.size());
        for (Integer index : indexes) out.writeInt(index);
    }

    private static byte[] readBytes(DataInputStream input, int limit, String field) throws IOException {
        if (input.available() < 4) throw new IllegalArgumentException(field + " length is missing");
        int length = input.readInt();
        if (length <= 0 || length > limit || input.available() < length) {
            throw new IllegalArgumentException(field + " length is invalid");
        }
        return readExact(input, length, field);
    }

    private static byte[] readExact(DataInputStream input, int length, String field) throws IOException {
        if (length < 0 || input.available() < length) {
            throw new IllegalArgumentException(field + " is truncated");
        }
        byte[] value = new byte[length];
        input.readFully(value);
        return value;
    }

    private static List<Integer> readIndexes(DataInputStream input, String field) throws IOException {
        if (input.available() < 4) throw new IllegalArgumentException(field + " count is missing");
        int count = input.readInt();
        if (count <= 0 || count > IncrementalPlanDescriptor.MAX_FIELD_INDEXES
                || input.available() < count * 4L) {
            throw new IllegalArgumentException(field + " count is invalid");
        }
        List<Integer> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) values.add(input.readInt());
        return values;
    }

    private static String strictUtf8(byte[] value) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(value)).toString();
        } catch (CharacterCodingException malformed) {
            throw new IllegalArgumentException("source operator UID is invalid UTF-8", malformed);
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
