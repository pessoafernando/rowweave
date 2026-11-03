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

/** Canonical bounded feed definition frame. The frame never contains its own fingerprint. */
public final class IncrementalDefinitionCodec {
    public static final int MAX_FRAME_BYTES = 4 * 1024 * 1024;

    private static final byte[] MAGIC = {'A', 'X', 'D', 'F'};
    private static final int SCHEMA_VERSION = 1;
    private static final int CODEC_VERSION = 1;
    private static final int DIGEST_BYTES = 32;
    private static final int HEADER_BYTES = 4 + 1 + 1 + 4;
    private static final int MAX_PAYLOAD_BYTES = MAX_FRAME_BYTES - HEADER_BYTES - DIGEST_BYTES;

    private IncrementalDefinitionCodec() {
    }

    public static byte[] encode(IncrementalDefinition definition) {
        if (definition == null) throw new IllegalArgumentException("incremental definition is required");
        byte[] descriptor = IncrementalPlanDescriptorCodec.encode(definition.planDescriptor());
        int payloadLength = payloadLength(definition, descriptor.length);
        try {
            ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream(payloadLength);
            DataOutputStream payload = new DataOutputStream(payloadBytes);
            writeBytes(payload, definition.definitionIdBytes());
            writeBytes(payload, descriptor);
            payload.writeInt(definition.operators().size());
            for (IncrementalDefinition.OperatorBinding operator : definition.operators()) {
                writeBytes(payload, operator.operatorUidBytes());
                payload.writeByte(operator.kind().wireId());
                writeBytes(payload, operator.typeIdBytes());
                payload.write(operator.schemaFingerprint());
                payload.writeInt(operator.inputOperatorUids().size());
                for (int i = 0; i < operator.inputOperatorUids().size(); i++) {
                    writeBytes(payload, operator.inputOperatorUidBytes(i));
                }
                payload.write(operator.implementationDigest());
            }
            payload.writeInt(definition.sinkBindingCount());
            for (int i = 0; i < definition.sinkBindingCount(); i++) {
                payload.write(definition.sinkBindingDigest(i));
            }
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
            throw new IllegalStateException("in-memory definition encoding failed", impossible);
        }
    }

    public static IncrementalDefinition decode(byte[] frame) {
        if (frame == null || frame.length < HEADER_BYTES + DIGEST_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("incremental definition frame length is invalid");
        }
        ByteBuffer header = ByteBuffer.wrap(frame);
        byte[] magic = new byte[MAGIC.length];
        header.get(magic);
        if (!Arrays.equals(MAGIC, magic)) throw new IllegalArgumentException("definition magic is invalid");
        if ((header.get() & 0xff) != SCHEMA_VERSION) {
            throw new IllegalArgumentException("unknown definition schema version");
        }
        if ((header.get() & 0xff) != CODEC_VERSION) {
            throw new IllegalArgumentException("unknown definition codec version");
        }
        int payloadLength = header.getInt();
        if (payloadLength < 0 || payloadLength > MAX_PAYLOAD_BYTES
                || payloadLength != frame.length - HEADER_BYTES - DIGEST_BYTES) {
            throw new IllegalArgumentException("definition payload length is invalid");
        }
        int bodyLength = HEADER_BYTES + payloadLength;
        if (!MessageDigest.isEqual(
                Arrays.copyOfRange(frame, bodyLength, frame.length),
                sha256(Arrays.copyOf(frame, bodyLength)))) {
            throw new IllegalArgumentException("definition digest mismatch");
        }
        byte[] payload = Arrays.copyOfRange(frame, HEADER_BYTES, bodyLength);
        IncrementalDefinition decoded = decodePayload(payload);
        if (!Arrays.equals(frame, encode(decoded))) {
            throw new IllegalArgumentException("definition frame is not canonical");
        }
        return decoded;
    }

    static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static int payloadLength(IncrementalDefinition definition, int descriptorLength) {
        long length = 4L + definition.definitionIdBytes().length + 4L + descriptorLength + 4L;
        for (IncrementalDefinition.OperatorBinding operator : definition.operators()) {
            length = add(length, 4L + operator.operatorUidBytes().length + 1L
                    + 4L + operator.typeIdBytes().length + 32L + 4L + 32L);
            for (int i = 0; i < operator.inputOperatorUids().size(); i++) {
                length = add(length, 4L + operator.inputOperatorUidBytes(i).length);
            }
        }
        length = add(length, 4L + 32L * definition.sinkBindingCount());
        if (length > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("incremental definition payload exceeds limit");
        }
        return (int) length;
    }

    private static long add(long left, long right) {
        if (right < 0L || left > Long.MAX_VALUE - right) {
            throw new IllegalArgumentException("incremental definition length overflows");
        }
        return left + right;
    }

    private static IncrementalDefinition decodePayload(byte[] payload) {
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload));
            String definitionId = readText(input, "definition ID");
            byte[] descriptorFrame = readBytes(
                    input, IncrementalPlanDescriptorCodec.MAX_FRAME_BYTES, "plan descriptor");
            IncrementalPlanDescriptor descriptor = IncrementalPlanDescriptorCodec.decode(descriptorFrame);
            int operatorCount = readCount(input, IncrementalDefinition.MAX_OPERATORS, false, "operators");
            if (input.available()
                    < operatorCount * (long) IncrementalDefinition.MIN_OPERATOR_WIRE_BYTES + 4L) {
                throw new IllegalArgumentException("incremental operators are truncated");
            }
            List<IncrementalDefinition.OperatorBinding> operators = new ArrayList<>(operatorCount);
            for (int i = 0; i < operatorCount; i++) {
                String uid = readText(input, "operator UID");
                if (input.available() < 1) throw new IllegalArgumentException("operator kind is truncated");
                IncrementalDefinition.OperatorKind kind =
                        IncrementalDefinition.OperatorKind.fromWire(input.readUnsignedByte());
                String type = readText(input, "operator type");
                byte[] schema = readExact(input, 32, "operator schema");
                int inputCount = readCount(input, 1, true, "operator inputs");
                List<String> inputs = new ArrayList<>(inputCount);
                for (int j = 0; j < inputCount; j++) inputs.add(readText(input, "input operator UID"));
                byte[] implementation = readExact(input, 32, "operator implementation");
                operators.add(new IncrementalDefinition.OperatorBinding(
                        uid, kind, type, schema, inputs, implementation));
            }
            int sinkCount = readCount(
                    input, IncrementalDefinition.MAX_SINK_BINDINGS, false, "Sink bindings");
            if (input.available() < sinkCount * 32L) {
                throw new IllegalArgumentException("Sink binding digests are truncated");
            }
            List<byte[]> sinks = new ArrayList<>(sinkCount);
            for (int i = 0; i < sinkCount; i++) {
                sinks.add(readExact(input, 32, "Sink binding digest"));
            }
            if (input.read() != -1) throw new IllegalArgumentException("definition trailing bytes");
            return new IncrementalDefinition(definitionId, descriptor, operators, sinks);
        } catch (IOException malformed) {
            throw new IllegalArgumentException("incremental definition payload is malformed", malformed);
        }
    }

    private static String readText(DataInputStream input, String field) throws IOException {
        byte[] value = readBytes(input, IncrementalDefinition.MAX_TEXT_BYTES, field);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(value)).toString();
        } catch (CharacterCodingException malformed) {
            throw new IllegalArgumentException(field + " is malformed UTF-8", malformed);
        }
    }

    private static byte[] readBytes(DataInputStream input, int max, String field) throws IOException {
        if (input.available() < 4) throw new IllegalArgumentException(field + " length is missing");
        int length = input.readInt();
        if (length <= 0 || length > max || input.available() < length) {
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

    private static int readCount(
            DataInputStream input, int max, boolean allowZero, String field) throws IOException {
        if (input.available() < 4) throw new IllegalArgumentException(field + " count is missing");
        int count = input.readInt();
        if (count < (allowZero ? 0 : 1) || count > max) {
            throw new IllegalArgumentException(field + " count is invalid");
        }
        return count;
    }

    private static void writeBytes(DataOutputStream output, byte[] value) throws IOException {
        output.writeInt(value.length);
        output.write(value);
    }
}
