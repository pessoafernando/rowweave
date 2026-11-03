package io.rowweave.stream.cluster;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Canonical, checksummed and bounded Stream control-plane codec. */
public final class StreamControlMessageCodec {
    private static final byte[] MAGIC =
            "RWXXSCV1".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 1;
    private static final int HEADER_BYTES = 16;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_BODY_BYTES =
            StreamControlMessage.MAX_PAYLOAD_BYTES + 2 * 1024;
    public static final int MAX_FRAME_BYTES =
            HEADER_BYTES + MAX_BODY_BYTES + DIGEST_BYTES;

    private StreamControlMessageCodec() { }

    public static byte[] magic() {
        return MAGIC.clone();
    }

    public static byte[] encode(StreamControlMessage message) {
        if (message == null) {
            throw new IllegalArgumentException("Stream control message is required");
        }
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            try (DataOutputStream body = new DataOutputStream(bodyBytes)) {
                body.writeInt(message.kind().wireId());
                body.writeLong(message.coordinatorTerm());
                writeText(body, message.coordinatorIncarnation());
                writeText(body, message.runId());
                writeText(body, message.attemptId());
                body.writeLong(message.fenceToken());
                body.writeLong(message.assignmentGeneration());
                writeText(body, message.workerIncarnation());
                body.writeLong(message.commandSequence());
                writeText(body, message.commandDigest());
                body.writeInt(message.payloadUnsafe().length);
                body.write(message.payloadUnsafe());
            }
            byte[] bytes = bodyBytes.toByteArray();
            if (bytes.length > MAX_BODY_BYTES) {
                throw failure(StreamClusterProtocolException.Code.LIMIT_EXCEEDED,
                        "Stream control body exceeds its limit");
            }
            ByteArrayOutputStream frame = new ByteArrayOutputStream(
                    HEADER_BYTES + bytes.length + DIGEST_BYTES);
            try (DataOutputStream output = new DataOutputStream(frame)) {
                output.write(MAGIC);
                output.writeInt(VERSION);
                output.writeInt(bytes.length);
                output.write(bytes);
                output.write(sha256(bytes, 0, bytes.length));
            }
            return frame.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory Stream control encode failed", impossible);
        }
    }

    public static StreamControlMessage decode(byte[] frame) {
        if (frame == null || frame.length < HEADER_BYTES + DIGEST_BYTES) {
            throw failure(StreamClusterProtocolException.Code.INVALID_CONTROL_FRAME,
                    "Stream control frame is too short");
        }
        if (frame.length > MAX_FRAME_BYTES) {
            throw failure(StreamClusterProtocolException.Code.LIMIT_EXCEEDED,
                    "Stream control frame exceeds its limit");
        }
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            if (!MessageDigest.isEqual(MAGIC, magic)) {
                throw failure(StreamClusterProtocolException.Code.INVALID_CONTROL_FRAME,
                        "Stream control magic is invalid");
            }
            if (input.readInt() != VERSION) {
                throw failure(
                        StreamClusterProtocolException.Code.UNSUPPORTED_CONTROL_VERSION,
                        "Stream control version is unsupported");
            }
            int bodyLength = input.readInt();
            if (bodyLength < 0 || bodyLength > MAX_BODY_BYTES
                    || HEADER_BYTES + (long) bodyLength + DIGEST_BYTES
                    != frame.length) {
                throw failure(StreamClusterProtocolException.Code.INVALID_CONTROL_FRAME,
                        "Stream control frame length is invalid");
            }
            byte[] actualDigest = sha256(frame, HEADER_BYTES, bodyLength);
            byte[] expectedDigest = new byte[DIGEST_BYTES];
            System.arraycopy(frame, HEADER_BYTES + bodyLength,
                    expectedDigest, 0, DIGEST_BYTES);
            if (!MessageDigest.isEqual(actualDigest, expectedDigest)) {
                throw failure(StreamClusterProtocolException.Code.CHECKSUM_MISMATCH,
                        "Stream control checksum is invalid");
            }
            try (DataInputStream body = new DataInputStream(
                    new ByteArrayInputStream(frame, HEADER_BYTES, bodyLength))) {
                StreamControlMessage message = new StreamControlMessage(
                        StreamControlMessage.Kind.fromWireId(body.readInt()),
                        body.readLong(), readText(body), readText(body),
                        readText(body), body.readLong(), body.readLong(),
                        readText(body), body.readLong(), readText(body),
                        readPayload(body));
                if (body.available() != 0) {
                    throw failure(
                            StreamClusterProtocolException.Code.INVALID_CONTROL_FRAME,
                            "Stream control body has trailing bytes");
                }
                return message;
            }
        } catch (StreamClusterProtocolException failure) {
            throw failure;
        } catch (IllegalArgumentException invalid) {
            throw failure(StreamClusterProtocolException.Code.INVALID_CONTROL_FRAME,
                    "Stream control body contains an invalid identity");
        } catch (EOFException truncated) {
            throw failure(StreamClusterProtocolException.Code.INVALID_CONTROL_FRAME,
                    "Stream control body is truncated");
        } catch (IOException malformed) {
            throw failure(StreamClusterProtocolException.Code.INVALID_CONTROL_FRAME,
                    "Stream control body is malformed");
        }
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readText(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > StreamControlMessage.MAX_TEXT_BYTES
                || length > input.available()) {
            throw failure(StreamClusterProtocolException.Code.LIMIT_EXCEEDED,
                    "Stream control text exceeds its limit");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException malformed) {
            throw failure(StreamClusterProtocolException.Code.INVALID_CONTROL_FRAME,
                    "Stream control text is malformed UTF-8");
        }
    }

    private static byte[] readPayload(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length < 0 || length > StreamControlMessage.MAX_PAYLOAD_BYTES
                || length > input.available()) {
            throw failure(StreamClusterProtocolException.Code.LIMIT_EXCEEDED,
                    "Stream control payload exceeds its limit");
        }
        byte[] payload = new byte[length];
        input.readFully(payload);
        return payload;
    }

    private static byte[] sha256(byte[] bytes, int offset, int length) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes, offset, length);
            return digest.digest();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is required", impossible);
        }
    }

    private static StreamClusterProtocolException failure(
            StreamClusterProtocolException.Code code, String message) {
        return new StreamClusterProtocolException(code, message);
    }
}
