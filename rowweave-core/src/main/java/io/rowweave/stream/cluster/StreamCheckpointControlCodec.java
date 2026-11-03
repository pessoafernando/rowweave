package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.CheckpointIdentity;

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

/** Canonical bounded payload for a fenced Worker checkpoint trigger. */
final class StreamCheckpointControlCodec {
    private static final byte[] MAGIC =
            "RWXXSCI1".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 1;
    private static final int MAX_TEXT_BYTES = 256;

    private StreamCheckpointControlCodec() { }

    static byte[] encode(CheckpointIdentity value) {
        if (value == null) throw new IllegalArgumentException(
                "checkpoint identity is required");
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.write(MAGIC);
                output.writeInt(VERSION);
                writeText(output, value.definitionId());
                writeText(output, value.artifactId());
                writeText(output, value.runId());
                writeText(output, value.attemptId());
                output.writeLong(value.fenceToken());
                output.writeLong(value.coordinatorTerm());
                writeText(output, value.coordinatorIncarnation());
                output.writeLong(value.checkpointSequence());
                writeText(output, value.planDigest());
                writeText(output, value.deploymentDigest());
                output.writeInt(value.dataProtocolVersion());
                output.writeInt(value.stateFormatVersion());
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    static CheckpointIdentity decode(byte[] frame) {
        if (frame == null || frame.length == 0
                || frame.length > StreamControlMessage.MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException(
                    "checkpoint identity frame is invalid");
        }
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            if (!MessageDigest.isEqual(MAGIC, magic)
                    || input.readInt() != VERSION) {
                throw new IllegalArgumentException(
                        "checkpoint identity header is invalid");
            }
            CheckpointIdentity value = new CheckpointIdentity(
                    readText(input), readText(input), readText(input),
                    readText(input), input.readLong(), input.readLong(),
                    readText(input), input.readLong(), readText(input),
                    readText(input), input.readInt(), input.readInt());
            if (input.available() != 0) {
                throw new IllegalArgumentException(
                        "checkpoint identity has trailing bytes");
            }
            return value;
        } catch (EOFException truncated) {
            throw new IllegalArgumentException(
                    "checkpoint identity is truncated", truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException(
                    "checkpoint identity is malformed", malformed);
        }
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0 || bytes.length > MAX_TEXT_BYTES) {
            throw new IllegalArgumentException(
                    "checkpoint identity text is invalid");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readText(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > MAX_TEXT_BYTES
                || length > input.available()) {
            throw new IllegalArgumentException(
                    "checkpoint identity text length is invalid");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException malformed) {
            throw new IllegalArgumentException(
                    "checkpoint identity text is malformed", malformed);
        }
    }
}
