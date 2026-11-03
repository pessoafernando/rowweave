package io.rowweave;

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

/** Canonical native Plan payload codec for provider configuration; no OIS hooks. */
public final class DurableSinkConfigurationEnvelopeCodec {

    public static final int MAX_FRAME_BYTES =
            DurableBatchSinkProviderContract.MAX_CONFIGURATION_BYTES + 1024;
    private static final int MAGIC = 0x52574345; // RWCE
    private static final int FRAME_VERSION = 1;
    private static final int DIGEST_BYTES = 32;
    private static final int MIN_FRAME_BYTES = 4 + 4 + 4 + 1 + 4 + 4 + 4 + DIGEST_BYTES;

    private DurableSinkConfigurationEnvelopeCodec() {}

    public static byte[] encode(
            DurableSinkCodecDescriptor configurationCodec,
            byte[] configurationBytes) {
        return encode(new DurableSinkConfigurationEnvelope(
                configurationCodec, configurationBytes));
    }

    public static byte[] encode(DurableSinkConfigurationEnvelope envelope) {
        if (envelope == null) {
            throw new IllegalArgumentException(
                    "provider configuration envelope is required");
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(256);
            DataOutputStream output = new DataOutputStream(bytes);
            output.writeInt(MAGIC);
            output.writeInt(FRAME_VERSION);
            DurableSinkCodecDescriptor codec = envelope.configurationCodec();
            writeText(output, codec.codecId());
            output.writeInt(codec.codecVersion());
            output.writeInt(codec.maxPayloadBytes());
            byte[] configuration = envelope.configurationBytes();
            output.writeInt(configuration.length);
            output.write(configuration);
            output.flush();
            byte[] body = bytes.toByteArray();
            if (body.length > MAX_FRAME_BYTES - DIGEST_BYTES) {
                throw new IllegalArgumentException(
                        "provider configuration frame exceeds bound");
            }
            byte[] frame = Arrays.copyOf(body, body.length + DIGEST_BYTES);
            byte[] digest = sha256(body, 0, body.length);
            System.arraycopy(digest, 0, frame, body.length, digest.length);
            return frame;
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory provider configuration encoding failed", impossible);
        }
    }

    public static DurableSinkConfigurationEnvelope decode(byte[] frame) {
        return decode(frame, 0, frame == null ? 0 : frame.length);
    }

    public static DurableSinkConfigurationEnvelope decode(
            byte[] frame, int offset, int length) {
        if (frame == null || offset < 0 || length < MIN_FRAME_BYTES
                || length > MAX_FRAME_BYTES || offset > frame.length - length) {
            throw new IllegalArgumentException(
                    "provider configuration frame length is invalid");
        }
        int bodyLength = length - DIGEST_BYTES;
        byte[] expected = sha256(frame, offset, bodyLength);
        byte[] actual = Arrays.copyOfRange(
                frame, offset + bodyLength, offset + length);
        if (!MessageDigest.isEqual(expected, actual)) {
            throw new IllegalArgumentException(
                    "provider configuration checksum is invalid");
        }
        try {
            DataInputStream input = new DataInputStream(
                    new ByteArrayInputStream(frame, offset, bodyLength));
            if (input.readInt() != MAGIC || input.readInt() != FRAME_VERSION) {
                throw new IllegalArgumentException(
                        "provider configuration frame header is invalid");
            }
            DurableSinkCodecDescriptor codec = new DurableSinkCodecDescriptor(
                    readText(input), input.readInt(), input.readInt());
            int configurationLength = input.readInt();
            if (configurationLength < 0
                    || configurationLength > codec.maxPayloadBytes()
                    || configurationLength
                    > DurableBatchSinkProviderContract.MAX_CONFIGURATION_BYTES
                    || configurationLength != input.available()) {
                throw new IllegalArgumentException(
                        "provider configuration payload length is invalid");
            }
            byte[] configuration = new byte[configurationLength];
            input.readFully(configuration);
            if (input.read() != -1) {
                throw new IllegalArgumentException(
                        "provider configuration frame has trailing bytes");
            }
            DurableSinkConfigurationEnvelope decoded =
                    new DurableSinkConfigurationEnvelope(codec, configuration);
            if (!MessageDigest.isEqual(
                    Arrays.copyOfRange(frame, offset, offset + length),
                    encode(decoded))) {
                throw new IllegalArgumentException(
                        "provider configuration frame is not canonical");
            }
            return decoded;
        } catch (EOFException truncated) {
            throw new IllegalArgumentException(
                    "provider configuration frame is truncated", truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException(
                    "provider configuration frame is invalid", malformed);
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
        if (length < 1 || length > DurableSinkCodecDescriptor.MAX_CODEC_ID_BYTES
                || length > input.available()) {
            throw new IllegalArgumentException(
                    "provider configuration codecId length is invalid");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        try {
            CharBuffer decoded = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes));
            return decoded.toString();
        } catch (CharacterCodingException invalid) {
            throw new IllegalArgumentException(
                    "provider configuration codecId is invalid UTF-8", invalid);
        }
    }

    private static byte[] sha256(byte[] bytes, int offset, int length) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes, offset, length);
            return digest.digest();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
