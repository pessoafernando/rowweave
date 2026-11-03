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

/** Canonical bounded wire form for an inert incremental source configuration. */
public final class IncrementalSourceConfigCodec {
    public static final int MAX_FRAME_BYTES = 20 * 1024;
    private static final int MAGIC = 0x52575343; // RWSC
    private static final int FRAME_VERSION = 1;
    private static final int HEADER_BYTES = 12;
    private static final int DIGEST_BYTES = 32;

    private IncrementalSourceConfigCodec() { }

    public static byte[] encode(IncrementalSourceConfig config) {
        if (config == null) {
            throw new IllegalArgumentException(
                    "incremental source config is required");
        }
        try {
            ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream(
                    config.configurationSize() + 256);
            DataOutputStream payload = new DataOutputStream(payloadBytes);
            writeText(payload, config.providerKey().connectorType());
            payload.writeInt(config.providerKey().connectorVersion());
            payload.write(config.contractDigest());
            byte[] configuration = config.configurationBytes();
            payload.writeInt(configuration.length);
            payload.write(configuration);
            payload.flush();
            byte[] body = payloadBytes.toByteArray();
            int total = Math.addExact(
                    Math.addExact(HEADER_BYTES, body.length), DIGEST_BYTES);
            if (total > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException(
                        "incremental source config frame exceeds bound");
            }
            ByteArrayOutputStream frameBytes = new ByteArrayOutputStream(total);
            DataOutputStream frame = new DataOutputStream(frameBytes);
            frame.writeInt(MAGIC);
            frame.writeInt(FRAME_VERSION);
            frame.writeInt(body.length);
            frame.write(body);
            frame.write(sha256(body));
            frame.flush();
            return frameBytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory incremental source config encoding failed",
                    impossible);
        }
    }

    public static IncrementalSourceConfig decode(byte[] frame) {
        if (frame == null || frame.length < HEADER_BYTES + DIGEST_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "incremental source config frame length is invalid");
        }
        try {
            DataInputStream input = new DataInputStream(
                    new ByteArrayInputStream(frame));
            if (input.readInt() != MAGIC
                    || input.readInt() != FRAME_VERSION) {
                throw new IllegalArgumentException(
                        "incremental source config header is invalid");
            }
            int payloadLength = input.readInt();
            long expected = (long) HEADER_BYTES + payloadLength + DIGEST_BYTES;
            if (payloadLength < 0 || expected != frame.length) {
                throw new IllegalArgumentException(
                        "incremental source config length is invalid");
            }
            byte[] payload = new byte[payloadLength];
            input.readFully(payload);
            byte[] checksum = new byte[DIGEST_BYTES];
            input.readFully(checksum);
            if (input.read() != -1 || !MessageDigest.isEqual(
                    checksum, sha256(payload))) {
                throw new IllegalArgumentException(
                        "incremental source config checksum is invalid");
            }
            DataInputStream body = new DataInputStream(
                    new ByteArrayInputStream(payload));
            IncrementalSourceProviderKey key =
                    new IncrementalSourceProviderKey(
                            readText(body), body.readInt());
            byte[] contractDigest = new byte[DIGEST_BYTES];
            body.readFully(contractDigest);
            int configurationLength = body.readInt();
            if (configurationLength < 0
                    || configurationLength
                    > IncrementalSourceProviderContract.MAX_CONFIGURATION_BYTES
                    || configurationLength > body.available()) {
                throw new IllegalArgumentException(
                        "incremental source configuration length is invalid");
            }
            byte[] configuration = new byte[configurationLength];
            body.readFully(configuration);
            if (body.read() != -1) {
                throw new IllegalArgumentException(
                        "incremental source config has trailing bytes");
            }
            IncrementalSourceConfig decoded = new IncrementalSourceConfig(
                    key, contractDigest, configuration);
            if (!Arrays.equals(frame, encode(decoded))) {
                throw new IllegalArgumentException(
                        "incremental source config is not canonical");
            }
            return decoded;
        } catch (EOFException truncated) {
            throw new IllegalArgumentException(
                    "incremental source config is truncated", truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException(
                    "incremental source config is malformed", malformed);
        }
    }

    public static byte[] digest(IncrementalSourceConfig config) {
        return sha256(encode(config));
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    private static String readText(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length <= 0
                || length > IncrementalSourceProviderKey.MAX_CONNECTOR_TYPE_BYTES
                || length > input.available()) {
            throw new IllegalArgumentException(
                    "incremental source connector type length is invalid");
        }
        byte[] encoded = new byte[length];
        input.readFully(encoded);
        try {
            CharBuffer value = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(encoded));
            return value.toString();
        } catch (CharacterCodingException invalid) {
            throw new IllegalArgumentException(
                    "incremental source connector type is invalid UTF-8",
                    invalid);
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
