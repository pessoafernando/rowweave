package io.rowweave.stream.cluster;

import io.rowweave.StreamSavepointRequest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** Canonical bounded payload used by local and remote savepoint commands. */
final class StreamSavepointRequestCodec {
    static final int MAX_CONFIGURATION_BYTES = 64 * 1024;
    private static final int VERSION = 1;

    private StreamSavepointRequestCodec() { }

    static byte[] encode(StreamSavepointRequest request) {
        if (request == null) {
            throw new IllegalArgumentException(
                    "Stream savepoint request is required");
        }
        byte[] provider = request.providerKey().getBytes(
                StandardCharsets.UTF_8);
        byte[] configuration = request.configurationFrame();
        if (provider.length == 0 || provider.length > 256
                || configuration.length > MAX_CONFIGURATION_BYTES) {
            throw new IllegalArgumentException(
                    "Stream savepoint request exceeds its bound");
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(VERSION);
                output.writeInt(provider.length);
                output.write(provider);
                output.writeInt(configuration.length);
                output.write(configuration);
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    static StreamSavepointRequest decode(byte[] frame) {
        if (frame == null || frame.length < 12
                || frame.length > MAX_CONFIGURATION_BYTES + 272) {
            throw invalid();
        }
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            if (input.readInt() != VERSION) throw invalid();
            int providerLength = input.readInt();
            if (providerLength <= 0 || providerLength > 256
                    || providerLength > input.available() - 4) {
                throw invalid();
            }
            byte[] provider = new byte[providerLength];
            input.readFully(provider);
            int configurationLength = input.readInt();
            if (configurationLength < 0
                    || configurationLength > MAX_CONFIGURATION_BYTES
                    || configurationLength != input.available()) {
                throw invalid();
            }
            byte[] configuration = new byte[configurationLength];
            input.readFully(configuration);
            String providerKey = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(java.nio.ByteBuffer.wrap(provider)).toString();
            if (!MessageDigest.isEqual(provider,
                    providerKey.getBytes(StandardCharsets.UTF_8))) {
                throw invalid();
            }
            return new StreamSavepointRequest(providerKey, configuration);
        } catch (StreamProductControlException failure) {
            throw failure;
        } catch (EOFException | java.nio.charset.CharacterCodingException
                malformed) {
            throw invalid();
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    static String digest(StreamSavepointRequest request) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    encode(request));
            char[] alphabet = "0123456789abcdef".toCharArray();
            char[] result = new char[digest.length * 2];
            for (int index = 0; index < digest.length; index++) {
                int current = digest[index] & 0xff;
                result[index * 2] = alphabet[current >>> 4];
                result[index * 2 + 1] = alphabet[current & 0x0f];
            }
            return new String(result);
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static StreamProductControlException invalid() {
        return new StreamProductControlException(
                StreamProductControlException.Code.INVALID_FRAME,
                "Stream savepoint request is malformed");
    }
}
