package io.rowweave.connector.s3;

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

final class S3CodecSupport {
    static final int DIGEST_BYTES = 32;

    private S3CodecSupport() { }

    interface BodyWriter {
        void write(DataOutputStream output) throws IOException;
    }

    interface BodyReader<T> {
        T read(DataInputStream input) throws IOException;
    }

    static byte[] encode(byte[] magic, int maximumFrameBytes,
            BodyWriter writer, String code) {
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            try (DataOutputStream body = new DataOutputStream(bodyBytes)) {
                body.writeInt(1);
                writer.write(body);
            }
            byte[] payload = bodyBytes.toByteArray();
            ByteArrayOutputStream frameBytes = new ByteArrayOutputStream();
            try (DataOutputStream frame = new DataOutputStream(frameBytes)) {
                frame.write(magic);
                frame.writeInt(payload.length);
                frame.write(payload);
                frame.write(sha256(payload));
            }
            byte[] frame = frameBytes.toByteArray();
            if (frame.length > maximumFrameBytes) throw invalid(code);
            return frame;
        } catch (IOException impossible) {
            throw new AssertionError(impossible);
        }
    }

    static <T> T decode(byte[] frame, byte[] magic, int maximumFrameBytes,
            BodyReader<T> reader, String code) {
        if (frame == null || frame.length < magic.length + 4 + 4
                + DIGEST_BYTES || frame.length > maximumFrameBytes) {
            throw invalid(code);
        }
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] actualMagic = new byte[magic.length];
            input.readFully(actualMagic);
            int bodyLength = input.readInt();
            if (!MessageDigest.isEqual(magic, actualMagic)
                    || bodyLength < 4
                    || magic.length + 4L + bodyLength + DIGEST_BYTES
                    != frame.length) {
                throw invalid(code);
            }
            byte[] body = new byte[bodyLength];
            input.readFully(body);
            byte[] expectedDigest = new byte[DIGEST_BYTES];
            input.readFully(expectedDigest);
            if (input.available() != 0 || !MessageDigest.isEqual(
                    expectedDigest, sha256(body))) {
                throw invalid(code);
            }
            try (DataInputStream bodyInput = new DataInputStream(
                    new ByteArrayInputStream(body))) {
                if (bodyInput.readInt() != 1) throw invalid(code);
                T value = reader.read(bodyInput);
                if (bodyInput.available() != 0) throw invalid(code);
                return value;
            }
        } catch (EOFException truncated) {
            throw invalid(code);
        } catch (IOException malformed) {
            throw invalid(code);
        } catch (RuntimeException invalid) {
            if (invalid instanceof IllegalArgumentException
                    && invalid.getMessage() != null
                    && invalid.getMessage().startsWith("S3_")) {
                throw invalid;
            }
            throw invalid(code);
        }
    }

    static void writeText(DataOutputStream output, String value, int maximum,
            String code) throws IOException {
        byte[] bytes = utf8(value, maximum, false, code);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    static String readText(DataInputStream input, int maximum, String code)
            throws IOException {
        int length = input.readInt();
        if (length < 1 || length > maximum) throw invalid(code);
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return decodeUtf8(bytes, code);
    }

    static byte[] utf8(String value, int maximum, boolean allowEmpty,
            String code) {
        if (value == null || (!allowEmpty && value.isEmpty())) {
            throw invalid(code);
        }
        for (int index = 0; index < value.length(); index++) {
            if (Character.isISOControl(value.charAt(index))) {
                throw invalid(code);
            }
        }
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            if (encoded.remaining() > maximum) throw invalid(code);
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException malformed) {
            throw invalid(code);
        }
    }

    static String decodeUtf8(byte[] bytes, String code) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException malformed) {
            throw invalid(code);
        }
    }

    static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    static String sha256Hex(byte[] bytes) {
        return hex(sha256(bytes));
    }

    static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(Character.forDigit((value >>> 4) & 0xf, 16));
            result.append(Character.forDigit(value & 0xf, 16));
        }
        return result.toString();
    }

    static byte[] parseSha256(String value, String code) {
        if (value == null || value.length() != 64) throw invalid(code);
        byte[] result = new byte[DIGEST_BYTES];
        for (int index = 0; index < result.length; index++) {
            int high = Character.digit(value.charAt(index * 2), 16);
            int low = Character.digit(value.charAt(index * 2 + 1), 16);
            if (high < 0 || low < 0
                    || Character.toLowerCase(value.charAt(index * 2))
                    != value.charAt(index * 2)
                    || Character.toLowerCase(value.charAt(index * 2 + 1))
                    != value.charAt(index * 2 + 1)) {
                throw invalid(code);
            }
            result[index] = (byte) ((high << 4) | low);
        }
        return result;
    }

    static IllegalArgumentException invalid(String code) {
        return new IllegalArgumentException(code);
    }
}
