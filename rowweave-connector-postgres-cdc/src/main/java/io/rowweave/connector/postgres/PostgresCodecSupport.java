package io.rowweave.connector.postgres;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

final class PostgresCodecSupport {
    static final int DIGEST_BYTES = 32;
    static final long MAX_UINT32 = 0xffff_ffffL;
    private static final BigInteger MAX_UINT64 =
            new BigInteger("18446744073709551615");

    private PostgresCodecSupport() { }

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
        } catch (RuntimeException malformed) {
            if (malformed instanceof IllegalArgumentException
                    && malformed.getMessage() != null
                    && malformed.getMessage().startsWith("POSTGRES_CDC_")) {
                throw malformed;
            }
            throw invalid(code);
        }
    }

    static void writeText(DataOutputStream output, String value,
            int maximumBytes, String code) throws IOException {
        byte[] bytes = utf8(value, maximumBytes, false, code);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    static String readText(DataInputStream input, int maximumBytes,
            String code) throws IOException {
        int length = input.readInt();
        if (length < 1 || length > maximumBytes) throw invalid(code);
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return decodeUtf8(bytes, code);
    }

    static byte[] utf8(String value, int maximumBytes,
            boolean allowControls, String code) {
        if (value == null || value.isEmpty()) throw invalid(code);
        if (!allowControls) {
            for (int index = 0; index < value.length(); index++) {
                if (Character.isISOControl(value.charAt(index))) {
                    throw invalid(code);
                }
            }
        } else if (value.indexOf('\0') >= 0) {
            throw invalid(code);
        }
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            if (encoded.remaining() > maximumBytes) throw invalid(code);
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

    static long requireUint32(long value, boolean allowZero, String code) {
        if (value < (allowZero ? 0L : 1L) || value > MAX_UINT32) {
            throw invalid(code);
        }
        return value;
    }

    static String requireSystemIdentifier(String value, String code) {
        if (value == null || value.isEmpty()
                || (value.length() > 1 && value.charAt(0) == '0')) {
            throw invalid(code);
        }
        for (int index = 0; index < value.length(); index++) {
            if (value.charAt(index) < '0' || value.charAt(index) > '9') {
                throw invalid(code);
            }
        }
        try {
            BigInteger number = new BigInteger(value);
            if (number.signum() <= 0 || number.compareTo(MAX_UINT64) > 0) {
                throw invalid(code);
            }
            return value;
        } catch (NumberFormatException malformed) {
            throw invalid(code);
        }
    }

    static long systemIdentifierBits(String value, String code) {
        return new BigInteger(requireSystemIdentifier(value, code))
                .longValue();
    }

    static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    static String hex(byte[] value) {
        StringBuilder result = new StringBuilder(value.length * 2);
        for (byte item : value) {
            result.append(Character.forDigit((item >>> 4) & 0xf, 16));
            result.append(Character.forDigit(item & 0xf, 16));
        }
        return result.toString();
    }

    static IllegalArgumentException invalid(String code) {
        return new IllegalArgumentException(code);
    }
}
