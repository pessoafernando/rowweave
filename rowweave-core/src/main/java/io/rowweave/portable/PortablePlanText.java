package io.rowweave.portable;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

final class PortablePlanText {
    private PortablePlanText() { }

    static String require(String value, String path, int maxBytes) {
        if (value == null || value.isEmpty()) {
            throw failure("INVALID_FIELD", path, "text is required");
        }
        return bounded(value, path, maxBytes);
    }

    static String value(String value, String path, int maxBytes) {
        if (value == null) {
            throw failure("INVALID_FIELD", path, "text is required");
        }
        return bounded(value, path, maxBytes);
    }

    private static String bounded(String value, String path, int maxBytes) {
        if (value.indexOf('\0') >= 0) {
            throw failure("INVALID_FIELD", path, "text contains a forbidden NUL");
        }
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            if (encoded.remaining() > maxBytes) {
                throw failure("LIMIT_EXCEEDED", path,
                        "UTF-8 text exceeds its contract limit");
            }
        } catch (CharacterCodingException invalid) {
            throw failure("INVALID_FIELD", path, "text is not canonical UTF-8");
        }
        return value;
    }

    static PortablePlanException failure(
            String code, String path, String message) {
        return new PortablePlanException(code, path, message);
    }

    static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    static String hex(byte[] bytes) {
        StringBuilder value = new StringBuilder(bytes.length * 2);
        for (byte current : bytes) {
            value.append(Character.forDigit((current >>> 4) & 0xf, 16));
            value.append(Character.forDigit(current & 0xf, 16));
        }
        return value.toString();
    }
}
