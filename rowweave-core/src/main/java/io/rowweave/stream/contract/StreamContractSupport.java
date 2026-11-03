package io.rowweave.stream.contract;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.Normalizer;

final class StreamContractSupport {
    static final int MAX_TEXT_BYTES = 4096;
    static final int MAX_FRAME_BYTES = 4 * 1024 * 1024;

    private StreamContractSupport() { }

    static StreamContractException failure(String code, String path, String message) {
        return new StreamContractException(code, path, message);
    }

    static String text(String value, String path, int maxBytes) {
        if (value == null || value.isEmpty() || value.indexOf('\0') >= 0
                || !Normalizer.isNormalized(value, Normalizer.Form.NFC)) {
            throw failure("NON_CANONICAL_TEXT", path, "canonical non-empty text is required");
        }
        if (value.getBytes(StandardCharsets.UTF_8).length > maxBytes) {
            throw failure("LIMIT_EXCEEDED", path, "text exceeds its byte limit");
        }
        return value;
    }

    static String digest(String value, String path) {
        if (value == null || !value.matches("[0-9a-f]{64}")) {
            throw failure("INVALID_DIGEST", path, "lowercase SHA-256 hex is required");
        }
        return value;
    }

    static byte[] frame(byte[] value, String path) {
        if (value == null || value.length > MAX_FRAME_BYTES) {
            throw failure("LIMIT_EXCEEDED", path, "frame exceeds its limit");
        }
        return value.clone();
    }

    static String sha256Hex(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte current : digest) {
                result.append(Character.forDigit((current >>> 4) & 0xf, 16));
                result.append(Character.forDigit(current & 0xf, 16));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
