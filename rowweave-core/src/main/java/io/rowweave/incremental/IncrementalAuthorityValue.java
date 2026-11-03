package io.rowweave.incremental;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.Normalizer;

final class IncrementalAuthorityValue {
    static final int DIGEST_BYTES = 32;
    static final int MAX_ID_BYTES = 256;

    private IncrementalAuthorityValue() {}

    static byte[] digest(byte[] value, String field) {
        if (value == null || value.length != DIGEST_BYTES) {
            throw new IllegalArgumentException(field + " must be SHA-256");
        }
        return value.clone();
    }

    static String text(String value, String field) {
        if (value == null || value.isEmpty() || value.indexOf('\0') >= 0
                || !Normalizer.isNormalized(value, Normalizer.Form.NFC)) {
            throw new IllegalArgumentException(field + " is not canonical text");
        }
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            if (encoded.remaining() > MAX_ID_BYTES) {
                throw new IllegalArgumentException(field + " exceeds text limit");
            }
            return value;
        } catch (CharacterCodingException invalid) {
            throw new IllegalArgumentException(field + " is invalid UTF-16", invalid);
        }
    }

    static String hexDigest(String value, String field) {
        text(value, field);
        if (value.length() != DIGEST_BYTES * 2) {
            throw new IllegalArgumentException(field + " must be a lowercase SHA-256 hex digest");
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                throw new IllegalArgumentException(field + " must be a lowercase SHA-256 hex digest");
            }
        }
        return value;
    }

    static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
