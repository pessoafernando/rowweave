package io.rowweave.release;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;

final class ReleaseContractText {
    static final int MAX_TEXT_BYTES = 512;

    private ReleaseContractText() { }

    static byte[] encode(String value, String path) {
        if (value == null || value.isEmpty() || value.indexOf('\0') >= 0
                || !Normalizer.isNormalized(value, Normalizer.Form.NFC)) {
            throw failure("NON_CANONICAL_TEXT", path,
                    "release contract text is empty or non-canonical");
        }
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            if (encoded.remaining() > MAX_TEXT_BYTES) {
                throw failure("LIMIT_EXCEEDED", path,
                        "release contract text exceeds byte limit");
            }
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException invalid) {
            throw new ReleaseContractException("MALFORMED_UTF8", path,
                    "release contract text is not valid UTF-8", invalid);
        }
    }

    static void requireDigest(String value, String path) {
        if (value == null || value.length() != 64) {
            throw failure("INVALID_DIGEST", path,
                    "release contract digest must be lowercase SHA-256 hex");
        }
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (!((current >= '0' && current <= '9')
                    || (current >= 'a' && current <= 'f'))) {
                throw failure("INVALID_DIGEST", path,
                        "release contract digest must be lowercase SHA-256 hex");
            }
        }
    }

    static ReleaseContractException failure(
            String code, String path, String message) {
        return new ReleaseContractException(code, path, message);
    }
}
