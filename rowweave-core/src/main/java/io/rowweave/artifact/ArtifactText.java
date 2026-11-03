package io.rowweave.artifact;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;

final class ArtifactText {

    private ArtifactText() {
    }

    static byte[] encode(String value, String path, int maxBytes) {
        validateCharacters(value, path);
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (encoded.length > maxBytes) {
            throw failure("LIMIT_EXCEEDED", path,
                    "UTF-8 text exceeds " + maxBytes + " bytes: " + encoded.length);
        }
        return encoded;
    }

    static String decode(byte[] encoded, String path, int maxBytes) {
        if (encoded.length > maxBytes) {
            throw failure("LIMIT_EXCEEDED", path,
                    "UTF-8 text exceeds " + maxBytes + " bytes: " + encoded.length);
        }
        final String decoded;
        try {
            CharBuffer chars = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(encoded));
            decoded = chars.toString();
        } catch (CharacterCodingException malformed) {
            throw new PlanArtifactValidationException(
                    "MALFORMED_UTF8", path, "malformed UTF-8 text", malformed);
        }
        validateCharacters(decoded, path);
        return decoded;
    }

    private static void validateCharacters(String value, String path) {
        if (value == null || value.isEmpty()) {
            throw failure("NON_CANONICAL_TEXT", path, "text must not be null or empty");
        }
        if (value.indexOf('\0') >= 0 || !Normalizer.isNormalized(value, Normalizer.Form.NFC)) {
            throw failure("NON_CANONICAL_TEXT", path, "text must be NUL-free NFC");
        }
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (Character.isHighSurrogate(current)) {
                if (i + 1 >= value.length() || !Character.isLowSurrogate(value.charAt(i + 1))) {
                    throw failure("NON_CANONICAL_TEXT", path, "text contains an unpaired surrogate");
                }
                i++;
            } else if (Character.isLowSurrogate(current)) {
                throw failure("NON_CANONICAL_TEXT", path, "text contains an unpaired surrogate");
            }
        }
    }

    static PlanArtifactValidationException failure(String code, String path, String message) {
        return new PlanArtifactValidationException(code, path, message);
    }
}
