package io.rowweave.run;

import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.text.Normalizer;

/** Strict UTF-8 and NFC boundary for Run control-plane text. */
final class RunText {
    static final int MAX_ID_BYTES = 256;
    static final int MAX_REASON_CODE_BYTES = 128;
    static final int MAX_MESSAGE_BYTES = 4096;

    private RunText() {
    }

    static String required(String value, String path, int maxBytes) {
        if (value == null || value.isEmpty() || value.indexOf('\0') >= 0
                || !Normalizer.isNormalized(value, Normalizer.Form.NFC)) {
            throw new IllegalArgumentException(path + " must be non-empty, NUL-free NFC text");
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (i + 1 >= value.length() || !Character.isLowSurrogate(value.charAt(i + 1))) {
                    throw new IllegalArgumentException(path + " contains an unpaired surrogate");
                }
                i++;
            } else if (Character.isLowSurrogate(c)) {
                throw new IllegalArgumentException(path + " contains an unpaired surrogate");
            }
        }
        int bytes = value.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > maxBytes) {
            throw new IllegalArgumentException(path + " exceeds " + maxBytes + " UTF-8 bytes");
        }
        return value;
    }

    static String optional(String value, String path, int maxBytes) {
        return value == null ? null : required(value, path, maxBytes);
    }

    static String decode(byte[] encoded, String path, int maxBytes) {
        if (encoded.length > maxBytes) {
            throw new IllegalArgumentException(path + " exceeds " + maxBytes + " UTF-8 bytes");
        }
        final String decoded;
        try {
            CharBuffer chars = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(encoded));
            decoded = chars.toString();
        } catch (CharacterCodingException malformed) {
            throw new IllegalArgumentException(path + " contains malformed UTF-8", malformed);
        }
        return required(decoded, path, maxBytes);
    }
}
