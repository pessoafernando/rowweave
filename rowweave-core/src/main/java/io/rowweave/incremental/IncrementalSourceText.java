package io.rowweave.incremental;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/** Strict bounded UTF-8 identities used by the incremental source SPI. */
final class IncrementalSourceText {
    private IncrementalSourceText() { }

    static String require(String value, String field, int maximumBytes) {
        if (value == null || value.isEmpty() || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(
                    "incremental source " + field + " is invalid");
        }
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            if (encoded.remaining() > maximumBytes) {
                throw new IllegalArgumentException(
                        "incremental source " + field
                                + " exceeds UTF-8 bound");
            }
        } catch (CharacterCodingException invalid) {
            throw new IllegalArgumentException(
                    "incremental source " + field
                            + " is not valid UTF-8", invalid);
        }
        return value;
    }
}
