package io.rowweave;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/** Shared strict UTF-8 validation for signed provider contract identities. */
final class DurableSinkContractText {

    private DurableSinkContractText() {}

    static String require(String value, String field, int maximumBytes) {
        if (value == null || value.isEmpty() || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(
                    "durable Sink " + field + " is invalid");
        }
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            if (encoded.remaining() > maximumBytes) {
                throw new IllegalArgumentException(
                        "durable Sink " + field + " exceeds UTF-8 bound");
            }
        } catch (CharacterCodingException invalid) {
            throw new IllegalArgumentException(
                    "durable Sink " + field + " is not valid UTF-8", invalid);
        }
        return value;
    }
}
