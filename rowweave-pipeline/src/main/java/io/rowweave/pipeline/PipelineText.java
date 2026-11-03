package io.rowweave.pipeline;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

final class PipelineText {
    private PipelineText() { }

    static String required(String value, String path, int maxBytes) {
        if (value == null || value.isEmpty()) {
            throw failure("INVALID_ID", path, "value is required");
        }
        for (int i = 0; i < value.length(); i++) {
            if (Character.isISOControl(value.charAt(i))) {
                throw failure("INVALID_ID", path,
                        "control characters are not allowed");
            }
        }
        final ByteBuffer encoded;
        try {
            encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
        } catch (CharacterCodingException invalid) {
            throw failure("INVALID_UTF8", path, "value is not canonical UTF-8");
        }
        if (encoded.remaining() > maxBytes) {
            throw failure("LIMIT_EXCEEDED", path, "UTF-8 value exceeds byte limit");
        }
        return value;
    }

    static String schema(String value, String path) {
        required(value, path, 64);
        if (!value.matches("[0-9a-f]{64}")) {
            throw failure("INVALID_SCHEMA_FINGERPRINT", path,
                    "schema fingerprint must be 64 lowercase hex characters");
        }
        return value;
    }

    static PipelineValidationException failure(
            String code, String path, String detail) {
        return new PipelineValidationException(code, path, detail);
    }
}
