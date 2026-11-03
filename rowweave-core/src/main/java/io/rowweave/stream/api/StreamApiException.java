package io.rowweave.stream.api;

/** Stable public failure raised while building or compiling a Stream job. */
public final class StreamApiException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    public enum Code {
        INVALID_CONFIGURATION,
        MISSING_OPERATOR_UID,
        DUPLICATE_OPERATOR_UID,
        MISSING_SINK,
        DELIVERY_GUARANTEE_MISMATCH,
        FUNCTION_ABI_MISMATCH,
        SCHEMA_MISMATCH,
        INVALID_STABLE_KEY,
        INCOMPATIBLE_KEY_CODEC,
        UNUSED_OUTPUT_TAG,
        MISSING_STABLE_KEY,
        TIME_MODE_MISMATCH,
        TIME_AUTHORITY_CONFLICT,
        CHANGELOG_MODE_MISMATCH,
        INVALID_WINDOW_FIELD_BINDING,
        MISSING_WATERMARK_POLICY,
        INVALID_STATE_DESCRIPTOR,
        INVALID_EXPRESSION,
        CLIENT_REQUIRED,
        INVALID_TOPOLOGY
    }

    private final Code code;

    public StreamApiException(Code code, String message) {
        super(message);
        if (code == null) throw new IllegalArgumentException("code is required");
        this.code = code;
    }

    public Code code() { return code; }
}
