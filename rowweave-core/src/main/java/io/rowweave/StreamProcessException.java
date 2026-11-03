package io.rowweave;

/** Stable fail-closed error raised by the keyed process SDK runtime. */
public final class StreamProcessException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public enum Code {
        UNDECLARED_STATE,
        STATE_KIND_MISMATCH,
        UNDECLARED_TIMER,
        UNDECLARED_OUTPUT_TAG,
        CALLBACK_SCOPE_VIOLATION,
        OUTPUT_QUOTA_EXCEEDED,
        STATE_QUOTA_EXCEEDED,
        TIMER_QUOTA_EXCEEDED,
        TIMER_REENTRANCE_REJECTED,
        MISSING_RECORD_TIMESTAMP,
        INVALID_STATE_VALUE,
        UNSUPPORTED_STATE_KIND,
        UNSUPPORTED_TIMER_RUNTIME
    }

    private final Code code;

    public StreamProcessException(Code code, String message) {
        super(message);
        if (code == null) throw new IllegalArgumentException("code is required");
        this.code = code;
    }

    public StreamProcessException(Code code, String message, Throwable cause) {
        super(message, cause);
        if (code == null) throw new IllegalArgumentException("code is required");
        this.code = code;
    }

    public Code code() { return code; }
}
