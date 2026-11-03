package io.rowweave.stream.runtime;

/** Stable final-window validation and execution failures. */
public final class StreamWindowException extends RuntimeException {
    public enum Code {
        INVALID_CONFIGURATION,
        WINDOW_LIMIT,
        WINDOW_ARITHMETIC,
        LATE_RECORD,
        CORRUPT_STATE,
        CORRUPT_TIMER,
        TERMINAL_FAILURE
    }

    private final Code code;

    StreamWindowException(Code code, String message) {
        super(message);
        this.code = code;
    }

    StreamWindowException(Code code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public Code code() {
        return code;
    }
}
