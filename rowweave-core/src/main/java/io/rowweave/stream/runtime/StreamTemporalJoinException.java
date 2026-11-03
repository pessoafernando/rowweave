package io.rowweave.stream.runtime;

/** Stable fail-closed errors for the bounded Stream temporal join runtime. */
public final class StreamTemporalJoinException extends RuntimeException {
    public enum Code {
        INVALID_CONFIGURATION,
        FRAME_LIMIT,
        UNOWNED_KEY_GROUP,
        LATE_RECORD,
        WATERMARK_REGRESSION,
        STATE_ENTRY_LIMIT,
        STATE_BYTE_LIMIT,
        MISSING_RETRACTION,
        IO_FAILURE,
        USER_FUNCTION_FAILURE,
        WRONG_THREAD,
        CLOSED
    }

    private final Code code;

    StreamTemporalJoinException(Code code, String message) {
        super(message);
        this.code = code;
    }

    StreamTemporalJoinException(Code code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public Code code() { return code; }
}
