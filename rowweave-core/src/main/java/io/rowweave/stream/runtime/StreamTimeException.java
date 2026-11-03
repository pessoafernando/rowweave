package io.rowweave.stream.runtime;

/** Stable fail-closed errors from task-local Stream time coordination. */
public final class StreamTimeException extends RuntimeException {
    public enum Code {
        INVALID_CONFIGURATION,
        INVALID_INPUT,
        INPUT_WATERMARK_REGRESSION,
        OPERATOR_WATERMARK_REGRESSION,
        STATUS_GENERATION,
        STATUS_TRANSITION,
        ACTIVATION_WATERMARK,
        IDLE_TIMEOUT,
        TIMER_LIMIT,
        INVALID_TIMER,
        CALLBACK_FAILURE,
        THREAD_OWNERSHIP
    }

    private final Code code;

    StreamTimeException(Code code, String message) {
        super(message);
        this.code = code;
    }

    StreamTimeException(Code code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public Code code() {
        return code;
    }
}
