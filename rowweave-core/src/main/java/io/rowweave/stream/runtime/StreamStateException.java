package io.rowweave.stream.runtime;

/** Stable fail-closed errors emitted by the local managed Stream state backend. */
public final class StreamStateException extends RuntimeException {
    public enum Code {
        INVALID_CONFIGURATION,
        FOREIGN_KEY_GROUP,
        THREAD_OWNERSHIP,
        FRAME_LIMIT,
        SPILL_LIMIT,
        WATERMARK_REGRESSION,
        CORRUPT_SPILL,
        CORRUPT_SNAPSHOT,
        IO_FAILURE,
        CLOSED
    }

    private final Code code;

    StreamStateException(Code code, String message) {
        super(message);
        this.code = code;
    }

    StreamStateException(Code code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public Code code() {
        return code;
    }
}
