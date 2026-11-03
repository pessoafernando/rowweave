package io.rowweave.stream.runtime;

/** Stable fail-closed errors for the resident Stream CDC enumerator. */
public final class StreamCdcException extends RuntimeException {
    public enum Code {
        INVALID_CONFIGURATION,
        INVALID_PHASE,
        SPLIT_ORDER,
        INVALID_SNAPSHOT_RECORD,
        SNAPSHOT_INCOMPLETE,
        CHECKPOINT_BOUNDARY,
        LOG_SEQUENCE,
        FRAME_LIMIT,
        OUTPUT_FAILURE,
        WRONG_THREAD
    }

    private final Code code;

    StreamCdcException(Code code, String message) {
        super(message);
        this.code = code;
    }

    StreamCdcException(Code code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public Code code() { return code; }
}
