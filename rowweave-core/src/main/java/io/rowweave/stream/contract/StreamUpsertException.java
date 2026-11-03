package io.rowweave.stream.contract;

/** Stable failures for the independent Stream UPSERT data frame. */
public final class StreamUpsertException extends RuntimeException {
    public enum Code {
        INVALID_RECORD,
        FRAME_LIMIT,
        MALFORMED_FRAME,
        CHECKSUM_MISMATCH
    }

    private final Code code;

    StreamUpsertException(Code code, String message) {
        super(message);
        this.code = code;
    }

    StreamUpsertException(Code code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public Code code() { return code; }
}
