package io.rowweave.stream.contract;

/** Stable failures for the independent Stream RETRACT data frame. */
public final class StreamChangeException extends RuntimeException {
    public enum Code {
        INVALID_RECORD,
        FRAME_LIMIT,
        MALFORMED_FRAME,
        CHECKSUM_MISMATCH
    }

    private final Code code;

    StreamChangeException(Code code, String message) {
        super(message);
        this.code = code;
    }

    StreamChangeException(Code code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public Code code() { return code; }
}
