package io.rowweave.stream.cluster;

/** Stable fail-closed error for the Stream cluster protocol and placement gate. */
public final class StreamClusterProtocolException extends IllegalArgumentException {
    public enum Code {
        INVALID_CONTROL_FRAME,
        UNSUPPORTED_CONTROL_VERSION,
        CHECKSUM_MISMATCH,
        UNKNOWN_CONTROL_KIND,
        LIMIT_EXCEEDED,
        INSUFFICIENT_CAPACITY,
        AMBIGUOUS_WORKER_INCARNATION,
        STALE_COORDINATOR,
        STALE_ATTEMPT,
        COMMAND_REPLAY,
        WORKER_INCARNATION_MISMATCH,
        RUN_CONFLICT
    }

    private final Code code;

    StreamClusterProtocolException(Code code, String message) {
        super(message);
        if (code == null) {
            throw new IllegalArgumentException("Stream cluster error code is required");
        }
        this.code = code;
    }

    public Code code() {
        return code;
    }
}
