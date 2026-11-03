package io.rowweave.stream.spi.conformance;

/** Stable, data-free failure emitted by the published connector kit. */
public final class StreamConnectorConformanceException
        extends RuntimeException {
    public enum Code {
        INVALID_FIXTURE,
        SOURCE_REPLAY_MISMATCH,
        SOURCE_POSITION_NOT_MONOTONIC,
        SOURCE_TERMINATION_PROTOCOL_INVALID,
        SOURCE_TIME_PROTOCOL_INVALID,
        SOURCE_CONTROL_TIMEOUT,
        FRAME_LIMIT_EXCEEDED,
        SINK_TRANSACTION_PROTOCOL_INVALID
    }

    private final Code code;

    StreamConnectorConformanceException(Code code, String message) {
        super(message);
        this.code = code;
    }

    StreamConnectorConformanceException(
            Code code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public Code code() { return code; }
}
