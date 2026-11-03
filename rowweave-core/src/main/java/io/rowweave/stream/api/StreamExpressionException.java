package io.rowweave.stream.api;

/** Stable public failure for typed Stream expression construction. */
public final class StreamExpressionException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;
    private final String code;

    StreamExpressionException(String code, String message) {
        super(message);
        this.code = code;
    }

    public String code() { return code; }
}
