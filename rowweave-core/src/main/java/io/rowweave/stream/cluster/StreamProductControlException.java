package io.rowweave.stream.cluster;

/** Stable internal control-plane failure translated by the public Stream client. */
public final class StreamProductControlException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    public enum Code {
        SUBMISSION_CONFLICT,
        COMMAND_CONFLICT,
        COMMAND_NOT_FOUND,
        JOB_NOT_FOUND,
        INVALID_STATE,
        INVALID_FRAME,
        LIMIT_EXCEEDED
    }

    private final Code code;

    public StreamProductControlException(Code code, String message) {
        super(message);
        if (code == null) throw new IllegalArgumentException("code is required");
        this.code = code;
    }

    public StreamProductControlException(
            Code code, String message, Throwable cause) {
        super(message, cause);
        if (code == null) throw new IllegalArgumentException("code is required");
        this.code = code;
    }

    public Code code() {
        return code;
    }
}
