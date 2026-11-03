package io.rowweave.stream.api;

/** Stable public failure from Stream submission, lifecycle or bounded waits. */
public final class StreamControlException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public enum Code {
        SUBMISSION_CONFLICT(1),
        COMMAND_CONFLICT(2),
        COMMAND_NOT_FOUND(3),
        JOB_NOT_FOUND(4),
        WAIT_TIMEOUT(5),
        WAIT_INTERRUPTED(6),
        TRANSPORT_FAILURE(7),
        UNSUPPORTED_OPERATION(8),
        INVALID_RESPONSE(9);

        private final int wireId;

        Code(int wireId) {
            this.wireId = wireId;
        }

        public int wireId() { return wireId; }

        public static Code fromWireId(int wireId) {
            for (Code code : values()) {
                if (code.wireId == wireId) return code;
            }
            throw new IllegalArgumentException(
                    "unknown Stream control error wire ID: " + wireId);
        }
    }

    private final Code code;

    public StreamControlException(Code code, String message) {
        super(message);
        if (code == null) throw new IllegalArgumentException("code is required");
        this.code = code;
    }

    public StreamControlException(Code code, String message, Throwable cause) {
        super(message, cause);
        if (code == null) throw new IllegalArgumentException("code is required");
        this.code = code;
    }

    public Code code() { return code; }
}
