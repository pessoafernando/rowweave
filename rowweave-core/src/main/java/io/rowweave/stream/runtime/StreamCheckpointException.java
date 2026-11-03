package io.rowweave.stream.runtime;

/** Stable fail-closed error raised by local Stream checkpoint execution. */
public final class StreamCheckpointException extends RuntimeException {
    public enum Code {
        INVALID_CONFIGURATION,
        THREAD_OWNERSHIP,
        CHECKPOINT_IN_FLIGHT,
        CHECKPOINT_SEQUENCE_REPLAY,
        UNEXPECTED_BARRIER,
        CHECKPOINT_MISMATCH,
        INPUT_BLOCKED,
        CHANNEL_STATE_LIMIT,
        CALLBACK_FAILURE,
        CHECKPOINT_ALREADY_DECIDED,
        FIRST_DECISION_CONFLICT,
        CORRUPT_AUTHORITY,
        CORRUPT_MEMBER_CATALOG,
        IO_FAILURE,
        COMMIT_PROOF_INVALID,
        DECISION_ALREADY_COMMITTED,
        SINK_FAILURE,
        SOURCE_POSITION_INVALID,
        PAYLOAD_LIMIT,
        CORRUPT_PAYLOAD,
        CHECKPOINT_NOT_COMPLETED,
        SAVEPOINT_CONFLICT,
        CORRUPT_SAVEPOINT,
        CORRUPT_CHUNK,
        CORRUPT_CHUNK_MANIFEST,
        SAVEPOINT_INCOMPATIBLE,
        STATE_MIGRATION_INCOMPATIBLE,
        STATE_MIGRATION_INCOMPLETE,
        STATE_MIGRATION_FAILED,
        STATE_MIGRATION_NON_DETERMINISTIC,
        STATE_MIGRATION_OUTPUT_MISMATCH,
        RESCALE_INCOMPATIBLE,
        RESCALE_EVIDENCE_MISMATCH
    }

    private final Code code;

    StreamCheckpointException(Code code, String message) {
        super(message);
        this.code = code;
    }

    StreamCheckpointException(Code code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public Code code() {
        return code;
    }
}
