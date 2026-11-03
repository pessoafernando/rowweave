package io.rowweave.stream.cluster;

/** Durable COMMIT_DECIDED requires reconciliation and cannot become FAILED. */
final class CheckpointCommitPendingException extends RuntimeException {
    CheckpointCommitPendingException(String message, Throwable cause) {
        super(message, cause);
    }
}
