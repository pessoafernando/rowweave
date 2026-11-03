package io.rowweave.stream.cluster;

/** Canonical completed-checkpoint observation owned by the active run. */
final class StreamCompletedCheckpointEvidence {
    private final String checkpointId;
    private final long checkpointSequence;
    private final long contentBytes;
    private final long managedStateBytes;

    StreamCompletedCheckpointEvidence(String checkpointId,
            long checkpointSequence, long contentBytes,
            long managedStateBytes) {
        if (checkpointId == null
                || !checkpointId.matches("stream-checkpoint:[0-9a-f]{64}")
                || checkpointSequence <= 0L || contentBytes < 0L
                || managedStateBytes < 0L
                || managedStateBytes > contentBytes) {
            throw new IllegalArgumentException(
                    "completed checkpoint evidence is invalid");
        }
        this.checkpointId = checkpointId;
        this.checkpointSequence = checkpointSequence;
        this.contentBytes = contentBytes;
        this.managedStateBytes = managedStateBytes;
    }

    String checkpointId() { return checkpointId; }
    long checkpointSequence() { return checkpointSequence; }
    long contentBytes() { return contentBytes; }
    long managedStateBytes() { return managedStateBytes; }
}
