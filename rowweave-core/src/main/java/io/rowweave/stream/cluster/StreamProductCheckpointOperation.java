package io.rowweave.stream.cluster;

import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamOperationStatus;

import java.util.Objects;

/** Bounded durable projection of one public checkpoint command. */
final class StreamProductCheckpointOperation {
    private final StreamCommandId commandId;
    private final String checkpointId;
    private final StreamOperationStatus status;
    private final long acceptedAtEpochMillis;
    private final long completedAtEpochMillis;

    StreamProductCheckpointOperation(StreamCommandId commandId,
            String checkpointId, StreamOperationStatus status) {
        this(commandId, checkpointId, status, 0L, 0L);
    }

    StreamProductCheckpointOperation(StreamCommandId commandId,
            String checkpointId, StreamOperationStatus status,
            long acceptedAtEpochMillis) {
        this(commandId, checkpointId, status, acceptedAtEpochMillis, 0L);
    }

    StreamProductCheckpointOperation(StreamCommandId commandId,
            String checkpointId, StreamOperationStatus status,
            long acceptedAtEpochMillis, long completedAtEpochMillis) {
        if (commandId == null || checkpointId == null
                || !checkpointId.matches("stream-checkpoint:[0-9a-f]{64}")
                || status == null || acceptedAtEpochMillis < 0L
                || completedAtEpochMillis < 0L
                || acceptedAtEpochMillis > 0L
                && completedAtEpochMillis > 0L
                && completedAtEpochMillis < acceptedAtEpochMillis) {
            throw new IllegalArgumentException(
                    "invalid Stream checkpoint operation");
        }
        this.commandId = commandId;
        this.checkpointId = checkpointId;
        this.status = status;
        this.acceptedAtEpochMillis = acceptedAtEpochMillis;
        this.completedAtEpochMillis = completedAtEpochMillis;
    }

    StreamCommandId commandId() { return commandId; }
    String checkpointId() { return checkpointId; }
    StreamOperationStatus status() { return status; }
    long acceptedAtEpochMillis() { return acceptedAtEpochMillis; }
    long completedAtEpochMillis() { return completedAtEpochMillis; }

    StreamProductCheckpointOperation withStatus(StreamOperationStatus next) {
        long completedAt = completedAtEpochMillis;
        if (next == StreamOperationStatus.SUCCEEDED && completedAt == 0L) {
            completedAt = Math.max(acceptedAtEpochMillis,
                    System.currentTimeMillis());
        }
        return new StreamProductCheckpointOperation(commandId, checkpointId,
                next, acceptedAtEpochMillis, completedAt);
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamProductCheckpointOperation)) return false;
        StreamProductCheckpointOperation that =
                (StreamProductCheckpointOperation) other;
        return commandId.equals(that.commandId)
                && checkpointId.equals(that.checkpointId)
                && status == that.status
                && acceptedAtEpochMillis == that.acceptedAtEpochMillis
                && completedAtEpochMillis == that.completedAtEpochMillis;
    }

    @Override public int hashCode() {
        return Objects.hash(commandId, checkpointId, status,
                acceptedAtEpochMillis, completedAtEpochMillis);
    }
}
