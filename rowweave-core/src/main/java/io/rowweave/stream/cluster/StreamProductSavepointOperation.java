package io.rowweave.stream.cluster;

import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamOperationStatus;

import java.util.Objects;

/** Bounded durable projection of one stop-with-savepoint command. */
final class StreamProductSavepointOperation {
    private final StreamCommandId commandId;
    private final String savepointId;
    private final String requestDigest;
    private final StreamOperationStatus status;

    StreamProductSavepointOperation(
            StreamCommandId commandId, String savepointId,
            String requestDigest, StreamOperationStatus status) {
        if (commandId == null || savepointId == null
                || !savepointId.matches("stream-savepoint:[0-9a-f]{64}")
                || requestDigest == null
                || !requestDigest.matches("[0-9a-f]{64}")
                || status == null) {
            throw new IllegalArgumentException(
                    "invalid Stream savepoint operation");
        }
        this.commandId = commandId;
        this.savepointId = savepointId;
        this.requestDigest = requestDigest;
        this.status = status;
    }

    StreamCommandId commandId() { return commandId; }
    String savepointId() { return savepointId; }
    String requestDigest() { return requestDigest; }
    StreamOperationStatus status() { return status; }

    StreamProductSavepointOperation withStatus(
            StreamOperationStatus next) {
        return new StreamProductSavepointOperation(
                commandId, savepointId, requestDigest, next);
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamProductSavepointOperation)) return false;
        StreamProductSavepointOperation that =
                (StreamProductSavepointOperation) other;
        return commandId.equals(that.commandId)
                && savepointId.equals(that.savepointId)
                && requestDigest.equals(that.requestDigest)
                && status == that.status;
    }

    @Override public int hashCode() {
        return Objects.hash(commandId, savepointId, requestDigest, status);
    }
}
