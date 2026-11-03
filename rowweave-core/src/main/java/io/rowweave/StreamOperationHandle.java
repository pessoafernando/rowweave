package io.rowweave;

import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamOperationStatus;

/** Reconnectable durable command projection. */
public interface StreamOperationHandle {
    StreamCommandId id();
    StreamOperationStatus status();
    StreamOperationStatus awaitCompletion(long timeoutMillis);
}
