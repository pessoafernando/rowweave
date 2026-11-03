package io.rowweave;

import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamOperationStatus;

/** Reconnectable handle for one checkpoint-driven live rescale command. */
public interface StreamRescaleHandle {
    StreamCommandId id();
    String rescaleId();
    int targetParallelism();
    StreamOperationStatus status();
    StreamOperationStatus awaitCompletion(long timeoutMillis);
}
