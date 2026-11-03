package io.rowweave;

import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamJobProgress;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobStatus;

/** Thread-safe reconnectable handle; implementations keep no executor/task reference. */
public interface StreamJobHandle {
    StreamJobId id();
    String artifactId();
    StreamJobStatus status();
    StreamJobProgress progress();
    default StreamJobEvidence evidence() {
        throw new UnsupportedOperationException(
                "Stream operational evidence is unavailable");
    }
    StreamCommandHandle cancel(StreamCommandId commandId);
    StreamCheckpointHandle triggerCheckpoint(StreamCommandId commandId);
    StreamSavepointHandle stopWithSavepoint(
            StreamCommandId commandId, StreamSavepointRequest request);
    default StreamRescaleHandle rescale(
            StreamCommandId commandId, int targetParallelism) {
        throw new UnsupportedOperationException(
                "live rescale control is unavailable");
    }
    StreamJobStatus awaitRunning(long timeoutMillis);
    StreamJobStatus awaitTermination(long timeoutMillis);
}
