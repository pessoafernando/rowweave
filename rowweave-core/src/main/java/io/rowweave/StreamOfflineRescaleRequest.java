package io.rowweave;

import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamRecoveryPointId;

/** Offline key-group redistribution request with explicit recovery provenance. */
public final class StreamOfflineRescaleRequest {
    private final StreamJobId sourceJobId;
    private final StreamRecoveryPointId recoveryPointId;
    private final int targetParallelism;
    private final int targetMaxParallelism;

    public StreamOfflineRescaleRequest(StreamJobId sourceJobId,
            StreamRecoveryPointId recoveryPointId, int targetParallelism,
            int targetMaxParallelism) {
        StreamRestoreRequest.validate(sourceJobId, recoveryPointId,
                targetParallelism, targetMaxParallelism);
        this.sourceJobId = sourceJobId;
        this.recoveryPointId = recoveryPointId;
        this.targetParallelism = targetParallelism;
        this.targetMaxParallelism = targetMaxParallelism;
    }

    public StreamJobId sourceJobId() { return sourceJobId; }
    public StreamRecoveryPointId recoveryPointId() { return recoveryPointId; }
    public int targetParallelism() { return targetParallelism; }
    public int targetMaxParallelism() { return targetMaxParallelism; }
}
