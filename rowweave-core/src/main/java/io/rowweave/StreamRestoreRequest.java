package io.rowweave;

import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamRecoveryPointId;

/** Explicit immutable provenance for a new job restored from a completed point. */
public final class StreamRestoreRequest {
    private final StreamJobId sourceJobId;
    private final StreamRecoveryPointId recoveryPointId;
    private final int targetParallelism;
    private final int targetMaxParallelism;

    public StreamRestoreRequest(StreamJobId sourceJobId,
            StreamRecoveryPointId recoveryPointId, int targetParallelism,
            int targetMaxParallelism) {
        validate(sourceJobId, recoveryPointId, targetParallelism,
                targetMaxParallelism);
        this.sourceJobId = sourceJobId;
        this.recoveryPointId = recoveryPointId;
        this.targetParallelism = targetParallelism;
        this.targetMaxParallelism = targetMaxParallelism;
    }

    static void validate(StreamJobId sourceJobId,
            StreamRecoveryPointId recoveryPointId, int targetParallelism,
            int targetMaxParallelism) {
        if (sourceJobId == null || recoveryPointId == null
                || targetParallelism < 1
                || targetMaxParallelism < targetParallelism
                || targetMaxParallelism > 32_768) {
            throw new IllegalArgumentException("invalid Stream recovery request");
        }
    }

    public StreamJobId sourceJobId() { return sourceJobId; }
    public StreamRecoveryPointId recoveryPointId() { return recoveryPointId; }
    public int targetParallelism() { return targetParallelism; }
    public int targetMaxParallelism() { return targetMaxParallelism; }
}
