package io.rowweave.stream.cluster;

import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamRecoveryPointId;

import java.util.Objects;

/** Durable provenance for a new restore/offline-rescale submission. */
final class StreamProductRecovery {
    private final StreamJobId sourceJobId;
    private final StreamRecoveryPointId recoveryPointId;
    private final int targetParallelism;
    private final int targetMaxParallelism;
    private final boolean offlineRescale;

    StreamProductRecovery(
            StreamJobId sourceJobId,
            StreamRecoveryPointId recoveryPointId,
            int targetParallelism, int targetMaxParallelism,
            boolean offlineRescale) {
        if (sourceJobId == null || recoveryPointId == null
                || targetParallelism < 1
                || targetMaxParallelism < targetParallelism
                || targetMaxParallelism > 32_768) {
            throw new IllegalArgumentException(
                    "invalid Stream product recovery provenance");
        }
        this.sourceJobId = sourceJobId;
        this.recoveryPointId = recoveryPointId;
        this.targetParallelism = targetParallelism;
        this.targetMaxParallelism = targetMaxParallelism;
        this.offlineRescale = offlineRescale;
    }

    StreamJobId sourceJobId() { return sourceJobId; }
    StreamRecoveryPointId recoveryPointId() { return recoveryPointId; }
    int targetParallelism() { return targetParallelism; }
    int targetMaxParallelism() { return targetMaxParallelism; }
    boolean offlineRescale() { return offlineRescale; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamProductRecovery)) return false;
        StreamProductRecovery that = (StreamProductRecovery) other;
        return targetParallelism == that.targetParallelism
                && targetMaxParallelism == that.targetMaxParallelism
                && offlineRescale == that.offlineRescale
                && sourceJobId.equals(that.sourceJobId)
                && recoveryPointId.equals(that.recoveryPointId);
    }

    @Override public int hashCode() {
        return Objects.hash(sourceJobId, recoveryPointId,
                targetParallelism, targetMaxParallelism, offlineRescale);
    }
}
