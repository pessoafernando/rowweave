package io.rowweave.pipeline;

import java.util.Objects;

/** Monotonically fenced producer attempt for one handoff key. */
public final class VersionedDatasetProducerAttempt {
    private final VersionedDatasetHandoffKey key;
    private final String producerRunId;
    private final String attemptId;
    private final long attemptNumber;
    private final long fenceToken;

    public VersionedDatasetProducerAttempt(
            VersionedDatasetHandoffKey key, String producerRunId,
            String attemptId, long attemptNumber, long fenceToken) {
        if (key == null) {
            throw PipelineText.failure("INVALID_HANDOFF_KEY", "attempt.key",
                    "handoff key is required");
        }
        this.key = key;
        this.producerRunId = VersionedDatasetHandoffSupport.id(
                producerRunId, "attempt.producerRunId");
        this.attemptId = VersionedDatasetHandoffSupport.id(
                attemptId, "attempt.attemptId");
        this.attemptNumber = VersionedDatasetHandoffSupport.positive(
                attemptNumber, "attempt.attemptNumber", "INVALID_ATTEMPT_FENCE");
        this.fenceToken = VersionedDatasetHandoffSupport.positive(
                fenceToken, "attempt.fenceToken", "INVALID_ATTEMPT_FENCE");
    }

    public VersionedDatasetHandoffKey key() { return key; }
    public String producerRunId() { return producerRunId; }
    public String attemptId() { return attemptId; }
    public long attemptNumber() { return attemptNumber; }
    public long fenceToken() { return fenceToken; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof VersionedDatasetProducerAttempt)) return false;
        VersionedDatasetProducerAttempt that = (VersionedDatasetProducerAttempt) other;
        return attemptNumber == that.attemptNumber
                && fenceToken == that.fenceToken
                && key.equals(that.key)
                && producerRunId.equals(that.producerRunId)
                && attemptId.equals(that.attemptId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, producerRunId, attemptId, attemptNumber, fenceToken);
    }
}
