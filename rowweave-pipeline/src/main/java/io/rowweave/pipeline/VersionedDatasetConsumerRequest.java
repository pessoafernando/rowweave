package io.rowweave.pipeline;

import java.util.Objects;

/** Request to immutably bind one consumer attempt to an exact visible winner. */
public final class VersionedDatasetConsumerRequest {
    private final VersionedDatasetHandoffKey key;
    private final String expectedWinnerDigest;
    private final String consumerStepId;
    private final String consumerSourceOperatorId;
    private final String consumerArtifactId;
    private final String consumerRunId;
    private final String consumerAttemptId;

    public VersionedDatasetConsumerRequest(
            VersionedDatasetHandoffKey key, String expectedWinnerDigest,
            String consumerStepId, String consumerSourceOperatorId,
            String consumerArtifactId, String consumerRunId,
            String consumerAttemptId) {
        if (key == null) {
            throw PipelineText.failure("INVALID_HANDOFF_KEY", "consumer.key",
                    "handoff key is required");
        }
        this.key = key;
        this.expectedWinnerDigest = VersionedDatasetHandoffSupport.digest(
                expectedWinnerDigest, "consumer.expectedWinnerDigest");
        this.consumerStepId = VersionedDatasetHandoffSupport.id(
                consumerStepId, "consumer.consumerStepId");
        this.consumerSourceOperatorId = VersionedDatasetHandoffSupport.id(
                consumerSourceOperatorId, "consumer.consumerSourceOperatorId");
        this.consumerArtifactId = VersionedDatasetHandoffSupport.id(
                consumerArtifactId, "consumer.consumerArtifactId");
        this.consumerRunId = VersionedDatasetHandoffSupport.id(
                consumerRunId, "consumer.consumerRunId");
        this.consumerAttemptId = VersionedDatasetHandoffSupport.id(
                consumerAttemptId, "consumer.consumerAttemptId");
    }

    public VersionedDatasetHandoffKey key() { return key; }
    public String expectedWinnerDigest() { return expectedWinnerDigest; }
    public String consumerStepId() { return consumerStepId; }
    public String consumerSourceOperatorId() { return consumerSourceOperatorId; }
    public String consumerArtifactId() { return consumerArtifactId; }
    public String consumerRunId() { return consumerRunId; }
    public String consumerAttemptId() { return consumerAttemptId; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof VersionedDatasetConsumerRequest)) return false;
        VersionedDatasetConsumerRequest that = (VersionedDatasetConsumerRequest) other;
        return key.equals(that.key)
                && expectedWinnerDigest.equals(that.expectedWinnerDigest)
                && consumerStepId.equals(that.consumerStepId)
                && consumerSourceOperatorId.equals(that.consumerSourceOperatorId)
                && consumerArtifactId.equals(that.consumerArtifactId)
                && consumerRunId.equals(that.consumerRunId)
                && consumerAttemptId.equals(that.consumerAttemptId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, expectedWinnerDigest, consumerStepId,
                consumerSourceOperatorId, consumerArtifactId,
                consumerRunId, consumerAttemptId);
    }
}
