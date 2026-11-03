package io.rowweave.incremental;

import io.rowweave.run.RunAttemptFence;

import java.util.Arrays;
import java.util.Objects;

/** Exact pending-Run binding consumed by the joint frontier claim/first-attempt CAS. */
public final class IncrementalClaimRequest {
    private final String feedId;
    private final CapturedTargetReceipt capturedTarget;
    private final byte[] frontierContractDigest;
    private final IncrementalIntervalIdentity identity;
    private final String artifactId;
    private final String attemptId;
    private final String publicationId;
    private final int maxAttempts;
    private final long deadlineEpochMillis;
    private final long eventTimeMillis;
    private final RunAttemptFence firstAttemptFence;
    private final byte[] transitionDigest;

    public IncrementalClaimRequest(
            String feedId, CapturedTargetReceipt capturedTarget,
            byte[] frontierContractDigest, String artifactId,
            String attemptId, String publicationId,
            int maxAttempts, long deadlineEpochMillis, long eventTimeMillis) {
        this.feedId = IncrementalAuthorityValue.text(feedId, "feed id");
        if (capturedTarget == null) {
            throw new IllegalArgumentException("captured target receipt is required");
        }
        if (maxAttempts < 1 || maxAttempts > IncrementalActiveInterval.MAX_ATTEMPTS
                || eventTimeMillis < 0L
                || (deadlineEpochMillis != 0L && deadlineEpochMillis < eventTimeMillis)) {
            throw new IllegalArgumentException("attempt limit, deadline or event time is invalid");
        }
        this.capturedTarget = capturedTarget;
        this.frontierContractDigest = IncrementalAuthorityValue.digest(
                frontierContractDigest, "frontier contract digest");
        this.identity = IncrementalIntervalIdentity.from(capturedTarget);
        this.artifactId = IncrementalAuthorityValue.text(artifactId, "artifact id");
        this.attemptId = IncrementalAuthorityValue.text(attemptId, "attempt id");
        this.publicationId = IncrementalAuthorityValue.text(publicationId, "publication id");
        this.maxAttempts = maxAttempts;
        this.deadlineEpochMillis = deadlineEpochMillis;
        this.eventTimeMillis = eventTimeMillis;
        this.firstAttemptFence = new RunAttemptFence(
                identity.candidateRunId(), attemptId, 1L);
        this.transitionDigest = IncrementalTransitionDigest.claim(this);
    }

    public String feedId() { return feedId; }
    public CapturedTargetReceipt capturedTarget() { return capturedTarget; }
    public byte[] frontierContractDigest() { return frontierContractDigest.clone(); }
    public String runId() { return identity.candidateRunId(); }
    public String intervalId() { return identity.intervalId(); }
    public String capturedTargetReceiptDigest() {
        return identity.capturedTargetReceiptDigest();
    }
    public String artifactId() { return artifactId; }
    public String attemptId() { return attemptId; }
    public String publicationId() { return publicationId; }
    public int maxAttempts() { return maxAttempts; }
    public long deadlineEpochMillis() { return deadlineEpochMillis; }
    public long eventTimeMillis() { return eventTimeMillis; }
    public RunAttemptFence firstAttemptFence() { return firstAttemptFence; }
    public byte[] transitionDigest() { return transitionDigest.clone(); }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalClaimRequest)) return false;
        IncrementalClaimRequest that = (IncrementalClaimRequest) other;
        return maxAttempts == that.maxAttempts
                && deadlineEpochMillis == that.deadlineEpochMillis
                && eventTimeMillis == that.eventTimeMillis
                && feedId.equals(that.feedId)
                && capturedTarget.equals(that.capturedTarget)
                && Arrays.equals(frontierContractDigest, that.frontierContractDigest)
                && artifactId.equals(that.artifactId)
                && attemptId.equals(that.attemptId)
                && publicationId.equals(that.publicationId)
                && Arrays.equals(transitionDigest, that.transitionDigest);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(feedId, capturedTarget, artifactId, attemptId,
                publicationId, maxAttempts, deadlineEpochMillis, eventTimeMillis);
        result = 31 * result + Arrays.hashCode(frontierContractDigest);
        return 31 * result + Arrays.hashCode(transitionDigest);
    }
}
