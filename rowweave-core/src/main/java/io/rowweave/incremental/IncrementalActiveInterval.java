package io.rowweave.incremental;

import io.rowweave.run.RunAttemptFence;

import java.util.Arrays;
import java.util.Objects;

/** Immutable bounds, artifact and current attempt for the sole active interval. */
public final class IncrementalActiveInterval {
    public static final int MAX_ATTEMPTS = 3;

    private final IncrementalIntervalIdentity identity;
    private final CapturedTargetReceipt capturedTarget;
    private final String artifactId;
    private final long claimedRevision;
    private final RunAttemptFence fence;
    private final int attemptOrdinal;
    private final IncrementalAttemptCommitPhase phase;
    private final byte[] commitDecisionDigest;

    public IncrementalActiveInterval(
            IncrementalIntervalIdentity identity, CapturedTargetReceipt capturedTarget,
            String artifactId, long claimedRevision, RunAttemptFence fence, int attemptOrdinal,
            IncrementalAttemptCommitPhase phase, byte[] commitDecisionDigest) {
        if (identity == null || capturedTarget == null || fence == null || phase == null) {
            throw new IllegalArgumentException("active interval identity, capture, fence and phase are required");
        }
        IncrementalIntervalIdentity expected = IncrementalIntervalIdentity.from(capturedTarget);
        if (!expected.equals(identity)) {
            throw new IllegalArgumentException("active interval identity does not match capture");
        }
        if (!identity.candidateRunId().equals(fence.runId())) {
            throw new IllegalArgumentException("active interval run does not match fence");
        }
        if (claimedRevision < 0L) {
            throw new IllegalArgumentException("claimed revision must be non-negative");
        }
        if (capturedTarget.mode() != IncrementalRunMode.BACKFILL_V1
                && capturedTarget.expectedSequence() != claimedRevision) {
            throw new IllegalArgumentException("normal capture does not match claimed revision");
        }
        if (attemptOrdinal < 1 || attemptOrdinal > MAX_ATTEMPTS) {
            throw new IllegalArgumentException("attempt ordinal must be in [1,3]");
        }
        if (phase == IncrementalAttemptCommitPhase.COMMIT_DECIDED) {
            this.commitDecisionDigest = IncrementalAuthorityValue.digest(
                    commitDecisionDigest, "commit decision digest");
        } else {
            if (commitDecisionDigest != null) {
                throw new IllegalArgumentException("commit decision exists before COMMIT_DECIDED");
            }
            this.commitDecisionDigest = null;
        }
        this.identity = identity;
        this.capturedTarget = capturedTarget;
        this.artifactId = IncrementalAuthorityValue.text(artifactId, "artifact id");
        this.claimedRevision = claimedRevision;
        this.fence = fence;
        this.attemptOrdinal = attemptOrdinal;
        this.phase = phase;
    }

    public IncrementalIntervalIdentity identity() { return identity; }
    public CapturedTargetReceipt capturedTarget() { return capturedTarget; }
    public String artifactId() { return artifactId; }
    public long claimedRevision() { return claimedRevision; }
    public RunAttemptFence fence() { return fence; }
    public int attemptOrdinal() { return attemptOrdinal; }
    public IncrementalAttemptCommitPhase phase() { return phase; }
    public byte[] commitDecisionDigest() {
        return commitDecisionDigest == null ? null : commitDecisionDigest.clone();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalActiveInterval)) return false;
        IncrementalActiveInterval that = (IncrementalActiveInterval) other;
        return claimedRevision == that.claimedRevision
                && attemptOrdinal == that.attemptOrdinal
                && identity.equals(that.identity)
                && capturedTarget.equals(that.capturedTarget)
                && artifactId.equals(that.artifactId)
                && fence.equals(that.fence)
                && phase == that.phase
                && Arrays.equals(commitDecisionDigest, that.commitDecisionDigest);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(identity, capturedTarget, artifactId, claimedRevision, fence,
                attemptOrdinal, phase) + Arrays.hashCode(commitDecisionDigest);
    }
}
