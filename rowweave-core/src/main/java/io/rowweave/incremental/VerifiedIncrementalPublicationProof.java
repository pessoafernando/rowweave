package io.rowweave.incremental;

import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RecoveryToken;

/** Strict-adapter proof of an exact immutable visible Manifest publication. */
public final class VerifiedIncrementalPublicationProof {
    private final String feedId;
    private final long feedIncarnation;
    private final byte[] frontierRecoveryTokenDigest;
    private final byte[] runRecoveryTokenDigest;
    private final String domainId;
    private final RunAttemptFence fence;
    private final String publicationId;
    private final long generation;
    private final byte[] manifestHash;
    private final byte[] commitDecisionDigest;
    private final byte[] proofDigest;

    VerifiedIncrementalPublicationProof(
            VerifiedIncrementalSinkCommitDecision decision,
            FrontierRecoveryToken frontierToken,
            RecoveryToken runToken) {
        if (decision == null || frontierToken == null || runToken == null
                || !frontierToken.owner().equals(runToken.owner())
                || !decision.fence().runId().equals(runToken.runId())) {
            throw new IllegalArgumentException(
                    "publication proof requires matching dual recovery ownership");
        }
        this.feedId = frontierToken.feedId();
        this.feedIncarnation = frontierToken.feedIncarnation();
        this.frontierRecoveryTokenDigest = frontierToken.tokenDigest();
        this.runRecoveryTokenDigest = runToken.tokenDigest();
        this.domainId = decision.domainId();
        this.fence = decision.fence();
        this.publicationId = decision.publicationId();
        this.generation = decision.generation();
        this.manifestHash = decision.manifestHash();
        this.commitDecisionDigest = decision.commitDecisionDigest();
        this.proofDigest = IncrementalProofDigest.publication(
                feedId, feedIncarnation, frontierRecoveryTokenDigest,
                runRecoveryTokenDigest, domainId, fence, publicationId, generation,
                manifestHash, commitDecisionDigest);
    }

    public String feedId() { return feedId; }
    public long feedIncarnation() { return feedIncarnation; }
    public byte[] frontierRecoveryTokenDigest() {
        return frontierRecoveryTokenDigest.clone();
    }
    public byte[] runRecoveryTokenDigest() { return runRecoveryTokenDigest.clone(); }
    public String domainId() { return domainId; }
    public RunAttemptFence fence() { return fence; }
    public String publicationId() { return publicationId; }
    public long generation() { return generation; }
    public byte[] manifestHash() { return manifestHash.clone(); }
    public byte[] commitDecisionDigest() { return commitDecisionDigest.clone(); }
    public byte[] proofDigest() { return proofDigest.clone(); }
}
