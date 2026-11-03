package io.rowweave.incremental;

import io.rowweave.run.RunAttemptFence;

/** Exact active interval + Sink intent + visible Manifest proof for one joint success CAS. */
public final class VerifiedIncrementalJointCommitProof {
    private final String feedId;
    private final long feedIncarnation;
    private final byte[] frontierRecoveryTokenDigest;
    private final byte[] runRecoveryTokenDigest;
    private final String domainId;
    private final String intervalId;
    private final String capturedTargetReceiptDigest;
    private final IncrementalRunMode mode;
    private final byte[] definitionFingerprint;
    private final String artifactId;
    private final RunAttemptFence fence;
    private final String publicationId;
    private final long generation;
    private final byte[] manifestHash;
    private final byte[] commitDecisionDigest;
    private final byte[] proofDigest;

    VerifiedIncrementalJointCommitProof(
            IncrementalActiveInterval active,
            VerifiedIncrementalPublicationProof publication) {
        this.feedId = publication.feedId();
        this.feedIncarnation = publication.feedIncarnation();
        this.frontierRecoveryTokenDigest =
                publication.frontierRecoveryTokenDigest();
        this.runRecoveryTokenDigest = publication.runRecoveryTokenDigest();
        this.domainId = publication.domainId();
        this.intervalId = active.identity().intervalId();
        this.capturedTargetReceiptDigest =
                active.identity().capturedTargetReceiptDigest();
        this.mode = active.capturedTarget().mode();
        this.definitionFingerprint = active.capturedTarget().definitionDigest();
        this.artifactId = active.artifactId();
        this.fence = active.fence();
        this.publicationId = publication.publicationId();
        this.generation = publication.generation();
        this.manifestHash = publication.manifestHash();
        this.commitDecisionDigest = publication.commitDecisionDigest();
        this.proofDigest = IncrementalProofDigest.joint(active, publication);
    }

    public String feedId() { return feedId; }
    public long feedIncarnation() { return feedIncarnation; }
    public byte[] frontierRecoveryTokenDigest() {
        return frontierRecoveryTokenDigest.clone();
    }
    public byte[] runRecoveryTokenDigest() { return runRecoveryTokenDigest.clone(); }
    public String domainId() { return domainId; }
    public String intervalId() { return intervalId; }
    public String capturedTargetReceiptDigest() { return capturedTargetReceiptDigest; }
    public IncrementalRunMode mode() { return mode; }
    public byte[] definitionFingerprint() { return definitionFingerprint.clone(); }
    public String artifactId() { return artifactId; }
    public RunAttemptFence fence() { return fence; }
    public String publicationId() { return publicationId; }
    public long generation() { return generation; }
    public byte[] manifestHash() { return manifestHash.clone(); }
    public byte[] commitDecisionDigest() { return commitDecisionDigest.clone(); }
    public byte[] proofDigest() { return proofDigest.clone(); }
}
