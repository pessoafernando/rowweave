package io.rowweave.incremental;

import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RunAttemptFence;

import java.security.MessageDigest;

/** Strict-adapter proof that the current intended Manifest outcome is durably conflicting. */
public final class VerifiedIncrementalConflictProof {
    private final String feedId;
    private final long feedIncarnation;
    private final String intervalId;
    private final String artifactId;
    private final RunAttemptFence fence;
    private final String domainId;
    private final String publicationId;
    private final long generation;
    private final byte[] manifestHash;
    private final byte[] commitDecisionDigest;
    private final byte[] frontierRecoveryTokenDigest;
    private final byte[] runRecoveryTokenDigest;
    private final byte[] proofDigest;

    VerifiedIncrementalConflictProof(
            IncrementalActiveInterval active,
            VerifiedIncrementalSinkCommitDecision decision,
            FrontierRecoveryToken frontierToken, RecoveryToken runToken) {
        if (active == null || decision == null || frontierToken == null
                || runToken == null
                || active.phase() != IncrementalAttemptCommitPhase.COMMIT_DECIDED
                || !active.fence().equals(decision.fence())
                || !frontierToken.owner().equals(runToken.owner())
                || !active.fence().runId().equals(runToken.runId())) {
            throw new IllegalArgumentException(
                    "conflict proof requires current active intent and dual ownership");
        }
        this.feedId = frontierToken.feedId();
        this.feedIncarnation = frontierToken.feedIncarnation();
        this.intervalId = active.identity().intervalId();
        this.artifactId = active.artifactId();
        this.fence = active.fence();
        this.domainId = decision.domainId();
        this.publicationId = decision.publicationId();
        this.generation = decision.generation();
        this.manifestHash = decision.manifestHash();
        this.commitDecisionDigest = decision.commitDecisionDigest();
        this.frontierRecoveryTokenDigest = frontierToken.tokenDigest();
        this.runRecoveryTokenDigest = runToken.tokenDigest();
        if (!MessageDigest.isEqual(
                active.commitDecisionDigest(), commitDecisionDigest)) {
            throw new IllegalArgumentException(
                    "conflict proof intent differs from active interval");
        }
        this.proofDigest = IncrementalProofDigest.conflict(
                feedId, feedIncarnation, intervalId, artifactId, fence,
                domainId, publicationId, generation, manifestHash,
                commitDecisionDigest, frontierRecoveryTokenDigest,
                runRecoveryTokenDigest);
    }

    public String feedId() { return feedId; }
    public long feedIncarnation() { return feedIncarnation; }
    public String intervalId() { return intervalId; }
    public String artifactId() { return artifactId; }
    public RunAttemptFence fence() { return fence; }
    public String domainId() { return domainId; }
    public String publicationId() { return publicationId; }
    public long generation() { return generation; }
    public byte[] manifestHash() { return manifestHash.clone(); }
    public byte[] commitDecisionDigest() { return commitDecisionDigest.clone(); }
    public byte[] frontierRecoveryTokenDigest() {
        return frontierRecoveryTokenDigest.clone();
    }
    public byte[] runRecoveryTokenDigest() { return runRecoveryTokenDigest.clone(); }
    public byte[] proofDigest() { return proofDigest.clone(); }
}
