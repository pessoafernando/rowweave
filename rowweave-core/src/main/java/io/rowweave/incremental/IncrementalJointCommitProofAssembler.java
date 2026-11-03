package io.rowweave.incremental;

import java.security.MessageDigest;

/** Internal exact join of active interval, durable Sink decision and visible publication proofs. */
final class IncrementalJointCommitProofAssembler {
    private IncrementalJointCommitProofAssembler() {}

    static VerifiedIncrementalJointCommitProof join(
            IncrementalActiveInterval active,
            VerifiedIncrementalSinkCommitDecision decision,
            VerifiedIncrementalPublicationProof publication) {
        if (active == null || decision == null || publication == null
                || active.phase() != IncrementalAttemptCommitPhase.COMMIT_DECIDED) {
            throw new IllegalArgumentException("joint commit requires all three exact proofs");
        }
        if (!active.fence().equals(decision.fence())
                || !active.fence().equals(publication.fence())
                || !decision.domainId().equals(publication.domainId())
                || !decision.publicationId().equals(publication.publicationId())
                || decision.generation() != publication.generation()
                || !MessageDigest.isEqual(
                active.commitDecisionDigest(), decision.commitDecisionDigest())
                || !MessageDigest.isEqual(
                decision.commitDecisionDigest(), publication.commitDecisionDigest())
                || !MessageDigest.isEqual(decision.manifestHash(), publication.manifestHash())) {
            throw new IllegalArgumentException("active, Sink decision and publication proof differ");
        }
        return new VerifiedIncrementalJointCommitProof(active, publication);
    }
}
