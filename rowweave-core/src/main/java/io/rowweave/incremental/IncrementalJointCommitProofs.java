package io.rowweave.incremental;

import io.rowweave.commit.ManifestFileStore;
import io.rowweave.run.ManifestCommitIntent;
import io.rowweave.run.ManifestCommitJournalCodec;
import io.rowweave.run.ManifestCommitJournalSnapshot;
import io.rowweave.run.RecoveryToken;

import java.security.MessageDigest;

/** Strict gateway projection used by cluster control to assemble joint success. */
public final class IncrementalJointCommitProofs {
    private IncrementalJointCommitProofs() { }

    public static VerifiedIncrementalJointCommitProof verify(
            IncrementalActiveInterval active,
            IncrementalAuthorityGateway authority,
            ManifestFileStore store,
            FrontierRecoveryToken frontierToken,
            RecoveryToken runToken) {
        if (active == null || authority == null
                || active.phase()
                != IncrementalAttemptCommitPhase.COMMIT_DECIDED) {
            throw new IllegalArgumentException(
                    "commit-decided active interval and joint authority are required");
        }
        ManifestCommitJournalSnapshot journal =
                authority.manifestCommitJournal(active.fence());
        ManifestCommitIntent intent = journal == null ? null : journal.intent();
        if (intent == null
                || !active.fence().equals(intent.fence())
                || journal.assignments().manifest().generation()
                != intent.assignmentGeneration()) {
            throw new IllegalArgumentException(
                    "joint Sink intent does not match the active interval");
        }
        byte[] decisionDigest = IncrementalAuthorityValue.sha256(
                ManifestCommitJournalCodec.encodeIntent(intent));
        if (!MessageDigest.isEqual(
                decisionDigest, active.commitDecisionDigest())) {
            throw new IllegalArgumentException(
                    "joint Sink intent digest differs from the active interval");
        }
        VerifiedIncrementalSinkCommitDecision sinkDecision =
                new VerifiedIncrementalSinkCommitDecision(
                        intent.domainId(), intent.fence(),
                        intent.publicationId(), intent.generation(),
                        intent.manifestHash(), decisionDigest);
        VerifiedIncrementalPublicationProof publication =
                IncrementalManifestStrictAdapter.verify(
                        sinkDecision, store, frontierToken, runToken);
        return IncrementalJointCommitProofAssembler.join(
                active, sinkDecision, publication);
    }
}
