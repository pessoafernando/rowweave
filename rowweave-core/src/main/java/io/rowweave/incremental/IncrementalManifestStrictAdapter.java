package io.rowweave.incremental;

import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestCommitOutcome;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.run.RecoveryToken;

import java.security.MessageDigest;

/** Internal adapter that validates query outcome and the reopened canonical immutable manifest. */
final class IncrementalManifestStrictAdapter {
    private IncrementalManifestStrictAdapter() {}

    static VerifiedIncrementalPublicationProof verify(
            VerifiedIncrementalSinkCommitDecision decision,
            ManifestFileStore store,
            FrontierRecoveryToken frontierToken,
            RecoveryToken runToken) {
        if (decision == null || store == null
                || frontierToken == null || runToken == null) {
            throw new IllegalArgumentException(
                    "Sink decision, Manifest store, and dual recovery tokens are required");
        }
        if (!frontierToken.owner().equals(runToken.owner())
                || !decision.fence().runId().equals(runToken.runId())) {
            throw new IllegalArgumentException(
                    "publication query recovery ownership does not match the Sink decision");
        }
        if (!decision.domainId().equals(store.domainId())) {
            throw new IllegalArgumentException("Manifest provider domain differs from Sink intent");
        }
        ManifestCommitOutcome outcome = store.queryOutcome(
                decision.publicationId(), decision.generation());
        if (outcome.state() != ManifestCommitOutcome.State.COMMITTED
                || outcome.generation() == null
                || outcome.generation().longValue() != decision.generation()
                || !MessageDigest.isEqual(outcome.manifestHash(), decision.manifestHash())) {
            throw new IllegalArgumentException("provider outcome is not the exact committed publication");
        }
        CommitManifest reopened = store.exact(decision.generation());
        if (reopened == null) {
            throw new IllegalArgumentException("committed Manifest disappeared before reopen");
        }
        byte[] canonicalFrame = reopened.encode();
        CommitManifest decoded = CommitManifest.decode(canonicalFrame);
        if (!decoded.equals(reopened)
                || !decision.domainId().equals(reopened.domainId())
                || decision.generation() != reopened.generation()
                || !decision.publicationId().equals(reopened.attemptId())
                || !MessageDigest.isEqual(
                IncrementalAuthorityValue.sha256(canonicalFrame), decision.manifestHash())) {
            throw new IllegalArgumentException("reopened manifest does not match durable Sink intent");
        }
        return new VerifiedIncrementalPublicationProof(
                decision, frontierToken, runToken);
    }

    static VerifiedIncrementalConflictProof verifyConflict(
            IncrementalActiveInterval active,
            VerifiedIncrementalSinkCommitDecision decision,
            ManifestFileStore store, FrontierRecoveryToken frontierToken,
            RecoveryToken runToken) {
        if (active == null || decision == null || store == null
                || frontierToken == null || runToken == null
                || !active.fence().equals(decision.fence())
                || !decision.domainId().equals(store.domainId())
                || !frontierToken.owner().equals(runToken.owner())
                || !decision.fence().runId().equals(runToken.runId())) {
            throw new IllegalArgumentException(
                    "conflict query requires matching active intent, domain and ownership");
        }
        ManifestCommitOutcome outcome = store.queryOutcome(
                decision.publicationId(), decision.generation());
        if (outcome.state()
                != ManifestCommitOutcome.State.CORRUPT_OR_CONFLICT
                || outcome.generation() == null
                || outcome.generation().longValue() != decision.generation()) {
            throw new IllegalArgumentException(
                    "provider outcome is not the exact conflicting publication");
        }
        return new VerifiedIncrementalConflictProof(
                active, decision, frontierToken, runToken);
    }
}
