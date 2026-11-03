package io.rowweave.incremental;

import io.rowweave.run.DurableManifestCommitIntentObservation;
import io.rowweave.run.DurableStageAssignmentAuthority;
import io.rowweave.run.ManifestCommitIntent;

import java.security.MessageDigest;

/** Internal bridge from the existing durable Manifest Sink authority into CDC proof space. */
final class IncrementalSinkIntentStrictAdapter {
    private IncrementalSinkIntentStrictAdapter() {}

    static VerifiedIncrementalSinkCommitDecision verify(
            IncrementalActiveInterval active, DurableStageAssignmentAuthority authority) {
        if (active == null || authority == null
                || active.phase() != IncrementalAttemptCommitPhase.COMMIT_DECIDED) {
            throw new IllegalArgumentException(
                    "commit-decided active interval and durable authority are required");
        }
        DurableManifestCommitIntentObservation observation =
                authority.manifestCommitIntentObservation(active.fence());
        if (observation == null) {
            throw new IllegalArgumentException("durable Sink intent does not exist");
        }
        ManifestCommitIntent intent = observation.intent();
        if (!active.fence().equals(observation.fence())
                || !active.fence().equals(intent.fence())
                || observation.assignmentGeneration() != intent.assignmentGeneration()) {
            throw new IllegalArgumentException("Sink intent fence does not match active interval");
        }
        byte[] digest = IncrementalAuthorityValue.sha256(
                observation.canonicalIntentFrame());
        if (!MessageDigest.isEqual(digest, active.commitDecisionDigest())) {
            throw new IllegalArgumentException("Sink intent digest does not match active interval");
        }
        return new VerifiedIncrementalSinkCommitDecision(
                intent.domainId(), intent.fence(), intent.publicationId(),
                intent.generation(), intent.manifestHash(), digest);
    }
}
