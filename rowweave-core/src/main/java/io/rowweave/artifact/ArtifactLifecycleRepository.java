package io.rowweave.artifact;

import io.rowweave.incremental.IncrementalClaimRequest;
import io.rowweave.incremental.IncrementalPendingRunDecision;

import java.util.function.LongConsumer;

/**
 * Controlled Artifact lifecycle operations. Deletion is not exposed.
 */
public interface ArtifactLifecycleRepository extends ArtifactRepository {

    /** Validates and publishes an Artifact, and publishes the exact Run reference inside the same per-artifact guard. */
    <T> T publishReferenced(byte[] encodedArtifact, LongConsumer reservation,
                            ArtifactReferencePublisher<T> publisher);

    /**
     * publish the content-addressed Artifact and its exact,
     * zero-attempt FRONTIER_PENDING Run reference under the same publication
     * guard. A rejected authority decision leaves only a bounded GC-managed
     * orphan Artifact and never a schedulable Run.
     */
    default IncrementalPendingRunDecision publishReferencedIncrementalPending(
            byte[] encodedArtifact, LongConsumer reservation,
            IncrementalClaimRequest request,
            IncrementalPendingRunPublisher publisher) {
        if (request == null || publisher == null) {
            throw new IllegalArgumentException(
                    "incremental pending publication fields are required");
        }
        return publishReferenced(encodedArtifact, reservation, artifactId -> {
            if (!artifactId.equals(request.artifactId())) {
                throw new IllegalArgumentException(
                        "incremental pending Run references another Artifact");
            }
            IncrementalPendingRunDecision decision = publisher.publish(request);
            if (decision == null) {
                throw new IllegalStateException(
                        "incremental pending authority returned no decision");
            }
            return decision;
        });
    }

    /** The file inventory migration barrier; deletion candidates are not scanned until it reports READY. */
    ArtifactBackfillStatus inventoryStatus();

    /** Handles at most maxEntries migration or scrub records in one call. */
    ArtifactBackfillStatus advanceInventoryBackfill(int maxEntries);

    /** How many usage observation domains exist; each janitor pass reads exactly one fixed-capacity domain. */
    int inventoryUsageShardCount();

    /** Reads one usage domain with a hard capacity bound; what monitoring observes is never a reason to delete. */
    ArtifactInventoryUsage inventoryUsage(int shard);

    /** Returns bounded LIVE candidates whose createdAt is at or below cutoffMillis. */
    ArtifactCandidatePage scanCandidates(String cursor, int limit, long cutoffMillis);

    /** Returns bounded DELETING intents, so a replacement leader can publish the exact successor epoch. */
    ArtifactRecoveryPage scanRecovering(String cursor, int limit);

    /**
     * Inside the same per-artifact publication guard, persist a DELETING intent, re-check for exact zero
     * references, delete under the fence and converge the inventory. While the authority is not READY or is unavailable, nothing is deleted.
     */
    ArtifactCollectResult collect(ArtifactCandidate candidate, ArtifactGcToken token,
                                   ArtifactReferenceAuthority references);
}
