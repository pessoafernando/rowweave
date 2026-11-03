package io.rowweave.artifact;

/** One durable DELETING intent; a new leader may only publish the exact successor of the observed epoch. */
public final class ArtifactRecoveryCandidate {
    private final ArtifactCandidate candidate;
    private final ArtifactGcToken observedToken;

    private ArtifactRecoveryCandidate(ArtifactInventoryEntry entry) {
        if (entry == null || entry.state() != ArtifactInventoryState.DELETING) {
            throw new IllegalArgumentException(
                    "only DELETING Artifact is recoverable");
        }
        this.candidate = ArtifactCandidate.recovering(entry);
        this.observedToken = entry.gcToken();
    }

    public static ArtifactRecoveryCandidate from(
            ArtifactInventoryEntry entry) {
        return new ArtifactRecoveryCandidate(entry);
    }

    public ArtifactCandidate candidate() { return candidate; }
    public ArtifactGcToken observedToken() { return observedToken; }
}
