package io.rowweave.artifact;

/** A candidate identity binds each observation to one exact LIVE inventory generation. */
public final class ArtifactCandidate {
    private final String artifactId;
    private final long encodedBytes;
    private final long createdAtMillis;
    private final long inventoryGeneration;

    private ArtifactCandidate(ArtifactInventoryEntry entry) {
        this.artifactId = entry.artifactId();
        this.encodedBytes = entry.encodedBytes();
        this.createdAtMillis = entry.createdAtMillis();
        this.inventoryGeneration = entry.generation();
    }

    public static ArtifactCandidate from(ArtifactInventoryEntry entry) {
        if (entry == null) throw new IllegalArgumentException("entry is required");
        if (entry.state() != ArtifactInventoryState.LIVE) {
            throw new IllegalArgumentException("only LIVE Artifact is a candidate");
        }
        return new ArtifactCandidate(entry);
    }

    static ArtifactCandidate recovering(ArtifactInventoryEntry entry) {
        if (entry == null || entry.state() != ArtifactInventoryState.DELETING) {
            throw new IllegalArgumentException(
                    "only DELETING Artifact is recoverable");
        }
        return new ArtifactCandidate(entry);
    }

    public String artifactId() { return artifactId; }
    public long encodedBytes() { return encodedBytes; }
    public long createdAtMillis() { return createdAtMillis; }
    public long inventoryGeneration() { return inventoryGeneration; }
}
