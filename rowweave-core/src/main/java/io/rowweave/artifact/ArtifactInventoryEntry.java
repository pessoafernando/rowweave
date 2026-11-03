package io.rowweave.artifact;

/** One bounded inventory entry; a PUBLISHING or DELETING intent also occupies quota. */
public final class ArtifactInventoryEntry {
    private final String artifactId;
    private final long encodedBytes;
    private final long createdAtMillis;
    private final long generation;
    private final ArtifactInventoryState state;
    private final ArtifactGcToken gcToken;

    private ArtifactInventoryEntry(String artifactId, long encodedBytes,
                                   long createdAtMillis, long generation,
                                   ArtifactInventoryState state,
                                   ArtifactGcToken gcToken) {
        this.artifactId = ArtifactRepositorySupport.validateId(artifactId);
        if (encodedBytes <= 0L || encodedBytes > PlanArtifactCodec.MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("encodedBytes is out of bounds");
        }
        if (createdAtMillis <= 0L) {
            throw new IllegalArgumentException("createdAtMillis must be > 0");
        }
        if (generation <= 0L) {
            throw new IllegalArgumentException("generation must be > 0");
        }
        if (state == null) throw new IllegalArgumentException("state is required");
        if ((state == ArtifactInventoryState.DELETING) != (gcToken != null)) {
            throw new IllegalArgumentException("GC token does not match inventory state");
        }
        if (gcToken != null && (!artifactId.equals(gcToken.artifactId())
                || generation != gcToken.inventoryGeneration())) {
            throw new IllegalArgumentException("GC token identity/generation mismatch");
        }
        this.encodedBytes = encodedBytes;
        this.createdAtMillis = createdAtMillis;
        this.generation = generation;
        this.state = state;
        this.gcToken = gcToken;
    }

    public static ArtifactInventoryEntry publishing(String artifactId,
            long encodedBytes, long createdAtMillis, long generation) {
        return new ArtifactInventoryEntry(artifactId, encodedBytes,
                createdAtMillis, generation, ArtifactInventoryState.PUBLISHING, null);
    }

    public static ArtifactInventoryEntry live(String artifactId,
            long encodedBytes, long createdAtMillis, long generation) {
        return new ArtifactInventoryEntry(artifactId, encodedBytes,
                createdAtMillis, generation, ArtifactInventoryState.LIVE, null);
    }

    public static ArtifactInventoryEntry deleting(String artifactId,
            long encodedBytes, long createdAtMillis, long generation,
            ArtifactGcToken token) {
        return new ArtifactInventoryEntry(artifactId, encodedBytes,
                createdAtMillis, generation, ArtifactInventoryState.DELETING, token);
    }

    public String artifactId() { return artifactId; }
    public long encodedBytes() { return encodedBytes; }
    public long createdAtMillis() { return createdAtMillis; }
    public long generation() { return generation; }
    public ArtifactInventoryState state() { return state; }
    public ArtifactGcToken gcToken() { return gcToken; }
}
