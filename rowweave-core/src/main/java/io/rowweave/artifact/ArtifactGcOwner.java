package io.rowweave.artifact;

import java.util.Objects;

/** The stable Master identity behind one Artifact GC takeover. */
public final class ArtifactGcOwner {
    private final String ownerId;
    private final long leaderTermToken;

    public ArtifactGcOwner(String ownerId, long leaderTermToken) {
        ArtifactLifecycleSupport.requireText(ownerId, "ownerId");
        if (leaderTermToken == 0L) {
            throw new IllegalArgumentException("leaderTermToken must be non-zero");
        }
        this.ownerId = ownerId;
        this.leaderTermToken = leaderTermToken;
    }

    public String ownerId() { return ownerId; }
    public long leaderTermToken() { return leaderTermToken; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof ArtifactGcOwner)) return false;
        ArtifactGcOwner that = (ArtifactGcOwner) other;
        return leaderTermToken == that.leaderTermToken && ownerId.equals(that.ownerId);
    }

    @Override
    public int hashCode() { return Objects.hash(ownerId, leaderTermToken); }
}
