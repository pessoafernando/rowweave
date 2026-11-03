package io.rowweave.artifact;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Objects;

/** The durable owner and generation fence required before a final Artifact unlink. */
public final class ArtifactGcToken {
    private final String artifactId;
    private final ArtifactGcOwner owner;
    private final long ownershipEpoch;
    private final long inventoryGeneration;
    private final byte[] tokenDigest;

    private ArtifactGcToken(String artifactId, ArtifactGcOwner owner,
                            long ownershipEpoch, long inventoryGeneration,
                            byte[] tokenDigest) {
        this.artifactId = ArtifactRepositorySupport.validateId(artifactId);
        if (owner == null) throw new IllegalArgumentException("owner is required");
        if (ownershipEpoch <= 0L) {
            throw new IllegalArgumentException("ownershipEpoch must be > 0");
        }
        if (inventoryGeneration <= 0L) {
            throw new IllegalArgumentException("inventoryGeneration must be > 0");
        }
        if (tokenDigest == null || tokenDigest.length != 32) {
            throw new IllegalArgumentException("tokenDigest must be SHA-256");
        }
        this.owner = owner;
        this.ownershipEpoch = ownershipEpoch;
        this.inventoryGeneration = inventoryGeneration;
        this.tokenDigest = tokenDigest.clone();
    }

    public static ArtifactGcToken create(String artifactId, ArtifactGcOwner owner,
                                         long ownershipEpoch,
                                         long inventoryGeneration) {
        byte[] digest = ArtifactLifecycleCodec.digestGcTokenFields(
                artifactId, owner, ownershipEpoch, inventoryGeneration);
        return new ArtifactGcToken(artifactId, owner, ownershipEpoch,
                inventoryGeneration, digest);
    }

    static ArtifactGcToken decoded(String artifactId, ArtifactGcOwner owner,
                                   long ownershipEpoch, long inventoryGeneration,
                                   byte[] tokenDigest) {
        ArtifactGcToken token = new ArtifactGcToken(artifactId, owner,
                ownershipEpoch, inventoryGeneration, tokenDigest);
        byte[] expected = ArtifactLifecycleCodec.digestGcTokenFields(
                artifactId, owner, ownershipEpoch, inventoryGeneration);
        if (!MessageDigest.isEqual(expected, tokenDigest)) {
            throw new IllegalArgumentException("Artifact GC token digest mismatch");
        }
        return token;
    }

    public String artifactId() { return artifactId; }
    public ArtifactGcOwner owner() { return owner; }
    public long ownershipEpoch() { return ownershipEpoch; }
    public long inventoryGeneration() { return inventoryGeneration; }
    public byte[] tokenDigest() { return tokenDigest.clone(); }

    static boolean isExactSuccessor(
            ArtifactGcToken current, ArtifactGcToken replacement) {
        if (current == null || replacement == null) return false;
        return current.ownershipEpoch != Long.MAX_VALUE
                && replacement.ownershipEpoch == current.ownershipEpoch + 1L;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof ArtifactGcToken)) return false;
        ArtifactGcToken that = (ArtifactGcToken) other;
        return ownershipEpoch == that.ownershipEpoch
                && inventoryGeneration == that.inventoryGeneration
                && artifactId.equals(that.artifactId)
                && owner.equals(that.owner)
                && MessageDigest.isEqual(tokenDigest, that.tokenDigest);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(artifactId, owner, ownershipEpoch,
                inventoryGeneration) + Arrays.hashCode(tokenDigest);
    }
}
