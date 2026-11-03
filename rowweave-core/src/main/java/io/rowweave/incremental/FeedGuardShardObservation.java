package io.rowweave.incremental;

import java.util.Arrays;
import java.util.Objects;

/** Exact persistent incarnation-shard observation used by a frontier ownership token. */
public final class FeedGuardShardObservation {
    public static final int SHARD_COUNT = 256;

    private final int shardIndex;
    private final long shardRevision;
    private final byte[] shardDigest;

    public FeedGuardShardObservation(int shardIndex, long shardRevision, byte[] shardDigest) {
        if (shardIndex < 0 || shardIndex >= SHARD_COUNT || shardRevision <= 0L) {
            throw new IllegalArgumentException("guard shard index or revision is invalid");
        }
        this.shardIndex = shardIndex;
        this.shardRevision = shardRevision;
        this.shardDigest = IncrementalAuthorityValue.digest(shardDigest, "guard shard digest");
    }

    public int shardIndex() { return shardIndex; }
    public long shardRevision() { return shardRevision; }
    public byte[] shardDigest() { return shardDigest.clone(); }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof FeedGuardShardObservation)) return false;
        FeedGuardShardObservation that = (FeedGuardShardObservation) other;
        return shardIndex == that.shardIndex && shardRevision == that.shardRevision
                && Arrays.equals(shardDigest, that.shardDigest);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(shardIndex, shardRevision) + Arrays.hashCode(shardDigest);
    }
}
