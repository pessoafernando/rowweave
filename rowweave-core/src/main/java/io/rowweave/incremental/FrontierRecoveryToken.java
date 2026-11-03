package io.rowweave.incremental;

import io.rowweave.run.RecoveryOwner;

import java.util.Arrays;
import java.util.Objects;

/** Authority-issued ownership token fenced by feed incarnation and persistent guard state. */
public final class FrontierRecoveryToken {
    private final String feedId;
    private final RecoveryOwner owner;
    private final long ownershipEpoch;
    private final long claimedRevision;
    private final long feedIncarnation;
    private final FeedGuardShardObservation guardShardObservation;
    private final long feedRootCzxid;
    private final byte[] tokenDigest;

    private FrontierRecoveryToken(
            String feedId, RecoveryOwner owner, long ownershipEpoch,
            long claimedRevision, long feedIncarnation,
            FeedGuardShardObservation guardShardObservation,
            long feedRootCzxid, byte[] tokenDigest, boolean verify) {
        this.feedId = IncrementalAuthorityValue.text(feedId, "feed id");
        if (owner == null || guardShardObservation == null) {
            throw new IllegalArgumentException("recovery owner and guard observation are required");
        }
        if (ownershipEpoch <= 0L || claimedRevision < 0L || feedIncarnation <= 0L
                || feedRootCzxid <= 0L) {
            throw new IllegalArgumentException(
                    "ownership/incarnation/root must be positive and revision non-negative");
        }
        this.owner = owner;
        this.ownershipEpoch = ownershipEpoch;
        this.claimedRevision = claimedRevision;
        this.feedIncarnation = feedIncarnation;
        this.guardShardObservation = guardShardObservation;
        this.feedRootCzxid = feedRootCzxid;
        byte[] expected = FrontierRecoveryTokenCodec.digestFields(
                feedId, owner, ownershipEpoch, claimedRevision, feedIncarnation,
                guardShardObservation, feedRootCzxid);
        this.tokenDigest = tokenDigest == null ? expected
                : IncrementalAuthorityValue.digest(tokenDigest, "frontier recovery token digest");
        if (verify && !java.security.MessageDigest.isEqual(expected, this.tokenDigest)) {
            throw new IllegalArgumentException("frontier recovery token digest mismatch");
        }
    }

    public static FrontierRecoveryToken create(
            String feedId, RecoveryOwner owner, long ownershipEpoch,
            long claimedRevision, long feedIncarnation,
            FeedGuardShardObservation guardShardObservation, long feedRootCzxid) {
        return new FrontierRecoveryToken(
                feedId, owner, ownershipEpoch, claimedRevision, feedIncarnation,
                guardShardObservation, feedRootCzxid, null, false);
    }

    static FrontierRecoveryToken decoded(
            String feedId, RecoveryOwner owner, long ownershipEpoch,
            long claimedRevision, long feedIncarnation,
            FeedGuardShardObservation guardShardObservation,
            long feedRootCzxid, byte[] tokenDigest) {
        return new FrontierRecoveryToken(
                feedId, owner, ownershipEpoch, claimedRevision, feedIncarnation,
                guardShardObservation, feedRootCzxid, tokenDigest, true);
    }

    public String feedId() { return feedId; }
    public RecoveryOwner owner() { return owner; }
    public long ownershipEpoch() { return ownershipEpoch; }
    public long claimedRevision() { return claimedRevision; }
    public long feedIncarnation() { return feedIncarnation; }
    public FeedGuardShardObservation guardShardObservation() { return guardShardObservation; }
    public long feedRootCzxid() { return feedRootCzxid; }
    public byte[] tokenDigest() { return tokenDigest.clone(); }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof FrontierRecoveryToken)) return false;
        FrontierRecoveryToken that = (FrontierRecoveryToken) other;
        return ownershipEpoch == that.ownershipEpoch
                && claimedRevision == that.claimedRevision
                && feedIncarnation == that.feedIncarnation
                && feedRootCzxid == that.feedRootCzxid
                && feedId.equals(that.feedId) && owner.equals(that.owner)
                && guardShardObservation.equals(that.guardShardObservation)
                && Arrays.equals(tokenDigest, that.tokenDigest);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(feedId, owner, ownershipEpoch, claimedRevision,
                feedIncarnation, guardShardObservation, feedRootCzxid)
                + Arrays.hashCode(tokenDigest);
    }
}
