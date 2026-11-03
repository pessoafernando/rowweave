package io.rowweave.run;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Objects;

/** Immutable fencing token for durable recovery ownership. */
public final class RecoveryToken {
    private final String runId;
    private final RecoveryOwner owner;
    private final long ownershipEpoch;
    private final long claimedRevision;
    private final byte[] tokenDigest;

    private RecoveryToken(String runId, RecoveryOwner owner, long ownershipEpoch,
                          long claimedRevision, byte[] tokenDigest) {
        this.runId = RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
        if (owner == null) throw new IllegalArgumentException("owner is required");
        if (ownershipEpoch <= 0L) {
            throw new IllegalArgumentException("ownershipEpoch must be > 0");
        }
        if (claimedRevision <= 0L) {
            throw new IllegalArgumentException("claimedRevision must be > 0");
        }
        if (tokenDigest == null || tokenDigest.length != 32) {
            throw new IllegalArgumentException("tokenDigest must be SHA-256");
        }
        this.owner = owner;
        this.ownershipEpoch = ownershipEpoch;
        this.claimedRevision = claimedRevision;
        this.tokenDigest = tokenDigest.clone();
    }

    public static RecoveryToken create(String runId, RecoveryOwner owner,
                                       long ownershipEpoch, long claimedRevision) {
        byte[] digest = RecoveryOwnershipCodec.digestTokenFields(
                runId, owner, ownershipEpoch, claimedRevision);
        return new RecoveryToken(runId, owner, ownershipEpoch, claimedRevision, digest);
    }

    static RecoveryToken decoded(String runId, RecoveryOwner owner, long ownershipEpoch,
                                 long claimedRevision, byte[] tokenDigest) {
        RecoveryToken token = new RecoveryToken(
                runId, owner, ownershipEpoch, claimedRevision, tokenDigest);
        byte[] expected = RecoveryOwnershipCodec.digestTokenFields(
                runId, owner, ownershipEpoch, claimedRevision);
        if (!MessageDigest.isEqual(expected, tokenDigest)) {
            throw new IllegalArgumentException("recovery token digest mismatch");
        }
        return token;
    }

    public String runId() { return runId; }
    public RecoveryOwner owner() { return owner; }
    public long ownershipEpoch() { return ownershipEpoch; }
    public long claimedRevision() { return claimedRevision; }
    public byte[] tokenDigest() { return tokenDigest.clone(); }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof RecoveryToken)) return false;
        RecoveryToken that = (RecoveryToken) other;
        return ownershipEpoch == that.ownershipEpoch
                && claimedRevision == that.claimedRevision
                && runId.equals(that.runId)
                && owner.equals(that.owner)
                && MessageDigest.isEqual(tokenDigest, that.tokenDigest);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(runId, owner, ownershipEpoch, claimedRevision)
                + Arrays.hashCode(tokenDigest);
    }
}
