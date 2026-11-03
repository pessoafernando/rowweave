package io.rowweave.run;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Objects;

/** Durable fencing token for the staged deletion of a terminal Run. */
public final class RetentionToken {
    private final String runId;
    private final RetentionOwner owner;
    private final long ownershipEpoch;
    private final long terminalRevision;
    private final long terminalAtMillis;
    private final byte[] tokenDigest;

    private RetentionToken(String runId, RetentionOwner owner, long ownershipEpoch,
                           long terminalRevision, long terminalAtMillis,
                           byte[] tokenDigest) {
        this.runId = RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
        if (owner == null) throw new IllegalArgumentException("owner is required");
        if (ownershipEpoch <= 0L) {
            throw new IllegalArgumentException("ownershipEpoch must be > 0");
        }
        if (terminalRevision <= 0L) {
            throw new IllegalArgumentException("terminalRevision must be > 0");
        }
        if (terminalAtMillis <= 0L) {
            throw new IllegalArgumentException("terminalAtMillis must be > 0");
        }
        if (tokenDigest == null || tokenDigest.length != 32) {
            throw new IllegalArgumentException("tokenDigest must be SHA-256");
        }
        this.owner = owner;
        this.ownershipEpoch = ownershipEpoch;
        this.terminalRevision = terminalRevision;
        this.terminalAtMillis = terminalAtMillis;
        this.tokenDigest = tokenDigest.clone();
    }

    public static RetentionToken create(String runId, RetentionOwner owner,
                                        long ownershipEpoch, long terminalRevision,
                                        long terminalAtMillis) {
        byte[] digest = RunRetentionCodec.digestTokenFields(
                runId, owner, ownershipEpoch, terminalRevision, terminalAtMillis);
        return new RetentionToken(runId, owner, ownershipEpoch, terminalRevision,
                terminalAtMillis, digest);
    }

    static RetentionToken decoded(String runId, RetentionOwner owner,
                                  long ownershipEpoch, long terminalRevision,
                                  long terminalAtMillis, byte[] tokenDigest) {
        RetentionToken token = new RetentionToken(runId, owner, ownershipEpoch,
                terminalRevision, terminalAtMillis, tokenDigest);
        byte[] expected = RunRetentionCodec.digestTokenFields(
                runId, owner, ownershipEpoch, terminalRevision, terminalAtMillis);
        if (!MessageDigest.isEqual(expected, tokenDigest)) {
            throw new IllegalArgumentException("retention token digest mismatch");
        }
        return token;
    }

    public String runId() { return runId; }
    public RetentionOwner owner() { return owner; }
    public long ownershipEpoch() { return ownershipEpoch; }
    public long terminalRevision() { return terminalRevision; }
    public long terminalAtMillis() { return terminalAtMillis; }
    public byte[] tokenDigest() { return tokenDigest.clone(); }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof RetentionToken)) return false;
        RetentionToken that = (RetentionToken) other;
        return ownershipEpoch == that.ownershipEpoch
                && terminalRevision == that.terminalRevision
                && terminalAtMillis == that.terminalAtMillis
                && runId.equals(that.runId)
                && owner.equals(that.owner)
                && MessageDigest.isEqual(tokenDigest, that.tokenDigest);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(runId, owner, ownershipEpoch,
                terminalRevision, terminalAtMillis) + Arrays.hashCode(tokenDigest);
    }
}
