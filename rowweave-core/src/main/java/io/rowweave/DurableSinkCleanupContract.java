package io.rowweave;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Objects;

/** Bounded orphan ownership, retention and safe-delete declaration. */
public final class DurableSinkCleanupContract implements Serializable {

    private static final long serialVersionUID = 1L;
    public static final long MAX_RETENTION_MILLIS = 365L * 24L * 60L * 60L * 1000L;
    public static final int MAX_BATCH_SIZE = 4096;

    private final long minimumRetentionMillis;
    private final int maxClaimBatch;
    private final int maxDeleteBatch;
    private final boolean leaseRequired;
    private final DurableBatchSinkProviderContract.OrphanOwnership orphanOwnership;
    private final DurableBatchSinkProviderContract.QuarantinePolicy quarantinePolicy;
    private final DurableBatchSinkProviderContract.SafeDeleteProof safeDeleteProof;

    public DurableSinkCleanupContract(
            long minimumRetentionMillis, int maxClaimBatch, int maxDeleteBatch,
            boolean leaseRequired,
            DurableBatchSinkProviderContract.OrphanOwnership orphanOwnership,
            DurableBatchSinkProviderContract.QuarantinePolicy quarantinePolicy,
            DurableBatchSinkProviderContract.SafeDeleteProof safeDeleteProof) {
        if (minimumRetentionMillis <= 0L
                || minimumRetentionMillis > MAX_RETENTION_MILLIS) {
            throw new IllegalArgumentException(
                    "durable Sink retention is invalid");
        }
        if (maxClaimBatch <= 0 || maxClaimBatch > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    "durable Sink cleanup claim bound is invalid");
        }
        if (maxDeleteBatch <= 0 || maxDeleteBatch > maxClaimBatch) {
            throw new IllegalArgumentException(
                    "durable Sink cleanup delete bound is invalid");
        }
        if (orphanOwnership == null || quarantinePolicy == null
                || safeDeleteProof == null) {
            throw new IllegalArgumentException(
                    "durable Sink cleanup semantics are required");
        }
        this.minimumRetentionMillis = minimumRetentionMillis;
        this.maxClaimBatch = maxClaimBatch;
        this.maxDeleteBatch = maxDeleteBatch;
        this.leaseRequired = leaseRequired;
        this.orphanOwnership = orphanOwnership;
        this.quarantinePolicy = quarantinePolicy;
        this.safeDeleteProof = safeDeleteProof;
    }

    public long minimumRetentionMillis() { return minimumRetentionMillis; }
    public int maxClaimBatch() { return maxClaimBatch; }
    public int maxDeleteBatch() { return maxDeleteBatch; }
    public boolean leaseRequired() { return leaseRequired; }
    public DurableBatchSinkProviderContract.OrphanOwnership orphanOwnership() {
        return orphanOwnership;
    }
    public DurableBatchSinkProviderContract.QuarantinePolicy quarantinePolicy() {
        return quarantinePolicy;
    }
    public DurableBatchSinkProviderContract.SafeDeleteProof safeDeleteProof() {
        return safeDeleteProof;
    }

    private Object readResolve() throws ObjectStreamException {
        return new DurableSinkCleanupContract(minimumRetentionMillis,
                maxClaimBatch, maxDeleteBatch, leaseRequired, orphanOwnership,
                quarantinePolicy, safeDeleteProof);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof DurableSinkCleanupContract)) return false;
        DurableSinkCleanupContract that = (DurableSinkCleanupContract) other;
        return minimumRetentionMillis == that.minimumRetentionMillis
                && maxClaimBatch == that.maxClaimBatch
                && maxDeleteBatch == that.maxDeleteBatch
                && leaseRequired == that.leaseRequired
                && orphanOwnership == that.orphanOwnership
                && quarantinePolicy == that.quarantinePolicy
                && safeDeleteProof == that.safeDeleteProof;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minimumRetentionMillis, maxClaimBatch,
                maxDeleteBatch, leaseRequired, orphanOwnership,
                quarantinePolicy, safeDeleteProof);
    }
}
