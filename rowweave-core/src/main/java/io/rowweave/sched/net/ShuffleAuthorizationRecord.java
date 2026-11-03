package io.rowweave.sched.net;

import io.rowweave.run.RunAttemptFence;

import java.io.Serializable;
import java.util.UUID;

/** Consumer-side, non-bearer authorization projection for one remote logical producer. */
public final class ShuffleAuthorizationRecord implements Serializable {

    private static final long serialVersionUID = 1L;
    public static final int DIGEST_BYTES = 32;

    private final UUID targetId;
    private final int producerOrdinal;
    private final WorkerDataIdentity expectedSourceIdentity;
    private final byte[] authorizationDigest;

    private ShuffleAuthorizationRecord(UUID targetId, int producerOrdinal,
                                       WorkerDataIdentity expectedSourceIdentity,
                                       byte[] authorizationDigest) {
        if (targetId == null || Frame.NO_TARGET.equals(targetId)) {
            throw new IllegalArgumentException("non-zero authorization target is required");
        }
        ShuffleProducerGrant.requireOrdinal(producerOrdinal);
        if (expectedSourceIdentity == null) {
            throw new IllegalArgumentException("expected source identity is required");
        }
        if (authorizationDigest == null || authorizationDigest.length != DIGEST_BYTES) {
            throw new IllegalArgumentException("authorization digest must be exactly 32 bytes");
        }
        this.targetId = targetId;
        this.producerOrdinal = producerOrdinal;
        this.expectedSourceIdentity = expectedSourceIdentity;
        this.authorizationDigest = authorizationDigest.clone();
    }

    public static ShuffleAuthorizationRecord authorize(
            RunAttemptFence fence, UUID targetId, int producerOrdinal,
            WorkerDataIdentity expectedSourceIdentity, ShuffleProducerGrant grant) {
        if (grant == null) throw new IllegalArgumentException("producer grant is required");
        return new ShuffleAuthorizationRecord(targetId, producerOrdinal,
                expectedSourceIdentity,
                grant.authorizationDigest(fence, targetId, producerOrdinal));
    }

    public static ShuffleAuthorizationRecord fromDigest(
            UUID targetId, int producerOrdinal,
            WorkerDataIdentity expectedSourceIdentity, byte[] authorizationDigest) {
        return new ShuffleAuthorizationRecord(targetId, producerOrdinal,
                expectedSourceIdentity, authorizationDigest);
    }

    public UUID targetId() { return targetId; }
    public int producerOrdinal() { return producerOrdinal; }
    public WorkerDataIdentity expectedSourceIdentity() { return expectedSourceIdentity; }
    public byte[] authorizationDigest() { return authorizationDigest.clone(); }

    @Override
    public String toString() {
        return "ShuffleAuthorizationRecord{redacted}";
    }
}
