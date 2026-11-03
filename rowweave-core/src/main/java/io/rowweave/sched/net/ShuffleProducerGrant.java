package io.rowweave.sched.net;

import io.rowweave.run.RunAttemptFence;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.UUID;

/** One attempt-local bearer secret for one remote logical shuffle producer. */
public final class ShuffleProducerGrant implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final byte[] DIGEST_DOMAIN =
            "rowweave.shuffle.grant.v1".getBytes(StandardCharsets.US_ASCII);
    public static final int BYTES = 32;
    public static final int MAX_PRODUCERS_PER_TARGET = 4096;

    private final byte[] secret;

    private ShuffleProducerGrant(byte[] secret) {
        if (secret == null || secret.length != BYTES) {
            throw new IllegalArgumentException("shuffle producer grant must be exactly 32 bytes");
        }
        this.secret = secret.clone();
    }

    public static ShuffleProducerGrant generate(SecureRandom random) {
        if (random == null) throw new IllegalArgumentException("secure random is required");
        byte[] secret = new byte[BYTES];
        random.nextBytes(secret);
        return new ShuffleProducerGrant(secret);
    }

    public static ShuffleProducerGrant fromBytes(byte[] secret) {
        return new ShuffleProducerGrant(secret);
    }

    /** A defensive copy for the explicit StageCommand codec and OPEN_AUTH payload only. */
    public byte[] copyBytes() {
        return secret.clone();
    }

    public byte[] authorizationDigest(RunAttemptFence fence, UUID targetId,
                                      int producerOrdinal) {
        if (fence == null || targetId == null || Frame.NO_TARGET.equals(targetId)) {
            throw new IllegalArgumentException("attempt fence and non-zero target are required");
        }
        requireOrdinal(producerOrdinal);
        MessageDigest digest = sha256();
        digest.update(DIGEST_DOMAIN);
        updateString(digest, fence.runId());
        updateString(digest, fence.attemptId());
        updateLong(digest, fence.fenceToken());
        updateLong(digest, targetId.getMostSignificantBits());
        updateLong(digest, targetId.getLeastSignificantBits());
        updateInt(digest, producerOrdinal);
        digest.update(secret);
        return digest.digest();
    }

    static void requireOrdinal(int producerOrdinal) {
        if (producerOrdinal < 1 || producerOrdinal > MAX_PRODUCERS_PER_TARGET) {
            throw new IllegalArgumentException(
                    "producer ordinal must be in [1,4096]");
        }
    }

    boolean constantTimeEquals(ShuffleProducerGrant other) {
        return other != null && MessageDigest.isEqual(secret, other.secret);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static void updateString(MessageDigest digest, String value) {
        if (value == null) throw new IllegalArgumentException("fence identity is required");
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        updateInt(digest, encoded.length);
        digest.update(encoded);
    }

    private static void updateInt(MessageDigest digest, int value) {
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
    }

    private static void updateLong(MessageDigest digest, long value) {
        digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
    }

    @Override
    public String toString() {
        return "ShuffleProducerGrant{redacted}";
    }
}
