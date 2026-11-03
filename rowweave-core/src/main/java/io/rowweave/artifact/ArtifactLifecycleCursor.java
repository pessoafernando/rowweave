package io.rowweave.artifact;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;

/** A domain-isolated, checksummed cursor with a fixed upper bound, used by inventory scan and reference backfill. */
public final class ArtifactLifecycleCursor {
    public enum Kind { INVENTORY, REFERENCE_BACKFILL }

    private static final int MAGIC = 0x52574143; // RWAC
    private static final byte VERSION = 1;
    private static final int CHECKSUM_BYTES = 32;
    private static final int MAX_TOKEN_CHARS = 128;

    private final Kind kind;
    private final long cycle;
    private final int shard;
    private final String afterDigest;

    private ArtifactLifecycleCursor(Kind kind, long cycle,
                                    int shard, String afterDigest) {
        if (kind == null) throw new IllegalArgumentException("cursor kind is required");
        if (cycle < 0L || shard < 0 || shard > 255) {
            throw new IllegalArgumentException("invalid Artifact cursor position");
        }
        if (afterDigest != null) {
            ArtifactRepositorySupport.validateId(afterDigest);
            if (ArtifactLifecycleSupport.shard(afterDigest) != shard) {
                throw new IllegalArgumentException("Artifact cursor shard mismatch");
            }
        }
        this.kind = kind;
        this.cycle = cycle;
        this.shard = shard;
        this.afterDigest = afterDigest;
    }

    public static ArtifactLifecycleCursor start(Kind kind) {
        return new ArtifactLifecycleCursor(kind, 0L, 0, null);
    }

    public static ArtifactLifecycleCursor at(Kind kind, long cycle,
                                             int shard, String afterDigest) {
        return new ArtifactLifecycleCursor(kind, cycle, shard, afterDigest);
    }

    public static ArtifactLifecycleCursor decode(Kind expectedKind, String token) {
        if (expectedKind == null) {
            throw new IllegalArgumentException("expectedKind is required");
        }
        if (token == null || token.isEmpty() || token.length() > MAX_TOKEN_CHARS) {
            throw new IllegalArgumentException("invalid Artifact cursor length");
        }
        final byte[] encoded;
        try {
            encoded = Base64.getUrlDecoder().decode(token);
        } catch (IllegalArgumentException malformed) {
            throw new IllegalArgumentException("invalid Artifact cursor", malformed);
        }
        int noDigest = 4 + 1 + 1 + 8 + 2 + 1 + CHECKSUM_BYTES;
        int withDigest = noDigest + 32;
        if (encoded.length != noDigest && encoded.length != withDigest) {
            throw new IllegalArgumentException("invalid Artifact cursor envelope length");
        }
        int bodyLength = encoded.length - CHECKSUM_BYTES;
        byte[] body = Arrays.copyOf(encoded, bodyLength);
        byte[] checksum = Arrays.copyOfRange(encoded, bodyLength, encoded.length);
        if (!MessageDigest.isEqual(sha256(body), checksum)) {
            throw new IllegalArgumentException("Artifact cursor checksum mismatch");
        }
        ByteBuffer in = ByteBuffer.wrap(body);
        if (in.getInt() != MAGIC || in.get() != VERSION) {
            throw new IllegalArgumentException("unsupported Artifact cursor envelope");
        }
        int kindOrdinal = in.get() & 0xff;
        Kind[] kinds = Kind.values();
        if (kindOrdinal >= kinds.length || kinds[kindOrdinal] != expectedKind) {
            throw new IllegalArgumentException("Artifact cursor domain mismatch");
        }
        long cycle = in.getLong();
        int shard = in.getShort() & 0xffff;
        int hasAfter = in.get() & 0xff;
        if (cycle < 0L || shard > 255 || (hasAfter != 0 && hasAfter != 1)
                || in.remaining() != (hasAfter == 1 ? 32 : 0)) {
            throw new IllegalArgumentException("invalid Artifact cursor fields");
        }
        String after = null;
        if (hasAfter == 1) {
            byte[] digest = new byte[32];
            in.get(digest);
            after = ArtifactLifecycleSupport.hex(digest);
        }
        return new ArtifactLifecycleCursor(expectedKind, cycle, shard, after);
    }

    public String encode() {
        byte[] after = afterDigest == null
                ? null : ArtifactLifecycleSupport.hexBytes(afterDigest);
        ByteBuffer body = ByteBuffer.allocate(4 + 1 + 1 + 8 + 2 + 1
                + (after == null ? 0 : after.length));
        body.putInt(MAGIC).put(VERSION).put((byte) kind.ordinal())
                .putLong(cycle).putShort((short) shard)
                .put((byte) (after == null ? 0 : 1));
        if (after != null) body.put(after);
        byte[] raw = body.array();
        byte[] encoded = Arrays.copyOf(raw, raw.length + CHECKSUM_BYTES);
        System.arraycopy(sha256(raw), 0, encoded, raw.length, CHECKSUM_BYTES);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(encoded);
    }

    public ArtifactLifecycleCursor nextShard() {
        if (shard == 255) {
            if (cycle == Long.MAX_VALUE) {
                throw new IllegalStateException("Artifact cursor cycle overflow");
            }
            return new ArtifactLifecycleCursor(kind, cycle + 1L, 0, null);
        }
        return new ArtifactLifecycleCursor(kind, cycle, shard + 1, null);
    }

    public Kind kind() { return kind; }
    public long cycle() { return cycle; }
    public int shard() { return shard; }
    public String afterDigest() { return afterDigest; }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
