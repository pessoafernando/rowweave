package io.rowweave.incremental;

import java.util.Arrays;

/** Immutable bounded cut frontier. */
public final class IncrementalFrontier {
    private static final int TIMESTAMP_PAYLOAD_OVERHEAD = 8 + 4;
    public enum Kind {
        BEGIN_V1(1),
        INT64_SEQUENCE_V1(2),
        TIMESTAMP_KEY_V1(3),
        MANIFEST_GENERATION_V1(4);

        private final int wireId;

        Kind(int wireId) {
            this.wireId = wireId;
        }

        int wireId() {
            return wireId;
        }

        static Kind fromWire(int wireId) {
            for (Kind kind : values()) if (kind.wireId == wireId) return kind;
            throw new IllegalArgumentException("unknown incremental frontier kind");
        }
    }

    private final Kind kind;
    private final long primary;
    private final OrderedFrontierKey orderedKey;
    private final byte[] manifestDigest;

    private IncrementalFrontier(Kind kind, long primary,
                                OrderedFrontierKey orderedKey, byte[] manifestDigest) {
        this.kind = kind;
        this.primary = primary;
        this.orderedKey = orderedKey;
        this.manifestDigest = manifestDigest;
    }

    public static IncrementalFrontier begin() {
        return new IncrementalFrontier(Kind.BEGIN_V1, 0L, null, null);
    }

    public static IncrementalFrontier int64Sequence(long sequence) {
        requireNonNegative(sequence, "sequence");
        return new IncrementalFrontier(Kind.INT64_SEQUENCE_V1, sequence, null, null);
    }

    public static IncrementalFrontier timestampKey(long epochMicros, OrderedFrontierKey key) {
        if (key == null) throw new IllegalArgumentException("ordered timestamp key is required");
        if (key.canonicalOrderBytes().length
                > IncrementalFrontierCodec.MAX_PAYLOAD_BYTES - TIMESTAMP_PAYLOAD_OVERHEAD) {
            throw new IllegalArgumentException("ordered timestamp key exceeds frontier payload limit");
        }
        return new IncrementalFrontier(Kind.TIMESTAMP_KEY_V1, epochMicros, key, null);
    }

    public static IncrementalFrontier manifestGeneration(long generation, byte[] digest) {
        requireNonNegative(generation, "manifest generation");
        if (digest == null || digest.length != 32) {
            throw new IllegalArgumentException("manifest digest must be SHA-256");
        }
        return new IncrementalFrontier(
                Kind.MANIFEST_GENERATION_V1, generation, null, digest.clone());
    }

    public Kind kind() {
        return kind;
    }

    public long sequence() {
        requireKind(Kind.INT64_SEQUENCE_V1);
        return primary;
    }

    public long epochMicros() {
        requireKind(Kind.TIMESTAMP_KEY_V1);
        return primary;
    }

    public OrderedFrontierKey orderedKey() {
        requireKind(Kind.TIMESTAMP_KEY_V1);
        return orderedKey;
    }

    public long generation() {
        requireKind(Kind.MANIFEST_GENERATION_V1);
        return primary;
    }

    public byte[] manifestDigest() {
        requireKind(Kind.MANIFEST_GENERATION_V1);
        return manifestDigest.clone();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalFrontier)) return false;
        IncrementalFrontier that = (IncrementalFrontier) other;
        return kind == that.kind && primary == that.primary
                && equal(orderedKey, that.orderedKey)
                && Arrays.equals(manifestDigest, that.manifestDigest);
    }

    @Override
    public int hashCode() {
        int result = kind.hashCode();
        result = 31 * result + (int) (primary ^ (primary >>> 32));
        result = 31 * result + (orderedKey == null ? 0 : orderedKey.hashCode());
        result = 31 * result + Arrays.hashCode(manifestDigest);
        return result;
    }

    private static boolean equal(Object left, Object right) {
        return left == null ? right == null : left.equals(right);
    }

    private static void requireNonNegative(long value, String field) {
        if (value < 0L) throw new IllegalArgumentException(field + " must be non-negative");
    }

    private void requireKind(Kind expected) {
        if (kind != expected) throw new IllegalStateException("frontier kind is " + kind);
    }
}
