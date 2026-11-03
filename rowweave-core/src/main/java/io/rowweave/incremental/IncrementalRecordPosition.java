package io.rowweave.incremental;

import java.util.Arrays;

/** Immutable canonical ordering identity for one record inside a captured interval. */
public final class IncrementalRecordPosition {
    public static final int DIGEST_BYTES = 32;
    public static final int MAX_PARTITION_ORDINAL = 4095;

    public enum Kind {
        SEQUENCE_V1(1),
        TIMESTAMP_KEY_V1(2),
        MANIFEST_ENTRY_V1(3),
        SNAPSHOT_ROW_V1(4);

        private final int wireId;

        Kind(int wireId) {
            this.wireId = wireId;
        }

        int wireId() {
            return wireId;
        }

        static Kind fromWire(int wireId) {
            for (Kind kind : values()) if (kind.wireId == wireId) return kind;
            throw new IllegalArgumentException("unknown record position kind");
        }
    }

    private final Kind kind;
    private final byte[] intervalDigest;
    private final int partitionOrdinal;
    private final long primary;
    private final long secondary;
    private final OrderedFrontierKey orderedKey;
    private final byte[] keyDigest;

    private IncrementalRecordPosition(
            Kind kind, byte[] intervalDigest, int partitionOrdinal,
            long primary, long secondary, OrderedFrontierKey orderedKey, byte[] keyDigest) {
        this.kind = kind;
        this.intervalDigest = requireDigest(intervalDigest, "interval digest");
        if (partitionOrdinal < 0 || partitionOrdinal > MAX_PARTITION_ORDINAL) {
            throw new IllegalArgumentException("partition ordinal is invalid");
        }
        this.partitionOrdinal = partitionOrdinal;
        this.primary = primary;
        this.secondary = secondary;
        this.orderedKey = orderedKey;
        this.keyDigest = requireDigest(keyDigest, "key digest");
    }

    public static IncrementalRecordPosition sequence(
            byte[] intervalDigest, int partitionOrdinal, long sequence, byte[] keyDigest) {
        requireNonNegative(sequence, "sequence");
        return new IncrementalRecordPosition(
                Kind.SEQUENCE_V1, intervalDigest, partitionOrdinal,
                sequence, 0L, null, keyDigest);
    }

    public static IncrementalRecordPosition timestampKey(
            byte[] intervalDigest, int partitionOrdinal, long epochMicros,
            OrderedFrontierKey orderedKey, byte[] keyDigest) {
        if (orderedKey == null) throw new IllegalArgumentException("ordered key is required");
        int fixedPayload = DIGEST_BYTES + 4 + 8 + 4 + DIGEST_BYTES;
        if (orderedKey.canonicalOrderBytes().length
                > IncrementalRecordPositionCodec.MAX_PAYLOAD_BYTES - fixedPayload) {
            throw new IllegalArgumentException("ordered key exceeds record position limit");
        }
        return new IncrementalRecordPosition(
                Kind.TIMESTAMP_KEY_V1, intervalDigest, partitionOrdinal,
                epochMicros, 0L, orderedKey, keyDigest);
    }

    public static IncrementalRecordPosition manifestEntry(
            byte[] intervalDigest, int partitionOrdinal, long generation,
            long entryOrdinal, byte[] keyDigest) {
        requireNonNegative(generation, "manifest generation");
        requireNonNegative(entryOrdinal, "manifest entry ordinal");
        return new IncrementalRecordPosition(
                Kind.MANIFEST_ENTRY_V1, intervalDigest, partitionOrdinal,
                generation, entryOrdinal, null, keyDigest);
    }

    public static IncrementalRecordPosition snapshotRow(
            byte[] intervalDigest, int partitionOrdinal, long rowOrdinal, byte[] keyDigest) {
        requireNonNegative(rowOrdinal, "snapshot row ordinal");
        return new IncrementalRecordPosition(
                Kind.SNAPSHOT_ROW_V1, intervalDigest, partitionOrdinal,
                rowOrdinal, 0L, null, keyDigest);
    }

    public Kind kind() {
        return kind;
    }

    public byte[] intervalDigest() {
        return intervalDigest.clone();
    }

    public int partitionOrdinal() {
        return partitionOrdinal;
    }

    public long sequence() {
        requireKind(Kind.SEQUENCE_V1);
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
        requireKind(Kind.MANIFEST_ENTRY_V1);
        return primary;
    }

    public long entryOrdinal() {
        requireKind(Kind.MANIFEST_ENTRY_V1);
        return secondary;
    }

    public long rowOrdinal() {
        requireKind(Kind.SNAPSHOT_ROW_V1);
        return primary;
    }

    public byte[] keyDigest() {
        return keyDigest.clone();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalRecordPosition)) return false;
        IncrementalRecordPosition that = (IncrementalRecordPosition) other;
        return kind == that.kind
                && partitionOrdinal == that.partitionOrdinal
                && primary == that.primary
                && secondary == that.secondary
                && Arrays.equals(intervalDigest, that.intervalDigest)
                && equal(orderedKey, that.orderedKey)
                && Arrays.equals(keyDigest, that.keyDigest);
    }

    @Override
    public int hashCode() {
        int result = kind.hashCode();
        result = 31 * result + Arrays.hashCode(intervalDigest);
        result = 31 * result + partitionOrdinal;
        result = 31 * result + (int) (primary ^ (primary >>> 32));
        result = 31 * result + (int) (secondary ^ (secondary >>> 32));
        result = 31 * result + (orderedKey == null ? 0 : orderedKey.hashCode());
        result = 31 * result + Arrays.hashCode(keyDigest);
        return result;
    }

    private static byte[] requireDigest(byte[] digest, String field) {
        if (digest == null || digest.length != DIGEST_BYTES) {
            throw new IllegalArgumentException(field + " must be SHA-256");
        }
        return digest.clone();
    }

    private static void requireNonNegative(long value, String field) {
        if (value < 0L) throw new IllegalArgumentException(field + " must be non-negative");
    }

    private void requireKind(Kind expected) {
        if (kind != expected) throw new IllegalStateException("record position kind is " + kind);
    }

    private static boolean equal(Object left, Object right) {
        return left == null ? right == null : left.equals(right);
    }
}
