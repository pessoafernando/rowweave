package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.SortField;

import java.math.BigDecimal;

/**
 * Normalized-key computer (inspired by Flink's NormalizedKeySorter): it encodes the **first sort field** into
 * an 8-byte normalized-key prefix such that {@link Long#compareUnsigned} agrees in **direction** with
 * {@link io.rowweave.ops.RowComparator} on that field.
 *
 * <p><b>Correctness invariant</b>: the prefix only accelerates and **never contradicts the authoritative
 * comparator**. If the unsigned prefix comparison is non-zero, its sign must match the comparator; on a
 * prefix tie it falls back to the full {@link io.rowweave.ops.RowComparator}, which is authoritative. So even
 * where the prefix is lossy — BigDecimal approximated through double, String keeping only its first four
 * chars, a multi-field sort encoding just the first field — only the speed-up is lost, never correctness.
 *
 * <p>Encoding, ordered ascending under unsigned comparison: Int=`(value^0x80000000)<<32`; Long=`value^MIN`;
 * BigDecimal = the sortable bits of the double (doubleValue is monotonic, so it cannot contradict);
 * String = the first 4 UTF-16 chars packed big-endian, matching String.compareTo's char order; Boolean = 0/1.
 * DESC=`~raw`, which reverses the unsigned order. null: NULLS_FIRST=0 (smallest), NULLS_LAST=-1L (largest,
 * placed absolutely so direction does not move it); under DEFAULT null is coerced (numeric to 0, otherwise
 * to the minimum) and then follows the direction reversal. Any other type has raw=0, which always ties and so
 * falls back on every comparison while remaining correct.
 */
public final class NormalizedKeyComputer {

    private final RowSerializer serializer;
    private final int fieldIndex;
    private final DataType type;
    private final boolean desc;
    private final SortField.NullOrder nullOrder;
    private final boolean fullyDetermined;
    private final boolean constantFallback;

    private NormalizedKeyComputer(DataSchema meta, SortField[] sortFields) {
        if (sortFields.length == 0) {
            throw new IllegalArgumentException("at least one SortField required");
        }
        this.serializer = new RowSerializer(meta);
        SortField first = sortFields[0];
        this.fieldIndex = first.getFieldIndex();
        this.type = meta.getField(fieldIndex).getDataType();
        this.desc = first.getDirection() == SortField.Direction.DESC;
        this.nullOrder = first.getNullOrder();
        this.constantFallback = false;
        // Never claim fullyDetermined: even for a single Int/Long/Boolean field, null is encoded as a
        // sentinel prefix (NULLS_FIRST=0 / NULLS_LAST=-1 / DEFAULT coerced), which collides with real extreme
        // values — null@NULLS_FIRST(0) equals the prefix of Integer/Long.MIN_VALUE (0), null@NULLS_LAST(-1)
        // equals MAX, and Boolean@DEFAULT null(0) equals false(0). So the prefix is not injective over a
        // domain containing null, and a prefix tie must fall back to the authoritative RowComparator.
        // Otherwise the nulls and extremes sharing a prefix would be ordered only by insertion seq, breaking
        // the null sort semantics.
        // The prefix still pays for itself: over nine tenths of comparisons are non-ties and resolve with a
        // single Long.compareUnsigned, avoiding deserialization, and only ties fall back.
        this.fullyDetermined = false;
    }

    /** Compatibility path for an arbitrary Comparator: the prefix always ties, so every ordering falls back to the authoritative comparator. */
    private NormalizedKeyComputer(DataSchema meta) {
        this.serializer = new RowSerializer(meta);
        this.fieldIndex = 0;
        this.type = DataType.NullType;
        this.desc = false;
        this.nullOrder = SortField.NullOrder.DEFAULT;
        this.fullyDetermined = false;
        this.constantFallback = true;
    }

    public static NormalizedKeyComputer of(DataSchema meta, SortField[] sortFields) {
        return new NormalizedKeyComputer(meta, sortFields);
    }

    static NormalizedKeyComputer comparatorFallback(DataSchema meta) {
        return new NormalizedKeyComputer(meta);
    }

    /** Whether the prefix covers the whole sort key; with full coverage the fallback comparison never fires and ordering is pure byte order. */
    public boolean isFullyDetermined() {
        return fullyDetermined;
    }

    /** Encode the first sort field of a row into an 8-byte unsigned-ordered prefix. */
    public long computePrefix(DataRow row) {
        if (constantFallback) {
            return 0L;
        }
        Object v = row.get(fieldIndex);
        if (v == null) {
            return nullPrefix();
        }
        long raw = rawOf(v);
        return desc ? ~raw : raw;
    }

    /** Compute the prefix from an already serialized row: a fixed-width, String or Timestamp first key is read directly, other types fall back to decoding the whole row. */
    public long computePrefixFromBytes(byte[] rowBytes, DataSchema meta) {
        if (constantFallback) {
            return 0L;
        }
        if (EncodedSortKeyComparator.canReadEncodedField(meta, fieldIndex, type)) {
            if (EncodedSortKeyComparator.encodedNull(rowBytes, meta, fieldIndex)) {
                return nullPrefix();
            }
            int offset = EncodedSortKeyComparator.encodedFieldOffset(rowBytes, meta, fieldIndex);
            long raw;
            switch (type) {
                case IntegerType:
                    raw = ((((long) EncodedSortKeyComparator.encodedInt(rowBytes, offset))
                            & 0xFFFFFFFFL) ^ 0x80000000L) << 32;
                    break;
                case LongType:
                case DateType:
                    raw = EncodedSortKeyComparator.encodedLong(rowBytes, offset) ^ Long.MIN_VALUE;
                    break;
                case TimestampType:
                    raw = EncodedSortKeyComparator.encodedTimestampMillis(rowBytes, offset)
                            ^ Long.MIN_VALUE;
                    break;
                case BooleanType:
                    raw = EncodedSortKeyComparator.encodedBoolean(rowBytes, offset) ? 1L : 0L;
                    break;
                case StringType:
                    raw = EncodedSortKeyComparator.encodedStringPrefix(rowBytes, offset);
                    break;
                default:
                    throw new IllegalStateException("unsupported fixed normalized-key type: " + type);
            }
            return desc ? ~raw : raw;
        }
        return computePrefix(serializer.deserialize(rowBytes));
    }

    private long nullPrefix() {
        if (nullOrder == SortField.NullOrder.NULLS_FIRST) {
            return 0L;
        }
        if (nullOrder == SortField.NullOrder.NULLS_LAST) {
            return -1L;
        }
        long rawNull = isNumeric(type) ? rawOf(BigDecimal.ZERO) : 0L;
        return desc ? ~rawNull : rawNull;
    }

    /** Ascending unsigned-ordered raw prefix, before desc and null handling are applied. */
    private long rawOf(Object v) {
        switch (type) {
            case IntegerType:
                return ((((long) ((Number) v).intValue()) & 0xFFFFFFFFL) ^ 0x80000000L) << 32;
            case LongType:
                return ((Number) v).longValue() ^ Long.MIN_VALUE;
            case BooleanType:
                return ((Boolean) v) ? 1L : 0L;
            case BigDecimalType:
                return sortableDouble(toDouble(v));
            case StringType:
                return packString((String) v);
            case DateType:
                return ((java.util.Date) v).getTime() ^ Long.MIN_VALUE;       // epoch millis,unsigned-ordered
            case TimestampType:
                return ((java.sql.Timestamp) v).getTime() ^ Long.MIN_VALUE;   // second-resolution prefix (nanosecond ties go to the fallback comparator)
            default:
                return 0L; // unsupported type -> constant prefix -> always tie -> always fallback (still correct)
        }
    }

    private static double toDouble(Object v) {
        if (v instanceof BigDecimal) {
            return ((BigDecimal) v).doubleValue();
        }
        return ((Number) v).doubleValue();
    }

    /** Maps a double to an unsigned-ordered long (flip negatives, set the high bit for positives), so Long.compareUnsigned gives ascending double order. */
    private static long sortableDouble(double d) {
        long bits = Double.doubleToLongBits(d);
        return (bits < 0) ? ~bits : (bits | Long.MIN_VALUE);
    }

    /** Packs the first 4 normalised UTF-16 chars big-endian; an unpaired surrogate becomes '?' here, as in RowSerializer. */
    private static long packString(String s) {
        long raw = 0L;
        int position = 0;
        int emitted = 0;
        while (emitted < 4) {
            if (position >= s.length()) {
                raw <<= 16;
                emitted++;
                continue;
            }
            char c = s.charAt(position++);
            if (Character.isHighSurrogate(c)) {
                if (position < s.length() && Character.isLowSurrogate(s.charAt(position))) {
                    raw = (raw << 16) | (c & 0xFFFFL);
                    emitted++;
                    if (emitted == 4) break;
                    c = s.charAt(position++);
                } else {
                    c = '?';
                }
            } else if (Character.isLowSurrogate(c)) {
                c = '?';
            }
            raw = (raw << 16) | (c & 0xFFFFL);
            emitted++;
        }
        return raw;
    }

    private static boolean isNumeric(DataType type) {
        return type == DataType.IntegerType || type == DataType.LongType || type == DataType.BigDecimalType;
    }
}
