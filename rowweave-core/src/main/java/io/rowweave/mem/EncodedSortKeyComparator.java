package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.MemorySegment;
import io.rowweave.ops.GroupKeyComparator;
import io.rowweave.ops.RowComparator;
import io.rowweave.ops.SortField;

import java.util.Comparator;

/**
 * Compares encoded bytes directly for the built-in fixed-width, String, decimal and Timestamp keys, where that is provably equivalent.
 * A type or comparator outside that set returns {@link #FALLBACK}, and the caller then uses the authoritative DataRow comparator.
 */
final class EncodedSortKeyComparator {

    static final int FALLBACK = Integer.MIN_VALUE;
    private static final long COMPACT_DECIMAL_LIMIT = 1_000_000_000_000_000_000L;
    private static final long[] POWERS_OF_TEN = {
            1L, 10L, 100L, 1_000L, 10_000L, 100_000L, 1_000_000L,
            10_000_000L, 100_000_000L, 1_000_000_000L, 10_000_000_000L,
            100_000_000_000L, 1_000_000_000_000L, 10_000_000_000_000L,
            100_000_000_000_000L, 1_000_000_000_000_000L,
            10_000_000_000_000_000L, 100_000_000_000_000_000L
    };

    private static final ByteAccess<byte[]> ARRAY_ACCESS = new ByteAccess<byte[]>() {
        @Override public byte getByte(byte[] source, int offset) { return source[offset]; }
        @Override public int getInt(byte[] source, int offset) {
            return ((source[offset] & 0xFF) << 24)
                    | ((source[offset + 1] & 0xFF) << 16)
                    | ((source[offset + 2] & 0xFF) << 8)
                    | (source[offset + 3] & 0xFF);
        }
        @Override public long getLong(byte[] source, int offset) {
            return ((source[offset] & 0xFFL) << 56)
                    | ((source[offset + 1] & 0xFFL) << 48)
                    | ((source[offset + 2] & 0xFFL) << 40)
                    | ((source[offset + 3] & 0xFFL) << 32)
                    | ((source[offset + 4] & 0xFFL) << 24)
                    | ((source[offset + 5] & 0xFFL) << 16)
                    | ((source[offset + 6] & 0xFFL) << 8)
                    | (source[offset + 7] & 0xFFL);
        }
    };

    private static final ByteAccess<MemorySegment> SEGMENT_ACCESS = new ByteAccess<MemorySegment>() {
        @Override public byte getByte(MemorySegment source, int offset) {
            return source.getByte(offset);
        }
        @Override public int getInt(MemorySegment source, int offset) {
            return source.getInt(offset);
        }
        @Override public long getLong(MemorySegment source, int offset) {
            return source.getLong(offset);
        }
    };

    private final DataSchema dataSchema;
    private final SortField[] sortFields;
    private final boolean groupKeyMode;
    private final boolean enabled;

    private EncodedSortKeyComparator(DataSchema dataSchema, SortField[] sortFields,
                                     boolean groupKeyMode, boolean enabled) {
        this.dataSchema = dataSchema;
        this.sortFields = sortFields.clone();
        this.groupKeyMode = groupKeyMode;
        this.enabled = enabled;
    }

    static EncodedSortKeyComparator create(DataSchema dataSchema, Comparator<DataRow> comparator,
                                           SortField[] sortFields) {
        boolean groupMode = false;
        boolean compatible = dataSchema != null && sortFields != null && sortFields.length > 0;
        if (compatible && comparator instanceof RowComparator) {
            RowComparator rows = (RowComparator) comparator;
            compatible = rows.getSchema() == dataSchema
                    && sameSortFields(rows.getSortFields(), sortFields);
        } else if (compatible && comparator instanceof GroupKeyComparator) {
            GroupKeyComparator groups = (GroupKeyComparator) comparator;
            groupMode = true;
            compatible = groups.getSchema() == dataSchema
                    && sameGroupFields(groups.getKeyIndices(), sortFields);
        } else {
            compatible = false;
        }
        if (compatible) {
            for (SortField field : sortFields) {
                int index = field.getFieldIndex();
                if (index < 0 || index >= dataSchema.getFieldCount()
                        || !supportsComparatorType(dataSchema.getField(index).getDataType())
                        || hasUntypedPredecessor(dataSchema, index)) {
                    compatible = false;
                    break;
                }
            }
        }
        return new EncodedSortKeyComparator(dataSchema, sortFields == null ? new SortField[0] : sortFields,
                groupMode, compatible);
    }

    int compare(byte[] left, byte[] right) {
        if (!enabled) {
            return FALLBACK;
        }
        return compare(left, left.length, right, right.length, ARRAY_ACCESS);
    }

    int compare(MemorySegment left, int leftLength,
                MemorySegment right, int rightLength) {
        if (!enabled) {
            return FALLBACK;
        }
        return compare(left, leftLength, right, rightLength, SEGMENT_ACCESS);
    }

    private <T> int compare(T left, int leftLength, T right, int rightLength,
                            ByteAccess<T> access) {
        int bitmapBytes = (dataSchema.getFieldCount() + 7) / 8;
        requireRange(0, bitmapBytes, leftLength);
        requireRange(0, bitmapBytes, rightLength);
        for (SortField field : sortFields) {
            int index = field.getFieldIndex();
            boolean leftNull = isNull(left, index, leftLength, access);
            boolean rightNull = isNull(right, index, rightLength, access);
            DataType type = dataSchema.getField(index).getDataType();
            int compared;
            if (leftNull || rightNull) {
                compared = compareNulls(left, leftLength, leftNull,
                        right, rightLength, rightNull, type, field, access);
            } else {
                int leftOffset = locateField(dataSchema, left, leftLength, index, access);
                int rightOffset = locateField(dataSchema, right, rightLength, index, access);
                compared = compareNonNull(left, leftOffset, leftLength,
                        right, rightOffset, rightLength, type, groupKeyMode, access);
                if (compared == FALLBACK) return FALLBACK;
                if (!groupKeyMode && field.getDirection() == SortField.Direction.DESC) {
                    compared = -compared;
                }
            }
            if (compared == FALLBACK) return FALLBACK;
            if (compared != 0) {
                return compared;
            }
        }
        return 0;
    }

    private <T> int compareNulls(T left, int leftLength, boolean leftNull,
                                 T right, int rightLength, boolean rightNull,
                                 DataType type, SortField field, ByteAccess<T> access) {
        if (leftNull && rightNull) {
            return 0;
        }
        if (groupKeyMode) {
            return leftNull ? 1 : -1;
        }
        if (field.getNullOrder() == SortField.NullOrder.NULLS_FIRST) {
            return leftNull ? -1 : 1;
        }
        if (field.getNullOrder() == SortField.NullOrder.NULLS_LAST) {
            return leftNull ? 1 : -1;
        }

        int compared;
        if (type == DataType.IntegerType || type == DataType.LongType
                || type == DataType.BigDecimalType) {
            if (leftNull) {
                int rightOffset = locateField(
                        dataSchema, right, rightLength, field.getFieldIndex(), access);
                compared = compareZeroTo(right, rightOffset, rightLength, type, access);
            } else {
                int leftOffset = locateField(
                        dataSchema, left, leftLength, field.getFieldIndex(), access);
                compared = -compareZeroTo(left, leftOffset, leftLength, type, access);
            }
        } else {
            compared = leftNull ? -1 : 1;
        }
        return field.getDirection() == SortField.Direction.DESC ? -compared : compared;
    }

    private static <T> int compareZeroTo(T source, int offset, int length,
                                         DataType type, ByteAccess<T> access) {
        if (type == DataType.IntegerType) {
            requireRange(offset, 4, length);
            return Integer.compare(0, access.getInt(source, offset));
        }
        if (type == DataType.LongType) {
            requireRange(offset, 8, length);
            return Long.compare(0L, access.getLong(source, offset));
        }
        int tag = validateBigDecimalPayload(source, offset, length, access);
        if (tag == 1) {
            int bytes = access.getInt(source, offset + 5);
            return -encodedBigIntegerSign(source, offset + 9, bytes, access);
        }
        long unscaled = access.getLong(source, offset + 5);
        validateCompactUnscaled(unscaled, offset);
        return Integer.compare(0, Long.compare(unscaled, 0L));
    }

    private static <T> int compareNonNull(T left, int leftOffset, int leftLength,
                                          T right, int rightOffset, int rightLength,
                                          DataType type, boolean scaleSensitive,
                                          ByteAccess<T> access) {
        switch (type) {
            case IntegerType:
                requireRange(leftOffset, 4, leftLength);
                requireRange(rightOffset, 4, rightLength);
                return Integer.compare(access.getInt(left, leftOffset), access.getInt(right, rightOffset));
            case LongType:
            case DateType:
                requireRange(leftOffset, 8, leftLength);
                requireRange(rightOffset, 8, rightLength);
                return Long.compare(access.getLong(left, leftOffset), access.getLong(right, rightOffset));
            case BooleanType:
                requireRange(leftOffset, 1, leftLength);
                requireRange(rightOffset, 1, rightLength);
                return Byte.compare(access.getByte(left, leftOffset), access.getByte(right, rightOffset));
            case TimestampType:
                return compareTimestamp(left, leftOffset, leftLength,
                        right, rightOffset, rightLength, access);
            case StringType:
                return SerializedStringOrder.compareUtf8AsUtf16(left, leftOffset, leftLength,
                        right, rightOffset, rightLength, access);
            case BigDecimalType:
                return compareEncodedDecimal(left, leftOffset, leftLength,
                        right, rightOffset, rightLength, scaleSensitive, access);
            default:
                throw new IllegalStateException("unsupported encoded sort type: " + type);
        }
    }

    private static <T> int compareTimestamp(T left, int leftOffset, int leftLength,
                                             T right, int rightOffset, int rightLength,
                                             ByteAccess<T> access) {
        validateTimestampPayload(left, leftOffset, leftLength, access);
        validateTimestampPayload(right, rightOffset, rightLength, access);
        int millisCompared = Long.compare(
                access.getLong(left, leftOffset), access.getLong(right, rightOffset));
        if (millisCompared != 0) return millisCompared;
        return Integer.compare(
                access.getInt(left, leftOffset + 8), access.getInt(right, rightOffset + 8));
    }

    private static <T> void validateTimestampPayload(
            T source, int offset, int length, ByteAccess<T> access) {
        requireRange(offset, 12, length);
        long millis = access.getLong(source, offset);
        int nanos = access.getInt(source, offset + 8);
        if (nanos < 0 || nanos > 999_999_999) {
            throw corrupt("Timestamp nanos out of range " + nanos, offset);
        }
        if (Math.floorMod(millis, 1_000L) != nanos / 1_000_000) {
            throw corrupt("Timestamp millis/nanos are inconsistent", offset);
        }
    }

    private static <T> int compareEncodedDecimal(T left, int leftOffset, int leftLength,
                                                  T right, int rightOffset, int rightLength,
                                                  boolean scaleSensitive, ByteAccess<T> access) {
        int leftTag = validateBigDecimalPayload(left, leftOffset, leftLength, access);
        int rightTag = validateBigDecimalPayload(right, rightOffset, rightLength, access);
        int leftScale = access.getInt(left, leftOffset);
        int rightScale = access.getInt(right, rightOffset);
        if (leftTag == 1 && rightTag == 1) {
            if (leftScale != rightScale) return FALLBACK;
            return compareCanonicalBigIntegers(
                    left, leftOffset + 9, access.getInt(left, leftOffset + 5),
                    right, rightOffset + 9, access.getInt(right, rightOffset + 5), access);
        }
        if (leftTag == 1 || rightTag == 1) return FALLBACK;

        long leftUnscaled = access.getLong(left, leftOffset + 5);
        long rightUnscaled = access.getLong(right, rightOffset + 5);
        validateCompactUnscaled(leftUnscaled, leftOffset);
        validateCompactUnscaled(rightUnscaled, rightOffset);

        int compared = compareCompactValues(
                leftUnscaled, leftScale, rightUnscaled, rightScale);
        if (compared != 0 || !scaleSensitive) return compared;
        return Integer.compare(leftScale, rightScale);
    }

    private static <T> int compareCanonicalBigIntegers(
            T left, int leftOffset, int leftBytes,
            T right, int rightOffset, int rightBytes, ByteAccess<T> access) {
        int leftSign = encodedBigIntegerSign(left, leftOffset, leftBytes, access);
        int rightSign = encodedBigIntegerSign(right, rightOffset, rightBytes, access);
        if (leftSign != rightSign) return Integer.compare(leftSign, rightSign);
        if (leftSign == 0) return 0;
        if (leftBytes != rightBytes) {
            int lengthCompared = Integer.compare(leftBytes, rightBytes);
            return leftSign < 0 ? -lengthCompared : lengthCompared;
        }
        int index = 0;
        int longBytes = leftBytes & ~7;
        while (index < longBytes) {
            long leftWord = access.getLong(left, leftOffset + index);
            long rightWord = access.getLong(right, rightOffset + index);
            if (leftWord != rightWord) return Long.compareUnsigned(leftWord, rightWord);
            index += 8;
        }
        for (; index < leftBytes; index++) {
            int leftByte = access.getByte(left, leftOffset + index) & 0xFF;
            int rightByte = access.getByte(right, rightOffset + index) & 0xFF;
            if (leftByte != rightByte) return Integer.compare(leftByte, rightByte);
        }
        return 0;
    }

    private static <T> int encodedBigIntegerSign(
            T source, int offset, int bytes, ByteAccess<T> access) {
        if (bytes <= 0) throw corrupt("empty BigDecimal BigInteger payload", offset);
        byte first = access.getByte(source, offset);
        if (bytes == 1 && first == 0) return 0;
        return first < 0 ? -1 : 1;
    }

    private static int compareCompactValues(long leftUnscaled, int leftScale,
                                            long rightUnscaled, int rightScale) {
        int leftSign = Long.compare(leftUnscaled, 0L);
        int rightSign = Long.compare(rightUnscaled, 0L);
        if (leftSign != rightSign) return Integer.compare(leftSign, rightSign);
        if (leftSign == 0) return 0;

        long leftMagnitude = Math.abs(leftUnscaled);
        long rightMagnitude = Math.abs(rightUnscaled);
        int leftPrecision = compactPrecision(leftMagnitude);
        int rightPrecision = compactPrecision(rightMagnitude);
        long leftAdjusted = (long) leftPrecision - leftScale;
        long rightAdjusted = (long) rightPrecision - rightScale;
        int magnitudeCompared = Long.compare(leftAdjusted, rightAdjusted);
        if (magnitudeCompared == 0) {
            int digits = Math.max(leftPrecision, rightPrecision);
            for (int index = 0; index < digits; index++) {
                int leftDigit = compactDigit(leftMagnitude, leftPrecision, index);
                int rightDigit = compactDigit(rightMagnitude, rightPrecision, index);
                if (leftDigit != rightDigit) {
                    magnitudeCompared = Integer.compare(leftDigit, rightDigit);
                    break;
                }
            }
        }
        return leftSign < 0 ? -magnitudeCompared : magnitudeCompared;
    }

    private static int compactPrecision(long magnitude) {
        int precision = 1;
        while (precision < POWERS_OF_TEN.length
                && magnitude >= POWERS_OF_TEN[precision]) {
            precision++;
        }
        return precision;
    }

    private static int compactDigit(long magnitude, int precision, int index) {
        if (index >= precision) return 0;
        long divisor = POWERS_OF_TEN[precision - index - 1];
        return (int) ((magnitude / divisor) % 10L);
    }

    private static void validateCompactUnscaled(long unscaled, int offset) {
        if (unscaled <= -COMPACT_DECIMAL_LIMIT || unscaled >= COMPACT_DECIMAL_LIMIT) {
            throw corrupt("non-canonical compact BigDecimal unscaled " + unscaled, offset);
        }
    }

    private static <T> int validateBigDecimalPayload(T source, int offset, int length,
                                                      ByteAccess<T> access) {
        requireRange(offset, 5, length);
        int tag = access.getByte(source, offset + 4) & 0xFF;
        if (tag == 0) {
            requireRange(offset, 13, length);
            return tag;
        }
        if (tag == 1) {
            requireRange(offset, 9, length);
            int bytes = access.getInt(source, offset + 5);
            if (bytes <= 0) throw corrupt("empty BigDecimal BigInteger payload", offset);
            advance(offset, 9L + bytes, length);
            validateCanonicalBigInteger(source, offset + 9, bytes, access);
            return tag;
        }
        throw corrupt("unknown BigDecimal tag " + tag, offset);
    }

    private static <T> void validateCanonicalBigInteger(
            T source, int offset, int bytes, ByteAccess<T> access) {
        if (bytes <= 1) return;
        int first = access.getByte(source, offset) & 0xFF;
        int second = access.getByte(source, offset + 1) & 0xFF;
        if (first == 0 && (second & 0x80) == 0) {
            throw corrupt("redundant positive BigInteger sign extension", offset);
        }
        if (first == 0xFF && (second & 0x80) != 0) {
            throw corrupt("redundant negative BigInteger sign extension", offset);
        }
    }

    static boolean encodedNull(byte[] source, DataSchema dataSchema, int fieldIndex) {
        requireField(dataSchema, fieldIndex);
        return isNull(source, fieldIndex, source.length, ARRAY_ACCESS);
    }

    static int encodedFieldOffset(byte[] source, DataSchema dataSchema, int fieldIndex) {
        requireField(dataSchema, fieldIndex);
        return locateField(dataSchema, source, source.length, fieldIndex, ARRAY_ACCESS);
    }

    static int encodedInt(byte[] source, int offset) {
        requireRange(offset, 4, source.length);
        return ARRAY_ACCESS.getInt(source, offset);
    }

    static long encodedLong(byte[] source, int offset) {
        requireRange(offset, 8, source.length);
        return ARRAY_ACCESS.getLong(source, offset);
    }

    static long encodedTimestampMillis(byte[] source, int offset) {
        validateTimestampPayload(source, offset, source.length, ARRAY_ACCESS);
        return ARRAY_ACCESS.getLong(source, offset);
    }

    static boolean encodedBoolean(byte[] source, int offset) {
        requireRange(offset, 1, source.length);
        return ARRAY_ACCESS.getByte(source, offset) != 0;
    }

    static long encodedStringPrefix(byte[] source, int offset) {
        return SerializedStringOrder.prefix(source, offset, ARRAY_ACCESS);
    }

    private static <T> int locateField(DataSchema dataSchema, T source, int length,
                                       int targetIndex, ByteAccess<T> access) {
        int offset = (dataSchema.getFieldCount() + 7) / 8;
        for (int index = 0; index < targetIndex; index++) {
            if (!isNull(source, index, length, access)) {
                offset = skipValue(source, offset, length,
                        dataSchema.getField(index).getDataType(), access);
            }
        }
        requireRange(offset, 0, length);
        return offset;
    }

    private static <T> int skipValue(T source, int offset, int length,
                                     DataType type, ByteAccess<T> access) {
        switch (type) {
            case StringType: {
                requireRange(offset, 4, length);
                int bytes = access.getInt(source, offset);
                if (bytes < 0) throw corrupt("negative string length", offset);
                return advance(offset, 4L + bytes, length);
            }
            case IntegerType:
                return advance(offset, 4, length);
            case LongType:
            case DateType:
                return advance(offset, 8, length);
            case TimestampType:
                return advance(offset, 12, length);
            case BooleanType:
                return advance(offset, 1, length);
            case BigDecimalType:
                return skipBigDecimal(source, offset, length, access);
            case NullType:
            default:
                throw corrupt("unsupported preceding encoded type " + type, offset);
        }
    }

    private static <T> int skipBigDecimal(T source, int offset, int length,
                                          ByteAccess<T> access) {
        requireRange(offset, 5, length);
        int tag = access.getByte(source, offset + 4) & 0xFF;
        if (tag == 0) {
            return advance(offset, 13, length);
        }
        if (tag == 1) {
            requireRange(offset + 5, 4, length);
            int bytes = access.getInt(source, offset + 5);
            if (bytes < 0) throw corrupt("negative BigDecimal magnitude length", offset);
            return advance(offset, 9L + bytes, length);
        }
        throw corrupt("unknown BigDecimal tag " + tag, offset);
    }

    private static <T> boolean isNull(T source, int fieldIndex, int length, ByteAccess<T> access) {
        int bitmapOffset = fieldIndex >>> 3;
        requireRange(bitmapOffset, 1, length);
        return (access.getByte(source, bitmapOffset) & (1 << (fieldIndex & 7))) != 0;
    }

    private static int advance(int offset, long amount, int length) {
        long end = offset + amount;
        if (amount < 0 || end < 0 || end > length) {
            throw corrupt("encoded field exceeds row length " + length, offset);
        }
        return (int) end;
    }

    private static void requireRange(int offset, int width, int length) {
        if (offset < 0 || width < 0 || (long) offset + width > length) {
            throw corrupt("encoded read exceeds row length " + length, offset);
        }
    }

    private static IllegalArgumentException corrupt(String message, int offset) {
        return new IllegalArgumentException(message + " at offset " + offset);
    }

    private static boolean supportsComparatorType(DataType type) {
        return supportsEncodedType(type) || type == DataType.BigDecimalType;
    }

    static boolean supportsEncodedType(DataType type) {
        return type == DataType.IntegerType || type == DataType.LongType
                || type == DataType.BooleanType || type == DataType.DateType
                || type == DataType.TimestampType || type == DataType.StringType;
    }

    static boolean canReadEncodedField(DataSchema dataSchema, int fieldIndex, DataType type) {
        return fieldIndex >= 0 && fieldIndex < dataSchema.getFieldCount()
                && supportsEncodedType(type) && !hasUntypedPredecessor(dataSchema, fieldIndex);
    }

    private static boolean hasUntypedPredecessor(DataSchema dataSchema, int fieldIndex) {
        for (int i = 0; i < fieldIndex; i++) {
            if (dataSchema.getField(i).getDataType() == DataType.NullType) {
                return true;
            }
        }
        return false;
    }

    private static void requireField(DataSchema dataSchema, int fieldIndex) {
        if (fieldIndex < 0 || fieldIndex >= dataSchema.getFieldCount()) {
            throw new IllegalArgumentException("field index out of bounds: " + fieldIndex);
        }
    }

    private static boolean sameSortFields(SortField[] left, SortField[] right) {
        if (left.length != right.length) return false;
        for (int i = 0; i < left.length; i++) {
            if (left[i].getFieldIndex() != right[i].getFieldIndex()
                    || left[i].getDirection() != right[i].getDirection()
                    || left[i].getNullOrder() != right[i].getNullOrder()) {
                return false;
            }
        }
        return true;
    }

    private static boolean sameGroupFields(int[] keyIndices, SortField[] fields) {
        if (keyIndices.length != fields.length) return false;
        for (int i = 0; i < keyIndices.length; i++) {
            if (keyIndices[i] != fields[i].getFieldIndex()
                    || fields[i].getDirection() != SortField.Direction.ASC
                    || fields[i].getNullOrder() != SortField.NullOrder.NULLS_LAST) {
                return false;
            }
        }
        return true;
    }

    private interface ByteAccess<T> extends SerializedStringOrder.ByteAccess<T> {
    }
}
