package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.MemorySegment;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/**
 * Deterministic binary encoding between a DataRow and a byte[].
 *
 * <p>Layout: [null bitmap of ceil(n/8) bytes] [non-null value encodings in field order].
 * Each value is encoded under the DataType declared by the DataSchema, so the same row always produces the same bytes.
 * A BigDecimal is encoded as its scale (int) followed by the unscaled bytes, which keeps the scale; byte-level determinism depends on it.
 *
 * <p>Moving to segmented storage changes only where the bytes live, never the format, so the operators over it are unaffected.
 */
public final class RowSerializer {

    private final DataSchema dataSchema;
    private final int fieldCount;

    public RowSerializer(DataSchema dataSchema) {
        this.dataSchema = dataSchema;
        this.fieldCount = dataSchema.getFieldCount();
    }

    public byte[] serialize(DataRow row) {
        byte[] encoded = new byte[serializedSize(row)];
        Encoder out = new ArrayEncoder(encoded);
        encodeRow(row, out);
        return encoded;
    }

    /**
     * Encodes the named columns of a source straight into this serializer's row format, without building a projected DataRow.
     * For a valid mapping the result is byte-identical to constructing a DataRow from sourceIndices and calling {@link #serialize(DataRow)}.
     */
    public byte[] serializeProjected(DataRow source, int[] sourceIndices) {
        validateProjection(source, sourceIndices);
        byte[] encoded = new byte[serializedProjectedSizeUnchecked(source, sourceIndices)];
        Encoder out = new ArrayEncoder(encoded);
        encodeProjectedRow(source, sourceIndices, out);
        return encoded;
    }

    /** Writes straight into a caller-owned segment without creating a temporary byte[] for the whole row. */
    public void serializeInto(DataRow row, MemorySegment segment, int offset) {
        int size = serializedSize(row);
        if (offset < 0 || offset + size > segment.size()) {
            throw new IndexOutOfBoundsException("offset=" + offset + " length=" + size
                    + " segmentSize=" + segment.size());
        }
        encodeRow(row, new SegmentEncoder(segment, offset));
    }

    private void encodeRow(DataRow row, Encoder out) {
        int bitmapBytes = (fieldCount + 7) / 8;
        for (int i = 0; i < bitmapBytes; i++) {
            out.writeByte(0);
        }
        for (int i = 0; i < fieldCount; i++) {
            if (row.get(i) == null) {
                out.orByte(i / 8, (byte) (1 << (i % 8)));
            }
        }
        out.pos = bitmapBytes;

        for (int i = 0; i < fieldCount; i++) {
            Object v = row.get(i);
            if (v == null) {
                continue;
            }
            encode(out, dataSchema.getField(i).getDataType(), v);
        }
    }

    private void encodeProjectedRow(DataRow source, int[] sourceIndices, Encoder out) {
        int bitmapBytes = (fieldCount + 7) / 8;
        for (int i = 0; i < bitmapBytes; i++) {
            out.writeByte(0);
        }
        for (int i = 0; i < fieldCount; i++) {
            if (source.get(sourceIndices[i]) == null) {
                out.orByte(i / 8, (byte) (1 << (i % 8)));
            }
        }
        out.pos = bitmapBytes;

        for (int i = 0; i < fieldCount; i++) {
            Object value = source.get(sourceIndices[i]);
            if (value != null) {
                encode(out, dataSchema.getField(i).getDataType(), value);
            }
        }
    }

    /** Computes the exact encoded length in advance without a whole-row byte[], so managed memory can reserve before allocating. */
    public int serializedSize(DataRow row) {
        validateRowWidth(row);
        long size = (fieldCount + 7L) / 8L;
        for (int i = 0; i < fieldCount; i++) {
            Object value = row.get(i);
            if (value != null) {
                size = Math.addExact(size, encodedValueSize(dataSchema.getField(i).getDataType(), value));
            }
        }
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("serialized row exceeds Integer.MAX_VALUE bytes: " + size);
        }
        return (int) size;
    }

    /** Computes the exact length of a direct projected encoding, under the same validation as {@link #serializeProjected(DataRow, int[])}. */
    public int serializedProjectedSize(DataRow source, int[] sourceIndices) {
        validateProjection(source, sourceIndices);
        return serializedProjectedSizeUnchecked(source, sourceIndices);
    }

    private int serializedProjectedSizeUnchecked(DataRow source, int[] sourceIndices) {
        long size = (fieldCount + 7L) / 8L;
        for (int i = 0; i < fieldCount; i++) {
            Object value = source.get(sourceIndices[i]);
            if (value != null) {
                size = Math.addExact(size, encodedValueSize(dataSchema.getField(i).getDataType(), value));
            }
        }
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("serialized projected row exceeds Integer.MAX_VALUE bytes: " + size);
        }
        return (int) size;
    }

    private void validateProjection(DataRow source, int[] sourceIndices) {
        if (source == null) {
            throw new IllegalArgumentException("source row must not be null");
        }
        if (sourceIndices == null) {
            throw new IllegalArgumentException("sourceIndices must not be null");
        }
        if (sourceIndices.length != fieldCount) {
            throw new IllegalArgumentException("sourceIndices length mismatch: expected " + fieldCount
                    + " got " + sourceIndices.length);
        }
        for (int sourceIndex : sourceIndices) {
            if (sourceIndex < 0 || sourceIndex >= source.size()) {
                throw new IndexOutOfBoundsException("source index " + sourceIndex
                        + " outside row field count " + source.size());
            }
        }
    }

    /** Credit for the largest variable-length field scratch array that direct encoding can hold at one moment. */
    public long serializerScratchCredit(DataRow row) {
        validateRowWidth(row);
        long max = 0;
        for (int i = 0; i < fieldCount; i++) {
            Object value = row.get(i);
            if (value == null) {
                continue;
            }
            DataType type = dataSchema.getField(i).getDataType();
            if (type == DataType.BigDecimalType || (type == DataType.NullType && value instanceof BigDecimal)) {
                BigDecimal decimal = (BigDecimal) value;
                if (decimal.precision() > 18) {
                    long bytes = ManagedMemoryLayout.arrayBytes(signedBigIntegerBytes(decimal.unscaledValue()));
                    max = Math.max(max, bytes);
                }
            }
        }
        return max;
    }

    /** Non-projected encoding rejects a schema width mismatch before the first field is read, so a wide row cannot be silently truncated. */
    private void validateRowWidth(DataRow row) {
        if (row == null || row.size() != fieldCount) {
            throw new IllegalArgumentException("row width mismatch: expected=" + fieldCount
                    + " actual=" + (row == null ? "null" : row.size()));
        }
    }

    public DataRow deserialize(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("serialized row payload must not be null");
        }
        Cursor cur = new Cursor(bytes);
        int bitmapBytes = (fieldCount + 7) / 8;
        // The bitmap is read by offset straight out of data rather than sliced into a new byte[], which saves one allocation per row on the cluster decode hot path.
        cur.require(bitmapBytes, "null bitmap");
        int bitmapPos = cur.pos;
        cur.pos += bitmapBytes;

        DataRow row = new DataRow(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            boolean isNull = (cur.data[bitmapPos + (i >> 3)] & (1 << (i & 7))) != 0;
            if (isNull) {
                continue;
            }
            row.set(i, decode(cur, dataSchema.getField(i).getDataType()));
        }
        cur.requireFullyConsumed();
        return row;
    }

    private void encode(Encoder out, DataType type, Object v) {
        switch (type) {
            case StringType:
                writeString(out, (String) v);
                break;
            case IntegerType:
                writeInt(out, (Integer) v);
                break;
            case LongType:
                writeLong(out, (Long) v);
                break;
            case BigDecimalType:
                encodeBigDecimal(out, (BigDecimal) v);
                break;
            case BooleanType:
                out.writeByte(((Boolean) v) ? 1 : 0);
                break;
            case DateType:
                writeLong(out, ((java.util.Date) v).getTime());
                break;
            case TimestampType: {
                java.sql.Timestamp ts = (java.sql.Timestamp) v;
                writeLong(out, ts.getTime());   // millis, second-level precision included
                writeInt(out, ts.getNanos());   // nanos, kept at full fidelity separately, as FileSink does
                break;
            }
            case NullType:
                // untyped/computed column (e.g. select expr): self-describe the runtime type.
                encodeUntyped(out, v);
                break;
            default:
                throw new IllegalArgumentException("Unsupported type for serialization: " + type);
        }
    }

    // BigDecimal encoding flag: 0 is the long fast path, storing the unscaled value as an 8-byte long and decoding through BigDecimal.valueOf without a BigInteger;
    // 1 is the BigInteger fallback for precision beyond a long, storing the unscaled bytes. The split avoids stripLeadingZeroBytes, a profiled hot spot when decoding.
    private static final int BD_LONG = 0;
    private static final int BD_BIGINT = 1;

    private void encodeBigDecimal(Encoder out, BigDecimal d) {
        writeInt(out, d.scale());
        // A precision of 18 or less keeps the unscaled value inside the long range, since 10^18 is below Long.MAX_VALUE, so the fast path stores a long.
        if (d.precision() <= 18) {
            out.writeByte(BD_LONG);
            writeLong(out, d.unscaledValue().longValue());
        } else {
            out.writeByte(BD_BIGINT);
            writeBytes(out, d.unscaledValue().toByteArray());
        }
    }

    private BigDecimal decodeBigDecimal(Cursor cur) {
        int scale = readInt(cur);
        int tag = cur.readByte() & 0xFF;
        if (tag == BD_LONG) {
            return BigDecimal.valueOf(readLong(cur), scale); // long fast path, no BigInteger byte scan
        }
        return new BigDecimal(new java.math.BigInteger(readBytes(cur)), scale);
    }

    // runtime-type tags for NullType (untyped) columns
    private static final int TAG_BIGDECIMAL = 0;
    private static final int TAG_STRING = 1;
    private static final int TAG_LONG = 2;
    private static final int TAG_INTEGER = 3;
    private static final int TAG_BOOLEAN = 4;
    private static final int TAG_TIMESTAMP = 5;
    private static final int TAG_DATE = 6;

    private void encodeUntyped(Encoder out, Object v) {
        // Order matters: java.sql.Timestamp extends java.util.Date, so Timestamp is tested before Date.
        if (v instanceof BigDecimal) {
            out.writeByte(TAG_BIGDECIMAL);
            encode(out, DataType.BigDecimalType, v);
        } else if (v instanceof String) {
            out.writeByte(TAG_STRING);
            encode(out, DataType.StringType, v);
        } else if (v instanceof Long) {
            out.writeByte(TAG_LONG);
            encode(out, DataType.LongType, v);
        } else if (v instanceof Integer) {
            out.writeByte(TAG_INTEGER);
            encode(out, DataType.IntegerType, v);
        } else if (v instanceof Boolean) {
            out.writeByte(TAG_BOOLEAN);
            out.writeByte(((Boolean) v) ? 1 : 0);
        } else if (v instanceof java.sql.Timestamp) {
            out.writeByte(TAG_TIMESTAMP);
            encode(out, DataType.TimestampType, v);
        } else if (v instanceof java.util.Date) {
            out.writeByte(TAG_DATE);
            encode(out, DataType.DateType, v);
        } else {
            throw new IllegalArgumentException("Unsupported runtime type for untyped column: "
                    + v.getClass().getName());
        }
    }

    private Object decodeUntyped(Cursor cur) {
        int tag = cur.readByte() & 0xFF;
        switch (tag) {
            case TAG_BIGDECIMAL: return decode(cur, DataType.BigDecimalType);
            case TAG_STRING: return decode(cur, DataType.StringType);
            case TAG_LONG: return decode(cur, DataType.LongType);
            case TAG_INTEGER: return decode(cur, DataType.IntegerType);
            case TAG_BOOLEAN: return (cur.readByte() & 0xFF) != 0;
            case TAG_TIMESTAMP: return decode(cur, DataType.TimestampType);
            case TAG_DATE: return decode(cur, DataType.DateType);
            default: throw new IllegalArgumentException("Unknown untyped-column tag: " + tag);
        }
    }

    private Object decode(Cursor cur, DataType type) {
        switch (type) {
            case StringType:
                return readString(cur);
            case IntegerType:
                return readInt(cur);
            case LongType:
                return readLong(cur);
            case BigDecimalType:
                return decodeBigDecimal(cur);
            case BooleanType:
                return (cur.readByte() & 0xFF) != 0;
            case DateType:
                return new java.util.Date(readLong(cur));
            case TimestampType: {
                java.sql.Timestamp ts = new java.sql.Timestamp(readLong(cur));
                ts.setNanos(readInt(cur));
                return ts;
            }
            case NullType:
                return decodeUntyped(cur);
            default:
                throw new IllegalArgumentException("Unsupported type for deserialization: " + type);
        }
    }

    private void writeBytes(Encoder out, byte[] b) {
        writeInt(out, b.length);
        out.writeBytes(b);
    }

    private void writeString(Encoder out, String value) {
        int length = utf8Length(value);
        writeInt(out, length);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < 0x80) {
                out.writeByte(c);
            } else if (c < 0x800) {
                out.writeByte(0xC0 | (c >>> 6));
                out.writeByte(0x80 | (c & 0x3F));
            } else if (Character.isHighSurrogate(c) && i + 1 < value.length()
                    && Character.isLowSurrogate(value.charAt(i + 1))) {
                int codePoint = Character.toCodePoint(c, value.charAt(++i));
                out.writeByte(0xF0 | (codePoint >>> 18));
                out.writeByte(0x80 | ((codePoint >>> 12) & 0x3F));
                out.writeByte(0x80 | ((codePoint >>> 6) & 0x3F));
                out.writeByte(0x80 | (codePoint & 0x3F));
            } else if (Character.isSurrogate(c)) {
                out.writeByte('?'); // StandardCharsets.UTF_8 replacement for malformed surrogate
            } else {
                out.writeByte(0xE0 | (c >>> 12));
                out.writeByte(0x80 | ((c >>> 6) & 0x3F));
                out.writeByte(0x80 | (c & 0x3F));
            }
        }
    }

    private int utf8Length(String value) {
        long length = 0;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < 0x80) {
                length++;
            } else if (c < 0x800) {
                length += 2;
            } else if (Character.isHighSurrogate(c) && i + 1 < value.length()
                    && Character.isLowSurrogate(value.charAt(i + 1))) {
                length += 4;
                i++;
            } else if (Character.isSurrogate(c)) {
                length++;
            } else {
                length += 3;
            }
        }
        if (length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("UTF-8 value exceeds Integer.MAX_VALUE bytes");
        }
        return (int) length;
    }

    private byte[] readBytes(Cursor cur) {
        int len = readInt(cur);
        return cur.read(len, "variable-length bytes");
    }

    /** A UTF-8 string is built directly over the original byte[] and its offset, without slicing an intermediate array. */
    private String readString(Cursor cur) {
        int len = readInt(cur);
        cur.require(len, "UTF-8 string bytes");
        String s = new String(cur.data, cur.pos, len, StandardCharsets.UTF_8);
        cur.pos += len;
        return s;
    }

    private void writeInt(Encoder out, int v) {
        out.writeInt(v);
    }

    private int readInt(Cursor cur) {
        cur.require(4, "int");
        byte[] d = cur.data;
        int p = cur.pos;
        cur.pos = p + 4;
        return ((d[p] & 0xFF) << 24) | ((d[p + 1] & 0xFF) << 16) | ((d[p + 2] & 0xFF) << 8) | (d[p + 3] & 0xFF);
    }

    private void writeLong(Encoder out, long v) {
        out.writeLong(v);
    }

    private long encodedValueSize(DataType type, Object value) {
        switch (type) {
            case StringType:
                return 4L + utf8Length((String) value);
            case IntegerType:
                return 4;
            case LongType:
            case DateType:
                return 8;
            case TimestampType:
                return 12;
            case BooleanType:
                return 1;
            case BigDecimalType: {
                BigDecimal decimal = (BigDecimal) value;
                return decimal.precision() <= 18 ? 13L
                        : 9L + signedBigIntegerBytes(decimal.unscaledValue());
            }
            case NullType:
                return 1L + untypedValueSize(value);
            default:
                throw new IllegalArgumentException("Unsupported type for serialization: " + type);
        }
    }

    private long untypedValueSize(Object value) {
        if (value instanceof BigDecimal) return encodedValueSize(DataType.BigDecimalType, value);
        if (value instanceof String) return encodedValueSize(DataType.StringType, value);
        if (value instanceof Long) return 8;
        if (value instanceof Integer) return 4;
        if (value instanceof Boolean) return 1;
        if (value instanceof java.sql.Timestamp) return 12;
        if (value instanceof java.util.Date) return 8;
        throw new IllegalArgumentException("Unsupported runtime type for untyped column: "
                + value.getClass().getName());
    }

    private int signedBigIntegerBytes(java.math.BigInteger value) {
        return value.bitLength() / 8 + 1;
    }

    private long readLong(Cursor cur) {
        cur.require(8, "long");
        byte[] d = cur.data;
        int p = cur.pos;
        cur.pos = p + 8;
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (d[p + i] & 0xFF);
        }
        return v;
    }

    private static final class Cursor {
        final byte[] data;
        int pos;

        Cursor(byte[] data) {
            this.data = data;
        }

        /** Variable-length byte slice for the BigInteger fallback: the remaining count is validated first, so an untrusted inner length cannot trigger an oversized allocation. */
        byte[] read(int n, String context) {
            require(n, context);
            byte[] slice = new byte[n];
            System.arraycopy(data, pos, slice, 0, n);
            pos += n;
            return slice;
        }

        byte readByte() {
            require(1, "byte");
            return data[pos++];
        }

        void require(int required, String context) {
            int remaining = data.length - pos;
            if (required < 0 || required > remaining) {
                throw new IllegalArgumentException("truncated serialized row payload: context=" + context
                        + " offset=" + pos + " required=" + required + " remaining=" + remaining
                        + " total=" + data.length);
            }
        }

        void requireFullyConsumed() {
            if (pos != data.length) {
                throw new IllegalArgumentException("trailing serialized row bytes: consumed=" + pos
                        + " total=" + data.length + " trailing=" + (data.length - pos));
            }
        }
    }

    private abstract static class Encoder {
        int pos;

        abstract void writeByte(int value);

        abstract void writeBytes(byte[] value);

        abstract void orByte(int offset, byte value);

        void writeInt(int value) {
            writeByte((value >>> 24) & 0xFF);
            writeByte((value >>> 16) & 0xFF);
            writeByte((value >>> 8) & 0xFF);
            writeByte(value & 0xFF);
        }

        void writeLong(long value) {
            for (int shift = 56; shift >= 0; shift -= 8) {
                writeByte((int) ((value >>> shift) & 0xFF));
            }
        }
    }

    private static final class ArrayEncoder extends Encoder {
        private final byte[] data;

        ArrayEncoder(byte[] data) {
            this.data = data;
        }

        @Override
        void writeByte(int value) {
            data[pos++] = (byte) value;
        }

        @Override
        void writeBytes(byte[] value) {
            System.arraycopy(value, 0, data, pos, value.length);
            pos += value.length;
        }

        @Override
        void writeInt(int value) {
            data[pos++] = (byte) (value >>> 24);
            data[pos++] = (byte) (value >>> 16);
            data[pos++] = (byte) (value >>> 8);
            data[pos++] = (byte) value;
        }

        @Override
        void writeLong(long value) {
            for (int shift = 56; shift >= 0; shift -= 8) {
                data[pos++] = (byte) (value >>> shift);
            }
        }

        @Override
        void orByte(int offset, byte value) {
            data[offset] |= value;
        }
    }

    private static final class SegmentEncoder extends Encoder {
        private final MemorySegment segment;
        private final int baseOffset;

        SegmentEncoder(MemorySegment segment, int baseOffset) {
            this.segment = segment;
            this.baseOffset = baseOffset;
        }

        @Override
        void writeByte(int value) {
            segment.putByte(baseOffset + pos++, (byte) value);
        }

        @Override
        void writeBytes(byte[] value) {
            segment.putBytes(baseOffset + pos, value);
            pos += value.length;
        }

        @Override
        void writeInt(int value) {
            segment.putInt(baseOffset + pos, value);
            pos += 4;
        }

        @Override
        void writeLong(long value) {
            segment.putLong(baseOffset + pos, value);
            pos += 8;
        }

        @Override
        void orByte(int offset, byte value) {
            int absolute = baseOffset + offset;
            segment.putByte(absolute, (byte) (segment.getByte(absolute) | value));
        }
    }
}
