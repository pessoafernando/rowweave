package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.seg.MemorySegment;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class RowSerializerTest {

    /** Every genuine prefix of a valid payload must be detected as truncated, without leaking an array implementation exception. */
    @Test
    void everyTruncatedPrefixFailsWithBoundedDecodeDiagnostic() {
        DataSchema meta = new DataSchema(
                new Field("i", DataType.IntegerType),
                new Field("l", DataType.LongType),
                new Field("s", DataType.StringType),
                new Field("big", DataType.BigDecimalType),
                new Field("dynamic", DataType.NullType),
                new Field("flag", DataType.BooleanType));
        RowSerializer serializer = new RowSerializer(meta);
        Timestamp dynamic = new Timestamp(1_780_000_000_123L);
        dynamic.setNanos(987_654_321);
        DataRow row = new DataRow(6);
        row.set(0, 17);
        row.set(1, Long.MIN_VALUE + 9);
        row.set(2, "unicode😀row");
        row.set(3, new BigDecimal("123456789012345678901234567890.001"));
        row.set(4, dynamic);
        row.set(5, true);

        byte[] complete = serializer.serialize(row);
        for (int cut = 0; cut < complete.length; cut++) {
            int prefixLength = cut;
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> serializer.deserialize(Arrays.copyOf(complete, prefixLength)), "cut=" + cut);
            assertTrue(failure.getMessage().contains("truncated serialized row payload"),
                    "cut=" + cut + " message=" + failure.getMessage());
            assertTrue(failure.getMessage().contains("required="), failure.getMessage());
            assertTrue(failure.getMessage().contains("remaining="), failure.getMessage());
        }

        assertArrayEquals(row.values(), serializer.deserialize(complete).values());
    }

    /** An inner length must be checked against the bytes remaining first, so a negative or out-of-range length never reaches an allocation or a constructor. */
    @Test
    void variableLengthFieldsRejectInvalidInnerLengthsBeforeUse() {
        RowSerializer strings = new RowSerializer(
                new DataSchema(new Field("s", DataType.StringType)));
        assertTruncatedPayload(() -> strings.deserialize(new byte[]{0, -1, -1, -1, -1}));
        assertTruncatedPayload(() -> strings.deserialize(new byte[]{0, 0, 0, 4, 0, 'x'}));

        RowSerializer decimals = new RowSerializer(
                new DataSchema(new Field("d", DataType.BigDecimalType)));
        // bitmap plus scale (4) plus the BIGINT tag (1) plus the inner length (4), but no declared body.
        assertTruncatedPayload(() -> decimals.deserialize(
                new byte[]{0, 0, 0, 0, 0, 1, -1, -1, -1, -1}));
        assertTruncatedPayload(() -> decimals.deserialize(
                new byte[]{0, 0, 0, 0, 0, 1, 0, 0, 4, 0}));
    }

    /** Any trailing bytes after a complete row mean a broken schema or structure and must not be swallowed silently. */
    @Test
    void trailingBytesAfterCompleteRowAreRejected() {
        RowSerializer serializer = new RowSerializer(
                new DataSchema(new Field("v", DataType.IntegerType)));
        DataRow row = new DataRow(1);
        row.set(0, 42);
        byte[] valid = serializer.serialize(row);

        for (int extra : new int[]{1, 7}) {
            byte[] corrupted = Arrays.copyOf(valid, valid.length + extra);
            Arrays.fill(corrupted, valid.length, corrupted.length, (byte) 0x5A);
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> serializer.deserialize(corrupted));
            assertTrue(failure.getMessage().contains("trailing serialized row bytes"), failure.getMessage());
            assertTrue(failure.getMessage().contains("trailing=" + extra), failure.getMessage());
        }
    }

    /** The non-projected encoding must reject null values, short rows and wide rows before any field is read or written. */
    @Test
    void nonProjectedSerializationRejectsRowsOutsideDeclaredWidth() {
        DataSchema meta = new DataSchema(
                new Field("id", DataType.IntegerType),
                new Field("name", DataType.StringType));
        RowSerializer serializer = new RowSerializer(meta);

        DataRow wide = new DataRow(3);
        wide.set(0, 7);
        wide.set(1, "wide");
        wide.set(2, "must-not-be-truncated");
        DataRow shortRow = new DataRow(1);
        shortRow.set(0, 8);

        HeapPool pool = new HeapPool();
        MemorySegment segment = pool.allocate(256);
        try {
            assertWidthFailure(() -> serializer.serialize(wide), 2, "3");
            assertWidthFailure(() -> serializer.serializedSize(wide), 2, "3");
            assertWidthFailure(() -> serializer.serializeInto(wide, segment, 0), 2, "3");
            assertWidthFailure(() -> serializer.serializerScratchCredit(wide), 2, "3");

            assertWidthFailure(() -> serializer.serialize(shortRow), 2, "1");
            assertWidthFailure(() -> serializer.serializedSize(shortRow), 2, "1");
            assertWidthFailure(() -> serializer.serializeInto(shortRow, segment, 0), 2, "1");
            assertWidthFailure(() -> serializer.serializerScratchCredit(shortRow), 2, "1");

            assertWidthFailure(() -> serializer.serialize(null), 2, "null");
            assertWidthFailure(() -> serializer.serializedSize(null), 2, "null");
            assertWidthFailure(() -> serializer.serializeInto(null, segment, 0), 2, "null");
            assertWidthFailure(() -> serializer.serializerScratchCredit(null), 2, "null");
        } finally {
            pool.free(segment);
        }
    }

    /** Cluster serialization for Boolean, Date and Timestamp, whose cases the encode and decode switches once omitted and threw by default. */
    @Test
    void booleanDateTimestampRoundTrip() {
        DataSchema meta = new DataSchema(
                new Field("flag", DataType.BooleanType),
                new Field("d", DataType.DateType),
                new Field("ts", DataType.TimestampType));
        RowSerializer ser = new RowSerializer(meta);

        Date date = new Date(1_600_000_000_000L);
        Timestamp ts = new Timestamp(1_600_000_000_123L);
        ts.setNanos(456_789_000);
        DataRow row = new DataRow(3);
        row.set(0, Boolean.TRUE);
        row.set(1, date);
        row.set(2, ts);

        DataRow back = ser.deserialize(ser.serialize(row));
        assertEquals(Boolean.TRUE, back.get(0));
        assertEquals(date, back.get(1));
        assertEquals(ts, back.get(2));
        assertEquals(456_789_000, ((Timestamp) back.get(2)).getNanos(), "the nanoseconds survive");
        assertArrayEquals(ser.serialize(row), ser.serialize(row), "the encoding is deterministic");
    }

    @Test
    void typedRoundTripPreservesValues() {
        DataSchema meta = new DataSchema(
                new Field("s", DataType.StringType),
                new Field("i", DataType.IntegerType),
                new Field("l", DataType.LongType),
                new Field("d", DataType.BigDecimalType));
        RowSerializer ser = new RowSerializer(meta);

        DataRow row = new DataRow(4);
        row.set(0, "héllo");
        row.set(1, 42);
        row.set(2, 9_000_000_000L);
        row.set(3, new BigDecimal("1.5000000000")); // scale must survive

        DataRow back = ser.deserialize(ser.serialize(row));
        assertEquals("héllo", back.get(0));
        assertEquals(42, back.get(1));
        assertEquals(9_000_000_000L, back.get(2));
        assertEquals(new BigDecimal("1.5000000000"), back.get(3));
        // scale preserved bit-for-bit (decimal)
        assertEquals(10, ((BigDecimal) back.get(3)).scale());
    }

    @Test
    void nullsRoundTripViaBitmap() {
        DataSchema meta = new DataSchema(
                new Field("a", DataType.StringType),
                new Field("b", DataType.IntegerType),
                new Field("c", DataType.BigDecimalType));
        RowSerializer ser = new RowSerializer(meta);

        DataRow row = new DataRow(3);
        row.set(0, null);
        row.set(1, 7);
        row.set(2, null);

        DataRow back = ser.deserialize(ser.serialize(row));
        assertNull(back.get(0));
        assertEquals(7, back.get(1));
        assertNull(back.get(2));
    }

    @Test
    void deterministicSameBytesForSameRow() {
        DataSchema meta = new DataSchema(new Field("d", DataType.BigDecimalType));
        RowSerializer ser = new RowSerializer(meta);
        DataRow row = new DataRow(1);
        row.set(0, new BigDecimal("3.1400000000"));
        assertArrayEquals(ser.serialize(row), ser.serialize(row));
    }

    /** Safety net: BigDecimal values across scales, precisions, signs and limits, where serialize then deserialize returns the same value and scale. */
    @Test
    void bigDecimalBoundaryValuesRoundTripExactly() {
        DataSchema meta = new DataSchema(new Field("d", DataType.BigDecimalType));
        RowSerializer ser = new RowSerializer(meta);
        String[] cases = {
                "0", "0.0000000000", "1", "-1", "123456789", "-123456789",
                "1.5000000000", "-1.5000000000", "0.0000000001", "-0.0000000001",
                "9223372036854775807",           // long max(precision 19)
                "-9223372036854775808",          // long min
                "9999999999999999999999999999",  // beyond long range, so the BigInteger fallback must engage
                "-9999999999999999999999999999",
                "123.456", "0.001", "1000000.0000000000",
                "12345678901234567.89",          // precision 19, scale 2
        };
        for (String c : cases) {
            BigDecimal v = new BigDecimal(c);
            DataRow row = new DataRow(1);
            row.set(0, v);
            DataRow back = ser.deserialize(ser.serialize(row));
            BigDecimal r = (BigDecimal) back.get(0);
            assertEquals(0, v.compareTo(r), "the values are equal: " + c);
            assertEquals(v.scale(), r.scale(), "the scale survives: " + c);
            assertEquals(v.unscaledValue(), r.unscaledValue(), "the unscaled value survives: " + c);
        }
    }

    /** Safety net: serialize and deserialize inside one process, several mixed field types, and nulls interleaved. */
    @Test
    void mixedTypesWithNullsRoundTrip() {
        DataSchema meta = new DataSchema(
                new Field("i", DataType.IntegerType),
                new Field("s", DataType.StringType),
                new Field("d", DataType.BigDecimalType),
                new Field("l", DataType.LongType),
                new Field("s2", DataType.StringType));
        RowSerializer ser = new RowSerializer(meta);
        DataRow row = new DataRow(5);
        row.set(0, Integer.MIN_VALUE);
        row.set(1, "");  // an empty string, which is not null
        row.set(2, null);
        row.set(3, Long.MAX_VALUE);
        row.set(4, "\u591A\u5B57\u8282\uD83D\uDE00\u6C49\u5B57");
        DataRow back = ser.deserialize(ser.serialize(row));
        assertEquals(Integer.MIN_VALUE, back.get(0));
        assertEquals("", back.get(1));
        assertNull(back.get(2));
        assertEquals(Long.MAX_VALUE, back.get(3));
        assertEquals("\u591A\u5B57\u8282\uD83D\uDE00\u6C49\u5B57", back.get(4));
    }

    /** The self-describing encoding when a NullType computed column carries a Date or Timestamp at run time, which encodeUntyped once failed to tag and threw. */
    @Test
    void untypedNullTypeColumnDateTimestamp() {
        DataSchema meta = new DataSchema(new Field("x", DataType.NullType));
        RowSerializer ser = new RowSerializer(meta);

        Date date = new Date(1_650_000_000_000L);
        DataRow dr = new DataRow(1);
        dr.set(0, date);
        assertEquals(date, ser.deserialize(ser.serialize(dr)).get(0));
        assertTrue(ser.deserialize(ser.serialize(dr)).get(0) instanceof Date, "decodes back as a Date");

        Timestamp ts = new Timestamp(1_650_000_000_321L);
        ts.setNanos(987_654_000);
        DataRow tr = new DataRow(1);
        tr.set(0, ts);
        Object back = ser.deserialize(ser.serialize(tr)).get(0);
        assertEquals(ts, back);
        assertTrue(back instanceof Timestamp, "decodes back as a Timestamp and is not mistaken for a Date");
        assertEquals(987_654_000, ((Timestamp) back).getNanos(), "the nanoseconds survive");
    }

    @Test
    void untypedNullTypeColumnSelfDescribesRuntimeType() {
        // NullType column (e.g. computed select expr) carries runtime type tag
        DataSchema meta = new DataSchema(new Field("x", DataType.NullType));
        RowSerializer ser = new RowSerializer(meta);

        DataRow bd = new DataRow(1);
        bd.set(0, new BigDecimal("2.7100000000"));
        assertEquals(new BigDecimal("2.7100000000"), ser.deserialize(ser.serialize(bd)).get(0));

        DataRow str = new DataRow(1);
        str.set(0, "hi");
        assertEquals("hi", ser.deserialize(ser.serialize(str)).get(0));

        DataRow lng = new DataRow(1);
        lng.set(0, 123L);
        assertEquals(123L, ser.deserialize(ser.serialize(lng)).get(0));
    }

    @Test
    void serializedSizeExactlyMatchesAllocationForVariableWidthValues() {
        DataSchema meta = new DataSchema(
                new Field("s", DataType.StringType),
                new Field("d", DataType.BigDecimalType),
                new Field("u", DataType.NullType));
        RowSerializer serializer = new RowSerializer(meta);
        Object[][] values = {
                {"ascii", new BigDecimal("1.25"), "\u52A8\u6001\uD83D\uDE00"},
                {"\u591A\u5B57\u8282\uD83D\uDE00\u6C49\u5B57", new BigDecimal("123456789012345678901234567890.123"), Long.MIN_VALUE},
                {"\uD800broken", new BigDecimal("-128"), new Timestamp(123456789L)},
                {null, null, null}
        };
        for (Object[] value : values) {
            DataRow row = new DataRow(3);
            row.set(0, value[0]);
            row.set(1, value[1]);
            row.set(2, value[2]);
            assertEquals(serializer.serializedSize(row), serializer.serialize(row).length);
        }
    }

    /** A direct projection must produce byte-for-byte the same output as building the key DataRow first. */
    @Test
    void projectedSerializationIsByteExactForAllTypesAndNulls() {
        DataRow source = new DataRow(10);
        source.set(0, "ignored");
        source.set(1, 42);
        source.set(2, 9_000_000_000L);
        source.set(3, new BigDecimal("1.2300000000"));
        source.set(4, Boolean.TRUE);
        source.set(5, new Date(1_650_000_000_000L));
        Timestamp ts = new Timestamp(1_650_000_000_321L);
        ts.setNanos(987_654_321);
        source.set(6, ts);
        source.set(7, "\u591A\u5B57\u8282\uD83D\uDE00\uD800");
        source.set(8, new BigDecimal("-9999999999999999999999999999.001"));
        source.set(9, null);

        int[] projection = {9, 8, 7, 6, 5, 4, 3, 2, 1};
        DataSchema keyMeta = new DataSchema(
                new Field("nil", DataType.NullType),
                new Field("big", DataType.BigDecimalType),
                new Field("s", DataType.StringType),
                new Field("ts", DataType.TimestampType),
                new Field("date", DataType.DateType),
                new Field("bool", DataType.BooleanType),
                new Field("compact", DataType.BigDecimalType),
                new Field("long", DataType.LongType),
                new Field("int", DataType.IntegerType));
        RowSerializer serializer = new RowSerializer(keyMeta);
        DataRow materialized = project(source, projection);

        byte[] expected = serializer.serialize(materialized);
        byte[] actual = serializer.serializeProjected(source, projection);
        assertArrayEquals(expected, actual);
        assertEquals(expected.length, serializer.serializedProjectedSize(source, projection));
        assertArrayEquals(serializer.deserialize(expected).values(), serializer.deserialize(actual).values());
    }

    /** Reordering, repeats and zero keys are all legal, and a mapping error must fail loudly at a clear boundary. */
    @Test
    void projectedSerializationSupportsShapeEdgesAndRejectsInvalidMappings() {
        DataRow source = new DataRow(3);
        source.set(0, "a");
        source.set(1, 7);
        source.set(2, "z");

        DataSchema reorderedMeta = new DataSchema(
                new Field("z", DataType.StringType),
                new Field("a1", DataType.StringType),
                new Field("a2", DataType.StringType));
        RowSerializer reordered = new RowSerializer(reorderedMeta);
        int[] repeatedProjection = {2, 0, 0};
        assertArrayEquals(reordered.serialize(project(source, repeatedProjection)),
                reordered.serializeProjected(source, repeatedProjection));

        RowSerializer zero = new RowSerializer(new DataSchema());
        assertArrayEquals(new byte[0], zero.serializeProjected(source, new int[0]));
        assertEquals(0, zero.serializedProjectedSize(source, new int[0]));

        assertThrows(IllegalArgumentException.class,
                () -> reordered.serializeProjected(source, null));
        assertThrows(IllegalArgumentException.class,
                () -> reordered.serializeProjected(source, new int[]{0, 1}));
        assertThrows(IndexOutOfBoundsException.class,
                () -> reordered.serializeProjected(source, new int[]{0, 1, 3}));
    }

    private static DataRow project(DataRow source, int[] projection) {
        DataRow projected = new DataRow(projection.length);
        for (int i = 0; i < projection.length; i++) {
            projected.set(i, source.get(projection[i]));
        }
        return projected;
    }

    private static void assertWidthFailure(org.junit.jupiter.api.function.Executable action,
                                           int expected, String actual) {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, action);
        assertTrue(failure.getMessage().contains("row width mismatch"), failure.getMessage());
        assertTrue(failure.getMessage().contains("expected=" + expected), failure.getMessage());
        assertTrue(failure.getMessage().contains("actual=" + actual), failure.getMessage());
    }

    private static void assertTruncatedPayload(org.junit.jupiter.api.function.Executable action) {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, action);
        assertTrue(failure.getMessage().contains("truncated serialized row payload"), failure.getMessage());
        assertTrue(failure.getMessage().contains("required="), failure.getMessage());
        assertTrue(failure.getMessage().contains("remaining="), failure.getMessage());
    }
}
