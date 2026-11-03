package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.seg.MemorySegment;
import org.junit.jupiter.api.Test;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.vm.VM;

import java.lang.reflect.Array;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The minimal, independently verifiable primitives that external sorting's exact memory accounting relies on. */
class SortMemoryPrimitivesTest {

    static {
        // CI and macOS forbid ptrace and self-attach; the JOL Unsafe layout fallback is enough to calibrate the shallow size.
        System.setProperty("jol.skipInstallAttach", "true");
        System.setProperty("jol.skipDynamicAttach", "true");
        System.setProperty("jol.skipHotspotSAAttach", "true");
    }

    @Test
    void directSegmentEncodingIsBitIdenticalForWideTypedAndUntypedDecimals() {
        DataSchema meta = new DataSchema(
                new Field("typed", DataType.BigDecimalType),
                new Field("untyped", DataType.NullType),
                new Field("text", DataType.StringType));
        RowSerializer serializer = new RowSerializer(meta);
        DataRow row = new DataRow(3);
        row.set(0, new BigDecimal("1234567890123456789012345678901234567890.1234567890"));
        row.set(1, new BigDecimal("-9999999999999999999999999999999999999999.0000000001"));
        row.set(2, "\u591A\u5B57\u8282\uD83D\uDE00direct-segment");

        byte[] expected = serializer.serialize(row);
        HeapPool pool = new HeapPool();
        MemorySegment segment = pool.allocate(expected.length + 9);
        serializer.serializeInto(row, segment, 5);
        byte[] actual = new byte[expected.length];
        segment.copyTo(5, actual, 0, actual.length);

        assertArrayEquals(expected, actual);
        assertTrue(serializer.serializerScratchCredit(row) >= 32,
                "high-precision BigInteger temporary array must be credited");
        pool.free(segment);
        assertEquals(0, pool.outstanding());
    }

    @Test
    void rowEstimatorAccountsEverySupportedObjectFamilyConservatively() {
        DataRow boxed = new DataRow(4);
        boxed.set(0, 1);
        boxed.set(1, 2L);
        boxed.set(2, Boolean.TRUE);
        boxed.set(3, "abc");

        DataRow temporal = new DataRow(2);
        temporal.set(0, new Date(123L));
        Timestamp timestamp = new Timestamp(456L);
        timestamp.setNanos(789);
        temporal.set(1, timestamp);

        DataRow decimal = new DataRow(1);
        decimal.set(0, new BigDecimal("12345678901234567890123456789012345678901234567890"));

        assertTrue(RowSizeEstimator.estimateRow(boxed) >= 160,
                "DataRow/Object[]/boxed/String must include maximum-layout headers and backing storage");
        assertTrue(RowSizeEstimator.estimateRow(temporal) >= 128,
                "Date and Timestamp must not use the old generic 16-byte fallback");
        assertTrue(RowSizeEstimator.estimateRow(decimal) >= 192,
                "BigDecimal must include BigInteger and magnitude array");
    }

    @Test
    void managedLayoutAndRowEstimatorDominateJolMeasurements() {
        assertTrue(ManagedMemoryLayout.arrayBytes(17)
                        >= ClassLayout.parseInstance(new byte[17]).instanceSize(),
                "maximum array header/alignment matrix must dominate the active VM");

        DataRow boxedAndText = new DataRow(5);
        boxedAndText.set(0, Integer.valueOf(1000));
        boxedAndText.set(1, Long.valueOf(1000));
        boxedAndText.set(2, Double.valueOf(1.25));
        boxedAndText.set(3, Boolean.TRUE);
        boxedAndText.set(4, new String("JOL-\u591A\u5B57\u8282-calibration"));

        DataRow temporal = new DataRow(2);
        temporal.set(0, new Date(123456789L));
        Timestamp timestamp = new Timestamp(987654321L);
        timestamp.setNanos(123456789);
        temporal.set(1, timestamp);

        DataRow decimalAndNull = new DataRow(2);
        decimalAndNull.set(0, new BigDecimal(
                "12345678901234567890123456789012345678901234567890.1234567890"));
        decimalAndNull.set(1, null);

        assertEstimateDominatesJol(boxedAndText);
        assertEstimateDominatesJol(temporal);
        assertEstimateDominatesJol(decimalAndNull);
    }

    @Test
    void directEncoderUsesPrimitiveAndBulkSegmentWrites() {
        DataSchema meta = new DataSchema(
                new Field("longValue", DataType.LongType),
                new Field("intValue", DataType.IntegerType),
                new Field("decimal", DataType.BigDecimalType),
                new Field("timestamp", DataType.TimestampType));
        DataRow row = new DataRow(4);
        row.set(0, 123456789012345L);
        row.set(1, 42);
        row.set(2, new BigDecimal("1234567890123456789012345678901234567890.1234567890"));
        Timestamp timestamp = new Timestamp(123456789L);
        timestamp.setNanos(987654321);
        row.set(3, timestamp);
        RowSerializer serializer = new RowSerializer(meta);
        byte[] expected = serializer.serialize(row);
        CountingSegment segment = new CountingSegment(expected.length);

        serializer.serializeInto(row, segment, 0);

        assertArrayEquals(expected, segment.getBytes(0, expected.length));
        assertTrue(segment.intWrites >= 4, "scale/length/int/timestamp nanos use primitive writes");
        assertTrue(segment.longWrites >= 2, "long/timestamp millis use primitive writes");
        assertTrue(segment.bulkWrites >= 1, "BigInteger magnitude uses one bulk copy");
    }

    private static void assertEstimateDominatesJol(DataRow row) {
        long actual = jolDeepSize(row);
        long estimated = RowSizeEstimator.estimateRow(row);
        assertTrue(estimated >= actual,
                "managed estimate " + estimated + " must dominate JOL retained size " + actual);
    }

    /** GraphLayout needs an agent or address probing, so only the real shallow size from the JOL VM is used here, with references walked by hand. */
    private static long jolDeepSize(Object root) {
        org.openjdk.jol.vm.VirtualMachine vm = VM.current();
        Set<Object> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Deque<Object> pending = new ArrayDeque<>();
        pending.add(root);
        long bytes = 0;
        while (!pending.isEmpty()) {
            Object value = pending.removeFirst();
            if (value == null || !visited.add(value)) continue;
            bytes = Math.addExact(bytes, vm.sizeOf(value));
            Class<?> type = value.getClass();
            if (type.isArray()) {
                if (!type.getComponentType().isPrimitive()) {
                    for (int i = 0; i < Array.getLength(value); i++) {
                        Object child = Array.get(value, i);
                        if (child != null) pending.addLast(child);
                    }
                }
                continue;
            }
            for (Class<?> cursor = type; cursor != null; cursor = cursor.getSuperclass()) {
                for (java.lang.reflect.Field field : cursor.getDeclaredFields()) {
                    if (Modifier.isStatic(field.getModifiers()) || field.getType().isPrimitive()) continue;
                    Object child = vm.getObject(value, vm.fieldOffset(field));
                    if (child != null) pending.addLast(child);
                }
            }
        }
        return bytes;
    }

    private static final class CountingSegment implements MemorySegment {
        private final byte[] data;
        int intWrites;
        int longWrites;
        int bulkWrites;

        private CountingSegment(int size) { this.data = new byte[size]; }
        private void range(int offset, int length) {
            if (offset < 0 || length < 0 || offset + length > data.length) {
                throw new IndexOutOfBoundsException();
            }
        }
        @Override public int size() { return data.length; }
        @Override public void putByte(int offset, byte value) { range(offset, 1); data[offset] = value; }
        @Override public byte getByte(int offset) { range(offset, 1); return data[offset]; }
        @Override public void putInt(int offset, int value) {
            intWrites++;
            range(offset, 4);
            data[offset] = (byte) (value >>> 24);
            data[offset + 1] = (byte) (value >>> 16);
            data[offset + 2] = (byte) (value >>> 8);
            data[offset + 3] = (byte) value;
        }
        @Override public int getInt(int offset) { throw new UnsupportedOperationException(); }
        @Override public void putLong(int offset, long value) {
            longWrites++;
            range(offset, 8);
            for (int i = 0; i < 8; i++) data[offset + i] = (byte) (value >>> (56 - i * 8));
        }
        @Override public long getLong(int offset) { throw new UnsupportedOperationException(); }
        @Override public void putBytes(int offset, byte[] src) {
            bulkWrites++;
            range(offset, src.length);
            System.arraycopy(src, 0, data, offset, src.length);
        }
        @Override public byte[] getBytes(int offset, int length) {
            range(offset, length);
            return java.util.Arrays.copyOfRange(data, offset, offset + length);
        }
        @Override public void copyTo(int offset, byte[] target, int targetOffset, int length) {
            range(offset, length);
            System.arraycopy(data, offset, target, targetOffset, length);
        }
        @Override public void clear() { java.util.Arrays.fill(data, (byte) 0); }
    }
}
