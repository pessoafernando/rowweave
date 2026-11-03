package io.rowweave.mem;

import io.rowweave.DataRow;

import java.math.BigDecimal;

/**
 * Estimates the on-heap footprint of a DataRow, used by the in-memory residency fast path to decide whether
 * the budget would be exceeded.
 *
 * <p><b>Overestimate rather than underestimate</b>, which is what keeps the job out of an OOM: an
 * underestimate would let the fast path hold more DataRows than the budget allows and hit an OOM, while an
 * overestimate only triggers the fallback to the secondary spill path a little earlier, which is slower but
 * safe. Every coefficient rounds conservatively upward. The estimate is incremental, O(1) per row, with no
 * pass over the whole set.
 */
public final class RowSizeEstimator {

    private RowSizeEstimator() {
    }

    /** Upper-bound estimate, in bytes, of the heap footprint of one DataRow. */
    public static long estimateRow(DataRow row) {
        ManagedMemoryLayout.requireSupported();
        int n = row.size();
        long bytes = ManagedMemoryLayout.align(ManagedMemoryLayout.MAX_OBJECT_HEADER
                + ManagedMemoryLayout.MAX_REFERENCE);
        bytes = Math.addExact(bytes, ManagedMemoryLayout.arrayBytes(
                Math.multiplyExact(ManagedMemoryLayout.MAX_REFERENCE, n)));
        for (int i = 0; i < n; i++) {
            bytes = Math.addExact(bytes, estimateField(row.get(i)));
        }
        return bytes;
    }

    /** Estimates the heap footprint of a byte[] of the given payload length, for scratch reserved across serialize and spill conversions. */
    public static long estimateByteArray(int payloadBytes) {
        ManagedMemoryLayout.requireSupported();
        if (payloadBytes < 0) {
            throw new IllegalArgumentException("negative byte[] payload: " + payloadBytes);
        }
        return ManagedMemoryLayout.arrayBytes(payloadBytes);
    }

    /**
     * Conservative bound the byte-native entry point can compute before deserialize: each payload byte in the encoding
     * is charged as a BigDecimal of three decimal digits at 8 bytes per digit, with the object header of every field on top.
     */
    public static long estimateEncodedUpperBound(int fieldCount, int encodedLength) {
        ManagedMemoryLayout.requireSupported();
        if (fieldCount < 0 || encodedLength < 0) {
            throw new IllegalArgumentException("negative encoded row dimensions");
        }
        long base = ManagedMemoryLayout.align(ManagedMemoryLayout.MAX_OBJECT_HEADER
                + ManagedMemoryLayout.MAX_REFERENCE);
        base = Math.addExact(base, ManagedMemoryLayout.arrayBytes(
                Math.multiplyExact(ManagedMemoryLayout.MAX_REFERENCE, fieldCount)));
        long payloadBound = Math.multiplyExact(32L, encodedLength);
        long fieldBound = Math.multiplyExact(128L, fieldCount);
        return Math.addExact(1024L, Math.addExact(base, Math.addExact(payloadBound, fieldBound)));
    }

    /** Conservative heap credit for a managed primitive or reference array. */
    public static long estimateReferenceArray(int elements) {
        return estimateArray(elements, (int) ManagedMemoryLayout.MAX_REFERENCE, "reference");
    }

    public static long estimateIntArray(int elements) {
        return estimateArray(elements, Integer.BYTES, "int");
    }

    public static long estimateBooleanArray(int elements) {
        return estimateArray(elements, 1, "boolean");
    }

    /** HeapSegment wrapper plus its backing byte[]; an off-heap implementation charging the same bound is only ever more conservative. */
    public static long estimateMemorySegment(int payloadBytes) {
        ManagedMemoryLayout.requireSupported();
        return Math.addExact(
                ManagedMemoryLayout.align(ManagedMemoryLayout.MAX_OBJECT_HEADER + 32L),
                estimateByteArray(payloadBytes));
    }

    /** A new DataRow plus its Object[] container; the field objects themselves stay owned by the input or decoded row. */
    public static long estimateRowContainer(int fieldCount) {
        ManagedMemoryLayout.requireSupported();
        if (fieldCount < 0) {
            throw new IllegalArgumentException("negative row field count: " + fieldCount);
        }
        return Math.addExact(
                ManagedMemoryLayout.align(ManagedMemoryLayout.MAX_OBJECT_HEADER
                        + ManagedMemoryLayout.MAX_REFERENCE),
                ManagedMemoryLayout.arrayBytes(
                        Math.multiplyExact(ManagedMemoryLayout.MAX_REFERENCE, fieldCount)));
    }

    private static long estimateArray(int elements, int elementBytes, String kind) {
        ManagedMemoryLayout.requireSupported();
        if (elements < 0) {
            throw new IllegalArgumentException("negative " + kind + " array length: " + elements);
        }
        return ManagedMemoryLayout.arrayBytes(Math.multiplyExact((long) elementBytes, elements));
    }

    private static long estimateField(Object v) {
        if (v == null) {
            return 0;
        }
        if (v instanceof String) {
            long stringObject = ManagedMemoryLayout.align(ManagedMemoryLayout.MAX_OBJECT_HEADER + 24);
            long backing = ManagedMemoryLayout.arrayBytes(Math.multiplyExact(2L, ((String) v).length()));
            return Math.addExact(stringObject, backing);
        }
        if (v instanceof BigDecimal) {
            BigDecimal decimal = (BigDecimal) v;
            long decimalObject = ManagedMemoryLayout.align(ManagedMemoryLayout.MAX_OBJECT_HEADER + 48);
            long bigIntegerObject = ManagedMemoryLayout.align(ManagedMemoryLayout.MAX_OBJECT_HEADER + 32);
            long bits = Math.addExact(Math.multiplyExact((long) decimal.precision(), 4L), 1L);
            long magnitudeInts = Math.max(1L, Math.addExact(bits, 31L) / 32L);
            long magnitude = ManagedMemoryLayout.arrayBytes(Math.multiplyExact(4L, magnitudeInts));
            long layoutBound = Math.addExact(decimalObject, Math.addExact(bigIntegerObject, magnitude));
            long decimalPrecisionBound = Math.addExact(48L,
                    Math.multiplyExact(8L, (long) decimal.precision()));
            return Math.max(layoutBound, decimalPrecisionBound);
        }
        if (v instanceof java.sql.Timestamp) {
            return ManagedMemoryLayout.align(ManagedMemoryLayout.MAX_OBJECT_HEADER + 32);
        }
        if (v instanceof java.util.Date) {
            return ManagedMemoryLayout.align(ManagedMemoryLayout.MAX_OBJECT_HEADER + 16);
        }
        if (v instanceof Integer || v instanceof Long || v instanceof Double || v instanceof Boolean) {
            return ManagedMemoryLayout.align(ManagedMemoryLayout.MAX_OBJECT_HEADER + 16);
        }
        throw new IllegalArgumentException("Unsupported runtime type for managed row estimate: "
                + v.getClass().getName());
    }
}
