package io.rowweave.ops;

import io.rowweave.DataSchema;
import io.rowweave.DataRow;

import java.util.Arrays;

/**
 * The unified key-hash partitioner: given a DataRow plus the join or grouping key indexes, it returns a partition number in [0, numPartitions).
 *
 * <p>The same key always lands in the same partition, which is the basis of keyed redistribution for parallel groupBy/join/coGroup. A null key is treated as a
 * stable value (0) by {@link Arrays#hashCode(Object[])} and therefore lands independently but consistently in one partition, which matches null participating in grouping and is orthogonal to the
 * null-never-matches-null matching semantics of join - matching is decided inside the operator, and partitioning only brings the same key together.
 *
 * <p>The hash uses Arrays.hashCode (the hashCode of String/Long/Integer/BigDecimal is deterministic across JVMs) normalized by floorMod, so the
 * partitioning of the same input is deterministic.
 */
public final class Partitioner {

    private final int[] keyIndices;
    private final int numPartitions;
    private final boolean numericNormalized;
    private final int fixedPartition;

    private Partitioner(int[] keyIndices, int numPartitions, boolean numericNormalized) {
        this(keyIndices, numPartitions, numericNormalized, -1);
    }

    private Partitioner(int[] keyIndices, int numPartitions,
                        boolean numericNormalized, int fixedPartition) {
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("numPartitions must be > 0");
        }
        if (fixedPartition < -1 || fixedPartition >= numPartitions) {
            throw new IllegalArgumentException(
                    "fixed partition is outside the partition range");
        }
        this.keyIndices = keyIndices;
        this.numPartitions = numPartitions;
        this.numericNormalized = numericNormalized;
        this.fixedPartition = fixedPartition;
    }

    public static Partitioner of(int[] keyIndices, int numPartitions) {
        return new Partitioner(keyIndices.clone(), numPartitions, false);
    }

    public static Partitioner ofFields(DataSchema dataSchema, int numPartitions, String... keyFields) {
        return new Partitioner(resolve(dataSchema, keyFields), numPartitions, false);
    }

    /**
     * <b>The join shuffle form only</b>: a <b>numerically normalized</b> partitioner. Numeric keys (Integer/Long/BigDecimal) are uniformly
     * converted with toBigDecimal and stripped of trailing zeros before hashing, so keys that differ in scale or type but are <b>equal in value</b> (`2.0`/`2.00`, Long 2 / BigDecimal 2)
     * land in one partition. This matches the scale-insensitive, cross-type matching of SortMergeJoin (toBigDecimal().compareTo()).
 *
     * <p><b>Used only by join</b>: groupBy and distinct are correctly scale-sensitive (`5` and `5.0` form separate groups) and must not use this mode.
     */
    public static Partitioner numericNormalizedOfFields(DataSchema dataSchema, int numPartitions, String... keyFields) {
        return new Partitioner(resolve(dataSchema, keyFields), numPartitions, true);
    }

    /** Routes every row from one physical producer task to its matching consumer task. */
    public static Partitioner fixed(int partition, int numPartitions) {
        return new Partitioner(new int[0], numPartitions, false, partition);
    }

    private static int[] resolve(DataSchema dataSchema, String... keyFields) {
        int[] idx = new int[keyFields.length];
        for (int i = 0; i < keyFields.length; i++) {
            int found = dataSchema.getFieldIndex(keyFields[i]);
            if (found < 0) {
                throw new IllegalArgumentException("Unknown key field: " + keyFields[i]);
            }
            idx[i] = found;
        }
        return idx;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public int partitionOf(DataRow row) {
        return fixedPartition >= 0
                ? fixedPartition
                : Math.floorMod(keyHash(row), numPartitions);
    }

    /** A deterministic hash of the key value array (null counts as 0). In numericNormalized mode a numeric key is normalized by value, independent of scale or type. */
    public int keyHash(DataRow row) {
        Object[] key = new Object[keyIndices.length];
        for (int i = 0; i < keyIndices.length; i++) {
            Object v = row.get(keyIndices[i]);
            key[i] = numericNormalized ? canonicalize(v) : v;
        }
        return Arrays.hashCode(key);
    }

    /** Numeric key normalization: Integer/Long/BigDecimal become one BigDecimal with trailing zeros stripped (equal values hash equal). */
    private static Object canonicalize(Object v) {
        if (v instanceof java.math.BigDecimal) {
            return normalizeDecimal((java.math.BigDecimal) v);
        }
        if (v instanceof Integer || v instanceof Long) {
            return normalizeDecimal(java.math.BigDecimal.valueOf(((Number) v).longValue()));
        }
        return v; // non-numeric (String and the like) stays as it is
    }

    private static java.math.BigDecimal normalizeDecimal(java.math.BigDecimal d) {
        java.math.BigDecimal s = d.stripTrailingZeros();
        // stripTrailingZeros on zero may yield a 0E-N scale; normalize it to BigDecimal.ZERO for consistency
        return s.signum() == 0 ? java.math.BigDecimal.ZERO : s;
    }
}
