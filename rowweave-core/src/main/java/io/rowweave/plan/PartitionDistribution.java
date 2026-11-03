package io.rowweave.plan;

import java.util.Arrays;

/** Immutable row distribution for one logical keyed stage. */
public final class PartitionDistribution {
    private final int logicalStage;
    private final PartitionRole role;
    private final int parallelism;
    private final TelemetryCoverage coverage;
    private final boolean[] present;
    private final long[] rowCounts;

    public PartitionDistribution(int logicalStage, PartitionRole role, int parallelism,
                                 TelemetryCoverage coverage, boolean[] present, long[] rowCounts) {
        this(logicalStage, role, parallelism, coverage, present, rowCounts, false);
    }

    private PartitionDistribution(int logicalStage, PartitionRole role, int parallelism,
                                  TelemetryCoverage coverage, boolean[] present, long[] rowCounts,
                                  boolean transferArrays) {
        if (logicalStage < 0 || role == null || parallelism < 1 || coverage == null) {
            throw new IllegalArgumentException("invalid partition distribution identity");
        }
        if (present == null || rowCounts == null
                || present.length != parallelism || rowCounts.length != parallelism) {
            throw new IllegalArgumentException("partition arrays must match parallelism");
        }
        if (coverage != TelemetryCoverage.COMPLETE
                && coverage != TelemetryCoverage.PARTIAL
                && coverage != TelemetryCoverage.UNAVAILABLE) {
            throw new IllegalArgumentException("invalid partition coverage");
        }
        boolean anyPresent = false;
        boolean allPresent = true;
        for (int i = 0; i < rowCounts.length; i++) {
            long count = rowCounts[i];
            if (count < 0 || (!present[i] && count != 0L)) {
                throw new IllegalArgumentException("absent partition rows must be zero");
            }
            anyPresent |= present[i];
            allPresent &= present[i];
        }
        if (coverage == TelemetryCoverage.COMPLETE && !allPresent) {
            throw new IllegalArgumentException("complete distribution requires every partition");
        }
        if (coverage == TelemetryCoverage.UNAVAILABLE && anyPresent) {
            throw new IllegalArgumentException("unavailable distribution cannot contain rows");
        }
        if (coverage == TelemetryCoverage.PARTIAL && (!anyPresent || allPresent)) {
            throw new IllegalArgumentException("partial distribution requires present and absent partitions");
        }
        this.logicalStage = logicalStage;
        this.role = role;
        this.parallelism = parallelism;
        this.coverage = coverage;
        this.present = transferArrays ? present : present.clone();
        this.rowCounts = transferArrays ? rowCounts : rowCounts.clone();
    }

    static PartitionDistribution decoded(int logicalStage, PartitionRole role, int parallelism,
                                         TelemetryCoverage coverage, boolean[] present,
                                         long[] rowCounts) {
        return new PartitionDistribution(logicalStage, role, parallelism, coverage,
                present, rowCounts, true);
    }

    public int logicalStage() { return logicalStage; }
    public PartitionRole role() { return role; }
    public int parallelism() { return parallelism; }
    public TelemetryCoverage coverage() { return coverage; }
    public boolean isPresent(int instanceIndex) { return present[instanceIndex]; }
    public boolean[] presenceBitmap() { return present.clone(); }
    public long[] rowCounts() { return rowCounts.clone(); }
    /** Package-private immutable backing view for bounded codecs/internal value conversion. */
    boolean[] borrowedPresenceBitmap() { return present; }
    /** Package-private immutable backing view for bounded codecs/internal value conversion. */
    long[] borrowedRowCounts() { return rowCounts; }

    public double skew() {
        if (coverage != TelemetryCoverage.COMPLETE) {
            throw new IllegalStateException("partition skew requires complete coverage");
        }
        long max = 0;
        long sum = 0;
        for (long count : rowCounts) {
            max = Math.max(max, count);
            if (Long.MAX_VALUE - sum < count) {
                throw new IllegalStateException("partition row sum overflow");
            }
            sum += count;
        }
        if (sum == 0) return 1.0;
        return max / ((double) sum / parallelism);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof PartitionDistribution)) return false;
        PartitionDistribution that = (PartitionDistribution) other;
        return logicalStage == that.logicalStage && parallelism == that.parallelism
                && role == that.role && coverage == that.coverage
                && Arrays.equals(present, that.present)
                && Arrays.equals(rowCounts, that.rowCounts);
    }

    @Override
    public int hashCode() {
        int result = logicalStage;
        result = 31 * result + role.hashCode();
        result = 31 * result + parallelism;
        result = 31 * result + coverage.hashCode();
        result = 31 * result + Arrays.hashCode(present);
        return 31 * result + Arrays.hashCode(rowCounts);
    }
}
