package io.rowweave.plan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Immutable, bounded telemetry value for one completed job attempt.
 * Decimal result rows remain in the RPC base fields; this object is advisory only.
 */
public final class ExecutionTelemetrySummary {
    public static final int MAX_DISTRIBUTIONS = 512;
    public static final int MAX_PARTITION_SLOTS = 4096;

    private static final ExecutionTelemetrySummary UNAVAILABLE =
            new ExecutionTelemetrySummary(TelemetryCoverage.UNAVAILABLE,
                    0, 0, 0, 0, 0, 0,
                    TelemetryCoverage.UNAVAILABLE,
                    Collections.<PartitionDistribution>emptyList());

    private final TelemetryCoverage resourceCoverage;
    private final long managedPeakReservedBytes;
    private final long managedAcquiredBytes;
    private final long pressureSpillCount;
    private final long spillRunCount;
    private final long spillLogicalBytesWritten;
    private final long spillPeakReservedBytes;
    private final TelemetryCoverage partitionCoverage;
    private final List<PartitionDistribution> distributions;

    public ExecutionTelemetrySummary(
            TelemetryCoverage resourceCoverage,
            long managedPeakReservedBytes,
            long managedAcquiredBytes,
            long pressureSpillCount,
            long spillRunCount,
            long spillLogicalBytesWritten,
            long spillPeakReservedBytes,
            TelemetryCoverage partitionCoverage,
            List<PartitionDistribution> distributions) {
        this(resourceCoverage, managedPeakReservedBytes, managedAcquiredBytes,
                pressureSpillCount, spillRunCount, spillLogicalBytesWritten,
                spillPeakReservedBytes, partitionCoverage, distributions, false);
    }

    private ExecutionTelemetrySummary(
            TelemetryCoverage resourceCoverage,
            long managedPeakReservedBytes,
            long managedAcquiredBytes,
            long pressureSpillCount,
            long spillRunCount,
            long spillLogicalBytesWritten,
            long spillPeakReservedBytes,
            TelemetryCoverage partitionCoverage,
            List<PartitionDistribution> distributions,
            boolean transferList) {
        requireResourceCoverage(resourceCoverage);
        requirePartitionCoverage(partitionCoverage);
        this.managedPeakReservedBytes = nonNegative(managedPeakReservedBytes);
        this.managedAcquiredBytes = nonNegative(managedAcquiredBytes);
        this.pressureSpillCount = nonNegative(pressureSpillCount);
        this.spillRunCount = nonNegative(spillRunCount);
        this.spillLogicalBytesWritten = nonNegative(spillLogicalBytesWritten);
        this.spillPeakReservedBytes = nonNegative(spillPeakReservedBytes);
        if (resourceCoverage == TelemetryCoverage.UNAVAILABLE
                && (managedPeakReservedBytes != 0L || managedAcquiredBytes != 0L
                || pressureSpillCount != 0L || spillRunCount != 0L
                || spillLogicalBytesWritten != 0L || spillPeakReservedBytes != 0L)) {
            throw new IllegalArgumentException(
                    "unavailable resource summary cannot contain measured values");
        }
        List<PartitionDistribution> safe = distributions == null
                ? Collections.<PartitionDistribution>emptyList()
                : (transferList ? distributions : new ArrayList<>(distributions));
        safe.sort(Comparator.comparingInt(PartitionDistribution::logicalStage));
        validateDistributions(partitionCoverage, safe);
        this.resourceCoverage = resourceCoverage;
        this.partitionCoverage = partitionCoverage;
        this.distributions = Collections.unmodifiableList(safe);
    }

    static ExecutionTelemetrySummary decoded(
            TelemetryCoverage resourceCoverage,
            long managedPeakReservedBytes,
            long managedAcquiredBytes,
            long pressureSpillCount,
            long spillRunCount,
            long spillLogicalBytesWritten,
            long spillPeakReservedBytes,
            TelemetryCoverage partitionCoverage,
            List<PartitionDistribution> distributions) {
        return new ExecutionTelemetrySummary(resourceCoverage, managedPeakReservedBytes,
                managedAcquiredBytes, pressureSpillCount, spillRunCount,
                spillLogicalBytesWritten, spillPeakReservedBytes, partitionCoverage,
                distributions, true);
    }

    public static ExecutionTelemetrySummary unavailable() { return UNAVAILABLE; }

    public TelemetryCoverage resourceCoverage() { return resourceCoverage; }
    public long managedPeakReservedBytes() { return managedPeakReservedBytes; }
    public long managedAcquiredBytes() { return managedAcquiredBytes; }
    public long pressureSpillCount() { return pressureSpillCount; }
    public long spillRunCount() { return spillRunCount; }
    public long spillLogicalBytesWritten() { return spillLogicalBytesWritten; }
    public long spillPeakReservedBytes() { return spillPeakReservedBytes; }
    public TelemetryCoverage partitionCoverage() { return partitionCoverage; }
    public List<PartitionDistribution> partitionDistributions() { return distributions; }

    public ExecutionReport toExecutionReport(long rowsOut) {
        return ExecutionReport.fromSummary(this, rowsOut);
    }

    private static void validateDistributions(
            TelemetryCoverage partitionCoverage,
            List<PartitionDistribution> distributions) {
        if (distributions.size() > MAX_DISTRIBUTIONS) {
            throw new IllegalArgumentException("too many partition distributions");
        }
        long slots = 0L;
        boolean allComplete = !distributions.isEmpty();
        boolean anyPresent = false;
        Set<Integer> logicalStages = new HashSet<>();
        for (PartitionDistribution distribution : distributions) {
            if (distribution == null) {
                throw new IllegalArgumentException("partition distribution is required");
            }
            if (distribution.parallelism() <= 1) {
                throw new IllegalArgumentException(
                        "only parallel keyed distributions are reportable");
            }
            if (!logicalStages.add(distribution.logicalStage())) {
                throw new IllegalArgumentException("duplicate logical partition stage");
            }
            slots += distribution.parallelism();
            if (slots > MAX_PARTITION_SLOTS) {
                throw new IllegalArgumentException("too many partition slots");
            }
            allComplete &= distribution.coverage() == TelemetryCoverage.COMPLETE;
            for (boolean present : distribution.borrowedPresenceBitmap()) anyPresent |= present;
        }
        switch (partitionCoverage) {
            case NOT_APPLICABLE:
                if (!distributions.isEmpty()) {
                    throw new IllegalArgumentException(
                            "not-applicable partition summary must be empty");
                }
                break;
            case COMPLETE:
                if (!allComplete) {
                    throw new IllegalArgumentException(
                            "complete partition summary requires complete distributions");
                }
                break;
            case PARTIAL:
                if (distributions.isEmpty() || !anyPresent || allComplete) {
                    throw new IllegalArgumentException(
                            "partial partition summary requires incomplete trusted rows");
                }
                break;
            case UNAVAILABLE:
                if (anyPresent) {
                    throw new IllegalArgumentException(
                            "unavailable partition summary cannot contain trusted rows");
                }
                break;
            default:
                throw new IllegalArgumentException("invalid partition summary coverage");
        }
    }

    private static void requireResourceCoverage(TelemetryCoverage coverage) {
        if (coverage != TelemetryCoverage.COMPLETE
                && coverage != TelemetryCoverage.PARTIAL
                && coverage != TelemetryCoverage.UNAVAILABLE) {
            throw new IllegalArgumentException("invalid resource summary coverage");
        }
    }

    private static void requirePartitionCoverage(TelemetryCoverage coverage) {
        if (coverage != TelemetryCoverage.COMPLETE
                && coverage != TelemetryCoverage.PARTIAL
                && coverage != TelemetryCoverage.NOT_APPLICABLE
                && coverage != TelemetryCoverage.UNAVAILABLE) {
            throw new IllegalArgumentException("invalid partition summary coverage");
        }
    }

    private static long nonNegative(long value) {
        if (value < 0L) throw new IllegalArgumentException("summary values must be non-negative");
        return value;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof ExecutionTelemetrySummary)) return false;
        ExecutionTelemetrySummary that = (ExecutionTelemetrySummary) other;
        return resourceCoverage == that.resourceCoverage
                && managedPeakReservedBytes == that.managedPeakReservedBytes
                && managedAcquiredBytes == that.managedAcquiredBytes
                && pressureSpillCount == that.pressureSpillCount
                && spillRunCount == that.spillRunCount
                && spillLogicalBytesWritten == that.spillLogicalBytesWritten
                && spillPeakReservedBytes == that.spillPeakReservedBytes
                && partitionCoverage == that.partitionCoverage
                && distributions.equals(that.distributions);
    }

    @Override
    public int hashCode() {
        int result = resourceCoverage.hashCode();
        result = 31 * result + Long.hashCode(managedPeakReservedBytes);
        result = 31 * result + Long.hashCode(managedAcquiredBytes);
        result = 31 * result + Long.hashCode(pressureSpillCount);
        result = 31 * result + Long.hashCode(spillRunCount);
        result = 31 * result + Long.hashCode(spillLogicalBytesWritten);
        result = 31 * result + Long.hashCode(spillPeakReservedBytes);
        result = 31 * result + partitionCoverage.hashCode();
        return 31 * result + distributions.hashCode();
    }
}
