package io.rowweave.plan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Memory measurements for one execution, returned per execute instead of exposed through a last-run
 * getter, so the values cannot be read across executions or mutated concurrently and their ownership is
 * unambiguous. {@code peakReservedBytes} is the high-water mark the MemoryManager reached during this
 * execution; {@code acquiredBytes} is the quota this job took from the pool, and a graph with no
 * pipeline breaker takes nothing. The no-OOM assertion is peak &lt;= acquired.
 *
 * <p>Further observability fields, all zero or empty by default, so the two-argument constructor and
 * withinBudget keep working:
 * <ul>
 * <li>{@code spillCount}: how many times this execution spilled to make room, a signal of skew or
 * memory pressure.</li>
 * <li>{@code rowsOut}: rows written to the Sink after aggregation, including the rows counted on the
 * parallel path. It is not interchangeable with {@code partitionRowCounts}: that one describes the
 * input distribution before aggregation, while rowsOut is the final output after aggregation or join,
 * so a skew consumer must not mix them.</li>
 * <li>{@code partitionRowCounts}: input rows received by each keyed operator instance on the parallel
 * path, an array of length P whose sum is the input row count, which may exceed R. This is the raw
 * material for the skew metric max/avg. The serial path leaves it empty.</li>
 * </ul>
 */
public final class ExecutionReport {

    private static final long[] EMPTY = new long[0];

    private final long peakReservedBytes;
    private final long acquiredBytes;
    private final long spillCount;
    private final long rowsOut;
    private final long[] partitionRowCounts;
    private final TelemetryCoverage resourceCoverage;
    private final TelemetryCoverage partitionCoverage;
    private final long spillRunCount;
    private final long spillLogicalBytesWritten;
    private final long spillPeakReservedBytes;
    private final List<PartitionDistribution> partitionDistributions;

    /** Two-argument constructor kept for compatibility: no spill, rows or partition metrics. */
    public ExecutionReport(long peakReservedBytes, long acquiredBytes) {
        this(peakReservedBytes, acquiredBytes, 0, 0, EMPTY);
    }

    /** Constructor taking every field. */
    public ExecutionReport(long peakReservedBytes, long acquiredBytes,
                           long spillCount, long rowsOut, long[] partitionRowCounts) {
        this(peakReservedBytes, acquiredBytes, spillCount, rowsOut, partitionRowCounts,
                TelemetryCoverage.UNAVAILABLE, TelemetryCoverage.UNAVAILABLE,
                0, 0, 0, Collections.<PartitionDistribution>emptyList());
    }

    private ExecutionReport(long peakReservedBytes, long acquiredBytes,
                            long spillCount, long rowsOut, long[] partitionRowCounts,
                            TelemetryCoverage resourceCoverage,
                            TelemetryCoverage partitionCoverage,
                            long spillRunCount, long spillLogicalBytesWritten,
                            long spillPeakReservedBytes,
                            List<PartitionDistribution> partitionDistributions) {
        this(peakReservedBytes, acquiredBytes, spillCount, rowsOut, partitionRowCounts,
                resourceCoverage, partitionCoverage, spillRunCount,
                spillLogicalBytesWritten, spillPeakReservedBytes,
                partitionDistributions, false);
    }

    private ExecutionReport(long peakReservedBytes, long acquiredBytes,
                            long spillCount, long rowsOut, long[] partitionRowCounts,
                            TelemetryCoverage resourceCoverage,
                            TelemetryCoverage partitionCoverage,
                            long spillRunCount, long spillLogicalBytesWritten,
                            long spillPeakReservedBytes,
                            List<PartitionDistribution> partitionDistributions,
                            boolean transferImmutableTelemetry) {
        this.peakReservedBytes = peakReservedBytes;
        this.acquiredBytes = acquiredBytes;
        this.spillCount = spillCount;
        this.rowsOut = rowsOut;
        this.partitionRowCounts = partitionRowCounts == null || partitionRowCounts.length == 0
                ? EMPTY : (transferImmutableTelemetry
                ? partitionRowCounts : partitionRowCounts.clone());
        this.resourceCoverage = resourceCoverage;
        this.partitionCoverage = partitionCoverage;
        this.spillRunCount = spillRunCount;
        this.spillLogicalBytesWritten = spillLogicalBytesWritten;
        this.spillPeakReservedBytes = spillPeakReservedBytes;
        this.partitionDistributions = transferImmutableTelemetry
                ? partitionDistributions
                : Collections.unmodifiableList(new ArrayList<>(partitionDistributions));
    }

    /** Truthful local/cluster factory; legacy public constructors deliberately remain unavailable. */
    static ExecutionReport complete(StageExecutionTelemetry telemetry, long rowsOut,
                                    List<PartitionDistribution> distributions) {
        if (telemetry == null || telemetry.coverage() != TelemetryCoverage.COMPLETE) {
            throw new IllegalArgumentException("complete report requires complete resource telemetry");
        }
        return fromTelemetry(telemetry, rowsOut, distributions);
    }

    static ExecutionReport fromTelemetry(StageExecutionTelemetry telemetry, long rowsOut,
                                         List<PartitionDistribution> distributions) {
        if (telemetry == null || (telemetry.coverage() != TelemetryCoverage.COMPLETE
                && telemetry.coverage() != TelemetryCoverage.UNAVAILABLE)) {
            throw new IllegalArgumentException("report requires complete or unavailable telemetry");
        }
        List<PartitionDistribution> safe = distributions == null
                ? Collections.<PartitionDistribution>emptyList() : distributions;
        TelemetryCoverage partitionCoverage = aggregatePartitionCoverage(safe);
        long[] legacy = safe.size() == 1
                && safe.get(0).coverage() == TelemetryCoverage.COMPLETE
                ? safe.get(0).borrowedRowCounts() : EMPTY;
        return new ExecutionReport(telemetry.managedPeakReservedBytes(),
                telemetry.managedAcquiredBytes(), telemetry.pressureSpillCount(), rowsOut,
                legacy, telemetry.coverage(), partitionCoverage,
                telemetry.spillRunCount(), telemetry.spillLogicalBytesWritten(),
                telemetry.spillPeakReservedBytes(), safe);
    }

    static ExecutionReport fromSummary(ExecutionTelemetrySummary summary, long rowsOut) {
        if (summary == null) throw new IllegalArgumentException("summary is required");
        List<PartitionDistribution> distributions = summary.partitionDistributions();
        long[] legacy = distributions.size() == 1
                && summary.partitionCoverage() == TelemetryCoverage.COMPLETE
                && distributions.get(0).coverage() == TelemetryCoverage.COMPLETE
                ? distributions.get(0).borrowedRowCounts() : EMPTY;
        return new ExecutionReport(summary.managedPeakReservedBytes(),
                summary.managedAcquiredBytes(), summary.pressureSpillCount(), rowsOut,
                legacy, summary.resourceCoverage(), summary.partitionCoverage(),
                summary.spillRunCount(), summary.spillLogicalBytesWritten(),
                summary.spillPeakReservedBytes(), distributions, true);
    }

    public long peakReservedBytes() {
        return peakReservedBytes;
    }

    public long acquiredBytes() {
        return acquiredBytes;
    }

    /** How many times this execution spilled to make room. */
    public long spillCount() {
        return spillCount;
    }

    public long pressureSpillCount() { return spillCount; }
    public long spillRunCount() { return spillRunCount; }
    public long spillLogicalBytesWritten() { return spillLogicalBytesWritten; }
    public long spillPeakReservedBytes() { return spillPeakReservedBytes; }
    public TelemetryCoverage resourceTelemetryCoverage() { return resourceCoverage; }
    public TelemetryCoverage partitionTelemetryCoverage() { return partitionCoverage; }

    public TelemetryCoverage telemetryCoverage() {
        if (resourceCoverage == TelemetryCoverage.UNAVAILABLE
                || partitionCoverage == TelemetryCoverage.UNAVAILABLE) {
            return TelemetryCoverage.UNAVAILABLE;
        }
        if (resourceCoverage == TelemetryCoverage.PARTIAL
                || partitionCoverage == TelemetryCoverage.PARTIAL) {
            return TelemetryCoverage.PARTIAL;
        }
        return resourceCoverage;
    }

    public BudgetStatus budgetStatus() {
        if (resourceCoverage != TelemetryCoverage.COMPLETE) return BudgetStatus.UNKNOWN;
        return withinBudget() ? BudgetStatus.WITHIN_BUDGET : BudgetStatus.VIOLATION;
    }

    public List<PartitionDistribution> partitionDistributions() {
        return partitionDistributions;
    }

    ExecutionReport withRowsOut(long replacementRowsOut) {
        return new ExecutionReport(peakReservedBytes, acquiredBytes, spillCount,
                replacementRowsOut, partitionRowCounts, resourceCoverage,
                partitionCoverage, spillRunCount, spillLogicalBytesWritten,
                spillPeakReservedBytes, partitionDistributions, true);
    }

    StageExecutionTelemetry resourceTelemetry() {
        return resourceCoverage == TelemetryCoverage.COMPLETE
                ? StageExecutionTelemetry.complete(peakReservedBytes, acquiredBytes,
                spillCount, spillRunCount, spillLogicalBytesWritten, spillPeakReservedBytes)
                : StageExecutionTelemetry.unavailable();
    }

    /** Rows written to the Sink, after aggregation. */
    public long rowsOut() {
        return rowsOut;
    }

    /** Input rows received by each partition, meaning each keyed operator instance, before aggregation: length P, empty on the serial path. Returns an immutable copy. */
    public long[] partitionRowCounts() {
        return partitionRowCounts.length == 0 ? EMPTY : partitionRowCounts.clone();
    }

    /** Package-private immutable backing view for internal value conversion/tests. */
    long[] borrowedPartitionRowCounts() { return partitionRowCounts; }

    /** Skew = max/avg, where 1.0 is an even spread and P means every row landed in one partition. Returns 1.0 when there is no partition data, as on the serial path. */
    public double partitionSkew() {
        if (partitionRowCounts.length == 0) {
            return 1.0;
        }
        long max = 0;
        long sum = 0;
        for (long c : partitionRowCounts) {
            if (c > max) {
                max = c;
            }
            sum += c;
        }
        if (sum == 0) {
            return 1.0;
        }
        double avg = (double) sum / partitionRowCounts.length;
        return max / avg;
    }

    /** The no-OOM invariant: the peak never exceeded the acquired quota. */
    public boolean withinBudget() {
        return peakReservedBytes <= acquiredBytes;
    }

    /**
     * Advisory: returns warning text when partitionSkew reaches the threshold, otherwise null. A pure
     * function that neither blocks nor mitigates; the caller, such as a metrics listener or a monitor
     * adapter, decides whether to log it. The serial path has skew 1.0 and never warns at a sane threshold.
     */
    public String skewWarning(double threshold) {
        double skew = partitionSkew();
        if (skew >= threshold) {
            return "partition skew " + String.format("%.2f", skew) + " >= threshold " + threshold
                    + " (rows concentrated in few partitions; consider key redesign)";
        }
        return null;
    }

    /** Advisory: a one-line metrics summary, taken and logged by the caller if it wants; the engine does not log it. */
    public String summary() {
        return "rowsOut=" + rowsOut + " peak=" + peakReservedBytes + " acquired=" + acquiredBytes
                + " pressureSpillCount=" + spillCount + " spillRunCount=" + spillRunCount
                + " spillLogicalBytesWritten=" + spillLogicalBytesWritten
                + " spillPeakReservedBytes=" + spillPeakReservedBytes
                + " resourceCoverage=" + resourceCoverage
                + " partitionCoverage=" + partitionCoverage
                + " skew=" + String.format("%.2f", partitionSkew());
    }

    private static TelemetryCoverage aggregatePartitionCoverage(
            List<PartitionDistribution> distributions) {
        if (distributions.isEmpty()) return TelemetryCoverage.NOT_APPLICABLE;
        boolean complete = true;
        boolean anyPresent = false;
        for (PartitionDistribution distribution : distributions) {
            if (distribution == null) {
                return TelemetryCoverage.UNAVAILABLE;
            }
            complete &= distribution.coverage() == TelemetryCoverage.COMPLETE;
            for (boolean present : distribution.borrowedPresenceBitmap()) anyPresent |= present;
        }
        if (complete) return TelemetryCoverage.COMPLETE;
        return anyPresent ? TelemetryCoverage.PARTIAL : TelemetryCoverage.UNAVAILABLE;
    }
}
