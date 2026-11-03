package io.rowweave.plan;

/** Fixed-width immutable resource facts for one physical stage assignment. */
public final class StageExecutionTelemetry {
    private static final StageExecutionTelemetry NOT_EXECUTED =
            new StageExecutionTelemetry(TelemetryCoverage.NOT_EXECUTED, 0, 0, 0, 0, 0, 0);
    private static final StageExecutionTelemetry UNAVAILABLE =
            new StageExecutionTelemetry(TelemetryCoverage.UNAVAILABLE, 0, 0, 0, 0, 0, 0);

    private final TelemetryCoverage coverage;
    private final long managedPeakReservedBytes;
    private final long managedAcquiredBytes;
    private final long pressureSpillCount;
    private final long spillRunCount;
    private final long spillLogicalBytesWritten;
    private final long spillPeakReservedBytes;

    private StageExecutionTelemetry(TelemetryCoverage coverage,
                                    long managedPeakReservedBytes,
                                    long managedAcquiredBytes,
                                    long pressureSpillCount,
                                    long spillRunCount,
                                    long spillLogicalBytesWritten,
                                    long spillPeakReservedBytes) {
        this.coverage = coverage;
        this.managedPeakReservedBytes = nonNegative(managedPeakReservedBytes);
        this.managedAcquiredBytes = nonNegative(managedAcquiredBytes);
        this.pressureSpillCount = nonNegative(pressureSpillCount);
        this.spillRunCount = nonNegative(spillRunCount);
        this.spillLogicalBytesWritten = nonNegative(spillLogicalBytesWritten);
        this.spillPeakReservedBytes = nonNegative(spillPeakReservedBytes);
    }

    public static StageExecutionTelemetry complete(long managedPeakReservedBytes,
                                                   long managedAcquiredBytes,
                                                   long pressureSpillCount,
                                                   long spillRunCount,
                                                   long spillLogicalBytesWritten,
                                                   long spillPeakReservedBytes) {
        return new StageExecutionTelemetry(TelemetryCoverage.COMPLETE,
                managedPeakReservedBytes, managedAcquiredBytes, pressureSpillCount,
                spillRunCount, spillLogicalBytesWritten, spillPeakReservedBytes);
    }

    public static StageExecutionTelemetry notExecuted() { return NOT_EXECUTED; }
    public static StageExecutionTelemetry unavailable() { return UNAVAILABLE; }

    public TelemetryCoverage coverage() { return coverage; }
    public long managedPeakReservedBytes() { return managedPeakReservedBytes; }
    public long managedAcquiredBytes() { return managedAcquiredBytes; }
    public long pressureSpillCount() { return pressureSpillCount; }
    public long spillRunCount() { return spillRunCount; }
    public long spillLogicalBytesWritten() { return spillLogicalBytesWritten; }
    public long spillPeakReservedBytes() { return spillPeakReservedBytes; }

    private static long nonNegative(long value) {
        if (value < 0) throw new IllegalArgumentException("telemetry values must be non-negative");
        return value;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof StageExecutionTelemetry)) return false;
        StageExecutionTelemetry that = (StageExecutionTelemetry) other;
        return coverage == that.coverage
                && managedPeakReservedBytes == that.managedPeakReservedBytes
                && managedAcquiredBytes == that.managedAcquiredBytes
                && pressureSpillCount == that.pressureSpillCount
                && spillRunCount == that.spillRunCount
                && spillLogicalBytesWritten == that.spillLogicalBytesWritten
                && spillPeakReservedBytes == that.spillPeakReservedBytes;
    }

    @Override
    public int hashCode() {
        int result = coverage.hashCode();
        result = 31 * result + Long.hashCode(managedPeakReservedBytes);
        result = 31 * result + Long.hashCode(managedAcquiredBytes);
        result = 31 * result + Long.hashCode(pressureSpillCount);
        result = 31 * result + Long.hashCode(spillRunCount);
        result = 31 * result + Long.hashCode(spillLogicalBytesWritten);
        return 31 * result + Long.hashCode(spillPeakReservedBytes);
    }
}
