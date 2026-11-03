package io.rowweave.mem.spill;

/** Immutable cumulative spill facts for one job workspace. */
public final class SpillJobTelemetrySnapshot {
    private final boolean complete;
    private final long runCount;
    private final long logicalBytesWritten;
    private final long peakReservedBytes;

    SpillJobTelemetrySnapshot(boolean complete, long runCount,
                              long logicalBytesWritten, long peakReservedBytes) {
        this.complete = complete;
        this.runCount = runCount;
        this.logicalBytesWritten = logicalBytesWritten;
        this.peakReservedBytes = peakReservedBytes;
    }

    public boolean complete() { return complete; }
    public long runCount() { return runCount; }
    public long logicalBytesWritten() { return logicalBytesWritten; }
    public long peakReservedBytes() { return peakReservedBytes; }
}
