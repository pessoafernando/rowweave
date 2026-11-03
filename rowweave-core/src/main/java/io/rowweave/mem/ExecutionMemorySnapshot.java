package io.rowweave.mem;

/** Immutable, close-stable execution-memory and spill telemetry. */
public final class ExecutionMemorySnapshot {
    private final boolean complete;
    private final long peakReservedBytes;
    private final long acquiredBytes;
    private final long pressureSpillCount;
    private final long spillRunCount;
    private final long spillLogicalBytesWritten;
    private final long spillPeakReservedBytes;

    ExecutionMemorySnapshot(boolean complete, long peakReservedBytes, long acquiredBytes,
                            long pressureSpillCount, long spillRunCount,
                            long spillLogicalBytesWritten, long spillPeakReservedBytes) {
        this.complete = complete;
        this.peakReservedBytes = peakReservedBytes;
        this.acquiredBytes = acquiredBytes;
        this.pressureSpillCount = pressureSpillCount;
        this.spillRunCount = spillRunCount;
        this.spillLogicalBytesWritten = spillLogicalBytesWritten;
        this.spillPeakReservedBytes = spillPeakReservedBytes;
    }

    public boolean complete() { return complete; }
    public long peakReservedBytes() { return peakReservedBytes; }
    public long acquiredBytes() { return acquiredBytes; }
    public long pressureSpillCount() { return pressureSpillCount; }
    public long spillRunCount() { return spillRunCount; }
    public long spillLogicalBytesWritten() { return spillLogicalBytesWritten; }
    public long spillPeakReservedBytes() { return spillPeakReservedBytes; }
}
