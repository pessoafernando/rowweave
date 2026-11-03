package io.rowweave.mem;

/** Immutable process-ledger snapshot sampled under one ManagedMemory lock. */
public final class ManagedMemorySnapshot {
    private final long totalBytes;
    private final long guaranteedBytes;
    private final long committedBytes;
    private final long reservedBytes;
    private final long peakReservedBytes;
    private final int activeOwnerCount;
    private final int activeLeafCount;

    ManagedMemorySnapshot(long totalBytes, long guaranteedBytes, long committedBytes,
                          long reservedBytes, long peakReservedBytes,
                          int activeOwnerCount, int activeLeafCount) {
        this.totalBytes = totalBytes;
        this.guaranteedBytes = guaranteedBytes;
        this.committedBytes = committedBytes;
        this.reservedBytes = reservedBytes;
        this.peakReservedBytes = peakReservedBytes;
        this.activeOwnerCount = activeOwnerCount;
        this.activeLeafCount = activeLeafCount;
    }

    public long totalBytes() { return totalBytes; }
    public long guaranteedBytes() { return guaranteedBytes; }
    public long committedBytes() { return committedBytes; }
    public long reservedBytes() { return reservedBytes; }
    public long peakReservedBytes() { return peakReservedBytes; }
    public int activeOwnerCount() { return activeOwnerCount; }
    public int activeLeafCount() { return activeLeafCount; }
}
