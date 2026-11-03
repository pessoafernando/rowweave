package io.rowweave.upgrade;

/** Immutable authoritative active-work observation used by QUIESCE. */
public final class UpgradeActiveWorkSnapshot {
    private final long activeRunCount;
    private final long activeIntervalCount;

    public UpgradeActiveWorkSnapshot(
            long activeRunCount, long activeIntervalCount) {
        if (activeRunCount < 0L || activeIntervalCount < 0L) {
            throw new IllegalArgumentException("active-work counts are invalid");
        }
        this.activeRunCount = activeRunCount;
        this.activeIntervalCount = activeIntervalCount;
    }

    public long activeRunCount() { return activeRunCount; }
    public long activeIntervalCount() { return activeIntervalCount; }
}
