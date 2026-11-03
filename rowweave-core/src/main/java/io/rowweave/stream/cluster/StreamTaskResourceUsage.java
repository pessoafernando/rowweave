package io.rowweave.stream.cluster;

/** Actual task-local resource high-water observations, never admission ceilings. */
final class StreamTaskResourceUsage {
    private static final StreamTaskResourceUsage UNAVAILABLE =
            new StreamTaskResourceUsage(0, 0L, 0, 0, false);
    private static final StreamTaskResourceUsage ZERO =
            new StreamTaskResourceUsage(0, 0L, 0, 0, true);

    private final int mailboxHighWaterEvents;
    private final long managedStateBytes;
    private final int timerHighWaterCount;
    private final int windowHighWaterCount;
    private final boolean complete;

    private StreamTaskResourceUsage(int mailboxHighWaterEvents,
            long managedStateBytes, int timerHighWaterCount,
            int windowHighWaterCount, boolean complete) {
        if (mailboxHighWaterEvents < 0 || managedStateBytes < 0L
                || timerHighWaterCount < 0 || windowHighWaterCount < 0) {
            throw new IllegalArgumentException(
                    "Stream task resource usage is invalid");
        }
        this.mailboxHighWaterEvents = mailboxHighWaterEvents;
        this.managedStateBytes = managedStateBytes;
        this.timerHighWaterCount = timerHighWaterCount;
        this.windowHighWaterCount = windowHighWaterCount;
        this.complete = complete;
    }

    static StreamTaskResourceUsage unavailable() { return UNAVAILABLE; }
    static StreamTaskResourceUsage zero() { return ZERO; }
    static StreamTaskResourceUsage complete(int mailboxHighWaterEvents,
            long managedStateBytes, int timerHighWaterCount,
            int windowHighWaterCount) {
        return new StreamTaskResourceUsage(mailboxHighWaterEvents,
                managedStateBytes, timerHighWaterCount,
                windowHighWaterCount, true);
    }

    int mailboxHighWaterEvents() { return mailboxHighWaterEvents; }
    long managedStateBytes() { return managedStateBytes; }
    int timerHighWaterCount() { return timerHighWaterCount; }
    int windowHighWaterCount() { return windowHighWaterCount; }
    boolean complete() { return complete; }
}
