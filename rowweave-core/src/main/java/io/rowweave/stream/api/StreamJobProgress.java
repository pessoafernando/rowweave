package io.rowweave.stream.api;

import java.util.Objects;

/** Data-free bounded operational counters safe for status APIs. */
public final class StreamJobProgress {
    private static final StreamJobProgress EMPTY =
            new StreamJobProgress(0, 0L, Long.MIN_VALUE,
                    StreamCdcPhase.NOT_APPLICABLE, 0,
                    0L, 0L, 0L, 0L, 0, 0L, 0L, 0, 0, 0L);
    private final int attempt;
    private final long completedCheckpoints;
    private final long operatorWatermark;
    private final StreamCdcPhase cdcPhase;
    private final int restarts;
    private final long recordsRead;
    private final long recordsWritten;
    private final long bytesRead;
    private final long bytesWritten;
    private final long backlogEvents;
    private final long backlogBytes;
    private final int mailboxHighWaterEvents;
    private final long channelHighWaterBytes;
    private final long managedStateBytes;
    private final int timerHighWaterCount;
    private final int windowHighWaterCount;
    private final long backpressuredMillis;

    /** Compatibility constructor for the original minimal progress projection. */
    public StreamJobProgress(int attempt, long completedCheckpoints, int restarts,
            long managedStateBytes, long backpressuredMillis) {
        this(attempt, completedCheckpoints, Long.MIN_VALUE,
                StreamCdcPhase.NOT_APPLICABLE, restarts,
                0L, 0L, 0L, 0L, 0, 0L, managedStateBytes,
                0, 0, backpressuredMillis);
    }

    public StreamJobProgress(int attempt, long completedCheckpoints,
            long operatorWatermark, StreamCdcPhase cdcPhase, int restarts,
            long recordsRead, long recordsWritten, long bytesRead, long bytesWritten,
            int mailboxHighWaterEvents, long channelHighWaterBytes,
            long managedStateBytes, int timerHighWaterCount,
            int windowHighWaterCount, long backpressuredMillis) {
        this(attempt, completedCheckpoints, operatorWatermark, cdcPhase,
                restarts, recordsRead, recordsWritten, bytesRead,
                bytesWritten, 0L, 0L, mailboxHighWaterEvents,
                channelHighWaterBytes, managedStateBytes,
                timerHighWaterCount, windowHighWaterCount,
                backpressuredMillis);
    }

    public StreamJobProgress(int attempt, long completedCheckpoints,
            long operatorWatermark, StreamCdcPhase cdcPhase, int restarts,
            long recordsRead, long recordsWritten, long bytesRead,
            long bytesWritten, long backlogEvents, long backlogBytes,
            int mailboxHighWaterEvents, long channelHighWaterBytes,
            long managedStateBytes, int timerHighWaterCount,
            int windowHighWaterCount, long backpressuredMillis) {
        if (attempt < 0 || completedCheckpoints < 0L || restarts < 0
                || recordsRead < 0L || recordsWritten < 0L
                || bytesRead < 0L || bytesWritten < 0L
                || backlogEvents < 0L || backlogBytes < 0L
                || mailboxHighWaterEvents < 0 || channelHighWaterBytes < 0L
                || managedStateBytes < 0L || timerHighWaterCount < 0
                || windowHighWaterCount < 0 || backpressuredMillis < 0L) {
            throw new IllegalArgumentException("Stream progress counters cannot be negative");
        }
        this.attempt = attempt;
        this.completedCheckpoints = completedCheckpoints;
        this.operatorWatermark = operatorWatermark;
        this.cdcPhase = Objects.requireNonNull(cdcPhase, "cdcPhase");
        this.restarts = restarts;
        this.recordsRead = recordsRead;
        this.recordsWritten = recordsWritten;
        this.bytesRead = bytesRead;
        this.bytesWritten = bytesWritten;
        this.backlogEvents = backlogEvents;
        this.backlogBytes = backlogBytes;
        this.mailboxHighWaterEvents = mailboxHighWaterEvents;
        this.channelHighWaterBytes = channelHighWaterBytes;
        this.managedStateBytes = managedStateBytes;
        this.timerHighWaterCount = timerHighWaterCount;
        this.windowHighWaterCount = windowHighWaterCount;
        this.backpressuredMillis = backpressuredMillis;
    }
    public static StreamJobProgress empty() { return EMPTY; }
    public int attempt() { return attempt; }
    public long completedCheckpoints() { return completedCheckpoints; }
    public long operatorWatermark() { return operatorWatermark; }
    public StreamCdcPhase cdcPhase() { return cdcPhase; }
    public int restarts() { return restarts; }
    public long recordsRead() { return recordsRead; }
    public long recordsWritten() { return recordsWritten; }
    public long bytesRead() { return bytesRead; }
    public long bytesWritten() { return bytesWritten; }
    public long backlogEvents() { return backlogEvents; }
    public long backlogBytes() { return backlogBytes; }
    public int mailboxHighWaterEvents() { return mailboxHighWaterEvents; }
    public long channelHighWaterBytes() { return channelHighWaterBytes; }
    public long managedStateBytes() { return managedStateBytes; }
    public int timerHighWaterCount() { return timerHighWaterCount; }
    public int windowHighWaterCount() { return windowHighWaterCount; }
    public long backpressuredMillis() { return backpressuredMillis; }
    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamJobProgress)) return false;
        StreamJobProgress that = (StreamJobProgress) other;
        return attempt == that.attempt
                && completedCheckpoints == that.completedCheckpoints
                && operatorWatermark == that.operatorWatermark
                && cdcPhase == that.cdcPhase
                && restarts == that.restarts
                && recordsRead == that.recordsRead
                && recordsWritten == that.recordsWritten
                && bytesRead == that.bytesRead
                && bytesWritten == that.bytesWritten
                && backlogEvents == that.backlogEvents
                && backlogBytes == that.backlogBytes
                && mailboxHighWaterEvents == that.mailboxHighWaterEvents
                && channelHighWaterBytes == that.channelHighWaterBytes
                && managedStateBytes == that.managedStateBytes
                && timerHighWaterCount == that.timerHighWaterCount
                && windowHighWaterCount == that.windowHighWaterCount
                && backpressuredMillis == that.backpressuredMillis;
    }
    @Override public int hashCode() {
        return Objects.hash(attempt, completedCheckpoints, operatorWatermark,
                cdcPhase, restarts, recordsRead, recordsWritten,
                bytesRead, bytesWritten, backlogEvents, backlogBytes,
                mailboxHighWaterEvents,
                channelHighWaterBytes, managedStateBytes, timerHighWaterCount,
                windowHighWaterCount, backpressuredMillis);
    }
}
