package io.rowweave.stream.runtime;

import java.util.Objects;

/** Immutable actual counters for one physical bounded Stream output lane. */
public final class StreamChannelMetrics {
    private static final StreamChannelMetrics UNAVAILABLE =
            new StreamChannelMetrics(false, 0L, 0L, 0L, 0L, 0L);

    private final boolean available;
    private final long currentEvents;
    private final long currentBytes;
    private final long highWaterEvents;
    private final long highWaterBytes;
    private final long blockedNanos;

    private StreamChannelMetrics(boolean available, long currentEvents,
            long currentBytes, long highWaterEvents, long highWaterBytes,
            long blockedNanos) {
        if (currentEvents < 0L || currentBytes < 0L
                || highWaterEvents < currentEvents
                || highWaterBytes < currentBytes || blockedNanos < 0L
                || !available && (currentEvents != 0L || currentBytes != 0L
                || highWaterEvents != 0L || highWaterBytes != 0L
                || blockedNanos != 0L)) {
            throw new IllegalArgumentException(
                    "Stream channel metrics are invalid");
        }
        this.available = available;
        this.currentEvents = currentEvents;
        this.currentBytes = currentBytes;
        this.highWaterEvents = highWaterEvents;
        this.highWaterBytes = highWaterBytes;
        this.blockedNanos = blockedNanos;
    }

    public static StreamChannelMetrics available(long currentEvents,
            long currentBytes, long highWaterEvents, long highWaterBytes,
            long blockedNanos) {
        return new StreamChannelMetrics(true, currentEvents, currentBytes,
                highWaterEvents, highWaterBytes, blockedNanos);
    }

    public static StreamChannelMetrics unavailable() { return UNAVAILABLE; }

    public static StreamChannelMetrics aggregateOutputs(
            Iterable<? extends StreamChannelOutput> outputs) {
        if (outputs == null) {
            throw new IllegalArgumentException(
                    "Stream channel outputs are required");
        }
        long currentEvents = 0L;
        long currentBytes = 0L;
        long highWaterEvents = 0L;
        long highWaterBytes = 0L;
        long blockedNanos = 0L;
        boolean found = false;
        for (StreamChannelOutput output : outputs) {
            if (output == null) {
                throw new IllegalArgumentException(
                        "Stream channel output is required");
            }
            StreamChannelMetrics metrics = output.metrics();
            if (!metrics.available()) return unavailable();
            found = true;
            currentEvents = saturatingAdd(
                    currentEvents, metrics.currentEvents());
            currentBytes = saturatingAdd(
                    currentBytes, metrics.currentBytes());
            highWaterEvents = saturatingAdd(
                    highWaterEvents, metrics.highWaterEvents());
            highWaterBytes = saturatingAdd(
                    highWaterBytes, metrics.highWaterBytes());
            blockedNanos = saturatingAdd(
                    blockedNanos, metrics.blockedNanos());
        }
        return found ? available(currentEvents, currentBytes,
                highWaterEvents, highWaterBytes, blockedNanos)
                : unavailable();
    }
    public boolean available() { return available; }
    public long currentEvents() { return currentEvents; }
    public long currentBytes() { return currentBytes; }
    public long highWaterEvents() { return highWaterEvents; }
    public long highWaterBytes() { return highWaterBytes; }
    public long blockedNanos() { return blockedNanos; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamChannelMetrics)) return false;
        StreamChannelMetrics that = (StreamChannelMetrics) other;
        return available == that.available
                && currentEvents == that.currentEvents
                && currentBytes == that.currentBytes
                && highWaterEvents == that.highWaterEvents
                && highWaterBytes == that.highWaterBytes
                && blockedNanos == that.blockedNanos;
    }

    @Override public int hashCode() {
        return Objects.hash(available, currentEvents, currentBytes,
                highWaterEvents, highWaterBytes, blockedNanos);
    }

    private static long saturatingAdd(long left, long right) {
        return left > Long.MAX_VALUE - right
                ? Long.MAX_VALUE : left + right;
    }
}
