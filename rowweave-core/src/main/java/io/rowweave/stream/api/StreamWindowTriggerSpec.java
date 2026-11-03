package io.rowweave.stream.api;

import java.util.Objects;

/** Built-in deterministic generic-window trigger declaration. */
public final class StreamWindowTriggerSpec {
    public enum Kind { EVENT_TIME_FINAL, PROCESSING_TIME, COUNT }

    private final Kind kind;
    private final long intervalOrCount;
    private final boolean purging;

    private StreamWindowTriggerSpec(Kind kind, long intervalOrCount,
            boolean purging) {
        if (kind == null || (kind == Kind.EVENT_TIME_FINAL
                ? intervalOrCount != 0L : intervalOrCount <= 0L)) {
            throw new IllegalArgumentException("invalid window trigger");
        }
        this.kind = kind;
        this.intervalOrCount = intervalOrCount;
        this.purging = purging;
    }

    public static StreamWindowTriggerSpec eventTimeFinal() {
        return new StreamWindowTriggerSpec(Kind.EVENT_TIME_FINAL, 0L, false);
    }

    public static StreamWindowTriggerSpec processingTime(long intervalMillis) {
        if (intervalMillis > StreamWindowAssignerSpec.MAX_TIME_MILLIS) {
            throw new IllegalArgumentException("processing trigger exceeds limit");
        }
        return new StreamWindowTriggerSpec(
                Kind.PROCESSING_TIME, intervalMillis, false);
    }

    public static StreamWindowTriggerSpec count(long count) {
        if (count > StreamWindowAssignerSpec.MAX_COUNT) {
            throw new IllegalArgumentException("count trigger exceeds limit");
        }
        return new StreamWindowTriggerSpec(Kind.COUNT, count, false);
    }

    public StreamWindowTriggerSpec purging() {
        return new StreamWindowTriggerSpec(kind, intervalOrCount, true);
    }

    public Kind kind() { return kind; }
    public long intervalOrCount() { return intervalOrCount; }
    public boolean isPurging() { return purging; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamWindowTriggerSpec)) return false;
        StreamWindowTriggerSpec that = (StreamWindowTriggerSpec) other;
        return kind == that.kind && intervalOrCount == that.intervalOrCount
                && purging == that.purging;
    }

    @Override public int hashCode() {
        return Objects.hash(kind, intervalOrCount, purging);
    }
}
