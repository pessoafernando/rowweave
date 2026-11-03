package io.rowweave.stream.api;

import java.util.Objects;

/** Inert bounded window assignment declaration. */
public final class StreamWindowAssignerSpec {
    public static final long MAX_TIME_MILLIS = 7L * 24L * 60L * 60L * 1000L;
    public static final long MAX_COUNT = 1_000_000_000L;

    public enum Kind {
        EVENT_TIME_TUMBLING,
        EVENT_TIME_SLIDING,
        EVENT_TIME_SESSION,
        PROCESSING_TIME_TUMBLING,
        PROCESSING_TIME_SLIDING,
        PROCESSING_TIME_SESSION,
        COUNT_TUMBLING,
        COUNT_SLIDING
    }

    private final Kind kind;
    private final String eventTimeFieldId;
    private final long sizeOrGap;
    private final long slide;

    private StreamWindowAssignerSpec(Kind kind, String eventTimeFieldId,
            long sizeOrGap, long slide) {
        if (kind == null || sizeOrGap <= 0L
                || (isCount(kind) ? sizeOrGap > MAX_COUNT
                : sizeOrGap > MAX_TIME_MILLIS)
                || (isSliding(kind) && (slide <= 0L
                || slide > sizeOrGap || (isCount(kind) ? slide > MAX_COUNT
                : slide > MAX_TIME_MILLIS)))
                || (!isSliding(kind) && slide != 0L)
                || (isEventTime(kind)
                && (eventTimeFieldId == null || eventTimeFieldId.isEmpty()))
                || (!isEventTime(kind) && eventTimeFieldId != null)) {
            throw new IllegalArgumentException("invalid bounded window assigner");
        }
        this.kind = kind;
        this.eventTimeFieldId = eventTimeFieldId;
        this.sizeOrGap = sizeOrGap;
        this.slide = slide;
    }

    public static StreamWindowAssignerSpec eventTimeTumbling(
            String fieldId, long sizeMillis) {
        return new StreamWindowAssignerSpec(Kind.EVENT_TIME_TUMBLING,
                fieldId, sizeMillis, 0L);
    }

    public static StreamWindowAssignerSpec eventTimeSliding(
            String fieldId, long sizeMillis, long slideMillis) {
        return new StreamWindowAssignerSpec(Kind.EVENT_TIME_SLIDING,
                fieldId, sizeMillis, slideMillis);
    }

    public static StreamWindowAssignerSpec eventTimeSession(
            String fieldId, long gapMillis) {
        return new StreamWindowAssignerSpec(Kind.EVENT_TIME_SESSION,
                fieldId, gapMillis, 0L);
    }

    public static StreamWindowAssignerSpec processingTimeTumbling(
            long sizeMillis) {
        return new StreamWindowAssignerSpec(Kind.PROCESSING_TIME_TUMBLING,
                null, sizeMillis, 0L);
    }

    public static StreamWindowAssignerSpec processingTimeSliding(
            long sizeMillis, long slideMillis) {
        return new StreamWindowAssignerSpec(Kind.PROCESSING_TIME_SLIDING,
                null, sizeMillis, slideMillis);
    }

    public static StreamWindowAssignerSpec processingTimeSession(
            long gapMillis) {
        return new StreamWindowAssignerSpec(Kind.PROCESSING_TIME_SESSION,
                null, gapMillis, 0L);
    }

    public static StreamWindowAssignerSpec countTumbling(long count) {
        return new StreamWindowAssignerSpec(Kind.COUNT_TUMBLING,
                null, count, 0L);
    }

    public static StreamWindowAssignerSpec countSliding(
            long size, long slide) {
        return new StreamWindowAssignerSpec(Kind.COUNT_SLIDING,
                null, size, slide);
    }

    public Kind kind() { return kind; }
    public String eventTimeFieldId() { return eventTimeFieldId; }
    public long sizeOrGap() { return sizeOrGap; }
    public long slide() { return slide; }

    public boolean eventTime() { return isEventTime(kind); }
    public boolean count() { return isCount(kind); }

    private static boolean isEventTime(Kind value) {
        return value == Kind.EVENT_TIME_TUMBLING
                || value == Kind.EVENT_TIME_SLIDING
                || value == Kind.EVENT_TIME_SESSION;
    }

    private static boolean isCount(Kind value) {
        return value == Kind.COUNT_TUMBLING
                || value == Kind.COUNT_SLIDING;
    }

    private static boolean isSliding(Kind value) {
        return value == Kind.EVENT_TIME_SLIDING
                || value == Kind.PROCESSING_TIME_SLIDING
                || value == Kind.COUNT_SLIDING;
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamWindowAssignerSpec)) return false;
        StreamWindowAssignerSpec that = (StreamWindowAssignerSpec) other;
        return kind == that.kind && sizeOrGap == that.sizeOrGap
                && slide == that.slide
                && Objects.equals(eventTimeFieldId, that.eventTimeFieldId);
    }

    @Override public int hashCode() {
        return Objects.hash(kind, eventTimeFieldId, sizeOrGap, slide);
    }
}
