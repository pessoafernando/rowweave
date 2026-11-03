package io.rowweave.stream.api;

import java.util.Objects;

/** Explicit field binding for the built-in final event-time decimal window. */
public final class StreamWindowSpec {
    public enum Type { TUMBLING, SLIDING }

    private final Type type;
    private final String eventTimeFieldId;
    private final String valueFieldId;
    private final long sizeMillis;
    private final long slideMillis;
    private final String startOutputFieldId;
    private final String endOutputFieldId;
    private final String valueOutputFieldId;

    private StreamWindowSpec(Type type, String eventTimeFieldId,
            String valueFieldId, long sizeMillis, long slideMillis,
            String startOutputFieldId, String endOutputFieldId,
            String valueOutputFieldId) {
        if (type == null || empty(eventTimeFieldId) || empty(valueFieldId)
                || sizeMillis <= 0L || slideMillis <= 0L
                || slideMillis > sizeMillis
                || (type == Type.TUMBLING && slideMillis != sizeMillis)
                || empty(startOutputFieldId) || empty(endOutputFieldId)
                || empty(valueOutputFieldId)) {
            throw new IllegalArgumentException("invalid finite Stream window");
        }
        this.type = type;
        this.eventTimeFieldId = eventTimeFieldId;
        this.valueFieldId = valueFieldId;
        this.sizeMillis = sizeMillis;
        this.slideMillis = slideMillis;
        this.startOutputFieldId = startOutputFieldId;
        this.endOutputFieldId = endOutputFieldId;
        this.valueOutputFieldId = valueOutputFieldId;
    }

    public static StreamWindowSpec tumblingDecimal(String eventTimeFieldId,
            String valueFieldId, long sizeMillis, String startOutputFieldId,
            String endOutputFieldId, String valueOutputFieldId) {
        return new StreamWindowSpec(Type.TUMBLING, eventTimeFieldId,
                valueFieldId, sizeMillis, sizeMillis, startOutputFieldId,
                endOutputFieldId, valueOutputFieldId);
    }

    public static StreamWindowSpec slidingDecimal(String eventTimeFieldId,
            String valueFieldId, long sizeMillis, long slideMillis,
            String startOutputFieldId, String endOutputFieldId,
            String valueOutputFieldId) {
        return new StreamWindowSpec(Type.SLIDING, eventTimeFieldId,
                valueFieldId, sizeMillis, slideMillis, startOutputFieldId,
                endOutputFieldId, valueOutputFieldId);
    }

    private static boolean empty(String value) {
        return value == null || value.isEmpty();
    }

    public Type type() { return type; }
    public String eventTimeFieldId() { return eventTimeFieldId; }
    public String valueFieldId() { return valueFieldId; }
    public long sizeMillis() { return sizeMillis; }
    public long slideMillis() { return slideMillis; }
    public String startOutputFieldId() { return startOutputFieldId; }
    public String endOutputFieldId() { return endOutputFieldId; }
    public String valueOutputFieldId() { return valueOutputFieldId; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamWindowSpec)) return false;
        StreamWindowSpec that = (StreamWindowSpec) other;
        return type == that.type && sizeMillis == that.sizeMillis
                && slideMillis == that.slideMillis
                && eventTimeFieldId.equals(that.eventTimeFieldId)
                && valueFieldId.equals(that.valueFieldId)
                && startOutputFieldId.equals(that.startOutputFieldId)
                && endOutputFieldId.equals(that.endOutputFieldId)
                && valueOutputFieldId.equals(that.valueOutputFieldId);
    }

    @Override public int hashCode() {
        return Objects.hash(type, eventTimeFieldId, valueFieldId, sizeMillis,
                slideMillis, startOutputFieldId, endOutputFieldId,
                valueOutputFieldId);
    }
}
