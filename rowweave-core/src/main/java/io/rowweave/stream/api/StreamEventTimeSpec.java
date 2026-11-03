package io.rowweave.stream.api;

import io.rowweave.portable.PortableDataType;

import java.util.Objects;

/** Bounded public timestamp extraction and watermark declaration. */
public final class StreamEventTimeSpec {
    public static final long MAX_OUT_OF_ORDER_MILLIS = 31_536_000_000L;
    public static final long MAX_IDLE_TIMEOUT_MILLIS = 2_592_000_000L;

    public enum WatermarkMode { MONOTONIC, BOUNDED_OUT_OF_ORDERNESS }
    public enum InvalidTimestampPolicy { FAIL, DROP }

    private final StreamExpression timestampExpression;
    private final WatermarkMode watermarkMode;
    private final long maximumOutOfOrderMillis;
    private final long idleTimeoutMillis;
    private final InvalidTimestampPolicy invalidTimestampPolicy;

    private StreamEventTimeSpec(StreamExpression timestampExpression,
            WatermarkMode watermarkMode, long maximumOutOfOrderMillis,
            long idleTimeoutMillis,
            InvalidTimestampPolicy invalidTimestampPolicy) {
        if (timestampExpression == null
                || timestampExpression.dataType()
                != PortableDataType.TIMESTAMP) {
            throw new IllegalArgumentException(
                    "event-time expression must produce TIMESTAMP");
        }
        if (watermarkMode == null || invalidTimestampPolicy == null
                || maximumOutOfOrderMillis < 0L
                || maximumOutOfOrderMillis > MAX_OUT_OF_ORDER_MILLIS
                || idleTimeoutMillis <= 0L
                || idleTimeoutMillis > MAX_IDLE_TIMEOUT_MILLIS
                || (watermarkMode == WatermarkMode.MONOTONIC
                && maximumOutOfOrderMillis != 0L)) {
            throw new IllegalArgumentException(
                    "event-time watermark bounds are invalid");
        }
        this.timestampExpression = timestampExpression;
        this.watermarkMode = watermarkMode;
        this.maximumOutOfOrderMillis = maximumOutOfOrderMillis;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.invalidTimestampPolicy = invalidTimestampPolicy;
    }

    public static StreamEventTimeSpec monotonic(
            StreamExpression timestampExpression, long idleTimeoutMillis,
            InvalidTimestampPolicy invalidTimestampPolicy) {
        return new StreamEventTimeSpec(timestampExpression,
                WatermarkMode.MONOTONIC, 0L, idleTimeoutMillis,
                invalidTimestampPolicy);
    }

    public static StreamEventTimeSpec boundedOutOfOrderness(
            StreamExpression timestampExpression,
            long maximumOutOfOrderMillis, long idleTimeoutMillis,
            InvalidTimestampPolicy invalidTimestampPolicy) {
        return new StreamEventTimeSpec(timestampExpression,
                WatermarkMode.BOUNDED_OUT_OF_ORDERNESS,
                maximumOutOfOrderMillis, idleTimeoutMillis,
                invalidTimestampPolicy);
    }

    public StreamExpression timestampExpression() {
        return timestampExpression;
    }
    public WatermarkMode watermarkMode() { return watermarkMode; }
    public long maximumOutOfOrderMillis() {
        return maximumOutOfOrderMillis;
    }
    public long idleTimeoutMillis() { return idleTimeoutMillis; }
    public InvalidTimestampPolicy invalidTimestampPolicy() {
        return invalidTimestampPolicy;
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamEventTimeSpec)) return false;
        StreamEventTimeSpec that = (StreamEventTimeSpec) other;
        return maximumOutOfOrderMillis == that.maximumOutOfOrderMillis
                && idleTimeoutMillis == that.idleTimeoutMillis
                && timestampExpression.equals(that.timestampExpression)
                && watermarkMode == that.watermarkMode
                && invalidTimestampPolicy == that.invalidTimestampPolicy;
    }

    @Override public int hashCode() {
        return Objects.hash(timestampExpression, watermarkMode,
                maximumOutOfOrderMillis, idleTimeoutMillis,
                invalidTimestampPolicy);
    }
}
