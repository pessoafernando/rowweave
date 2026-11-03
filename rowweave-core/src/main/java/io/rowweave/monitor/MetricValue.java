package io.rowweave.monitor;

import java.util.Objects;

/**
 * Immutable typed metric observation. The value is stored as {@code double}
 * for Prometheus compatibility; {@code longValue()} casts back for callers
 * that need the raw integer form.
 */
public final class MetricValue {

    private final String name;
    private final MetricKind kind;
    private final double value;
    private final String unit;
    private final long timestamp;

    public MetricValue(String name, MetricKind kind, long value,
                       String unit, long timestamp) {
        this(name, kind, (double) value, unit, timestamp);
    }

    public MetricValue(String name, MetricKind kind, double value,
                       String unit, long timestamp) {
        if (name == null || name.codePoints().allMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException("metric name is required");
        }
        if (kind == null) {
            throw new IllegalArgumentException("metric kind is required");
        }
        if (unit == null || unit.codePoints().allMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException("metric unit is required");
        }
        this.name = name;
        this.kind = kind;
        this.value = value;
        this.unit = unit;
        this.timestamp = timestamp;
    }

    public String name() { return name; }
    public MetricKind kind() { return kind; }
    public double value() { return value; }
    public long longValue() { return (long) value; }
    public String unit() { return unit; }
    public long timestamp() { return timestamp; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof MetricValue)) return false;
        MetricValue that = (MetricValue) other;
        return name.equals(that.name)
                && kind == that.kind
                && Double.doubleToLongBits(value) == Double.doubleToLongBits(that.value)
                && unit.equals(that.unit)
                && timestamp == that.timestamp;
    }

    @Override public int hashCode() {
        return Objects.hash(name, kind, Double.doubleToLongBits(value), unit, timestamp);
    }

    @Override public String toString() {
        return name + "=" + value + " " + unit + " (" + kind + ")";
    }
}
