package io.rowweave.monitor;

/** How a metric value should be interpreted and exported. */
public enum MetricKind {
    /** Point-in-time value that can increase or decrease. */
    GAUGE,
    /** Monotonically non-decreasing value since job start. */
    COUNTER
}
