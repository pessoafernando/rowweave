package io.rowweave.plan;

/** Explicit availability for execution telemetry; zero is never an unknown sentinel. */
public enum TelemetryCoverage {
    COMPLETE,
    PARTIAL,
    NOT_EXECUTED,
    NOT_APPLICABLE,
    UNAVAILABLE
}
