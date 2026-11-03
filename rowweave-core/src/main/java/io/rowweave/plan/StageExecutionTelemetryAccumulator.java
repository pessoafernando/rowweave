package io.rowweave.plan;

/** Non-throwing checked aggregation. Invalid input or overflow invalidates the whole snapshot. */
public final class StageExecutionTelemetryAccumulator {
    private final long[] totals = new long[6];
    private boolean completeSeen;
    private boolean notExecutedSeen;
    private boolean invalid;
    private StageExecutionTelemetry frozen;

    public synchronized void add(StageExecutionTelemetry telemetry) {
        if (frozen != null) return;
        if (telemetry == null || invalid) {
            invalid = true;
            return;
        }
        if (telemetry.coverage() == TelemetryCoverage.NOT_EXECUTED) {
            notExecutedSeen = true;
            if (completeSeen) invalid = true;
            return;
        }
        if (telemetry.coverage() != TelemetryCoverage.COMPLETE || notExecutedSeen) {
            invalid = true;
            return;
        }
        completeSeen = true;
        long[] values = {
                telemetry.managedPeakReservedBytes(), telemetry.managedAcquiredBytes(),
                telemetry.pressureSpillCount(), telemetry.spillRunCount(),
                telemetry.spillLogicalBytesWritten(), telemetry.spillPeakReservedBytes()
        };
        for (int i = 0; i < totals.length; i++) {
            if (Long.MAX_VALUE - totals[i] < values[i]) {
                invalid = true;
                return;
            }
        }
        for (int i = 0; i < totals.length; i++) totals[i] += values[i];
    }

    public synchronized StageExecutionTelemetry freeze() {
        if (frozen != null) return frozen;
        if (invalid || (!completeSeen && !notExecutedSeen)) {
            frozen = StageExecutionTelemetry.unavailable();
        } else if (notExecutedSeen) {
            frozen = StageExecutionTelemetry.notExecuted();
        } else {
            frozen = StageExecutionTelemetry.complete(
                    totals[0], totals[1], totals[2], totals[3], totals[4], totals[5]);
        }
        return frozen;
    }

    public synchronized StageExecutionTelemetry snapshot() { return freeze(); }
}
