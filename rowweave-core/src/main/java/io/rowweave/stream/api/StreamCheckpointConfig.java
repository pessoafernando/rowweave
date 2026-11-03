package io.rowweave.stream.api;

import java.util.Objects;

/** Checkpoint mode, scheduling and retention contract for one Stream job. */
public final class StreamCheckpointConfig {
    private final StreamCheckpointMode mode;
    private final long intervalMillis;
    private final long timeoutMillis;
    private final int retainedCompleted;

    private StreamCheckpointConfig(StreamCheckpointMode mode,
                                   long intervalMillis, long timeoutMillis,
                                   int retainedCompleted) {
        if (mode == null || intervalMillis < 0L || timeoutMillis <= 0L
                || retainedCompleted < 1 || retainedCompleted > 128) {
            throw new IllegalArgumentException("invalid checkpoint configuration");
        }
        this.mode = mode;
        this.intervalMillis = intervalMillis;
        this.timeoutMillis = timeoutMillis;
        this.retainedCompleted = retainedCompleted;
    }

    public static StreamCheckpointConfig manualOnly(long timeoutMillis,
                                                     int retainedCompleted) {
        return new StreamCheckpointConfig(StreamCheckpointMode.ALIGNED,
                0L, timeoutMillis, retainedCompleted);
    }

    public static StreamCheckpointConfig periodic(long intervalMillis,
            long timeoutMillis, int retainedCompleted) {
        if (intervalMillis <= 0L) {
            throw new IllegalArgumentException("periodic checkpoint interval must be positive");
        }
        return new StreamCheckpointConfig(StreamCheckpointMode.ALIGNED,
                intervalMillis, timeoutMillis,
                retainedCompleted);
    }

    public static StreamCheckpointConfig manualOnlyUnaligned(
            long timeoutMillis, int retainedCompleted) {
        return new StreamCheckpointConfig(StreamCheckpointMode.UNALIGNED,
                0L, timeoutMillis, retainedCompleted);
    }

    public static StreamCheckpointConfig periodicUnaligned(
            long intervalMillis, long timeoutMillis, int retainedCompleted) {
        if (intervalMillis <= 0L) {
            throw new IllegalArgumentException(
                    "periodic checkpoint interval must be positive");
        }
        return new StreamCheckpointConfig(StreamCheckpointMode.UNALIGNED,
                intervalMillis, timeoutMillis, retainedCompleted);
    }

    public StreamCheckpointMode mode() { return mode; }
    public boolean periodic() { return intervalMillis > 0L; }
    public long intervalMillis() { return intervalMillis; }
    public long timeoutMillis() { return timeoutMillis; }
    public int retainedCompleted() { return retainedCompleted; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamCheckpointConfig)) return false;
        StreamCheckpointConfig that = (StreamCheckpointConfig) other;
        return mode == that.mode
                && intervalMillis == that.intervalMillis
                && timeoutMillis == that.timeoutMillis
                && retainedCompleted == that.retainedCompleted;
    }

    @Override public int hashCode() {
        return Objects.hash(mode, intervalMillis, timeoutMillis,
                retainedCompleted);
    }
}
