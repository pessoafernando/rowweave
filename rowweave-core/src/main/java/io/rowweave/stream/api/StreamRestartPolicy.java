package io.rowweave.stream.api;

import java.util.Objects;

/** Whole-graph fenced restart policy. */
public final class StreamRestartPolicy {
    private final int maximumAttempts;
    private final long backoffMillis;

    private StreamRestartPolicy(int maximumAttempts, long backoffMillis) {
        if (maximumAttempts < 0 || maximumAttempts > 1_000
                || backoffMillis < 0L || backoffMillis > 86_400_000L) {
            throw new IllegalArgumentException("invalid restart policy");
        }
        this.maximumAttempts = maximumAttempts;
        this.backoffMillis = backoffMillis;
    }

    public static StreamRestartPolicy fixedDelay(int maximumAttempts,
                                                  long backoffMillis) {
        return new StreamRestartPolicy(maximumAttempts, backoffMillis);
    }

    public int maximumAttempts() { return maximumAttempts; }
    public long backoffMillis() { return backoffMillis; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamRestartPolicy)) return false;
        StreamRestartPolicy that = (StreamRestartPolicy) other;
        return maximumAttempts == that.maximumAttempts
                && backoffMillis == that.backoffMillis;
    }

    @Override public int hashCode() {
        return Objects.hash(maximumAttempts, backoffMillis);
    }
}
