package io.rowweave.stream.api;

import java.util.Objects;

/** Provider-emitted event-time progress with a finite idle convergence bound. */
public final class StreamWatermarkPolicy {
    private static final long MAX_IDLE_TIMEOUT_MILLIS = 7L * 24L * 60L * 60L * 1000L;
    private final long idleTimeoutMillis;

    private StreamWatermarkPolicy(long idleTimeoutMillis) {
        if (idleTimeoutMillis <= 0L
                || idleTimeoutMillis > MAX_IDLE_TIMEOUT_MILLIS) {
            throw new IllegalArgumentException(
                    "watermark idle timeout must be within 1ms..7d");
        }
        this.idleTimeoutMillis = idleTimeoutMillis;
    }

    public static StreamWatermarkPolicy providerEvents(long idleTimeoutMillis) {
        return new StreamWatermarkPolicy(idleTimeoutMillis);
    }

    public long idleTimeoutMillis() { return idleTimeoutMillis; }

    @Override public boolean equals(Object other) {
        return other instanceof StreamWatermarkPolicy
                && idleTimeoutMillis
                == ((StreamWatermarkPolicy) other).idleTimeoutMillis;
    }

    @Override public int hashCode() { return Objects.hash(idleTimeoutMillis); }
}
