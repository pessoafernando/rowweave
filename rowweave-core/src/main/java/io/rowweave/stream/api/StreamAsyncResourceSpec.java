package io.rowweave.stream.api;

import java.util.Objects;

/** Capacity, timeout, retry and result ceilings for supervised async reads. */
public final class StreamAsyncResourceSpec {
    public static final int MAX_CONCURRENCY = 65_536;
    public static final long MAX_PENDING_BYTES = 1024L * 1024L * 1024L * 1024L;
    public static final long MAX_TIMEOUT_MILLIS = 7L * 24L * 60L * 60L * 1000L;
    public static final int MAX_ATTEMPTS = 100;

    private final int maximumConcurrency;
    private final long maximumPendingBytes;
    private final long timeoutMillis;
    private final int maximumAttempts;
    private final long initialBackoffMillis;
    private final long maximumBackoffMillis;
    private final StreamEmissionLimit resultEmissionLimit;

    private StreamAsyncResourceSpec(int maximumConcurrency,
            long maximumPendingBytes, long timeoutMillis, int maximumAttempts,
            long initialBackoffMillis, long maximumBackoffMillis,
            StreamEmissionLimit resultEmissionLimit) {
        if (maximumConcurrency <= 0 || maximumConcurrency > MAX_CONCURRENCY
                || maximumPendingBytes <= 0L
                || maximumPendingBytes > MAX_PENDING_BYTES
                || timeoutMillis <= 0L || timeoutMillis > MAX_TIMEOUT_MILLIS
                || maximumAttempts <= 0 || maximumAttempts > MAX_ATTEMPTS
                || initialBackoffMillis < 0L
                || maximumBackoffMillis < initialBackoffMillis
                || maximumBackoffMillis > MAX_TIMEOUT_MILLIS
                || resultEmissionLimit == null) {
            throw new IllegalArgumentException("invalid async resource bounds");
        }
        this.maximumConcurrency = maximumConcurrency;
        this.maximumPendingBytes = maximumPendingBytes;
        this.timeoutMillis = timeoutMillis;
        this.maximumAttempts = maximumAttempts;
        this.initialBackoffMillis = initialBackoffMillis;
        this.maximumBackoffMillis = maximumBackoffMillis;
        this.resultEmissionLimit = resultEmissionLimit;
    }

    public static StreamAsyncResourceSpec of(int maximumConcurrency,
            long maximumPendingBytes, long timeoutMillis, int maximumAttempts,
            long initialBackoffMillis, long maximumBackoffMillis,
            StreamEmissionLimit resultEmissionLimit) {
        return new StreamAsyncResourceSpec(maximumConcurrency,
                maximumPendingBytes, timeoutMillis, maximumAttempts,
                initialBackoffMillis, maximumBackoffMillis,
                resultEmissionLimit);
    }

    public int maximumConcurrency() { return maximumConcurrency; }
    public long maximumPendingBytes() { return maximumPendingBytes; }
    public long timeoutMillis() { return timeoutMillis; }
    public int maximumAttempts() { return maximumAttempts; }
    public long initialBackoffMillis() { return initialBackoffMillis; }
    public long maximumBackoffMillis() { return maximumBackoffMillis; }
    public StreamEmissionLimit resultEmissionLimit() {
        return resultEmissionLimit;
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamAsyncResourceSpec)) return false;
        StreamAsyncResourceSpec that = (StreamAsyncResourceSpec) other;
        return maximumConcurrency == that.maximumConcurrency
                && maximumPendingBytes == that.maximumPendingBytes
                && timeoutMillis == that.timeoutMillis
                && maximumAttempts == that.maximumAttempts
                && initialBackoffMillis == that.initialBackoffMillis
                && maximumBackoffMillis == that.maximumBackoffMillis
                && resultEmissionLimit.equals(that.resultEmissionLimit);
    }

    @Override public int hashCode() {
        return Objects.hash(maximumConcurrency, maximumPendingBytes,
                timeoutMillis, maximumAttempts, initialBackoffMillis,
                maximumBackoffMillis, resultEmissionLimit);
    }
}
