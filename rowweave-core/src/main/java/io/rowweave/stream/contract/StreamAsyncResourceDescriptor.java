package io.rowweave.stream.contract;

import io.rowweave.stream.api.StreamAsyncResourceSpec;
import io.rowweave.stream.api.StreamEmissionLimit;

import java.util.Objects;

/** Portable capacity and retry limits for async wait. */
public final class StreamAsyncResourceDescriptor {
    private final int maximumConcurrency;
    private final long maximumPendingBytes;
    private final long timeoutMillis;
    private final int maximumAttempts;
    private final long initialBackoffMillis;
    private final long maximumBackoffMillis;
    private final int maximumResults;
    private final long maximumResultBytes;

    public StreamAsyncResourceDescriptor(int maximumConcurrency,
            long maximumPendingBytes, long timeoutMillis, int maximumAttempts,
            long initialBackoffMillis, long maximumBackoffMillis,
            int maximumResults, long maximumResultBytes) {
        if (maximumConcurrency <= 0
                || maximumConcurrency > StreamAsyncResourceSpec.MAX_CONCURRENCY
                || maximumPendingBytes <= 0L
                || maximumPendingBytes
                > StreamAsyncResourceSpec.MAX_PENDING_BYTES
                || timeoutMillis <= 0L
                || timeoutMillis > StreamAsyncResourceSpec.MAX_TIMEOUT_MILLIS
                || maximumAttempts <= 0
                || maximumAttempts > StreamAsyncResourceSpec.MAX_ATTEMPTS
                || initialBackoffMillis < 0L
                || maximumBackoffMillis < initialBackoffMillis
                || maximumBackoffMillis
                > StreamAsyncResourceSpec.MAX_TIMEOUT_MILLIS
                || maximumResults <= 0
                || maximumResults > StreamEmissionLimit.MAX_OUTPUTS_PER_INPUT
                || maximumResultBytes <= 0L
                || maximumResultBytes
                > StreamEmissionLimit.MAX_OUTPUT_BYTES_PER_INPUT) {
            throw StreamContractSupport.failure("INVALID_ASYNC_RESOURCE_LIMIT",
                    "nodes.async.resources", "async resources are invalid");
        }
        this.maximumConcurrency = maximumConcurrency;
        this.maximumPendingBytes = maximumPendingBytes;
        this.timeoutMillis = timeoutMillis;
        this.maximumAttempts = maximumAttempts;
        this.initialBackoffMillis = initialBackoffMillis;
        this.maximumBackoffMillis = maximumBackoffMillis;
        this.maximumResults = maximumResults;
        this.maximumResultBytes = maximumResultBytes;
    }

    public int maximumConcurrency() { return maximumConcurrency; }
    public long maximumPendingBytes() { return maximumPendingBytes; }
    public long timeoutMillis() { return timeoutMillis; }
    public int maximumAttempts() { return maximumAttempts; }
    public long initialBackoffMillis() { return initialBackoffMillis; }
    public long maximumBackoffMillis() { return maximumBackoffMillis; }
    public int maximumResults() { return maximumResults; }
    public long maximumResultBytes() { return maximumResultBytes; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamAsyncResourceDescriptor)) return false;
        StreamAsyncResourceDescriptor that =
                (StreamAsyncResourceDescriptor) other;
        return maximumConcurrency == that.maximumConcurrency
                && maximumPendingBytes == that.maximumPendingBytes
                && timeoutMillis == that.timeoutMillis
                && maximumAttempts == that.maximumAttempts
                && initialBackoffMillis == that.initialBackoffMillis
                && maximumBackoffMillis == that.maximumBackoffMillis
                && maximumResults == that.maximumResults
                && maximumResultBytes == that.maximumResultBytes;
    }

    @Override public int hashCode() {
        return Objects.hash(maximumConcurrency, maximumPendingBytes,
                timeoutMillis, maximumAttempts, initialBackoffMillis,
                maximumBackoffMillis, maximumResults, maximumResultBytes);
    }
}
