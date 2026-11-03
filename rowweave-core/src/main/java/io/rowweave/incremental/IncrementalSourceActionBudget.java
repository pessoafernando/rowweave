package io.rowweave.incremental;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Objects;

/** Per-action resource declaration beneath the Phase-2 engine ceilings. */
public final class IncrementalSourceActionBudget implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final long MAX_NORMAL_RESIDENT_BYTES = 1024L * 1024L;
    public static final long MAX_LARGE_RESIDENT_BYTES = 24L * 1024L * 1024L;
    public static final long MAX_ACTION_TIMEOUT_MILLIS = 60_000L;

    private final long maxNormalResidentBytes;
    private final long maxLargeResidentBytes;
    private final int maxProviderHandles;
    private final long actionTimeoutMillis;

    public IncrementalSourceActionBudget(
            long maxNormalResidentBytes, long maxLargeResidentBytes,
            int maxProviderHandles, long actionTimeoutMillis) {
        if (maxNormalResidentBytes <= 0L
                || maxNormalResidentBytes > MAX_NORMAL_RESIDENT_BYTES) {
            throw new IllegalArgumentException(
                    "incremental source normal resident budget is invalid");
        }
        if (maxLargeResidentBytes < maxNormalResidentBytes
                || maxLargeResidentBytes > MAX_LARGE_RESIDENT_BYTES) {
            throw new IllegalArgumentException(
                    "incremental source large resident budget is invalid");
        }
        if (maxProviderHandles != 1) {
            throw new IllegalArgumentException(
                    "incremental source action must use exactly one provider handle");
        }
        if (actionTimeoutMillis <= 0L
                || actionTimeoutMillis > MAX_ACTION_TIMEOUT_MILLIS) {
            throw new IllegalArgumentException(
                    "incremental source action timeout is invalid");
        }
        this.maxNormalResidentBytes = maxNormalResidentBytes;
        this.maxLargeResidentBytes = maxLargeResidentBytes;
        this.maxProviderHandles = maxProviderHandles;
        this.actionTimeoutMillis = actionTimeoutMillis;
    }

    public long maxNormalResidentBytes() { return maxNormalResidentBytes; }
    public long maxLargeResidentBytes() { return maxLargeResidentBytes; }
    public int maxProviderHandles() { return maxProviderHandles; }
    public long actionTimeoutMillis() { return actionTimeoutMillis; }

    private Object readResolve() throws ObjectStreamException {
        return new IncrementalSourceActionBudget(
                maxNormalResidentBytes, maxLargeResidentBytes,
                maxProviderHandles, actionTimeoutMillis);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalSourceActionBudget)) return false;
        IncrementalSourceActionBudget that =
                (IncrementalSourceActionBudget) other;
        return maxNormalResidentBytes == that.maxNormalResidentBytes
                && maxLargeResidentBytes == that.maxLargeResidentBytes
                && maxProviderHandles == that.maxProviderHandles
                && actionTimeoutMillis == that.actionTimeoutMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxNormalResidentBytes, maxLargeResidentBytes,
                maxProviderHandles, actionTimeoutMillis);
    }
}
