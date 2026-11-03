package io.rowweave;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Objects;

/** Provider-declared hard resource ceilings for one durable Sink action. */
public final class DurableSinkAttemptBudget implements Serializable {

    private static final long serialVersionUID = 1L;
    public static final long MAX_RESIDENT_BYTES = 1024L * 1024L * 1024L;
    public static final int MAX_OPEN_HANDLES = 4096;
    public static final int MAX_LOCATOR_BYTES = 16 * 1024;
    public static final int MAX_CONTROL_BYTES = 16 * 1024 * 1024;
    public static final long MAX_ACTION_TIMEOUT_MILLIS = 24L * 60L * 60L * 1000L;

    private final long maxResidentBytes;
    private final int maxOpenHandles;
    private final int maxLocatorBytes;
    private final int maxControlBytes;
    private final long actionTimeoutMillis;

    public DurableSinkAttemptBudget(
            long maxResidentBytes, int maxOpenHandles, int maxLocatorBytes,
            int maxControlBytes, long actionTimeoutMillis) {
        if (maxResidentBytes <= 0L || maxResidentBytes > MAX_RESIDENT_BYTES) {
            throw new IllegalArgumentException(
                    "durable Sink resident byte budget is invalid");
        }
        if (maxOpenHandles <= 0 || maxOpenHandles > MAX_OPEN_HANDLES) {
            throw new IllegalArgumentException(
                    "durable Sink open handle budget is invalid");
        }
        if (maxLocatorBytes <= 0 || maxLocatorBytes > MAX_LOCATOR_BYTES) {
            throw new IllegalArgumentException(
                    "durable Sink locator byte budget is invalid");
        }
        if (maxControlBytes <= 0 || maxControlBytes > MAX_CONTROL_BYTES) {
            throw new IllegalArgumentException(
                    "durable Sink control byte budget is invalid");
        }
        if (actionTimeoutMillis <= 0L
                || actionTimeoutMillis > MAX_ACTION_TIMEOUT_MILLIS) {
            throw new IllegalArgumentException(
                    "durable Sink action timeout is invalid");
        }
        this.maxResidentBytes = maxResidentBytes;
        this.maxOpenHandles = maxOpenHandles;
        this.maxLocatorBytes = maxLocatorBytes;
        this.maxControlBytes = maxControlBytes;
        this.actionTimeoutMillis = actionTimeoutMillis;
    }

    public long maxResidentBytes() { return maxResidentBytes; }
    public int maxOpenHandles() { return maxOpenHandles; }
    public int maxLocatorBytes() { return maxLocatorBytes; }
    public int maxControlBytes() { return maxControlBytes; }
    public long actionTimeoutMillis() { return actionTimeoutMillis; }

    private Object readResolve() throws ObjectStreamException {
        return new DurableSinkAttemptBudget(maxResidentBytes, maxOpenHandles,
                maxLocatorBytes, maxControlBytes, actionTimeoutMillis);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof DurableSinkAttemptBudget)) return false;
        DurableSinkAttemptBudget that = (DurableSinkAttemptBudget) other;
        return maxResidentBytes == that.maxResidentBytes
                && maxOpenHandles == that.maxOpenHandles
                && maxLocatorBytes == that.maxLocatorBytes
                && maxControlBytes == that.maxControlBytes
                && actionTimeoutMillis == that.actionTimeoutMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxResidentBytes, maxOpenHandles, maxLocatorBytes,
                maxControlBytes, actionTimeoutMillis);
    }
}
