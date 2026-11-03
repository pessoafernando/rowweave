package io.rowweave.portable;

import java.util.Objects;

/** Bounded function claims used for admission before loading application code. */
public final class FunctionResourceProfile {
    public static final long MAX_HEAP_BYTES = 8L * 1024 * 1024 * 1024;
    public static final long MAX_DIRECT_BYTES = 8L * 1024 * 1024 * 1024;
    public static final int MAX_OUTPUT_ROWS_PER_INPUT = 1_000_000;

    private final long maximumHeapBytes;
    private final long maximumDirectBytes;
    private final int maximumOutputRowsPerInput;

    public FunctionResourceProfile(
            long maximumHeapBytes, long maximumDirectBytes,
            int maximumOutputRowsPerInput) {
        if (maximumHeapBytes <= 0 || maximumDirectBytes < 0
                || maximumOutputRowsPerInput <= 0) {
            throw PortablePlanText.failure("INVALID_RESOURCE_PROFILE",
                    "function.resources", "resource claims must be positive");
        }
        if (maximumHeapBytes > MAX_HEAP_BYTES
                || maximumDirectBytes > MAX_DIRECT_BYTES
                || maximumOutputRowsPerInput > MAX_OUTPUT_ROWS_PER_INPUT) {
            throw PortablePlanText.failure("LIMIT_EXCEEDED", "function.resources",
                    "resource claims exceed the portable v1 ceiling");
        }
        this.maximumHeapBytes = maximumHeapBytes;
        this.maximumDirectBytes = maximumDirectBytes;
        this.maximumOutputRowsPerInput = maximumOutputRowsPerInput;
    }

    public long maximumHeapBytes() { return maximumHeapBytes; }
    public long maximumDirectBytes() { return maximumDirectBytes; }
    public int maximumOutputRowsPerInput() { return maximumOutputRowsPerInput; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof FunctionResourceProfile)) return false;
        FunctionResourceProfile that = (FunctionResourceProfile) other;
        return maximumHeapBytes == that.maximumHeapBytes
                && maximumDirectBytes == that.maximumDirectBytes
                && maximumOutputRowsPerInput == that.maximumOutputRowsPerInput;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maximumHeapBytes, maximumDirectBytes,
                maximumOutputRowsPerInput);
    }
}
