package io.rowweave.stream.api;

import java.util.Objects;

/** Per-input hard output quota for a push-based Stream function. */
public final class StreamEmissionLimit {
    public static final int MAX_OUTPUTS_PER_INPUT = 1_000_000;
    public static final long MAX_OUTPUT_BYTES_PER_INPUT = 1024L * 1024L * 1024L;

    private final int maximumOutputsPerInput;
    private final long maximumOutputBytesPerInput;

    private StreamEmissionLimit(int maximumOutputsPerInput,
            long maximumOutputBytesPerInput) {
        if (maximumOutputsPerInput <= 0
                || maximumOutputsPerInput > MAX_OUTPUTS_PER_INPUT
                || maximumOutputBytesPerInput <= 0L
                || maximumOutputBytesPerInput > MAX_OUTPUT_BYTES_PER_INPUT) {
            throw new IllegalArgumentException(
                    "Stream emission limits must be positive and bounded");
        }
        this.maximumOutputsPerInput = maximumOutputsPerInput;
        this.maximumOutputBytesPerInput = maximumOutputBytesPerInput;
    }

    public static StreamEmissionLimit of(int maximumOutputsPerInput,
            long maximumOutputBytesPerInput) {
        return new StreamEmissionLimit(maximumOutputsPerInput,
                maximumOutputBytesPerInput);
    }

    public int maximumOutputsPerInput() { return maximumOutputsPerInput; }
    public long maximumOutputBytesPerInput() {
        return maximumOutputBytesPerInput;
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamEmissionLimit)) return false;
        StreamEmissionLimit that = (StreamEmissionLimit) other;
        return maximumOutputsPerInput == that.maximumOutputsPerInput
                && maximumOutputBytesPerInput
                == that.maximumOutputBytesPerInput;
    }

    @Override public int hashCode() {
        return Objects.hash(maximumOutputsPerInput,
                maximumOutputBytesPerInput);
    }
}
