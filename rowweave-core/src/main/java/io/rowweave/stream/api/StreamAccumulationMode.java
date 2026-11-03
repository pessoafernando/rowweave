package io.rowweave.stream.api;

/** Whether a generic window retains emitted contents until deterministic cleanup. */
public enum StreamAccumulationMode {
    DISCARDING,
    ACCUMULATING
}
