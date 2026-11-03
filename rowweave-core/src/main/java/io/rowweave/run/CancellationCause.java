package io.rowweave.run;

/** What triggered the two-phase stop; a Run enters CANCELING before any terminal state. */
public enum CancellationCause {
    USER,
    TIMEOUT,
    FENCE
}
