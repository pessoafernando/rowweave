package io.rowweave.run;

/** Machine-readable disposition carried only by durable SUBMIT_RUN results. */
public enum DurableRunDisposition {
    SUCCEEDED,
    REJECTED_BEFORE_ACCEPTANCE,
    TERMINAL_FAILED,
    ACCEPTED_NON_TERMINAL,
    UNKNOWN
}
