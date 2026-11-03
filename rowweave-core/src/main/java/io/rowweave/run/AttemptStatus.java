package io.rowweave.run;

/** Authoritative state of one physical execution generation inside a Run. */
public enum AttemptStatus {
    CREATED,
    DISPATCHING,
    RUNNING,
    COMMITTING,
    RECONCILING,
    CANCELING,
    SUCCEEDED,
    FAILED,
    CANCELED,
    TIMED_OUT,
    FENCED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == CANCELED
                || this == TIMED_OUT || this == FENCED;
    }
}
