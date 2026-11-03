package io.rowweave.run;

/** Authoritative state of one logical Run. */
public enum RunStatus {
    READY,
    RUNNING,
    CANCELING,
    RECONCILING,
    SUCCEEDED,
    FAILED,
    CANCELED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == CANCELED;
    }
}
