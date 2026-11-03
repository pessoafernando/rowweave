package io.rowweave.run;

/** immutable terminal outcome for one accepted physical assignment. */
public enum StageTerminalOutcome {
    SUCCEEDED,
    FAILED,
    CANCELED,
    LOST
}
