package io.rowweave.run;

/** Lifecycle of Run history; under DELETING the snapshot is still queryable but the event history is frozen. */
public enum RunLifecycleState {
    ACTIVE,
    TERMINAL,
    DELETING
}
