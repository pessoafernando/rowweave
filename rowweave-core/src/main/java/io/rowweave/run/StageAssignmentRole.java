package io.rowweave.run;

/** Stable Stage role in the durable manifest, kept separate from the execution command. */
public enum StageAssignmentRole {
    PRODUCER,
    CONSUMER,
    ROUTER,
    KEYED_OP,
    SINK,
    WHOLE_GRAPH,
    JOIN_CONSUMER,
    SOURCE_STAGE,
    SYSTEM_CONTROL
}
