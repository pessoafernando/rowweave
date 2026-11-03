package io.rowweave.cluster.rpc;

/** The single lifecycle owner of control plane resident memory. */
public enum ControlResidentOwner {
    DECODE_OWNED,
    HANDLER_OWNED,
    RUN_OWNED,
    ASSIGNMENT_OWNED,
    RELEASED
}
