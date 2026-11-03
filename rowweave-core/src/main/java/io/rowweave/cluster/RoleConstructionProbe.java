package io.rowweave.cluster;

/** Compile-time constrained construction fault injection; never reads runtime configuration. */
@FunctionalInterface
interface RoleConstructionProbe {
    RoleConstructionProbe NOOP = step -> { };

    void after(Enum<?> step);
}

enum MasterConstructionStep {
    OWNED_RUNTIME,
    RPC_CACHE,
    TERMINAL_JANITOR,
    ARTIFACT_JANITOR,
    RECOVERY_INVENTORY,
    ASSIGN_DISPATCHER,
    CANCEL_COORDINATOR,
    ASSIGN_LANES,
    RPC_BOUND,
    RPC_ACTIVE,
    HEARTBEAT
}

enum WorkerConstructionStep {
    OWNED_RUNTIME,
    LIVE_CLAIM,
    STAGE_POOL,
    LOCAL_SHUFFLE,
    REPORT_CACHE,
    HEARTBEAT_CACHE,
    DATA_INBOUND,
    DATA_AUTHZ,
    MUX_POOL,
    PREPARED_OUTPUTS,
    RPC_BOUND,
    RPC_ACTIVE,
    LEADER_SUBSCRIPTION,
    WORKER_LEASE,
    MASTER_REGISTRATION,
    HEARTBEAT
}
