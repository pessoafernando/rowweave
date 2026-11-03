package io.rowweave.cluster.rpc;

import java.util.EnumSet;

/** Deny-by-default policy for request types per endpoint, layered with certificate roles under mTLS. */
public enum RpcRequestPolicy {
    MASTER(EnumSet.of(
            RpcMessage.Type.SUBMIT_JOB, RpcMessage.Type.SUBMIT_PLAN_ARTIFACT,
            RpcMessage.Type.SUBMIT_RUN, RpcMessage.Type.QUERY_RUN,
            RpcMessage.Type.QUERY_RUN_EVENTS, RpcMessage.Type.CANCEL_RUN,
            RpcMessage.Type.QUERY_QUALITY_GATE_DECISION,
            RpcMessage.Type.SUBMIT_INCREMENTAL_RUN,
            RpcMessage.Type.INITIALIZE_INCREMENTAL_FEED,
            RpcMessage.Type.QUERY_INCREMENTAL_FRONTIER,
            RpcMessage.Type.QUERY_BATCH_SINK_OUTCOME,
            RpcMessage.Type.QUERY_MANIFEST_OUTCOME, RpcMessage.Type.REGISTER_WORKER,
            RpcMessage.Type.HEARTBEAT, RpcMessage.Type.DEREGISTER_WORKER,
            RpcMessage.Type.PORT_REPORT, RpcMessage.Type.PROGRESS_REPORT,
            RpcMessage.Type.STAGE_REPORT, RpcMessage.Type.ASSIGNMENT_ACCEPTED)),
    WORKER(EnumSet.of(
            RpcMessage.Type.ASSIGN_STAGE, RpcMessage.Type.CANCEL_STAGE,
            RpcMessage.Type.COMMIT_PREPARED_OUTPUT, RpcMessage.Type.ROLLBACK_PREPARED_OUTPUT,
            RpcMessage.Type.RENEW_PREPARED_OUTPUT,
            RpcMessage.Type.FINALIZE_PREPARED_OUTPUT, RpcMessage.Type.INSTALL_MANIFEST_MEMBER,
            RpcMessage.Type.PUBLISH_MANIFEST_GROUP, RpcMessage.Type.SEAL_MANIFEST_MEMBER,
            RpcMessage.Type.RECONCILE_DURABLE_STAGE,
            RpcMessage.Type.EXECUTE_PROVIDER_GROUP,
            RpcMessage.Type.EXECUTE_INCREMENTAL_SOURCE_ACTION,
            RpcMessage.Type.INSTALL_INCREMENTAL_CONTROL_MEMBER,
            RpcMessage.Type.PUBLISH_INCREMENTAL_MANIFEST)),
    GENERAL(union(MASTER.allowed, WORKER.allowed));

    private final EnumSet<RpcMessage.Type> allowed;

    RpcRequestPolicy(EnumSet<RpcMessage.Type> allowed) {
        this.allowed = allowed;
    }

    public boolean allows(RpcMessage.Type type) {
        return type != null && allowed.contains(type);
    }

    void requireAllowed(RpcMessage.Type type) throws java.io.IOException {
        if (!allows(type)) {
            throw new java.io.IOException("RPC request type rejected by " + name() + ": " + type);
        }
    }

    public void requireAllowedDirect(RpcMessage.Type type) {
        if (!allows(type)) {
            throw new IllegalArgumentException("RPC request type rejected by " + name() + ": " + type);
        }
    }

    private static EnumSet<RpcMessage.Type> union(EnumSet<RpcMessage.Type> left,
                                                   EnumSet<RpcMessage.Type> right) {
        EnumSet<RpcMessage.Type> result = EnumSet.copyOf(left);
        result.addAll(right);
        return result;
    }
}
