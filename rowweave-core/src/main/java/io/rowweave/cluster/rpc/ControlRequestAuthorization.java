package io.rowweave.cluster.rpc;

import java.util.EnumSet;

/** Closed allowlist over the MTLS endpoint, the certificate role and the request ordinal. */
public final class ControlRequestAuthorization {
    private static final EnumSet<RpcMessage.Type> CLIENT_TO_MASTER = EnumSet.of(
            RpcMessage.Type.SUBMIT_PLAN_ARTIFACT, RpcMessage.Type.SUBMIT_RUN,
            RpcMessage.Type.QUERY_RUN, RpcMessage.Type.QUERY_RUN_EVENTS,
            RpcMessage.Type.QUERY_QUALITY_GATE_DECISION,
            RpcMessage.Type.SUBMIT_INCREMENTAL_RUN,
            RpcMessage.Type.INITIALIZE_INCREMENTAL_FEED,
            RpcMessage.Type.QUERY_INCREMENTAL_FRONTIER,
            RpcMessage.Type.CANCEL_RUN,
            RpcMessage.Type.QUERY_MANIFEST_OUTCOME,
            RpcMessage.Type.QUERY_BATCH_SINK_OUTCOME);
    private static final EnumSet<RpcMessage.Type> WORKER_TO_MASTER = EnumSet.of(
            RpcMessage.Type.REGISTER_WORKER, RpcMessage.Type.HEARTBEAT,
            RpcMessage.Type.DEREGISTER_WORKER, RpcMessage.Type.PORT_REPORT,
            RpcMessage.Type.PROGRESS_REPORT, RpcMessage.Type.STAGE_REPORT,
            RpcMessage.Type.ASSIGNMENT_ACCEPTED);
    private static final EnumSet<RpcMessage.Type> MASTER_TO_WORKER = EnumSet.of(
            RpcMessage.Type.ASSIGN_STAGE, RpcMessage.Type.CANCEL_STAGE,
            RpcMessage.Type.COMMIT_PREPARED_OUTPUT, RpcMessage.Type.ROLLBACK_PREPARED_OUTPUT,
            RpcMessage.Type.RENEW_PREPARED_OUTPUT,
            RpcMessage.Type.FINALIZE_PREPARED_OUTPUT,
            RpcMessage.Type.INSTALL_MANIFEST_MEMBER,
            RpcMessage.Type.PUBLISH_MANIFEST_GROUP,
            RpcMessage.Type.SEAL_MANIFEST_MEMBER,
            RpcMessage.Type.RECONCILE_DURABLE_STAGE,
            RpcMessage.Type.EXECUTE_PROVIDER_GROUP,
            RpcMessage.Type.EXECUTE_INCREMENTAL_SOURCE_ACTION,
            RpcMessage.Type.INSTALL_INCREMENTAL_CONTROL_MEMBER,
            RpcMessage.Type.PUBLISH_INCREMENTAL_MANIFEST);

    private ControlRequestAuthorization() {
    }

    public static boolean allows(RpcRequestPolicy endpoint,
                                 EnumSet<ControlRole> peerRoles,
                                 RpcMessage.Type type) {
        if (endpoint == null || peerRoles == null || type == null) return false;
        if ((endpoint == RpcRequestPolicy.MASTER || endpoint == RpcRequestPolicy.GENERAL)
                && ((peerRoles.contains(ControlRole.CLIENT) && CLIENT_TO_MASTER.contains(type))
                || (peerRoles.contains(ControlRole.WORKER) && WORKER_TO_MASTER.contains(type)))) {
            return true;
        }
        return (endpoint == RpcRequestPolicy.WORKER || endpoint == RpcRequestPolicy.GENERAL)
                && peerRoles.contains(ControlRole.MASTER)
                && MASTER_TO_WORKER.contains(type);
    }

    public static void requireAllowed(RpcRequestPolicy endpoint,
                                      EnumSet<ControlRole> peerRoles,
                                      RpcMessage.Type type) throws java.io.IOException {
        if (!allows(endpoint, peerRoles, type)) {
            throw new java.io.IOException("mTLS RPC role authorization rejected request type: "
                    + type);
        }
    }

    static void requireRemoteEndpoint(EnumSet<ControlRole> peerRoles,
                                      RpcMessage.Type requestType) throws java.io.IOException {
        ControlRole required;
        if (CLIENT_TO_MASTER.contains(requestType) || WORKER_TO_MASTER.contains(requestType)) {
            required = ControlRole.MASTER;
        } else if (MASTER_TO_WORKER.contains(requestType)) {
            required = ControlRole.WORKER;
        } else {
            throw new java.io.IOException(
                    "mTLS client request type has no remote endpoint role: " + requestType);
        }
        if (peerRoles == null || !peerRoles.contains(required)) {
            throw new java.io.IOException(
                    "mTLS remote certificate lacks required " + required + " role");
        }
    }
}
