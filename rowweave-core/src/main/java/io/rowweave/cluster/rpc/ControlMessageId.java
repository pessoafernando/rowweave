package io.rowweave.cluster.rpc;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Explicit v1 control protocol IDs. Legacy enum ordinals are intentionally not exposed here. */
public final class ControlMessageId {
    private static final Map<Integer, RpcMessage.Type> TYPES_BY_ID;

    static {
        Map<Integer, RpcMessage.Type> values = new LinkedHashMap<>();
        for (RpcMessage.Type type : RpcMessage.Type.values()) {
            int id = explicitId(type);
            if (values.put(id, type) != null) {
                throw new ExceptionInInitializerError("duplicate control message ID " + id);
            }
        }
        TYPES_BY_ID = Collections.unmodifiableMap(values);
    }

    private ControlMessageId() { }

    public static int wireId(RpcMessage.Type type) {
        if (type == null) throw new IllegalArgumentException("message type is required");
        return explicitId(type);
    }

    public static RpcMessage.Type fromWireId(int wireId) {
        RpcMessage.Type type = TYPES_BY_ID.get(wireId);
        if (type == null) {
            throw new IllegalArgumentException("unknown control message ID: " + wireId);
        }
        return type;
    }

    public static Set<Integer> allWireIds() { return TYPES_BY_ID.keySet(); }

    private static int explicitId(RpcMessage.Type type) {
        switch (type) {
            case REGISTER_WORKER: return 0x1001;
            case HEARTBEAT: return 0x1002;
            case SUBMIT_JOB: return 0x1003;
            case ASSIGN_STAGE: return 0x1004;
            case STAGE_REPORT: return 0x1005;
            case JOB_RESULT: return 0x1006;
            case ACK: return 0x1007;
            case PORT_REPORT: return 0x1008;
            case CANCEL_STAGE: return 0x1009;
            case PROGRESS_REPORT: return 0x100a;
            case DEREGISTER_WORKER: return 0x100b;
            case COMMIT_PREPARED_OUTPUT: return 0x100c;
            case ROLLBACK_PREPARED_OUTPUT: return 0x100d;
            case FINALIZE_PREPARED_OUTPUT: return 0x100e;
            case INSTALL_MANIFEST_MEMBER: return 0x100f;
            case MANIFEST_MEMBER_INSTALLED: return 0x1010;
            case PUBLISH_MANIFEST_GROUP: return 0x1011;
            case QUERY_MANIFEST_OUTCOME: return 0x1012;
            case MANIFEST_OUTCOME: return 0x1013;
            case SUBMIT_PLAN_ARTIFACT: return 0x1014;
            case SUBMIT_RUN: return 0x1015;
            case QUERY_RUN: return 0x1016;
            case RUN_SNAPSHOT: return 0x1017;
            case ASSIGNMENT_ACCEPTED: return 0x1018;
            case SEAL_MANIFEST_MEMBER: return 0x1019;
            case RECONCILE_DURABLE_STAGE: return 0x101a;
            case DURABLE_STAGE_RECONCILED: return 0x101b;
            case QUERY_BATCH_SINK_OUTCOME: return 0x101c;
            case BATCH_SINK_OUTCOME: return 0x101d;
            case QUERY_RUN_EVENTS: return 0x101e;
            case RUN_EVENTS_PAGE: return 0x101f;
            case CANCEL_RUN: return 0x1020;
            case RUN_CANCELLATION_RESULT: return 0x1021;
            case WORKER_REGISTERED: return 0x1022;
            case QUERY_QUALITY_GATE_DECISION: return 0x1023;
            case QUALITY_GATE_DECISION: return 0x1024;
            case EXECUTE_PROVIDER_GROUP: return 0x1025;
            case PROVIDER_GROUP_RESULT: return 0x1026;
            case QUERY_INCREMENTAL_FRONTIER: return 0x1027;
            case INCREMENTAL_FRONTIER_VIEW: return 0x1028;
            case SUBMIT_INCREMENTAL_RUN: return 0x1029;
            case EXECUTE_INCREMENTAL_SOURCE_ACTION: return 0x102a;
            case INCREMENTAL_SOURCE_ACTION_RESULT: return 0x102b;
            case INSTALL_INCREMENTAL_CONTROL_MEMBER: return 0x102c;
            case PUBLISH_INCREMENTAL_MANIFEST: return 0x102d;
            case INITIALIZE_INCREMENTAL_FEED: return 0x102e;
            case RENEW_PREPARED_OUTPUT: return 0x102f;
            default: throw new IllegalArgumentException("unassigned control message type ID: " + type);
        }
    }
}
