package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RpcRequestPolicyTest {

    @Test
    void masterAndWorkerPoliciesAreExactAndResponseTypesAreNeverRequests() {
        EnumSet<RpcMessage.Type> master = EnumSet.of(
                RpcMessage.Type.SUBMIT_JOB, RpcMessage.Type.SUBMIT_PLAN_ARTIFACT,
                RpcMessage.Type.SUBMIT_RUN, RpcMessage.Type.QUERY_RUN,
                RpcMessage.Type.QUERY_RUN_EVENTS, RpcMessage.Type.CANCEL_RUN,
                RpcMessage.Type.QUERY_QUALITY_GATE_DECISION,
                RpcMessage.Type.SUBMIT_INCREMENTAL_RUN,
                RpcMessage.Type.INITIALIZE_INCREMENTAL_FEED,
                RpcMessage.Type.QUERY_INCREMENTAL_FRONTIER,
                RpcMessage.Type.QUERY_MANIFEST_OUTCOME,
                RpcMessage.Type.QUERY_BATCH_SINK_OUTCOME,
                RpcMessage.Type.REGISTER_WORKER,
                RpcMessage.Type.HEARTBEAT, RpcMessage.Type.DEREGISTER_WORKER,
                RpcMessage.Type.PORT_REPORT, RpcMessage.Type.PROGRESS_REPORT,
                RpcMessage.Type.STAGE_REPORT, RpcMessage.Type.ASSIGNMENT_ACCEPTED);
        EnumSet<RpcMessage.Type> worker = EnumSet.of(
                RpcMessage.Type.ASSIGN_STAGE, RpcMessage.Type.CANCEL_STAGE,
                RpcMessage.Type.COMMIT_PREPARED_OUTPUT, RpcMessage.Type.ROLLBACK_PREPARED_OUTPUT,
                RpcMessage.Type.RENEW_PREPARED_OUTPUT,
                RpcMessage.Type.FINALIZE_PREPARED_OUTPUT, RpcMessage.Type.INSTALL_MANIFEST_MEMBER,
                RpcMessage.Type.PUBLISH_MANIFEST_GROUP, RpcMessage.Type.SEAL_MANIFEST_MEMBER,
                RpcMessage.Type.RECONCILE_DURABLE_STAGE,
                RpcMessage.Type.EXECUTE_PROVIDER_GROUP,
                RpcMessage.Type.EXECUTE_INCREMENTAL_SOURCE_ACTION,
                RpcMessage.Type.INSTALL_INCREMENTAL_CONTROL_MEMBER,
                RpcMessage.Type.PUBLISH_INCREMENTAL_MANIFEST);

        for (RpcMessage.Type type : RpcMessage.Type.values()) {
            assertEquals(master.contains(type), RpcRequestPolicy.MASTER.allows(type), "master " + type);
            assertEquals(worker.contains(type), RpcRequestPolicy.WORKER.allows(type), "worker " + type);
        }

        for (RpcMessage.Type response : EnumSet.of(
                RpcMessage.Type.JOB_RESULT, RpcMessage.Type.ACK,
                RpcMessage.Type.MANIFEST_MEMBER_INSTALLED, RpcMessage.Type.MANIFEST_OUTCOME,
                RpcMessage.Type.RUN_SNAPSHOT, RpcMessage.Type.DURABLE_STAGE_RECONCILED,
                RpcMessage.Type.BATCH_SINK_OUTCOME, RpcMessage.Type.RUN_EVENTS_PAGE,
                RpcMessage.Type.RUN_CANCELLATION_RESULT,
                RpcMessage.Type.QUALITY_GATE_DECISION,
                RpcMessage.Type.PROVIDER_GROUP_RESULT,
                RpcMessage.Type.INCREMENTAL_FRONTIER_VIEW,
                RpcMessage.Type.INCREMENTAL_SOURCE_ACTION_RESULT)) {
            assertFalse(RpcRequestPolicy.GENERAL.allows(response), response.name());
        }
        assertTrue(RpcRequestPolicy.GENERAL.allows(RpcMessage.Type.SUBMIT_JOB));
    }
}

