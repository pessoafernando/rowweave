package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControlAuthzPolicyTest {
    private static final String CLIENT = repeat('a');
    private static final String MASTER = repeat('b');
    private static final String WORKER = repeat('c');

    @Test
    void canonicalSetIdIgnoresLineAndRoleOrderButPreservesExactPolicy() {
        ControlAuthzPolicy first = ControlAuthzPolicy.parse(
                WORKER + "=WORKER\n" + CLIENT + "=MASTER,CLIENT\n");
        ControlAuthzPolicy reordered = ControlAuthzPolicy.parse(
                CLIENT + "=CLIENT,MASTER\n" + WORKER + "=WORKER\n");
        ControlAuthzPolicy changed = ControlAuthzPolicy.parse(
                CLIENT + "=CLIENT\n" + WORKER + "=WORKER\n");

        assertEquals(first.authzSetId(), reordered.authzSetId());
        assertFalse(first.authzSetId().equals(changed.authzSetId()));
        assertEquals(EnumSet.of(ControlRole.CLIENT, ControlRole.MASTER),
                first.roles(CLIENT));
        assertTrue(first.roles(MASTER).isEmpty());
    }

    @Test
    void malformedDuplicateUnknownAndEmptyPoliciesFailClosed() {
        assertThrows(IllegalArgumentException.class, () -> ControlAuthzPolicy.parse(""));
        assertThrows(IllegalArgumentException.class,
                () -> ControlAuthzPolicy.parse(CLIENT + "=\n"));
        assertThrows(IllegalArgumentException.class,
                () -> ControlAuthzPolicy.parse(CLIENT + "=ADMIN\n"));
        assertThrows(IllegalArgumentException.class,
                () -> ControlAuthzPolicy.parse(CLIENT + "=CLIENT\n" + CLIENT + "=WORKER\n"));
        assertThrows(IllegalArgumentException.class,
                () -> ControlAuthzPolicy.parse("not-a-fingerprint=CLIENT\n"));
    }

    @Test
    void everyMessageOrdinalHasAnExplicitMtlsRoleDecision() {
        for (RpcMessage.Type type : RpcMessage.Type.values()) {
            boolean clientToMaster = ControlRequestAuthorization.allows(
                    RpcRequestPolicy.MASTER, EnumSet.of(ControlRole.CLIENT), type);
            boolean workerToMaster = ControlRequestAuthorization.allows(
                    RpcRequestPolicy.MASTER, EnumSet.of(ControlRole.WORKER), type);
            boolean masterToWorker = ControlRequestAuthorization.allows(
                    RpcRequestPolicy.WORKER, EnumSet.of(ControlRole.MASTER), type);

            assertEquals(clientRequests().contains(type), clientToMaster, type.name());
            assertEquals(workerRequests().contains(type), workerToMaster, type.name());
            assertEquals(masterRequests().contains(type), masterToWorker, type.name());
            assertFalse(ControlRequestAuthorization.allows(
                    RpcRequestPolicy.WORKER, EnumSet.of(ControlRole.CLIENT, ControlRole.WORKER), type),
                    "non-master certificate must never call Worker: " + type);
        }
    }

    private static EnumSet<RpcMessage.Type> clientRequests() {
        return EnumSet.of(RpcMessage.Type.SUBMIT_PLAN_ARTIFACT, RpcMessage.Type.SUBMIT_RUN,
                RpcMessage.Type.SUBMIT_INCREMENTAL_RUN,
                RpcMessage.Type.INITIALIZE_INCREMENTAL_FEED,
                RpcMessage.Type.QUERY_RUN, RpcMessage.Type.QUERY_RUN_EVENTS,
                RpcMessage.Type.QUERY_QUALITY_GATE_DECISION,
                RpcMessage.Type.QUERY_INCREMENTAL_FRONTIER,
                RpcMessage.Type.CANCEL_RUN,
                RpcMessage.Type.QUERY_MANIFEST_OUTCOME,
                RpcMessage.Type.QUERY_BATCH_SINK_OUTCOME);
    }

    private static EnumSet<RpcMessage.Type> workerRequests() {
        return EnumSet.of(RpcMessage.Type.REGISTER_WORKER, RpcMessage.Type.HEARTBEAT,
                RpcMessage.Type.DEREGISTER_WORKER, RpcMessage.Type.PORT_REPORT,
                RpcMessage.Type.PROGRESS_REPORT, RpcMessage.Type.STAGE_REPORT,
                RpcMessage.Type.ASSIGNMENT_ACCEPTED);
    }

    private static EnumSet<RpcMessage.Type> masterRequests() {
        return EnumSet.of(RpcMessage.Type.ASSIGN_STAGE, RpcMessage.Type.CANCEL_STAGE,
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
    }

    private static String repeat(char value) {
        char[] chars = new char[64];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }
}

