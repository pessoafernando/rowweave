package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.RunAttemptFence;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MasterDagRunAttemptFenceTest {

    @Test
    void everyGenericDagRoleUsesOneNonLegacyAttemptFence() {
        String host = "dag-fence-worker";
        int port = 61_067;
        String incarnation = "inc-dag-fence";
        Set<RunAttemptFence> envelopes = ConcurrentHashMap.newKeySet();
        Set<RunAttemptFence> commands = ConcurrentHashMap.newKeySet();
        Set<StageCommand.Role> roles = ConcurrentHashMap.newKeySet();
        AtomicInteger assignments = new AtomicInteger();

        try (MasterServer master = new MasterServer(60_000L, 0, new InProcessRegistry())) {
            InProcessRpcRegistry.Registration endpoint = InProcessRpcRegistry.register(host, port, request -> {
                if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                    assignments.incrementAndGet();
                    RunAttemptFence envelope = request.runAttemptFence();
                    assertNotNull(envelope);
                    envelopes.add(envelope);
                    try {
                        StageCommand command = StageCommandCodecV2.preflight(
                                request.stageCommandBlob(), ignored -> { }).materialize(
                                ObjectTrustContext.localTrusted(getClass().getClassLoader()), ignored -> { });
                        assertNotNull(command.runAttemptFence());
                        commands.add(command.runAttemptFence());
                        roles.add(command.role());
                        assertEquals(envelope, command.runAttemptFence());
                    } catch (Exception failure) {
                        return RpcMessage.jobResult(false, 0, failure.toString());
                    }
                    master.handler().apply(RpcMessage.stageReport(
                            request.workerId(), request.stageId(), true, 0L, 0L, null,
                            incarnation, envelope));
                }
                return RpcMessage.ack();
            });
            try {
                // dataPort > 0 keeps the fake Worker on the mux path, so all assignment roles can finish without ports.
                master.handler().apply(RpcMessage.registerWorker(
                        "w", host, port, 8, 62_067, incarnation));
                RpcMessage result = master.handler().apply(RpcMessage.submitJob(genericDag().toBytes()));

                assertTrue(result.success(), result.errorMessage());
                assertTrue(assignments.get() >= 2, "generic DAG must assign more than one stage");
                assertTrue(roles.contains(StageCommand.Role.SINK), roles.toString());
                assertTrue(roles.stream().anyMatch(role -> role != StageCommand.Role.SINK), roles.toString());
                assertEquals(1, envelopes.size(), "one job must use exactly one execution attempt fence");
                assertEquals(envelopes, commands);
            } finally {
                endpoint.close();
            }
        }
    }

    private static PlanGraph genericDag() {
        DataSchema meta = new DataSchema(new Field("v", DataType.IntegerType));
        return new PlanGraph(Arrays.asList(
                PlanNode.source(meta, Collections.emptyList()),
                PlanNode.unary("distinct", meta, 0)
                        .listParam("keys", Collections.singletonList("v")).build(),
                PlanNode.sink(meta, 1, new DiscardSink(meta)).build()), 2);
    }
}
