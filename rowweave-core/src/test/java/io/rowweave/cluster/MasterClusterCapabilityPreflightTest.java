package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MasterClusterCapabilityPreflightTest {

    private static final DataSchema META = new DataSchema(new Field("v", DataType.IntegerType));

    @Test
    void unsupportedBinaryFailsBeforeSinkAssignmentOrTracking() {
        String host = "capability-worker";
        int port = 61_044;
        AtomicInteger assignments = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        try (MasterServer master = new MasterServer(60_000L, 0, new InProcessRegistry())) {
            InProcessRpcRegistry.Registration endpointRegistration = InProcessRpcRegistry.register(host, port, request -> {
                if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                    assignments.incrementAndGet();
                    master.handler().apply(RpcMessage.portReport(
                            request.workerId(), request.stageId(), 19_001, 0, "inc-capability",
                            request.runAttemptFence()));
                    master.handler().apply(RpcMessage.stageReport(
                            request.workerId(), request.stageId(), true, 0, 0, null, "inc-capability",
                            request.runAttemptFence()));
                } else if (request.type() == RpcMessage.Type.CANCEL_STAGE) {
                    cancellations.incrementAndGet();
                }
                return RpcMessage.ack();
            });
            try {
                master.handler().apply(RpcMessage.registerWorker(
                        "w", host, port, 8, -1, "inc-capability"));
                RpcMessage result = master.handler().apply(RpcMessage.submitJob(unknownBinaryGraph().toBytes()));

                assertFalse(result.success());
                assertTrue(result.errorMessage().contains("mysteryBinary"), result.errorMessage());
                assertAll("a capability rejection must be a zero-side-effect transaction",
                        () -> assertEquals(0, assignments.get(), "the sink must not be dispatched before an unsupported upstream BINARY is discovered"),
                        () -> assertEquals(0, cancellations.get(), "nothing is cancellable during the pre-check"),
                        () -> assertEquals(0, master.activeStageTrackerCount(), "no StageTracker may be left behind"),
                        () -> assertEquals(0, master.activeStageAssignmentCount(), "no StageOwner may be left behind"));
            } finally {
                endpointRegistration.close();
            }
        }
    }

    @Test
    void capabilityFailurePrecedesWorkerDiscovery() {
        try (MasterServer master = new MasterServer(60_000L, 0, new InProcessRegistry())) {
            RpcMessage result = master.handler().apply(RpcMessage.submitJob(unknownBinaryGraph().toBytes()));

            assertFalse(result.success());
            assertTrue(result.errorMessage().contains("mysteryBinary"), result.errorMessage());
            assertFalse(result.errorMessage().contains("no registered worker"), result.errorMessage());
            assertEquals(0, master.activeStageTrackerCount());
            assertEquals(0, master.activeStageAssignmentCount());
        }
    }

    @Test
    void knownJoinStrategiesPassCapabilityPreflight() {
        assertDoesNotThrow(() -> ClusterPlanCapabilityValidator.validate(joinGraph(null)));
        assertDoesNotThrow(() -> ClusterPlanCapabilityValidator.validate(joinGraph("AUTO")));
        assertDoesNotThrow(() -> ClusterPlanCapabilityValidator.validate(joinGraph("HASH")));
        assertDoesNotThrow(() -> ClusterPlanCapabilityValidator.validate(joinGraph("SORT_MERGE")));
    }

    @Test
    void masterLetsKnownStrategiesReachWorkerDiscovery() {
        try (MasterServer master = new MasterServer(60_000L, 0, new InProcessRegistry())) {
            for (String hint : Arrays.asList("HASH", "AUTO", "SORT_MERGE")) {
                RpcMessage result = master.handler().apply(
                        RpcMessage.submitJob(joinGraph(hint).toBytes()));
                assertFalse(result.success());
                assertTrue(result.errorMessage().contains("no registered worker"), result.errorMessage());
                assertFalse(result.errorMessage().contains("joinHint"), result.errorMessage());
                assertEquals(0, master.activeStageTrackerCount());
                assertEquals(0, master.activeStageAssignmentCount());
            }
        }
    }

    @Test
    void unknownJoinHintIsRejectedButSupportedBinaryModesRemainAccepted() {
        IllegalArgumentException unknown = assertThrows(IllegalArgumentException.class,
                () -> ClusterPlanCapabilityValidator.validate(joinGraph("FUTURE_JOIN")));
        assertTrue(unknown.getMessage().contains("joinHint 'FUTURE_JOIN'"), unknown.getMessage());

        assertDoesNotThrow(() -> ClusterPlanCapabilityValidator.validate(joinGraph(null)));
        assertDoesNotThrow(() -> ClusterPlanCapabilityValidator.validate(joinGraph("AUTO")));
        assertDoesNotThrow(() -> ClusterPlanCapabilityValidator.validate(joinGraph("HASH")));
        assertDoesNotThrow(() -> ClusterPlanCapabilityValidator.validate(joinGraph("SORT_MERGE")));
        assertDoesNotThrow(() -> ClusterPlanCapabilityValidator.validate(binaryGraph("coGroup")));
        assertDoesNotThrow(() -> ClusterPlanCapabilityValidator.validate(binaryGraph("union")));
    }

    @Test
    void currentBinaryCapabilityWhitelistRemainsAccepted() {
        for (String typeId : Arrays.asList("join", "coGroup", "union")) {
            assertDoesNotThrow(() -> ClusterPlanCapabilityValidator.validate(binaryGraph(typeId)), typeId);
        }
    }

    private static PlanGraph unknownBinaryGraph() {
        return binaryGraph("mysteryBinary");
    }

    private static PlanGraph binaryGraph(String typeId) {
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.emptyList()),
                PlanNode.source(META, Collections.emptyList()),
                PlanNode.binary(typeId, META, 0, 1).build(),
                PlanNode.sink(META, 2, new DiscardSink(META)).build()));
    }

    private static PlanGraph joinGraph(String hint) {
        PlanNode.Builder join = PlanNode.binary("join", META, 0, 1)
                .param("joinType", "INNER")
                .listParam("leftKeys", Collections.singletonList("v"))
                .listParam("rightKeys", Collections.singletonList("v"));
        if (hint != null) {
            join.param("joinHint", hint);
        }
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.emptyList()),
                PlanNode.source(META, Collections.emptyList()),
                join.build(),
                PlanNode.sink(META, 2, new DiscardSink(META)).build()));
    }
}

