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
import io.rowweave.plan.PlanOps;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MasterClusterUnaryCapabilityPreflightTest {

    private static final DataSchema META = new DataSchema(new Field("v", DataType.IntegerType));

    @Test
    void unsupportedUnaryCannotBeDispatchedOrAckedIntoSuccess() {
        String host = "unary-capability-worker";
        int port = 61_045;
        AtomicInteger assignments = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        try (MasterServer master = new MasterServer(60_000L, 0, new InProcessRegistry())) {
            InProcessRpcRegistry.Registration endpointRegistration = InProcessRpcRegistry.register(host, port, request -> {
                if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                    assignments.incrementAndGet();
                    master.handler().apply(RpcMessage.portReport(
                            request.workerId(), request.stageId(), 19_002, 0, "inc-unary-capability",
                            request.runAttemptFence()));
                    master.handler().apply(RpcMessage.stageReport(
                            request.workerId(), request.stageId(), true, 0, 0, null, "inc-unary-capability",
                            request.runAttemptFence()));
                } else if (request.type() == RpcMessage.Type.CANCEL_STAGE) {
                    cancellations.incrementAndGet();
                }
                return RpcMessage.ack();
            });
            try {
                master.handler().apply(RpcMessage.registerWorker(
                        "w", host, port, 8, -1, "inc-unary-capability"));

                RpcMessage result = master.handler().apply(RpcMessage.submitJob(unknownUnaryGraph().toBytes()));

                assertAll("an unknown UNARY must not enter the Worker trust domain",
                        () -> assertFalse(result.success(), "a success report from a fake Worker must not cover up a Master that lacks the capability"),
                        () -> assertTrue(result.errorMessage() != null
                                && result.errorMessage().contains("mysteryUnary"), String.valueOf(result.errorMessage())),
                        () -> assertEquals(0, assignments.get(), "no stage may be dispatched before a capability rejection"),
                        () -> assertEquals(0, cancellations.get(), "nothing is cancellable during the pre-check"),
                        () -> assertEquals(0, master.activeStageTrackerCount()),
                        () -> assertEquals(0, master.activeStageAssignmentCount()));
            } finally {
                endpointRegistration.close();
            }
        }
    }

    @Test
    void unaryCapabilityFailurePrecedesWorkerDiscovery() {
        try (MasterServer master = new MasterServer(60_000L, 0, new InProcessRegistry())) {
            RpcMessage result = master.handler().apply(RpcMessage.submitJob(unknownUnaryGraph().toBytes()));

            assertFalse(result.success());
            assertTrue(result.errorMessage().contains("mysteryUnary"), result.errorMessage());
            assertFalse(result.errorMessage().contains("no registered worker"), result.errorMessage());
        }
    }

    @Test
    void compilerUnaryCapabilityWhitelistIsExhaustive() {
        for (String typeId : Arrays.asList(
                "filter", "filterFn", "filterParams", "map", "flatMap", "select", "selectByIndex",
                "addFields", "schemaConvert", "rowValidate", "partitionByHash", "groupBySum", "groupByCount", "groupByMax", "groupByMin",
                "groupBy", "distinct", "orderBy", "top", "topN", "wholeAgg", "reduceGroup",
                "combineGroup", "combineReduceGroup")) {
            assertTrue(PlanOps.supportsUnaryType(typeId), typeId);
        }
        assertFalse(PlanOps.supportsUnaryType("mysteryUnary"));
        assertFalse(PlanOps.supportsUnaryType(null));
    }

    private static PlanGraph unknownUnaryGraph() {
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.emptyList()),
                PlanNode.unary("mysteryUnary", META, 0).build(),
                PlanNode.sink(META, 1, new DiscardSink(META)).build()));
    }
}

