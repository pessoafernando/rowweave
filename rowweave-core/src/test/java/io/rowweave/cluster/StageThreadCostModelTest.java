package io.rowweave.cluster;

import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.ops.join.JoinPhysicalStrategy;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.sched.net.ShuffleEndpoint;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** every assignment has a static pre-StageTask thread cost. */
class StageThreadCostModelTest {

    private static final long GIB = 1024L * 1024L * 1024L;
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));

    @Test
    void linearPipelineAndLegacySocketThreadsAreCountedSeparately() {
        StageCommand command = StageCommand.sourceStage(
                META, META, 8, emptyInput(), linear("filter"),
                new ShuffleEndpoint("127.0.0.1", 7001));

        StageThreadCostModel.Cost cost = StageThreadCostModel.estimate(
                command, dataPlane(true));

        assertEquals(3, cost.pipelineThreads());
        assertEquals(0, cost.operatorHelperThreads());
        assertEquals(1, cost.networkThreads());
        assertEquals(4, cost.totalThreads());
    }

    @Test
    void muxTargetRemovesPerAssignmentSocketThreadAndOrderByCountsWorkers() {
        StageCommand command = StageCommand.sourceStage(
                        META, META, 8, emptyInput(), linear("orderBy"),
                        endpointWithTarget(7002))
                .withDownTargets(Collections.singletonList(
                        ShuffleTarget.socket(endpointWithTarget(7002))));

        StageThreadCostModel.Cost cost = StageThreadCostModel.estimate(
                command, dataPlane(true));

        assertEquals(3, cost.pipelineThreads());
        assertEquals(3, cost.operatorHelperThreads());
        assertEquals(0, cost.networkThreads());
        assertEquals(6, cost.totalThreads());
    }

    @Test
    void wholeGraphAndJoinFallbackHaveConservativeClosedCosts() {
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META,
                        Collections.singletonList(emptyInput())),
                PlanNode.unary("orderBy", META, 0).build(),
                PlanNode.sink(META, 1, new DiscardSink(META)).build()));
        StageThreadCostModel.Cost whole = StageThreadCostModel.estimate(
                StageCommand.wholeGraph(graph), dataPlane(true));
        assertEquals(5, whole.pipelineThreads());
        assertEquals(3, whole.operatorHelperThreads());
        assertEquals(8, whole.totalThreads());

        StageCommand join = StageCommand.joinConsumer(
                META, META, META, 8, 1, 1,
                new String[] {"id"}, new String[] {"id"}, "INNER",
                JoinPhysicalStrategy.AUTO, endpointWithTarget(7003))
                .withInboundTargetIds(UUID.randomUUID(), UUID.randomUUID());
        StageThreadCostModel.Cost joinCost = StageThreadCostModel.estimate(
                join, dataPlane(true));
        assertEquals(6, joinCost.operatorHelperThreads());
        assertEquals(0, joinCost.networkThreads());
        assertEquals(6, joinCost.totalThreads());
    }

    @Test
    void blockingFanoutAndLegacyInboundCannotHideThreadExplosion() {
        StageCommand.BroadcastGroup first = blockingGroup(7101);
        StageCommand.BroadcastGroup second = blockingGroup(7102);
        StageCommand.BroadcastGroup third = blockingGroup(7103);
        StageCommand join = StageCommand.joinConsumer(
                        META, META, META, 8, 4, 4,
                        new String[] {"id"}, new String[] {"id"}, "INNER",
                        JoinPhysicalStrategy.SORT_MERGE, endpointWithTarget(7104))
                .withInboundTargetIds(UUID.randomUUID(), UUID.randomUUID())
                .withBroadcastGroups(Arrays.asList(first, second, third));
        StageThreadCostModel.Cost blocking = StageThreadCostModel.estimate(
                join, dataPlane(true));
        assertEquals(3, blocking.blockingReplayThreads());
        assertEquals(9, blocking.totalThreads());

        StageCommand sink = StageCommand.sink(
                        META, 8, 4,
                        new PlanGraph(Collections.singletonList(
                                PlanNode.placeholderSource(META))),
                        new DiscardSink(META))
                .withLocalInbound(1, "local");
        StageThreadCostModel.Cost legacy = StageThreadCostModel.estimate(
                sink, dataPlane(false));
        assertEquals(2, legacy.pipelineThreads());
        assertEquals(5, legacy.networkThreads());
        assertEquals(7, legacy.totalThreads());
    }

    @Test
    void limitFailureUsesStableCap005BeforeStageTaskExists() {
        StageCommand command = StageCommand.sourceStage(
                META, META, 8, emptyInput(), linear("orderBy"),
                endpointWithTarget(7201));
        ProcessCapacityException failure = assertThrows(
                ProcessCapacityException.class,
                () -> StageThreadCostModel.requireWithin(
                        command, dataPlane(true), 5));
        assertEquals(ProcessCapacityException.Code.STAGE_THREADS, failure.code());
    }

    private static PlanGraph linear(String typeId) {
        return new PlanGraph(Arrays.asList(
                PlanNode.placeholderSource(META),
                PlanNode.unary(typeId, META, 0).build()));
    }

    private static CollectionSource emptyInput() {
        return new CollectionSource(META, "thread-cost", Collections.emptyList());
    }

    private static ShuffleEndpoint endpointWithTarget(int port) {
        return ShuffleEndpoint.mux("127.0.0.1", port, UUID.randomUUID());
    }

    private static StageCommand.BroadcastGroup blockingGroup(int port) {
        return new StageCommand.BroadcastGroup(
                Collections.singletonList(
                        ShuffleTarget.socket(endpointWithTarget(port))),
                null, false, true);
    }

    private static DataPlaneConfig dataPlane(boolean mux) {
        Properties properties = new Properties();
        properties.setProperty("rowweave.shuffle.mux", Boolean.toString(mux));
        return DataPlaneConfig.fromProperties(properties,
                io.rowweave.cluster.rpc.ControlEndpointConfig
                        .fromProperties(properties), 4L * GIB);
    }
}
