package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StageAssignmentTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();

    @Test
    void groupBySumShuffleExpansion() {
        PlanNode src = PlanNode.source(META, Collections.emptyList());
        PlanNode gb = PlanNode.unary("groupBySum", GB, 0)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode sink = PlanNode.sink(GB, 1, "collecting").build();
        ExecutionGraph eg = ExecutionGraphBuilder.build(new PlanGraph(Arrays.asList(src, gb, sink)));

        PhysicalPlan plan = StageAssignment.assign(eg, 2, Arrays.asList("w0", "w1"));
        assertFalse(plan.isWholeGraph(), "a shuffle edge rules out WHOLE_GRAPH");
        assertFalse(plan.shuffleEdges().isEmpty(), "the groupBy upstream has a shuffle closing edge");

        int sinkStage = eg.rootStage().index();
        List<PhysicalPlan.Instance> sinkInst = plan.instancesOf(sinkStage);
        assertEquals(1, sinkInst.size(), "the sink has a single instance");
        assertTrue(sinkInst.get(0).listening(), "the sink listens for the upstream connection");
    }

    @Test
    void deterministicRoundRobin() {
        PlanNode src = PlanNode.source(META, Collections.emptyList());
        PlanNode gb = PlanNode.unary("groupBySum", GB, 0)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode sink = PlanNode.sink(GB, 1, "collecting").build();
        ExecutionGraph eg = ExecutionGraphBuilder.build(new PlanGraph(Arrays.asList(src, gb, sink)));

        PhysicalPlan a = StageAssignment.assign(eg, 2, Arrays.asList("w0", "w1"));
        PhysicalPlan b = StageAssignment.assign(eg, 2, Arrays.asList("w0", "w1"));
        assertEquals(a.instances().size(), b.instances().size());
        for (int i = 0; i < a.instances().size(); i++) {
            assertEquals(a.instances().get(i).workerId(), b.instances().get(i).workerId(),
                    "instance " + i + " has a deterministic assignment");
            assertEquals(a.instances().get(i).logicalStage(), b.instances().get(i).logicalStage());
        }
    }

    @Test
    void sinkRoundRobinAnchorsAtTheFirstSinkGlobalPosition() {
        PlanNode src = PlanNode.source(META, Collections.emptyList());
        PlanNode gb = PlanNode.unary("groupBySum", GB, 0)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode sink0 = PlanNode.sink(GB, 1, "collecting-0").build();
        PlanNode sink1 = PlanNode.sink(GB, 1, "collecting-1").build();
        ExecutionGraph eg = ExecutionGraphBuilder.build(
                new PlanGraph(Arrays.asList(src, gb, sink0, sink1), 2));

        PhysicalPlan plan = StageAssignment.assign(eg, 2, Arrays.asList("w0", "w1"));
        List<PhysicalPlan.Instance> sinks = new java.util.ArrayList<>();
        for (Stage stage : eg.stages()) {
            if (stage.isSink()) {
                sinks.addAll(plan.instancesOf(stage.index()));
            }
        }

        assertEquals(2, sinks.size());
        assertEquals("w1", sinks.get(0).workerId(),
                "after one source and two keyed instances the first SINK continues the global cursor onto w1");
        assertEquals("w0", sinks.get(1).workerId(),
                "the second SINK rotates independently from the anchor instead of co-locating with the first");
    }

    @Test
    void linearGraphIsWholeGraph() {
        PlanNode src = PlanNode.source(META, Collections.emptyList());
        PlanNode filter = PlanNode.unary("filter", META, 0).param("expr", "amt > 1").build();
        PlanNode sink = PlanNode.sink(META, 1, "collecting").build();
        ExecutionGraph eg = ExecutionGraphBuilder.build(new PlanGraph(Arrays.asList(src, filter, sink)));

        PhysicalPlan plan = StageAssignment.assign(eg, 4, Arrays.asList("w0", "w1"));
        assertTrue(plan.isWholeGraph(), "a linear graph with no shuffle runs as WHOLE_GRAPH");
        assertEquals(1, plan.instances().size(), "WHOLE_GRAPH has a single instance");
    }

    @Test
    void passThroughRunsParallelWithPartition() {
        PlanNode src = PlanNode.source(META, Collections.emptyList());
        PlanNode part = PlanNode.unary("partitionByHash", META, 0).listParam("keys", Arrays.asList("k")).build();
        PlanNode map = PlanNode.unary("map", META, 1).build();
        PlanNode gb = PlanNode.unary("groupBySum", GB, 2)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode sink = PlanNode.sink(GB, 3, "collecting").build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, part, map, gb, sink));
        ExecutionGraph eg = ExecutionGraphBuilder.build(g);

        PhysicalPlan plan = StageAssignment.assign(eg, 2, Arrays.asList("w0", "w1"));
        Stage mapStage = eg.stages().stream()
                .filter(s -> s.members().stream().anyMatch(i -> "map".equals(g.node(i).typeId())))
                .findFirst().orElseThrow();
        assertEquals("partitionByHash", mapStage.headTypeId(), "map chains into the partitionByHash stage");
        assertEquals(2, plan.instancesOf(mapStage.index()).size(), "a stage containing map at P=2 keeps map parallel rather than collapsing it");
    }

    @Test
    void singleInstanceBreakerCountsAsOneInDemand() {
        PlanNode src = PlanNode.source(META, Collections.emptyList());
        PlanNode gb = PlanNode.unary("groupBySum", GB, 0)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode ob = PlanNode.unary("orderBy", GB, 1)
                .listParam("sort", Arrays.asList("0:ASC:NULLS_LAST")).build();
        PlanNode sink = PlanNode.sink(GB, 2, "collecting").build();
        ExecutionGraph eg = ExecutionGraphBuilder.build(new PlanGraph(Arrays.asList(src, gb, ob, sink)));

        PhysicalPlan plan = StageAssignment.assign(eg, 4, Arrays.asList("w0", "w1"));
        Stage gbStage = eg.stages().stream()
                .filter(s -> "groupBySum".equals(s.headTypeId())).findFirst().orElseThrow();
        assertEquals(4, plan.instancesOf(gbStage.index()).size(), "keyed groupBy produces P=4 instances");
        Stage obStage = eg.stages().stream()
                .filter(s -> "orderBy".equals(s.headTypeId())).findFirst().orElseThrow();
        assertEquals(1, plan.instancesOf(obStage.index()).size(), "a total-order orderBy is forced to 1 instance rather than P");
        int totalDemand = plan.instances().size();
        assertTrue(totalDemand < 4 * eg.stages().size(),
                "slot demand follows the real instance count, so a single-instance operator claims only 1 (demand=" + totalDemand + " « P×stage=" + (4 * eg.stages().size()) + ")");
    }

    @Test
    void chainedOperatorsCountPerStageNotPerOperator() {
        PlanNode src = PlanNode.source(META, Collections.emptyList());
        PlanNode filter = PlanNode.unary("filter", META, 0).param("expr", "amt > 1").build();
        PlanNode select = PlanNode.unary("select", META, 1).build();
        PlanNode map = PlanNode.unary("map", META, 2).build();
        PlanNode gb = PlanNode.unary("groupBySum", GB, 3)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode sink = PlanNode.sink(GB, 4, "collecting").build();
        ExecutionGraph eg = ExecutionGraphBuilder.build(
                new PlanGraph(Arrays.asList(src, filter, select, map, gb, sink)));

        PhysicalPlan plan = StageAssignment.assign(eg, 4, Arrays.asList("w0", "w1"));
        int stageCount = eg.stages().size();
        assertTrue(stageCount <= 3, "three pass-through operators fuse into one stage and the stage total stays at or below 3 (measured " + stageCount + ")");
        int demand = plan.instances().size();
        assertTrue(demand < 6 * 4, "demand uses per-stage instance counts (a chain is not counted twice), far below 6 nodes x P4 = 24 (measured " + demand + ")");
    }

    @Test
    void rejectsNoWorkers() {
        PlanNode src = PlanNode.source(META, Collections.emptyList());
        PlanNode sink = PlanNode.sink(META, 0, "collecting").build();
        ExecutionGraph eg = ExecutionGraphBuilder.build(new PlanGraph(Arrays.asList(src, sink)));
        assertThrows(IllegalArgumentException.class,
                () -> StageAssignment.assign(eg, 2, Collections.emptyList()));
    }

    @Test
    void sourceShardingParallelism() {
        java.util.List<io.rowweave.Source> splits = new java.util.ArrayList<>();
        for (int i = 0; i < 3; i++) {
            splits.add(new io.rowweave.Source() {
                @Override public DataSchema getSchema() { return META; }
                @Override public java.util.Iterator<io.rowweave.DataRow> iterator() { return Collections.emptyIterator(); }
            });
        }
        PlanNode src = PlanNode.sourceFromInputs(META, splits);
        PlanNode gb = PlanNode.unary("groupBySum", GB, 0)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode sink = PlanNode.sink(GB, 1, "collecting").build();
        ExecutionGraph eg = ExecutionGraphBuilder.build(new PlanGraph(Arrays.asList(src, gb, sink)));
        PhysicalPlan plan = StageAssignment.assign(eg, 2, Arrays.asList("w0", "w1"));
        Stage srcStage = eg.stages().stream().filter(st -> st.inputStages().isEmpty() && !st.isSink())
                .findFirst().orElseThrow();
        assertEquals(2, plan.instancesOf(srcStage.index()).size(), "3 splits with P=2 gives the source R=2 sharded parallel instances");

        PlanNode src1 = PlanNode.sourceFromInputs(META, Collections.singletonList(splits.get(0)));
        ExecutionGraph eg1 = ExecutionGraphBuilder.build(new PlanGraph(Arrays.asList(src1, gb, sink)));
        PhysicalPlan plan1 = StageAssignment.assign(eg1, 4, Arrays.asList("w0", "w1"));
        Stage srcStage1 = eg1.stages().stream().filter(st -> st.inputStages().isEmpty() && !st.isSink())
                .findFirst().orElseThrow();
        assertEquals(1, plan1.instancesOf(srcStage1.index()).size(), "a single Source gives one source instance (no regression)");
    }

    @Test
    void mapPartitionKeepsOneInvocationStagePerUpstreamPartition() {
        java.util.List<io.rowweave.Source> splits = new java.util.ArrayList<>();
        for (int i = 0; i < 2; i++) {
            splits.add(new io.rowweave.Source() {
                @Override public DataSchema getSchema() { return META; }
                @Override public java.util.Iterator<io.rowweave.DataRow> iterator() {
                    return Collections.emptyIterator();
                }
            });
        }
        PlanNode source = PlanNode.sourceFromInputs(META, splits);
        java.util.Map<String, String> limits = new java.util.LinkedHashMap<>();
        limits.put("maxOutputRowsPerPartition", "1");
        limits.put("maxOutputBytesPerPartition", "64");
        PlanNode mapPartition = PlanNode.unaryFnWithParams(
                "mapPartition", META, 0,
                limits, Collections.emptyMap(), new TestMapPartition());
        PlanNode sink = PlanNode.sink(META, 1, "collecting").build();
        PlanGraph graph = new PlanGraph(
                Arrays.asList(source, mapPartition, sink), 2);
        ExecutionGraph execution = ExecutionGraphBuilder.build(graph);
        PhysicalPlan physical = StageAssignment.assign(
                execution, 2, Arrays.asList("w0", "w1"));

        Stage functionStage = execution.stages().stream()
                .filter(stage -> "mapPartition".equals(stage.headTypeId()))
                .findFirst().orElseThrow();
        assertFalse(functionStage.isSink());
        assertEquals(Collections.singletonList(1), functionStage.members());
        assertEquals(2, physical.instancesOf(functionStage.index()).size());
    }

    private static final class TestMapPartition
            implements io.rowweave.MapPartitionFunction {
        private static final long serialVersionUID = 1L;
        @Override public void mapPartition(
                io.rowweave.PartitionDataRowCursor rows,
                io.rowweave.Collector collector) { }
        @Override public DataSchema getResultSchema() { return META; }
    }
}

