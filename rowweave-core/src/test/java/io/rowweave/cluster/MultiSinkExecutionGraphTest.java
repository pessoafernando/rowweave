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

class MultiSinkExecutionGraphTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();

    @Test
    void singleSinkUnchanged() {
        PlanNode src = PlanNode.source(META, Collections.emptyList());
        PlanNode flt = PlanNode.unary("filter", META, 0).param("expr", "amt >= 0").build();
        PlanNode gb = PlanNode.unary("groupBySum", GB, 1)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode sink = PlanNode.sink(GB, 2, "collecting").build();
        ExecutionGraph eg = ExecutionGraphBuilder.build(new PlanGraph(Arrays.asList(src, flt, gb, sink)));
        long sinks = eg.stages().stream().filter(Stage::isSink).count();
        assertEquals(1, sinks, "a single sink produces 1 SINK stage");
    }

    @Test
    void fanoutSharedSubgraphDedup() {
        PlanNode src = PlanNode.source(META, Collections.emptyList());
        PlanNode flt = PlanNode.unary("filter", META, 0).param("expr", "amt >= 0").build();
        PlanNode sink0 = PlanNode.sink(META, 1, "collecting").build();
        PlanNode gb = PlanNode.unary("groupBySum", GB, 1)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode sink1 = PlanNode.sink(GB, 3, "collecting").build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, flt, sink0, gb, sink1));

        ExecutionGraph eg = ExecutionGraphBuilder.build(g);

        long sinkStages = eg.stages().stream().filter(Stage::isSink).count();
        assertEquals(2, sinkStages, "a multi-sink graph must produce 2 SINK stages");

        int filterOccur = 0;
        for (Stage s : eg.stages()) {
            for (int m : s.members()) {
                if ("filter".equals(g.node(m).typeId())) filterOccur++;
            }
        }
        assertEquals(1, filterOccur, "the shared filter is built once (de-duplicated) rather than copied into each sink chain");

        int srcOccur = 0;
        for (Stage s : eg.stages()) {
            for (int m : s.members()) {
                if (g.node(m).kind() == PlanNode.Kind.SOURCE) srcOccur++;
            }
        }
        assertEquals(1, srcOccur, "the shared source is built once");
    }

    @Test
    void twoDisjointBranchesSinkHasInputs() {
        PlanNode srcA = PlanNode.source(META, Collections.emptyList());
        PlanNode flt = PlanNode.unary("filter", META, 0).param("expr", "amt >= 0").build();
        PlanNode sinkA = PlanNode.sink(META, 1, "collecting").build();
        PlanNode srcB = PlanNode.source(META, Collections.emptyList());
        PlanNode gb = PlanNode.unary("groupBySum", GB, 3)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode sinkB = PlanNode.sink(GB, 4, "collecting").build();
        PlanGraph g = new PlanGraph(Arrays.asList(srcA, flt, sinkA, srcB, gb, sinkB));

        ExecutionGraph eg = ExecutionGraphBuilder.build(g);

        long sinkStages = eg.stages().stream().filter(Stage::isSink).count();
        assertEquals(2, sinkStages, "two branches produce 2 SINK stages");

        for (Stage s : eg.stages()) {
            if (s.isSink()) {
                assertFalse(s.inputStages().isEmpty(),
                        "SINK stage(index=" + s.index() + ") must have non-empty inputStages and must not be a source-fused SINK with no input");
            }
        }

        int filterOccurrences = 0;
        boolean filterInSinkStage = false;
        for (Stage s : eg.stages()) {
            for (int m : s.members()) {
                if ("filter".equals(g.node(m).typeId())) {
                    filterOccurrences++;
                    filterInSinkStage |= s.isSink();
                }
            }
        }
        assertEquals(1, filterOccurrences, "the filter appears exactly once");
        assertFalse(filterInSinkStage, "the stage holding the filter must not be a SINK stage (the final SINK is split from its upstream)");

        int srcOccurrences = 0;
        boolean filterFusedWithSource = false;
        for (Stage s : eg.stages()) {
            boolean hasFilter = false, hasSource = false;
            for (int m : s.members()) {
                if (g.node(m).kind() == PlanNode.Kind.SOURCE) {
                    srcOccurrences++;
                    hasSource = true;
                }
                if ("filter".equals(g.node(m).typeId())) hasFilter = true;
            }
            filterFusedWithSource |= hasFilter && hasSource && !s.isSink();
        }
        assertEquals(2, srcOccurrences, "both sources are built once each (two branches)");
        assertTrue(filterFusedWithSource, "the filter fuses with its upstream source into one non-sink stage (no per-operator split)");
    }

    @Test
    void binaryWithIndependentSideBranchesKeepsAllSinkRoots() {
        // 0 srcA,1 srcB,2 join(A,B),3 groupBy(join),4 sink0,5 filter(A),6 sink1,7 distinct(B),8 sink2
        PlanNode srcA = PlanNode.source(META, Collections.emptyList());
        PlanNode srcB = PlanNode.source(META, Collections.emptyList());
        PlanNode join = PlanNode.binary("join", META, 0, 1)
                .listParam("leftKeys", Collections.singletonList("k"))
                .listParam("rightKeys", Collections.singletonList("k"))
                .param("joinType", "INNER").build();
        PlanNode gb = PlanNode.unary("groupBySum", GB, 2)
                .listParam("keys", Collections.singletonList("k")).param("measure", "amt").build();
        PlanNode sink0 = PlanNode.sink(GB, 3, "collecting").build();
        PlanNode filter = PlanNode.unary("filter", META, 0).param("expr", "amt >= 0").build();
        PlanNode sink1 = PlanNode.sink(META, 5, "collecting").build();
        PlanNode distinct = PlanNode.unary("distinct", META, 1)
                .listParam("fields", Collections.singletonList("k")).build();
        PlanNode sink2 = PlanNode.sink(META, 7, "collecting").build();
        PlanGraph graph = new PlanGraph(Arrays.asList(
                srcA, srcB, join, gb, sink0, filter, sink1, distinct, sink2));

        ExecutionGraph execution = ExecutionGraphBuilder.build(graph);

        assertEquals(3, execution.stages().stream().filter(Stage::isSink).count(),
                "all three SINK roots of the PlanGraph must reach the ExecutionGraph");
        int sourceOccurrences = 0;
        for (Stage stage : execution.stages()) {
            for (int member : stage.members()) {
                if (graph.node(member).kind() == PlanNode.Kind.SOURCE) {
                    sourceOccurrences++;
                }
            }
        }
        assertEquals(2, sourceOccurrences, "both shared sources are built once each, with no per-sink copy and no dropped root");
    }

    @Test
    void multiSinkStageAssignmentFanoutEdgesAndOrder() {
        PlanNode src = PlanNode.source(META, Collections.emptyList());
        PlanNode flt = PlanNode.unary("filter", META, 0).param("expr", "amt >= 0").build();
        PlanNode sink0 = PlanNode.sink(META, 1, "collecting").build();
        PlanNode gb = PlanNode.unary("groupBySum", GB, 1)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode sink1 = PlanNode.sink(GB, 3, "collecting").build();
        PlanGraph g = new PlanGraph(Arrays.asList(src, flt, sink0, gb, sink1));
        ExecutionGraph eg = ExecutionGraphBuilder.build(g);

        PhysicalPlan plan = StageAssignment.assign(eg, 2, Arrays.asList("w0", "w1"));
        assertFalse(plan.isWholeGraph(), "multi-sink with a shuffle edge is not wholeGraph");

        int sharedStage = -1;
        for (Stage s : eg.stages()) {
            for (int m : s.members()) if ("filter".equals(g.node(m).typeId())) sharedStage = s.index();
        }
        assertTrue(sharedStage >= 0, "a shared filter stage exists");
        final int sharedIdx = sharedStage;
        long fanoutEdges = plan.shuffleEdges().stream().filter(e -> e[0] == sharedIdx).count();
        assertEquals(2, fanoutEdges, "the shared stage has 2 output edges (fan-out to sink0 plus groupBy)");

        for (int[] e : plan.shuffleEdges()) {
            if (e[0] == sharedIdx) {
                assertTrue(e[0] < e[1], "the shared upstream stage is ordered before the downstream stage (topological order, measured " + e[0] + "->" + e[1] + ")");
            }
        }

        long sinkInstances = plan.instances().stream()
                .filter(i -> eg.stages().get(i.logicalStage()).isSink()).count();
        assertEquals(2, sinkInstances, "each of the 2 sinks has 1 instance");
    }
}

