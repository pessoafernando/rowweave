package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.MapFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class OperatorChainingTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("amt", DataType.BigDecimalType));
    private static final MapFunction ID = in -> in;

    @Test
    void filterMapFilterChainIntoOneStage() {
        DataSchema gb = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();
        PlanNode src = PlanNode.source(META, Collections.emptyList());
        PlanNode f1 = PlanNode.unary("filter", META, 0).param("expr", "amt >= 0").build();
        PlanNode map = PlanNode.unaryFn("map", META, 1, (java.io.Serializable) ID);
        PlanNode f2 = PlanNode.unary("filter", META, 2).param("expr", "amt >= 0").build();
        PlanNode g = PlanNode.unary("groupBySum", gb, 3)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode sink = PlanNode.sink(gb, 4, "collecting").build();
        ExecutionGraph eg = ExecutionGraphBuilder.build(
                new PlanGraph(Arrays.asList(src, f1, map, f2, g, sink)));

        PlanGraph g2 = new PlanGraph(Arrays.asList(src, f1, map, f2, g, sink));
        Stage chainStage = eg.stages().stream()
                .filter(s -> !s.isSink() && !StageAssignment.needsKeyedShuffle(s.headTypeId())
                        && s.members().stream().anyMatch(i -> "map".equals(g2.node(i).typeId())))
                .findFirst().orElse(null);
        assertNotNull(chainStage, "a pass-through stage containing map exists");
        long fCnt = chainStage.members().stream().filter(i -> "filter".equals(g2.node(i).typeId())).count();
        boolean mp = chainStage.members().stream().anyMatch(i -> "map".equals(g2.node(i).typeId()));
        assertEquals(2, fCnt, "a filter-map-filter chain must not be broken by map and holds 2 filters");
        assertTrue(mp, "the chained stage contains map");
    }
}

