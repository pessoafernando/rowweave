package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BlockingForkGraphTest {

    private static final DataSchema M = new DataSchema(new Field("k", DataType.IntegerType));

    @AfterEach
    void clearFlag() {
        System.clearProperty("rowweave.cluster.materializeFanout");
    }

    private static PlanNode src() {
        return PlanNode.source(M, new ArrayList<DataRow>());
    }

    private static PlanNode filter(int child) {
        return PlanNode.unary("filter", M, child).build();
    }

    private static PlanNode join(int l, int r) {
        return PlanNode.binary("join", M, l, r)
                .listParam("leftKeys", Arrays.asList("k"))
                .listParam("rightKeys", Arrays.asList("k")).build();
    }

    private static PlanNode sink(int child) {
        return PlanNode.sink(M, child, "collecting").build();
    }

    @Test
    void diamondForkBuiltOnceAndBlockingWhenEnabled() {
        System.setProperty("rowweave.cluster.materializeFanout", "true");
        // 0 src(fork), 1 A1=filter(0), 2 A2=filter(0), 3 join(1,2), 4 sink(3)
        PlanGraph g = new PlanGraph(Arrays.asList(src(), filter(0), filter(0), join(1, 2), sink(3)));
        ExecutionGraph eg = ExecutionGraphBuilder.build(g);

        List<Stage> forkStages = new ArrayList<>();
        for (Stage s : eg.stages()) {
            if (s.members().contains(0)) {
                forkStages.add(s);
            }
        }
        assertEquals(1, forkStages.size(), "the shared fork (node 0) builds a single stage (computed once)");
        assertTrue(forkStages.get(0).blockingFanout(), "the shared fork stage is marked blockingFanout (its output edge materializes)");
    }

    @Test
    void diamondNotBlockingWhenDisabled() {
        System.setProperty("rowweave.cluster.materializeFanout", "false");
        PlanGraph g = new PlanGraph(Arrays.asList(src(), filter(0), filter(0), join(1, 2), sink(3)));
        ExecutionGraph eg = ExecutionGraphBuilder.build(g);
        for (Stage s : eg.stages()) {
            assertFalse(s.blockingFanout(), "with the gate off no stage is marked blocking (the duplicate-computation path)");
        }
    }

    @Test
    void linearNotBlockingEvenWhenEnabled() {
        System.setProperty("rowweave.cluster.materializeFanout", "true");
        // 0 src, 1 filter(0), 2 sink(1)
        PlanGraph g = new PlanGraph(Arrays.asList(src(), filter(0), sink(1)));
        ExecutionGraph eg = ExecutionGraphBuilder.build(g);
        for (Stage s : eg.stages()) {
            assertFalse(s.blockingFanout(), "a linear graph has no fork re-join, so nothing is marked blocking");
        }
    }
}

