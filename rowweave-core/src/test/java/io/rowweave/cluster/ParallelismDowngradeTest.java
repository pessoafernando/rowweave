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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ParallelismDowngradeTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();

    private static ExecutionGraph keyedDag() {
        PlanNode src = PlanNode.source(META, Collections.emptyList());
        PlanNode gb = PlanNode.unary("groupBySum", GB, 0)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode ob = PlanNode.unary("orderBy", GB, 1)
                .listParam("sort", Arrays.asList("0:ASC:NULLS_LAST")).build();
        PlanNode sink = PlanNode.sink(GB, 2, "collecting").build();
        return ExecutionGraphBuilder.build(new PlanGraph(Arrays.asList(src, gb, ob, sink)));
    }

    private static Map<String, Integer> cap(Object... kv) {
        Map<String, Integer> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], (Integer) kv[i + 1]);
        return m;
    }

    private static final List<String> WORKERS = Arrays.asList("w0", "w1");

    @Test
    void noDowngradeWhenCapacityAmple() {
        ExecutionGraph eg = keyedDag();
        int p = ParallelismDowngrade.highestFeasible(eg, 4, WORKERS, cap("w0", 64, "w1", 64));
        assertEquals(4, p, "ample capacity gives effective parallelism equal to the desired 4");
    }

    @Test
    void downgradesToHighestFeasible() {
        ExecutionGraph eg = keyedDag();
        int p8 = ParallelismDowngrade.highestFeasible(eg, 8, WORKERS, cap("w0", 2, "w1", 2));
        assertTrue(p8 >= 1 && p8 < 8, "small capacity downgrades to a feasible parallelism below 8 (measured " + p8 + ")");
        int pBig = ParallelismDowngrade.highestFeasible(eg, 8, WORKERS, cap("w0", 8, "w1", 8));
        assertTrue(pBig >= p8, "raising capacity never lowers effective parallelism (" + pBig + " >= " + p8 + ")");
    }

    @Test
    void returnsZeroWhenEvenP1Infeasible() {
        ExecutionGraph eg = keyedDag();
        int p = ParallelismDowngrade.highestFeasible(eg, 4, WORKERS, cap("w0", 0, "w1", 0));
        assertEquals(0, p, "zero capacity fits nothing at parallelism 1 and returns 0 so the caller fails");
    }

    @Test
    void demandMonotonicInP() {
        ExecutionGraph eg = keyedDag();
        int prev = -1;
        for (int p = 1; p <= 6; p++) {
            int total = ParallelismDowngrade.totalDemand(eg, p, WORKERS, "w0");
            assertTrue(total >= prev, "demand(P=" + p + ")=" + total + " must be at least the demand at parallelism " + (p - 1) + ")=" + prev);
            prev = total;
        }
    }
}

