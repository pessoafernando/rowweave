package io.rowweave.plan;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.plan.PlanNode.Kind;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Fan-out materialization decided by pure graph analysis: which fork nodes must be materialized
 * to break the deadlock cycle.
 *
 * <p><b>The decision is an intersection of reachability sets, not an enumeration of shapes, so it
 * cannot miss one.</b> An operator that consumes its inputs in order (join, co-group and union are
 * all BINARY) drains one side fully before the other, so when its two input edges come from the same
 * fork, the broadcast fed to both sides deadlocks. Equivalently: for every BINARY node B compute
 * <code>reach(child0) intersection reach(child1)</code> (the intersection of the reachability
 * closures along the child edges, endpoints included) -- a non-empty intersection means that fork
 * must be materialized. The union over all BINARY nodes is the set of forks to materialize.
 *
 * <p>Self-join (both children are mid) yields mid; the asymmetric diamond yields fork A; the two
 * binaries sharing forks (D=join(B1, C2), E=join(A2, C2), where A2 is itself a fork) yield A and A2
 * but not C2, which sits on only one input edge of each.
 */
class MaterializationPlanTest {

    private static final DataSchema M = new DataSchema(new Field("v", DataType.IntegerType));

    private static PlanNode src() {
        return PlanNode.source(M, new ArrayList<DataRow>());
    }

    private static PlanNode filter(int child) {
        return PlanNode.unary("filter", M, child).build();
    }

    private static PlanNode join(int l, int r) {
        return PlanNode.binary("join", M, l, r)
                .listParam("leftKeys", Arrays.asList("v"))
                .listParam("rightKeys", Arrays.asList("v")).build();
    }

    private static PlanNode union(int l, int r) {
        return PlanNode.binary("union", M, l, r).build();
    }

    private static PlanNode sink(int child) {
        return PlanNode.sink(M, child, "collecting").build();
    }

    private static Set<Integer> plan(PlanGraph g) {
        return MaterializationPlan.forkNodesToMaterialize(g);
    }

    /** Linear chain into a single sink: no fan-out, so nothing is materialized. */
    @Test
    void linearSingleSinkNoMaterialization() {
        PlanGraph g = new PlanGraph(Arrays.asList(src(), filter(0), sink(1)));
        assertTrue(plan(g).isEmpty());
    }

    /** Pure fan-out to two sinks with no BINARY re-join: nothing is materialized, so the broadcast stays pipelined. */
    @Test
    void pureFanoutMultiSinkNoMaterialization() {
        // 0 src, 1 filter(0), 2 sink(1), 3 sink(1): filter has out-degree 2 but everything downstream is a SINK, so no binary re-join.
        PlanGraph g = new PlanGraph(Arrays.asList(src(), filter(0), sink(1), sink(1)));
        assertTrue(plan(g).isEmpty(), "a plain fan-out to several SINK nodes needs no materialization");
    }

    /** Two independent sources joined together with nothing shared: no duplicate input edge from one fork, so nothing is materialized. */
    @Test
    void disjointJoinNoMaterialization() {
        // 0 srcA, 1 srcB, 2 join(0,1), 3 sink(2)
        PlanGraph g = new PlanGraph(Arrays.asList(src(), src(), join(0, 1), sink(2)));
        assertTrue(plan(g).isEmpty(), "a join of two independent sources has no fan-out and needs no materialization");
    }

    /** self-join (mid.join(mid)): both join inputs are mid, so mid is materialized. */
    @Test
    void selfJoinMaterializesMid() {
        // 0 src, 1 mid=filter(0), 2 join(1,1), 3 sink(2)
        PlanGraph g = new PlanGraph(Arrays.asList(src(), filter(0), join(1, 1), sink(2)));
        assertEquals(new java.util.HashSet<>(Arrays.asList(1)), plan(g), "the self-join materializes the middle node (node 1)");
    }

    /** Symmetric diamond (fork to A1 and A2, then join): both join inputs reach back to the fork, so the fork is materialized. */
    @Test
    void symmetricDiamondMaterializesFork() {
        // 0 src(fork), 1 A1=filter(0), 2 A2=filter(0), 3 join(1,2), 4 sink(3)
        PlanGraph g = new PlanGraph(Arrays.asList(src(), filter(0), filter(0), join(1, 2), sink(3)));
        assertEquals(new java.util.HashSet<>(Arrays.asList(0)), plan(g), "the diamond shape materializes the fork (node 0)");
    }

    /** Asymmetric diamond whose branches differ in depth (A to A1 to B1, A to A2 to B2 to C2, then joined): the fork is materialized once. */
    @Test
    void asymmetricDiamondMaterializesFork() {
        // 0 A=src (the fork), 1 A1=filter(0), 2 B1=filter(1): the left branch spans two hops
        // 3 A2=filter(0), 4 B2=filter(3), 5 C2=filter(4): the right branch spans three hops
        // 6 join(2,5), 7 sink(6)
        PlanGraph g = new PlanGraph(Arrays.asList(
                src(), filter(0), filter(1), filter(0), filter(3), filter(4), join(2, 5), sink(6)));
        assertEquals(new java.util.HashSet<>(Arrays.asList(0)), plan(g), "the asymmetric diamond materializes fork A (node 0)");
    }

    /** Two binaries sharing forks: D=join(B1, C2) and E=join(A2, C2), where A2 is itself a fork, yields A and A2 but not C2 -- exactly, without over-materializing. */
    @Test
    void twoBinariesSharedForkMaterializesAandA2NotC2() {
        // edges: A to A1 and A2, A1 to B1, A2 to B2, B2 to C2, D=join(B1, C2), E=join(A2, C2)
        // indices: 0 A=src, 1 A1=filter(0), 2 B1=filter(1),
        // 3 A2=filter(0), 4 B2=filter(3), 5 C2=filter(4),
        // 6 D=join(2,5), 7 E=join(3,5), 8 sinkD(6), 9 sinkE(7)
        PlanGraph g = new PlanGraph(Arrays.asList(
                src(), filter(0), filter(1),
                filter(0), filter(3), filter(4),
                join(2, 5), join(3, 5), sink(6), sink(7)));
        // D=join(B1=2, C2=5): reach(2)={2,1,0}, reach(5)={5,4,3,0}, intersection {0} -> materialize A
        // E=join(A2=3, C2=5): reach(3)={3,0}, reach(5)={5,4,3,0}, intersection {3,0} -> materialize A2 (3) and A (0)
        // C2 (5) is in no intersection, so it is not materialized
        assertEquals(new java.util.HashSet<>(Arrays.asList(0, 3)), plan(g),
                "two binary operators sharing one fork materialize {A=0, A2=3} and leave C2=5 alone");
    }

    /** Fan-out from a fork into an n-ary union through two branches: a duplicate input edge from one fork, so the fork is materialized. */
    @Test
    void fanoutToUnionMaterializesFork() {
        // 0 src(fork), 1 filter(0), 2 filter(0), 3 union(1,2), 4 sink(3)
        PlanGraph g = new PlanGraph(Arrays.asList(src(), filter(0), filter(0), union(1, 2), sink(3)));
        assertEquals(new java.util.HashSet<>(Arrays.asList(0)), plan(g), "a diamond feeding a union materializes the fork (node 0)");
    }
}
