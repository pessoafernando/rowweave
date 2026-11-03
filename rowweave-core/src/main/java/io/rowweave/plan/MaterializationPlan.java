package io.rowweave.plan;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Decides which fan-out fork nodes must be materialized, so that a fan-out which reconverges into an
 * operator that consumes its inputs in turn cannot deadlock. A pure function over {@link PlanGraph}: it
 * does not execute, does not read a Source and holds no DataRow, and looks only at the graph structure.
 *
 * <p><b>The algorithm is an intersection of reachability</b>, not an enumeration of shapes, so no nesting
 * or corner case escapes it. An operator that consumes its inputs in turn (join, coGroup and union are
 * all {@link PlanNode.Kind#BINARY} in this IR) drains one side fully before the other, so when its two
 * input edges reach the same fork node along child edges, feeding both sides by broadcast deadlocks. For
 * each BINARY node B, compute {@code reach(child0) and reach(child1)}, the closure over child edges
 * including the endpoints: every node in that intersection is a fork where the two inputs come from one
 * source and must be materialized. The union of those intersections over every BINARY node is the set of
 * forks to materialize.
 *
 * <p>MaterializationPlanTest walks the cases: a self-join whose children are the same node yields that
 * node; a symmetric or asymmetric diamond yields the fork; two binaries that share one branch yield the
 * shared ancestors and not a node sitting on a single input edge of each; a pure fan-out to several
 * SINKs, a disjoint join, and a single linear sink all yield the empty set and stay pipelined.
 *
 * <p><b>Why this is exact</b>: the intersection is non-empty if and only if a fan-out that backpressures
 * feeds both input edges of an operator that consumes them in turn, which is exactly the deadlock. The
 * same condition is therefore minimal as well: nothing extra is materialized and nothing is missed.
 */
public final class MaterializationPlan {

    private MaterializationPlan() {
    }

    /**
     * The indexes of fork nodes that must be materialized, ordered so the result is deterministic and
     * handled in topological order. An empty set means there is no deadlock shape and everything pipelines.
     */
    public static Set<Integer> forkNodesToMaterialize(PlanGraph graph) {
        Set<Integer> result = new TreeSet<>();
        int n = graph.nodeCount();
        for (int i = 0; i < n; i++) {
            PlanNode node = graph.node(i);
            if (node.kind() != PlanNode.Kind.BINARY) {
                continue;
            }
            int[] children = node.childIndices();
            if (children.length < 2) {
                continue;  // defensive: a BINARY node always has two children
            }
            Set<Integer> reachLeft = reachClosure(graph, children[0]);
            Set<Integer> reachRight = reachClosure(graph, children[1]);
            // The intersection is where the two input edges converge on one fork, and it also covers a
            // self-join, where child0 and child1 are already the same node.
            Set<Integer> intersection = new HashSet<>();
            for (int node2 : reachLeft) {
                if (reachRight.contains(node2)) {
                    intersection.add(node2);
                }
            }
            // Keep only the maximal elements, which is what makes materialization minimal: drop any
            // deeper node in the same intersection that a kept node reaches along child edges, because
            // materializing the higher convergence point already buffers the fan-out beneath it. In a
            // self-join {mid, src}, src is dropped since mid reaches it and only mid is kept. Refine per
            // binary, then union across binaries, because each binary needs its own convergence point.
            for (int cand : intersection) {
                boolean reachableFromOther = false;
                for (int other : intersection) {
                    if (other != cand && reachClosure(graph, other).contains(cand)) {
                        reachableFromOther = true;
                        break;
                    }
                }
                if (!reachableFromOther) {
                    result.add(cand);
                }
            }
        }
        return result;
    }

    /** All nodes reachable from {@code start} along child edges, toward the sources, including {@code start} itself. */
    private static Set<Integer> reachClosure(PlanGraph graph, int start) {
        Set<Integer> seen = new HashSet<>();
        Deque<Integer> stack = new ArrayDeque<>();
        stack.push(start);
        seen.add(start);
        while (!stack.isEmpty()) {
            int cur = stack.pop();
            for (int child : graph.node(cur).childIndices()) {
                if (seen.add(child)) {
                    stack.push(child);
                }
            }
        }
        return seen;
    }
}
