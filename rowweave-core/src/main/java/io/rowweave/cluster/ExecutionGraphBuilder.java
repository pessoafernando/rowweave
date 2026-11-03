package io.rowweave.cluster;

import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Translates a {@link PlanGraph} into an {@link ExecutionGraph} once, statically, at compile time.
 * It is a pure function: it runs no operator, reads no Source and holds no DataRow. It looks only at
 * the graph structure and the node type ids to decide where stage boundaries fall.
 *
 * <p>The cut points:
 * <ul>
 * <li>pass-through (filter, select) and SINK: one FORWARD input, so the node joins its upstream in
 * the same stage, which is operator chaining.</li>
 * <li>keyed and breaker nodes (the groupBy family, distinct, orderBy, top, whole-aggregate): the
 * upstream is converged through a shuffle, so this node heads a new stage.</li>
 * <li>BINARY (join, union): left and right upstreams each form their own stage.</li>
 * </ul>
 *
 * <p>The result is in topological order, upstream first and the SINK stage last.
 */
public final class ExecutionGraphBuilder {

    private ExecutionGraphBuilder() {
    }

    public static ExecutionGraph build(PlanGraph graph) {
        return build(graph, false);
    }

    /** Even one manifest sink is cut into its own SINK stage, so the master runs the commit protocol. */
    public static ExecutionGraph buildWithSinkBoundary(PlanGraph graph) {
        return build(graph, true);
    }

    private static ExecutionGraph build(PlanGraph graph, boolean forceSinkBoundary) {
        int[] outDeg = new int[graph.nodeCount()];
        int sinkCount = 0;
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode n = graph.node(i);
            if (n.kind() == PlanNode.Kind.SINK) {
                sinkCount++;
            }
            for (int c = 0; c < n.childCount(); c++) {
                outDeg[n.child(c)]++;
            }
        }
        // The switch rowweave.cluster.materializeFanout. When it is on, a diamond or a self-join,
        // which fans out and re-joins into a BINARY that consumes in order, also takes the multi-root
        // memo build, so a shared fork is built as one stage and computed once, and the shared fork
        // stages that need materialising are marked blockingFanout: the worker materialises them and
        // replays each subpartition concurrently, which breaks the deadlock ring. When it is off, the
        // existing recompute path is byte for byte unchanged. The decision comes from the pure
        // function forkNodesToMaterialize, which analyses reachability in the downstream graph and
        // is exact, including asymmetric diamonds and two binaries sharing one fork. It is on by
        // default, because it removes the double computation on a cluster while the output sets stay
        // the same; setting the system property to false falls back to recomputation.
        boolean materializeFanout = Boolean.parseBoolean(
                System.getProperty("rowweave.cluster.materializeFanout", "true"));
        java.util.Set<Integer> matForks = materializeFanout
                ? io.rowweave.plan.MaterializationPlan.forkNodesToMaterialize(graph)
                : java.util.Collections.emptySet();
        boolean blockingDiamond = !matForks.isEmpty();
        // With several sinks the graph is always built from every root. The earlier guard that asked
        // whether a shared subtree fed a binary would send a legal graph, where one branch of the
        // source goes into a join and the other is written on its own, back to the single-root build,
        // and that silently dropped the other SINK nodes. Whether a re-join needs its ring broken is
        // decided only by matForks and blockingDiamond; it is not a reason to remove an execution
        // root. One sink with no blocking re-join still takes the fast path.
        boolean multiSinkTee = sinkCount > 1 || forceSinkBoundary;
        List<Stage> stages = new ArrayList<>();
        if (!multiSinkTee && !blockingDiamond) {
            buildStageSingleRoot(graph, graph.rootIndex(), stages);  // the single-root path, which ignores out-degree, so a fork is not a boundary
            return new ExecutionGraph(stages);
        }
        // memo maps a PlanNode index to the stage already built for it, which deduplicates shared
        // subtrees; the graph is built from every SINK root, and a node with out-degree above one is
        // a fork boundary.
        Map<Integer, Integer> nodeToStage = new HashMap<>();
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (graph.node(i).kind() == PlanNode.Kind.SINK) {
                buildStage(graph, i, stages, outDeg, nodeToStage);
            }
        }
        if (blockingDiamond) {
            stages = markBlockingForks(stages, matForks);
        }
        return new ExecutionGraph(stages);
    }

    /**
     * Marks the stages that hold a shared fork needing materialisation with blockingFanout, which
     * makes their out-edges BLOCKING. {@code matForks} comes from the pure function
     * {@link io.rowweave.plan.MaterializationPlan#forkNodesToMaterialize}, an intersection of
     * downstream reachability, so nothing is missed. A fork stage's output node is the last of its
     * members, the end closest to the sink in execution order; when that node is in {@code matForks}
     * the stage is marked blocking.
     */
    private static List<Stage> markBlockingForks(List<Stage> stages, java.util.Set<Integer> matForks) {
        List<Stage> out = new ArrayList<>(stages.size());
        for (Stage s : stages) {
            int forkNode = s.members().get(s.members().size() - 1);  // this stage's output node, the end closest to the sink
            if (matForks.contains(forkNode)) {
                out.add(new Stage(s.index(), s.members(), s.inputStages(), s.isSink(),
                        s.headTypeId(), s.headSourceInputCount(), true));
            } else {
                out.add(s);
            }
        }
        return out;
    }

    /**
     * Single-root build: one DFS from rootIndex, where out-degree is not consulted, so a fork is not
     * a boundary. Used for one sink with no blocking re-join.
     */
    private static int buildStageSingleRoot(PlanGraph graph, int headIndex, List<Stage> stages) {
        List<Integer> members = new ArrayList<>();
        List<Integer> inputStages = new ArrayList<>();
        walkChainSingleRoot(graph, headIndex, stages, members, inputStages);
        Collections.reverse(members);
        boolean isSink = graph.node(headIndex).kind() == PlanNode.Kind.SINK;
        PlanNode headNode = graph.node(members.get(0));
        int headSourceInputCount = (headNode.kind() == PlanNode.Kind.SOURCE && inputStages.isEmpty()
                && headNode.inputs() != null) ? headNode.inputs().size() : 0;
        Stage stage = new Stage(stages.size(), members, inputStages, isSink, headNode.typeId(), headSourceInputCount);
        stages.add(stage);
        return stage.index();
    }

    private static void walkChainSingleRoot(PlanGraph graph, int nodeIndex, List<Stage> stages,
                                            List<Integer> members, List<Integer> inputStages) {
        members.add(nodeIndex);
        PlanNode n = graph.node(nodeIndex);
        if (n.childCount() == 1 && isForwardConsumer(n)
                && graph.node(n.child(0)).kind() != PlanNode.Kind.BINARY
                && !isPartitionTaskBoundary(graph.node(n.child(0)))) {
            walkChainSingleRoot(graph, n.child(0), stages, members, inputStages);
            return;
        }
        for (int c = 0; c < n.childCount(); c++) {
            inputStages.add(buildStageSingleRoot(graph, n.child(c), stages));
        }
    }

    /**
     * Builds one stage headed at {@code headIndex}, the end closest to the sink, absorbing upstream
     * pass-through nodes and the SINK along single-input FORWARD edges. It stops at a shuffle edge, at
     * a breaker or keyed child, at a node with several inputs, which is a BINARY, and at a shared
     * fan-out point with out-degree above one. Each such child is built as its own upstream stage,
     * recursively, reused when the memo already has it, and recorded as one input edge. Returns the
     * index of the stage built here.
     */
    private static int buildStage(PlanGraph graph, int headIndex, List<Stage> stages,
                                  int[] outDeg, Map<Integer, Integer> nodeToStage) {
        // Deduplication: this node already heads a stage, because several downstream nodes reference
        // the shared subtree, so it is reused rather than built again.
        Integer existing = nodeToStage.get(headIndex);
        if (existing != null) {
            return existing;
        }
        List<Integer> members = new ArrayList<>();
        List<Integer> inputStages = new ArrayList<>();
        walkChain(graph, headIndex, stages, members, inputStages, outDeg, nodeToStage);

        Collections.reverse(members);  // collected head-first towards the source, reversed into execution order
        boolean isSink = graph.node(headIndex).kind() == PlanNode.Kind.SINK;
        PlanNode headNode = graph.node(members.get(0));
        String headTypeId = headNode.typeId();
        int headSourceInputCount = (headNode.kind() == PlanNode.Kind.SOURCE && inputStages.isEmpty()
                && headNode.inputs() != null) ? headNode.inputs().size() : 0;
        int stageIdx = stages.size();
        Stage stage = new Stage(stageIdx, members, inputStages, isSink, headTypeId, headSourceInputCount);
        stages.add(stage);
        // Record the head and every member of this stage in the memo, so a node inside the chain is
        // not absorbed twice elsewhere.
        for (int m : members) {
            nodeToStage.putIfAbsent(m, stageIdx);
        }
        nodeToStage.put(headIndex, stageIdx);
        return stageIdx;
    }

    private static void walkChain(PlanGraph graph, int nodeIndex, List<Stage> stages,
                                  List<Integer> members, List<Integer> inputStages,
                                  int[] outDeg, Map<Integer, Integer> nodeToStage) {
        members.add(nodeIndex);
        PlanNode n = graph.node(nodeIndex);

        // A SINK that absorbs its child chain can walk all the way up to a SOURCE, which would fuse
        // source, pass-through and sink into one stage that is a sink with no input stage at all, an
        // illegal shape. So the cut is made just before the SINK and the child becomes its own stage.
        if (n.kind() == PlanNode.Kind.SINK && fusablePathEndsAtSource(graph, nodeIndex, outDeg)) {
            for (int c = 0; c < n.childCount(); c++) {
                inputStages.add(buildStage(graph, n.child(c), stages, outDeg, nodeToStage));
            }
            return;
        }

        // Chainable when this node consumes its single child through one FORWARD input, that child is
        // not BINARY, and that child's out-degree is exactly one, so it is not a shared fan-out point.
        // A child with out-degree above one is left as its own upstream stage, deduplicated through the
        // memo and shared by every downstream consumer.
        if (n.childCount() == 1 && isForwardConsumer(n)
                && graph.node(n.child(0)).kind() != PlanNode.Kind.BINARY
                && !isPartitionTaskBoundary(graph.node(n.child(0)))
                && outDeg[n.child(0)] <= 1) {
            walkChain(graph, n.child(0), stages, members, inputStages, outDeg, nodeToStage);
            return;
        }
        // Otherwise each child is its own upstream stage, across a shuffle edge or a shared fan-out
        // point, and the memo reuses whatever is already built.
        for (int c = 0; c < n.childCount(); c++) {
            int childStage = buildStage(graph, n.child(c), stages, outDeg, nodeToStage);
            inputStages.add(childStage);
        }
    }

    /**
     * Whether {@code nodeIndex}, a SINK, would reach a SOURCE by walking up the chain it could absorb,
     * where each node consumes its single child through one FORWARD input and that child is not BINARY
     * and has out-degree at most one. If it does, absorbing the chain would leave a stage that is a
     * sink with no input stage, an illegal shape, so the cut must be made before the SINK. Only
     * walkChain calls this, on the multi-root path; the single-root build does not consult it and so
     * its behaviour is unchanged.
     */
    private static boolean fusablePathEndsAtSource(PlanGraph graph, int nodeIndex, int[] outDeg) {
        int cur = nodeIndex;
        while (true) {
            PlanNode n = graph.node(cur);
            if (n.kind() == PlanNode.Kind.SOURCE) {
                return true;
            }
            if (!(n.childCount() == 1 && isForwardConsumer(n)
                    && graph.node(n.child(0)).kind() != PlanNode.Kind.BINARY
                    && !isPartitionTaskBoundary(graph.node(n.child(0)))
                    && outDeg[n.child(0)] <= 1)) {
                return false;
            }
            cur = n.child(0);
        }
    }

    /** Whether this node consumes its single child through one FORWARD input: a SINK or pass-through. */
    private static boolean isForwardConsumer(PlanNode n) {
        return n.kind() == PlanNode.Kind.SINK || isPassThrough(n);
    }

    private static boolean isPassThrough(PlanNode node) {
        String typeId = node.typeId();
        // Any stateless row-by-row pass-through can be chained: map, flatMap, filter and the rest.
        // Inside a chain the rows flow through an in-memory channel in order, not over a socket
        // shuffle, so the resulting sets are identical. Breakers such as groupBy, sort, distinct and
        // join are not here, because they need the whole input, a total order, or more than one edge.
        return "filter".equals(typeId) || "filterFn".equals(typeId) || "filterParams".equals(typeId)
                || "select".equals(typeId) || "selectByIndex".equals(typeId) || "topN".equals(typeId)
                || "addFields".equals(typeId)
                || "map".equals(typeId) || "flatMap".equals(typeId)
                // A position must be consumed directly inside a stage headed by a SOURCE; it cannot
                // survive a shuffle.
                || io.rowweave.plan.SourcePositionPlanValidator
                .isPositionConsumer(node)
                || io.rowweave.plan.TypedRowClassificationPlanValidator.isRoute(node)
                || io.rowweave.plan.ObjectStorageQualityPlanValidator.isRoute(node);
        // partitionByHash is deliberately not here: on a cluster it must head a keyed shuffle stage,
        // as StageAssignment.needsKeyedShuffle decides. Treated as pass-through it would be absorbed
        // into a chain and the data would never actually be reshuffled.
    }

    /** map-partition owns one callback lifecycle per physical stage instance. */
    private static boolean isPartitionTaskBoundary(PlanNode node) {
        return node != null && "mapPartition".equals(node.typeId());
    }
}
