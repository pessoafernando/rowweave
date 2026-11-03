package io.rowweave.cluster;

import io.rowweave.DataSchema;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Turns the member nodes of a {@link Stage} into a standalone executable {@link StagePlan}
 * taken from the original {@link PlanGraph}. The work is purely structural: nothing runs, no
 * row is read and no DataRow is held.<p>The sub-graph is rebuilt along the members with fresh
 * indices. Where a member's child lies outside this stage, that is across a stage boundary, the
 * child is replaced by a placeholder source, an empty source carrying the meta of the node it
 * stands for, and a {@link StageBoundary} records the pair of that placeholder source and the
 * upstream stage owning the child. The head of the sub-plan is the last member, the sink side.
 */
public final class StageBoundaryRebuilder {

    private StageBoundaryRebuilder() {
    }

    public static StagePlan rebuild(PlanGraph original, ExecutionGraph eg, Stage stage) {
        Set<Integer> memberSet = new HashSet<>(stage.members());
        int head = stage.members().get(stage.members().size() - 1);  // the chain head is the last member, on the sink side

        Map<Integer, Integer> nodeToStage = nodeToStage(eg);

        List<PlanNode> newNodes = new ArrayList<>();
        Map<Integer, Integer> oldToNew = new HashMap<>();
        List<StageBoundary> boundaries = new ArrayList<>();
        rebuildNode(original, head, memberSet, nodeToStage, newNodes, oldToNew, boundaries);

        return new StagePlan(stage.index(), new PlanGraph(newNodes), boundaries,
                original.node(head).kind() == PlanNode.Kind.SINK);
    }

    /** Rebuilds the members in post-order; a child outside the stage becomes a placeholder source plus a boundary, and returns the new index. */
    private static int rebuildNode(PlanGraph original, int oldIndex, Set<Integer> memberSet,
                                   Map<Integer, Integer> nodeToStage, List<PlanNode> newNodes,
                                   Map<Integer, Integer> oldToNew, List<StageBoundary> boundaries) {
        Integer existing = oldToNew.get(oldIndex);
        if (existing != null) {
            return existing;
        }
        PlanNode old = original.node(oldIndex);

        int[] childNew = new int[old.childCount()];
        for (int c = 0; c < old.childCount(); c++) {
            int childOld = old.child(c);
            if (memberSet.contains(childOld)) {
                childNew[c] = rebuildNode(original, childOld, memberSet, nodeToStage, newNodes, oldToNew, boundaries);
            } else {
                childNew[c] = placeholderSource(original, childOld, nodeToStage, newNodes, boundaries);
            }
        }

        PlanNode rebuilt = rebuildWithChildren(old, childNew);
        newNodes.add(rebuilt);
        int newIdx = newNodes.size() - 1;
        oldToNew.put(oldIndex, newIdx);
        return newIdx;
    }

    private static int placeholderSource(PlanGraph original, int childOld, Map<Integer, Integer> nodeToStage,
                                         List<PlanNode> newNodes, List<StageBoundary> boundaries) {
        DataSchema meta = original.node(childOld).outMeta();
        newNodes.add(PlanNode.placeholderSource(meta));  // placeholder source with no real source, bound at execution to the upstream shuffle channel
        int newIdx = newNodes.size() - 1;
        Integer upstreamStage = nodeToStage.get(childOld);
        boundaries.add(new StageBoundary(newIdx, upstreamStage == null ? -1 : upstreamStage));
        return newIdx;
    }

    /** Rebuilds a node with new child indices, keeping its type id, parameters and output; a SOURCE with no child is a member leaf and its source is reused. */
    private static PlanNode rebuildWithChildren(PlanNode old, int[] childNew) {
        switch (old.kind()) {
            case SOURCE:
                return old.withSourceSplitBindings(old.sourceSplitBindings());
            case UNARY: {
                // Rebuilt from the serializable user function and the type id, with no in-memory stage to fall back to.
                if (old.userFunction() != null) {
                    return old.params().isEmpty() && old.listParams().isEmpty()
                            ? PlanNode.unaryFn(old.typeId(), old.outMeta(), childNew[0], old.userFunction())
                            : PlanNode.unaryFnWithParams(old.typeId(), old.outMeta(), childNew[0],
                                    old.params(), old.listParams(), old.userFunction());
                }
                PlanNode.Builder b = PlanNode.unary(old.typeId(), old.outMeta(), childNew[0]);
                copyParams(old, b);
                return b.build();
            }
            case BINARY: {
                if (old.userFunction() != null) {
                    return PlanNode.binaryFn(old.typeId(), old.outMeta(), childNew[0], childNew[1],
                            old.params(), old.listParams(), old.userFunction());
                }
                PlanNode.Builder b = PlanNode.binary(old.typeId(), old.outMeta(), childNew[0], childNew[1]);
                copyParams(old, b);
                return b.build();
            }
            case SINK: {
                PlanNode.Builder b = (old.output() != null)
                        ? PlanNode.sink(old.outMeta(), childNew[0], old.output())
                        : PlanNode.sink(old.outMeta(), childNew[0], old.param(PlanNode.OUTPUT_TYPE_PARAM));
                copyParams(old, b);
                return b.build();
            }
            default:
                throw new IllegalStateException("unknown kind: " + old.kind());
        }
    }

    private static void copyParams(PlanNode src, PlanNode.Builder b) {
        for (Map.Entry<String, String> e : src.params().entrySet()) {
            b.param(e.getKey(), e.getValue());
        }
        for (Map.Entry<String, List<String>> e : src.listParams().entrySet()) {
            b.listParam(e.getKey(), e.getValue());
        }
    }

    private static Map<Integer, Integer> nodeToStage(ExecutionGraph eg) {
        Map<Integer, Integer> map = new HashMap<>();
        for (Stage s : eg.stages()) {
            for (int member : s.members()) {
                map.put(member, s.index());
            }
        }
        return map;
    }
}
