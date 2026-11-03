package io.rowweave.plan;

import io.rowweave.Source;
import io.rowweave.DataRow;
import io.rowweave.sched.PipelineExecutor;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Compiles a linear section of a PlanGraph, meaning exactly one SOURCE with only UNARY nodes above it and
 * no BINARY, into a {@link PipelineExecutor.Stage} sequence running from source through each operator to
 * the root, and hands it to PipelineExecutor to run as a push pipeline.
 *
 * <p>SOURCE becomes {@code PipelineExecutor.source}, which concatenates the Sources lazily and reads
 * nothing ahead; UNARY becomes {@link PlanOps#stageFor}. Operators are not fused into a single stage:
 * that is an optimization rather than a correctness property, and is left to separate work.
 *
 * <p>{@link #compile(PlanGraph, Map)} binds placeholder sources: at a cluster stage boundary a SOURCE
 * carrying no Sources is bound, by node index, to the {@link RowStream} of its incoming edge, the shuffle
 * channel, and pulls rows from there instead of iterating its own empty Source.
 */
public final class PlanCompiler {

    private final int parallelism;
    private final io.rowweave.mem.ExecutionMemory mem;

    /** Passes ExecutionMemory through as engine-internal context to {@link PlanOps#stageFor}, so a breaker gets its spill wiring. */
    public PlanCompiler(int parallelism, io.rowweave.mem.ExecutionMemory mem) {
        this.parallelism = parallelism;
        this.mem = mem;
    }

    /** Compile a linear PlanGraph into the stage list [source, op1, ..., root], with no placeholder source bindings. */
    public List<PipelineExecutor.Stage> compile(PlanGraph graph) {
        return compile(graph, Collections.emptyMap());
    }

    /**
     * As {@link #compile(PlanGraph)}, except that a SOURCE whose node index appears in
     * {@code sourceBindings} reads from the bound incoming {@link RowStream}, which is the placeholder
     * source at a cluster stage boundary, instead of from its own Sources. A SOURCE with no binding keeps
     * the lazy Source path.
     */
    public List<PipelineExecutor.Stage> compile(PlanGraph graph, Map<Integer, RowStream> sourceBindings) {
        return compileFrom(graph, graph.rootIndex(), sourceBindings);
    }

    /**
     * Compiles the linear section starting at {@code headIndex} and following child(0), keeping the node
     * index space of the original graph rather than renumbering, so indexes used by {@code sourceBindings}
     * remain correct. A SINK stage worker uses this to compile directly from the child of its SINK node,
     * with no second structural rewrite and no coupling between two index spaces.
     */
    public List<PipelineExecutor.Stage> compileFrom(PlanGraph graph, int headIndex,
                                                     Map<Integer, RowStream> sourceBindings) {
        // Walk child(0) from headIndex down to the SOURCE, which gives head to source, then reverse it to source to head.
        List<Integer> order = new ArrayList<>();
        int idx = headIndex;
        while (true) {
            order.add(idx);
            PlanNode n = graph.node(idx);
            if (n.kind() == PlanNode.Kind.SOURCE) {
                break;
            }
            if (n.kind() != PlanNode.Kind.UNARY) {
                throw new IllegalArgumentException(
                        "PlanCompiler requires a linear (SOURCE + UNARY chain) graph; got "
                                + n.kind() + " at node " + idx);
            }
            idx = n.child(0);
        }
        Collections.reverse(order); // source → … → root

        List<PipelineExecutor.Stage> stages = new ArrayList<>(order.size());
        for (int orderIndex = 0; orderIndex < order.size(); orderIndex++) {
            int nodeIdx = order.get(orderIndex);
            PlanNode node = graph.node(nodeIdx);
            if (node.kind() == PlanNode.Kind.SOURCE) {
                RowStream bound = sourceBindings.get(nodeIdx);
                boolean positioned = orderIndex + 1 < order.size()
                        && SourcePositionPlanValidator.isPositionConsumer(
                        graph.node(order.get(orderIndex + 1)));
                stages.add(bound != null
                        ? boundSource(bound, positioned)
                        : positioned
                        ? PipelineExecutor.sourceFromBindings(
                                node.param(PlanNode.OPERATOR_UID_PARAM),
                                node.sourceSplitBindings())
                        : PipelineExecutor.sourceFromInputs(node.inputs()));  // through SourceExecutor, with close as the backstop
            } else {
                stages.add(PlanOps.stageFor(graph, nodeIdx, parallelism, mem));
            }
        }
        return stages;
    }

    /**
     * Binds a source stage: pull rows one at a time from the incoming {@link RowStream} and emit them
     * downstream. A RECORD becomes emitRow; an Error passes through and fails fast; EOS and any other
     * control event are ignored, since closing the channel ends the stage. This is how a placeholder
     * source at a cluster stage boundary is wired to its shuffle channel.
     */
    private static PipelineExecutor.Stage boundSource(
            RowStream upstream, boolean preservePositionedRecord) {
        return (ignoredIn, out) -> {
            StreamEvent e;
            while ((e = upstream.next()) != null) {
                if (e.isRecord()) {
                    if (preservePositionedRecord) {
                        out.emit(e);
                    } else {
                        out.emitRow(e.row());
                    }
                } else if (e.isError()) {
                    out.emit(e);  // pass the upstream stage error through, fail fast
                    return;
                }
            }
        };
    }

}
