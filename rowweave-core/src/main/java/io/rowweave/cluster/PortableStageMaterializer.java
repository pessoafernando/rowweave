package io.rowweave.cluster;

import io.rowweave.AssignedSourceBatch;
import io.rowweave.CoGroupFunction;
import io.rowweave.Source;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.ops.SortField;
import io.rowweave.plan.InputSharding;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PlanOps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Rebinds a metadata-only Master assignment to exact signed Worker objects. */
final class PortableStageMaterializer {
    private PortableStageMaterializer() { }

    static StageCommand materialize(
            PlanGraph runtimeGraph, StageCommand.WireFields fields,
            PortableStageSpec spec) {
        if (runtimeGraph == null || fields == null || spec == null) {
            throw new IllegalArgumentException(
                    "portable runtime graph, routing, and stage slice are required");
        }
        validateMembers(runtimeGraph, fields.role, spec);
        List<Integer> upstream = fields.role == StageCommand.Role.SOURCE_STAGE
                ? Collections.<Integer>emptyList()
                : Collections.singletonList(0);
        int first = spec.members().get(0);
        PlanNode head = runtimeGraph.node(first);
        Stage stage = new Stage(spec.logicalStage(), spec.members(), upstream,
                fields.role == StageCommand.Role.SINK, head.typeId(),
                head.kind() == PlanNode.Kind.SOURCE
                        ? head.sourceSplitBindings().size() : 0);

        Source input = null;
        Sink output = null;
        CoGroupFunction coGroup = null;
        PlanGraph fragment = null;
        PlanGraph sinkFragment = null;
        if (fields.role == StageCommand.Role.SOURCE_STAGE) {
            List<List<io.rowweave.BatchSourceSplitBinding>> shards =
                    InputSharding.shardSourceBindings(
                            head.sourceSplitBindings(), spec.instanceCount());
            if (shards.size() != spec.instanceCount()) {
                throw new IllegalArgumentException(
                        "portable source instance count does not match split shards");
            }
            input = new AssignedSourceBatch(
                    shards.get(spec.instanceIndex()));
            fragment = stageFragment(runtimeGraph, stage, fields.dataSchema);
        } else if (fields.role == StageCommand.Role.KEYED_OP) {
            fragment = stageFragment(runtimeGraph, stage, fields.dataSchema);
        } else if (fields.role == StageCommand.Role.SINK) {
            PlanNode sink = runtimeGraph.node(
                    spec.members().get(spec.members().size() - 1));
            output = sink.output();
            sinkFragment = stageFragment(
                    runtimeGraph, stage, fields.dataSchema);
            if (spec.canonicalizeSink()) {
                sinkFragment = appendOrderByAllFields(sinkFragment);
            }
        } else if (fields.role == StageCommand.Role.JOIN_CONSUMER
                && "coGroup".equals(head.typeId())) {
            coGroup = (CoGroupFunction) head.userFunction();
        }
        StageCommand command = StageCommand.fromWire(
                fields, input, output, coGroup, fragment, sinkFragment);
        command.attachPortableRuntimeGraph(runtimeGraph);
        return command;
    }

    private static void validateMembers(
            PlanGraph graph, StageCommand.Role role,
            PortableStageSpec spec) {
        for (int member : spec.members()) {
            if (member < 0 || member >= graph.nodeCount()) {
                throw new IllegalArgumentException(
                        "portable stage member is outside runtime graph");
            }
        }
        PlanNode first = graph.node(spec.members().get(0));
        PlanNode last = graph.node(
                spec.members().get(spec.members().size() - 1));
        if ((role == StageCommand.Role.SOURCE_STAGE)
                != (first.kind() == PlanNode.Kind.SOURCE)
                || (role == StageCommand.Role.SINK)
                != (last.kind() == PlanNode.Kind.SINK)
                || spec.canonicalizeSink()
                && role != StageCommand.Role.SINK) {
            throw new IllegalArgumentException(
                    "portable stage slice does not match assignment role");
        }
    }

    private static PlanGraph stageFragment(
            PlanGraph graph, Stage stage, DataSchema inMeta) {
        List<PlanNode> nodes = new ArrayList<>();
        PlanNode placeholder = PlanNode.placeholderSource(inMeta);
        if (stage.inputStages().isEmpty()) {
            int sourceIndex = stage.members().get(0);
            if (io.rowweave.plan.SourcePositionPlanValidator
                    .sourceFeedsPositionConsumer(graph, sourceIndex)) {
                String sourceUid = graph.node(sourceIndex)
                        .param(PlanNode.OPERATOR_UID_PARAM);
                if (sourceUid != null) {
                    placeholder = placeholder.withParam(
                            PlanNode.OPERATOR_UID_PARAM, sourceUid);
                }
            }
        }
        nodes.add(placeholder);
        int previous = 0;
        for (int member : stage.members()) {
            PlanNode node = graph.node(member);
            if (node.kind() == PlanNode.Kind.SOURCE
                    || node.kind() == PlanNode.Kind.SINK) {
                continue;
            }
            if (node.kind() != PlanNode.Kind.UNARY) {
                throw new IllegalArgumentException(
                        "portable stage fragment contains a non-unary member");
            }
            nodes.add(rebuildUnaryOnChild(node, previous));
            previous = nodes.size() - 1;
        }
        return new PlanGraph(nodes);
    }

    private static PlanNode rebuildUnaryOnChild(
            PlanNode node, int newChild) {
        if (node.userFunction() != null) {
            return PlanNode.unaryFnWithParams(
                    node.typeId(), node.outMeta(), newChild,
                    node.params(), node.listParams(), node.userFunction());
        }
        PlanNode.Builder builder = PlanNode.unary(
                node.typeId(), node.outMeta(), newChild);
        for (Map.Entry<String, String> entry : node.params().entrySet()) {
            builder.param(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, List<String>> entry
                : node.listParams().entrySet()) {
            builder.listParam(entry.getKey(), entry.getValue());
        }
        return builder.build();
    }

    private static PlanGraph appendOrderByAllFields(PlanGraph fragment) {
        List<PlanNode> nodes = new ArrayList<>();
        for (int index = 0; index < fragment.nodeCount(); index++) {
            nodes.add(fragment.node(index));
        }
        DataSchema outMeta = fragment.root().outMeta();
        List<String> sort = new ArrayList<>();
        for (int index = 0; index < outMeta.getFieldCount(); index++) {
            sort.add(PlanOps.encodeSortField(new SortField(
                    index, SortField.Direction.ASC,
                    SortField.NullOrder.NULLS_LAST)));
        }
        nodes.add(PlanNode.unary(
                "orderBy", outMeta, fragment.rootIndex())
                .listParam("sort", sort).build());
        return new PlanGraph(nodes);
    }
}
