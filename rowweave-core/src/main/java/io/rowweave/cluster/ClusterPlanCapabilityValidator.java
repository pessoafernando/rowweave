package io.rowweave.cluster;

import io.rowweave.ops.join.JoinPhysicalStrategy;
import io.rowweave.ops.SchemaConversion;
import io.rowweave.ops.RowValidation;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PlanOps;

/** Validates that a complete plan can be executed before cluster scheduling has any side effect. */
final class ClusterPlanCapabilityValidator {

    private ClusterPlanCapabilityValidator() {
    }

    static void validate(PlanGraph graph) {
        if (graph == null) {
            throw new IllegalArgumentException("cluster plan graph must not be null");
        }
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() == PlanNode.Kind.UNARY && !PlanOps.supportsUnaryType(node.typeId())) {
                throw new IllegalArgumentException("cluster DAG does not support UNARY node " + i
                        + " typeId '" + node.typeId() + "'");
            }
            if (node.kind() == PlanNode.Kind.UNARY && "schemaConvert".equals(node.typeId())) {
                SchemaConversion.fromDescribable(
                        graph.node(node.child(0)).outMeta(), node.outMeta(),
                        node.listParam("sourceIndices"), node.listParam("casts"),
                        node.listParam("nullPolicies"), node.listParam("defaultKinds"),
                        node.listParam("defaultValues"));
            }
            if (node.kind() == PlanNode.Kind.UNARY && "rowValidate".equals(node.typeId())) {
                RowValidation.fromDescribable(
                        graph.node(node.child(0)).outMeta(), node.outMeta(),
                        node.listParam("ruleIds"), node.listParam("ruleKinds"),
                        node.listParam("fieldIndices"), node.listParam("lowerBounds"),
                        node.listParam("upperBounds"));
            }
            if (node.kind() == PlanNode.Kind.BINARY && !supportsBinary(node)) {
                throw unsupportedBinary(node);
            }
        }
        io.rowweave.plan.SourcePositionPlanValidator.validate(graph);
        io.rowweave.plan.ObjectStorageQualityPlanValidator.validateFullGraph(graph);
        io.rowweave.plan.TypedQualityPublicationPlanValidator.validateFullGraph(graph);
        io.rowweave.plan.TypedRowClassificationPlanValidator.validateFullGraph(graph);
    }

    static boolean supportsBinary(PlanNode node) {
        if (node.kind() != PlanNode.Kind.BINARY) {
            return false;
        }
        String typeId = node.typeId();
        if ("join".equals(typeId)) {
            String hint = node.param("joinHint");
            return hint == null || "AUTO".equals(hint)
                    || "HASH".equals(hint) || "SORT_MERGE".equals(hint);
        }
        return "coGroup".equals(typeId) || "union".equals(typeId);
    }

    static IllegalArgumentException unsupportedBinary(PlanNode node) {
        if (node.kind() == PlanNode.Kind.BINARY && "join".equals(node.typeId())) {
            String hint = node.param("joinHint");
            try {
                JoinPhysicalStrategy.fromPlanHint(hint);
            } catch (IllegalArgumentException unknown) {
                return new IllegalArgumentException("cluster DAG does not support joinHint '"
                        + hint + "' for BINARY operator 'join' (expected AUTO, HASH or SORT_MERGE)");
            }
        }
        return new IllegalArgumentException("Cluster DAG does not yet support BINARY operator '"
                + node.typeId() + "' (join/coGroup/union are supported)");
    }
}
