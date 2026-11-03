package io.rowweave.plan;

import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.sched.stream.SourceRecordPosition;

import java.util.HashSet;
import java.util.Set;

/** fail-closed topology and exact-source admission shared by Master and Worker rebuild. */
public final class SourcePositionPlanValidator {

    public static final String MODE_PARAM = "sourcePositionMode";
    public static final String EXACT_SOURCE_V1 = "EXACT_SOURCE_V1";

    private SourcePositionPlanValidator() {
    }

    public static void validate(PlanGraph graph) {
        if (graph == null) throw invalid("plan graph is required");
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() == PlanNode.Kind.UNARY
                    && "rowValidate".equals(node.typeId())
                    && node.param(MODE_PARAM) != null) {
                validateNode(graph, i);
            }
        }
    }

    public static void validateNode(PlanGraph graph, int validationNodeIndex) {
        PlanNode validation = graph.node(validationNodeIndex);
        String mode = validation.param(MODE_PARAM);
        if (!EXACT_SOURCE_V1.equals(mode)) {
            throw invalid("source position mode is unsupported");
        }
        if (validation.kind() != PlanNode.Kind.UNARY
                || !"rowValidate".equals(validation.typeId())
                || validation.childCount() != 1) {
            throw invalid("positioned validation node is malformed");
        }
        PlanNode source = graph.node(validation.child(0));
        if (source.kind() != PlanNode.Kind.SOURCE) {
            throw invalid("positioned validation must be directly adjacent to SOURCE");
        }
        String sourceUid = source.param(PlanNode.OPERATOR_UID_PARAM);
        if (sourceUid == null || source.sourceSplitDescriptors().isEmpty()) {
            throw invalid("positioned validation requires an explicit exact SOURCE");
        }
        source.validateCurrentSourceBindings();
        Set<String> identities = new HashSet<>();
        for (BatchSourceSplitDescriptor descriptor : source.sourceSplitDescriptors()) {
            final SourceRecordPosition initial;
            try {
                initial = SourceRecordPosition.of(sourceUid, descriptor, 0L);
            } catch (IllegalArgumentException invalidPosition) {
                throw invalid("SOURCE identity or replay contract is invalid");
            }
            if (!identities.add(initial.splitIdentitySha256())) {
                throw invalid("SOURCE split identity is duplicated");
            }
        }
    }

    public static boolean isPositionedValidation(PlanNode node) {
        return node.kind() == PlanNode.Kind.UNARY
                && "rowValidate".equals(node.typeId())
                && EXACT_SOURCE_V1.equals(node.param(MODE_PARAM));
    }

    /** Any direct SOURCE consumer that requires positioned RECORD events. */
    public static boolean isPositionConsumer(PlanNode node) {
        return isPositionedValidation(node)
                || TypedRowClassificationPlanValidator.isClassifier(node)
                // Object decoder already embeds its original physical ordinal, but retaining
                // exact bindings lets every Worker re-prove the engine-owned wrapper before open.
                || ObjectStorageQualityPlanValidator.isRoute(node);
    }

    public static boolean sourceFeedsPositionedValidation(
            PlanGraph graph, int sourceNodeIndex) {
        for (int i = sourceNodeIndex + 1; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (isPositionedValidation(node)
                    && node.childCount() == 1
                    && node.child(0) == sourceNodeIndex) {
                return true;
            }
        }
        return false;
    }

    public static boolean sourceFeedsPositionConsumer(
            PlanGraph graph, int sourceNodeIndex) {
        for (int i = sourceNodeIndex + 1; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (isPositionConsumer(node)
                    && node.childCount() == 1
                    && node.child(0) == sourceNodeIndex) {
                return true;
            }
        }
        return false;
    }

    private static IllegalArgumentException invalid(String detail) {
        return new IllegalArgumentException("QUALITY_SOURCE_POSITION_INVALID: " + detail);
    }
}
