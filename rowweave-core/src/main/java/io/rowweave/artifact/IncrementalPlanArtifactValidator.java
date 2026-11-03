package io.rowweave.artifact;

import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.incremental.IncrementalPlanDescriptor;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;

/** Verifies the artifact feature gate against the one canonical incremental SOURCE descriptor. */
final class IncrementalPlanArtifactValidator {
    private IncrementalPlanArtifactValidator() {
    }

    static void validate(PlanArtifact artifact, PlanGraph graph) {
        boolean feature = artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_BOUNDED_INCREMENTAL_FRONTIER);
        int totalSources = 0;
        int incrementalSources = 0;
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() == PlanNode.Kind.SOURCE) totalSources++;
            if (!node.hasIncrementalPlanDescriptor()) continue;
            incrementalSources++;
            if (node.kind() != PlanNode.Kind.SOURCE) {
                throw invalid("descriptor is attached to a non-SOURCE node");
            }
            validateSource(node);
        }
        if (!feature && incrementalSources != 0) {
            throw invalid("incremental descriptor requires the required feature");
        }
        if (feature && (totalSources != 1 || incrementalSources != 1)) {
            throw invalid("incremental feature requires exactly one SOURCE with one descriptor");
        }
    }

    private static void validateSource(PlanNode source) {
        IncrementalPlanDescriptor descriptor = source.incrementalPlanDescriptor();
        String operatorUid = source.param(PlanNode.OPERATOR_UID_PARAM);
        if (operatorUid == null || !operatorUid.equals(descriptor.sourceOperatorUid())) {
            throw invalid("incremental SOURCE UID differs from its descriptor");
        }
        int fields = source.outMeta().getFieldCount();
        validateIndexes(descriptor.primaryKeyFieldIndexes(), fields, "primary key");
        validateIndexes(descriptor.recordPositionFieldIndexes(), fields, "record position");
        if (descriptor.operationFieldIndex() >= fields) {
            throw invalid("operation field index exceeds SOURCE schema");
        }
        String expectedSchema = SchemaFingerprint.of(source.outMeta());
        String actualSchema = PlanArtifactCodec.hex(descriptor.outputSchemaFingerprint());
        if (!expectedSchema.equals(actualSchema)) {
            throw invalid("incremental descriptor output schema fingerprint differs from SOURCE");
        }
    }

    private static void validateIndexes(java.util.List<Integer> indexes, int fieldCount, String field) {
        for (Integer index : indexes) {
            if (index >= fieldCount) {
                throw invalid(field + " field index exceeds SOURCE schema");
            }
        }
    }

    private static PlanArtifactValidationException invalid(String detail) {
        return ArtifactText.failure(
                "INVALID_INCREMENTAL_PLAN", "payload.incremental", detail);
    }
}
