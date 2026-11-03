package io.rowweave.pipeline;

/** Safe public derivation of runtime handoff identities from compiled Pipeline metadata. */
public final class VersionedDatasetHandoffSdk {
    private VersionedDatasetHandoffSdk() { }

    public static VersionedDatasetHandoffKey handoffKey(
            PipelineDefinition definition, PipelineExecutionPlan plan,
            String edgeId, String logicalRunId) {
        requirePlan(definition, plan);
        String checkedEdgeId = VersionedDatasetHandoffSupport.id(
                edgeId, "handoff.edgeId");
        PipelineEdge edge = definitionEdge(definition, checkedEdgeId);
        if (!containsEdge(plan, edge)) {
            throw PipelineText.failure("EDGE_NOT_SELECTED", "handoff.edgeId",
                    "edge is not retained by the compiled requested-output plan");
        }
        PipelineExecutionUnit producer = unit(
                plan, edge.producerStepId(), "handoff.producerStepId");
        return new VersionedDatasetHandoffKey(
                definition.pipelineId(), definition.revisionId(),
                definition.definitionDigest(), edge.edgeId(),
                edge.materializationId(), edge.schemaFingerprint(),
                edge.producerStepId(), edge.producerSinkOperatorId(),
                producer.artifact().artifactId(), logicalRunId);
    }

    public static VersionedDatasetProducerAttempt producerAttempt(
            VersionedDatasetHandoffKey key, String producerRunId,
            String attemptId, long attemptNumber, long fenceToken) {
        return new VersionedDatasetProducerAttempt(
                key, producerRunId, attemptId, attemptNumber, fenceToken);
    }

    public static VersionedDatasetConsumerRequest consumerRequest(
            PipelineDefinition definition, PipelineExecutionPlan plan,
            VersionedDatasetHandoffKey key,
            VersionedDatasetVisibleWinner winner,
            String consumerRunId, String consumerAttemptId) {
        if (key == null || winner == null) {
            throw PipelineText.failure("INVALID_BINDING_INPUT", "consumer",
                    "handoff key and visible winner are required");
        }
        VersionedDatasetHandoffKey expected = handoffKey(
                definition, plan, key.edgeId(), key.logicalRunId());
        if (!expected.equals(key)) {
            throw PipelineText.failure("HANDOFF_KEY_MISMATCH", "consumer.key",
                    "handoff key is not derived from the compiled edge");
        }
        if (!winner.evidence().attempt().key().equals(key)) {
            throw PipelineText.failure("WINNER_KEY_MISMATCH", "consumer.winner",
                    "visible winner belongs to another handoff key");
        }
        PipelineEdge edge = selectedEdge(plan, key.edgeId());
        PipelineExecutionUnit consumer = unit(
                plan, edge.consumerStepId(), "consumer.consumerStepId");
        return new VersionedDatasetConsumerRequest(
                key, winner.winnerDigest(), edge.consumerStepId(),
                edge.consumerSourceOperatorId(),
                consumer.artifact().artifactId(), consumerRunId,
                consumerAttemptId);
    }

    private static void requirePlan(
            PipelineDefinition definition, PipelineExecutionPlan plan) {
        if (definition == null || plan == null) {
            throw PipelineText.failure("INVALID_PIPELINE_PLAN", "handoff.plan",
                    "definition and compiled plan are required");
        }
        if (!definition.pipelineId().equals(plan.pipelineId())
                || !definition.revisionId().equals(plan.revisionId())
                || !definition.definitionDigest().equals(plan.definitionDigest())) {
            throw PipelineText.failure("PLAN_DEFINITION_MISMATCH", "handoff.plan",
                    "compiled plan does not belong to the supplied definition");
        }
    }

    private static PipelineEdge definitionEdge(
            PipelineDefinition definition, String edgeId) {
        for (PipelineEdge edge : definition.edges()) {
            if (edge.edgeId().equals(edgeId)) return edge;
        }
        throw PipelineText.failure("UNKNOWN_EDGE", "handoff.edgeId",
                "edge is absent from the Pipeline definition");
    }

    private static boolean containsEdge(
            PipelineExecutionPlan plan, PipelineEdge expected) {
        for (PipelineEdge edge : plan.materializedEdges()) {
            if (edge.equals(expected)) return true;
        }
        return false;
    }

    private static PipelineEdge selectedEdge(
            PipelineExecutionPlan plan, String edgeId) {
        for (PipelineEdge edge : plan.materializedEdges()) {
            if (edge.edgeId().equals(edgeId)) return edge;
        }
        throw PipelineText.failure("EDGE_NOT_SELECTED", "handoff.edgeId",
                "edge is not retained by the compiled plan");
    }

    private static PipelineExecutionUnit unit(
            PipelineExecutionPlan plan, String stepId, String path) {
        for (PipelineExecutionUnit unit : plan.orderedUnits()) {
            if (unit.stepId().equals(stepId)) return unit;
        }
        throw PipelineText.failure("STEP_NOT_SELECTED", path,
                "edge endpoint is absent from the compiled plan");
    }
}
