package io.rowweave.pipeline;

import io.rowweave.artifact.PlanArtifact;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Fluent public SDK for constructing the bounded Pipeline composition contract. */
public final class PipelineDefinitionBuilder {
    private final String pipelineId;
    private final String revisionId;
    private final List<String> optionalFeatures = new ArrayList<>();
    private final List<PipelineStep> steps = new ArrayList<>();
    private final List<PipelineEdge> edges = new ArrayList<>();
    private final List<PipelineOutput> outputs = new ArrayList<>();

    PipelineDefinitionBuilder(String pipelineId, String revisionId) {
        this.pipelineId = PipelineText.required(
                pipelineId, "pipeline.pipelineId", PipelineLimits.MAX_ID_BYTES);
        this.revisionId = PipelineText.required(
                revisionId, "pipeline.revisionId", PipelineLimits.MAX_ID_BYTES);
    }

    public PipelineDefinitionBuilder optionalFeature(String featureId) {
        optionalFeatures.add(PipelineText.required(featureId,
                "pipeline.optionalFeatures", PipelineLimits.MAX_FEATURE_BYTES));
        return this;
    }

    public PipelineDefinitionBuilder addStep(PipelineStep step) {
        if (step == null) {
            throw PipelineText.failure("INVALID_STEP", "pipeline.steps",
                    "step is required");
        }
        steps.add(step);
        return this;
    }

    public PipelineDefinitionBuilder addBatchStep(
            String stepId, PlanArtifact artifact) {
        return addStep(PipelineArtifactAdapter.forBatch(stepId, artifact));
    }

    public PipelineDefinitionBuilder addStreamStep(
            String stepId, PlanArtifact artifact) {
        return addStep(PipelineArtifactAdapter.forStream(stepId, artifact));
    }

    public PipelineDefinitionBuilder addEdge(PipelineEdge edge) {
        if (edge == null) {
            throw PipelineText.failure("INVALID_EDGE", "pipeline.edges",
                    "edge is required");
        }
        edges.add(edge);
        return this;
    }

    public PipelineDefinitionBuilder addVersionedDatasetEdge(
            String edgeId, String producerStepId, String producerSinkOperatorId,
            String consumerStepId, String consumerSourceOperatorId,
            String materializationId, String schemaFingerprint) {
        return addEdge(new PipelineEdge(edgeId, producerStepId,
                producerSinkOperatorId, consumerStepId, consumerSourceOperatorId,
                materializationId, schemaFingerprint,
                PipelineEdge.BoundaryKind.VERSIONED_DATASET));
    }

    public PipelineDefinitionBuilder addOutput(PipelineOutput output) {
        if (output == null) {
            throw PipelineText.failure("INVALID_OUTPUT", "pipeline.outputs",
                    "output is required");
        }
        outputs.add(output);
        return this;
    }

    public PipelineDefinitionBuilder addOutput(
            String outputId, String stepId, String sinkOperatorId) {
        return addOutput(new PipelineOutput(outputId, stepId, sinkOperatorId));
    }

    public PipelineDefinition build() {
        return new PipelineDefinition(
                PipelineDefinition.CURRENT_FORMAT_VERSION,
                PipelineDefinition.CURRENT_READER_VERSION,
                pipelineId, revisionId,
                Collections.singletonList(PipelineDefinition.FEATURE_COMPOSITION_V1),
                optionalFeatures, steps, edges, outputs);
    }
}
