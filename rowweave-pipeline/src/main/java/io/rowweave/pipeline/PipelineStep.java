package io.rowweave.pipeline;

import io.rowweave.artifact.PlanArtifact;

import java.util.Objects;

/** One atomic Batch or Stream execution unit in a Pipeline definition. */
public final class PipelineStep {
    private final String stepId;
    private final PlanArtifact artifact;

    public PipelineStep(String stepId, PlanArtifact artifact) {
        this.stepId = PipelineText.required(
                stepId, "step.stepId", PipelineLimits.MAX_ID_BYTES);
        if (artifact == null) {
            throw PipelineText.failure("INVALID_ARTIFACT", "step.artifact",
                    "artifact is required");
        }
        this.artifact = artifact;
    }

    public String stepId() { return stepId; }
    public PlanArtifact artifact() { return artifact; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof PipelineStep)) return false;
        PipelineStep that = (PipelineStep) other;
        return stepId.equals(that.stepId)
                && artifact.artifactId().equals(that.artifact.artifactId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(stepId, artifact.artifactId());
    }
}
