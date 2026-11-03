package io.rowweave.pipeline;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactValidationException;
import io.rowweave.artifact.PlanArtifactValidator;

/** Public metadata-only seam from Batch/Stream PlanArtifact to PipelineStep. */
public final class PipelineArtifactAdapter {
    private PipelineArtifactAdapter() { }

    public static PipelineStep forBatch(String stepId, PlanArtifact artifact) {
        return adapt(stepId, artifact, PlanArtifact.RuntimeTarget.BATCH);
    }

    public static PipelineStep forStream(String stepId, PlanArtifact artifact) {
        return adapt(stepId, artifact, PlanArtifact.RuntimeTarget.STREAM);
    }

    static void validateMetadata(PlanArtifact artifact, String path) {
        try {
            if (artifact == null) {
                PlanArtifactValidator.validateReadableMetadata(null);
            } else if (artifact.runtimeTarget() == PlanArtifact.RuntimeTarget.BATCH) {
                PlanArtifactValidator.validateForPublicV1Metadata(artifact);
            } else {
                PlanArtifactValidator.validateForStreamProductMetadata(artifact);
            }
        } catch (PlanArtifactValidationException invalid) {
            throw PipelineText.failure("INVALID_ARTIFACT_METADATA", path,
                    invalid.code() + " at " + invalid.path());
        }
    }

    private static PipelineStep adapt(
            String stepId, PlanArtifact artifact,
            PlanArtifact.RuntimeTarget expectedRuntime) {
        if (artifact == null || artifact.runtimeTarget() != expectedRuntime) {
            throw PipelineText.failure("RUNTIME_TARGET_MISMATCH",
                    "step.artifact.runtimeTarget",
                    "expected " + expectedRuntime);
        }
        validateMetadata(artifact, "step.artifact");
        return new PipelineStep(stepId, artifact);
    }
}
