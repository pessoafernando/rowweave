package io.rowweave.pipeline;

import io.rowweave.artifact.PlanArtifact;

/** One complete, atomic Batch or Stream artifact selected for execution. */
public final class PipelineExecutionUnit {
    private final String stepId;
    private final PlanArtifact artifact;

    PipelineExecutionUnit(PipelineStep step) {
        this.stepId = step.stepId();
        this.artifact = step.artifact();
    }

    public String stepId() { return stepId; }
    public PlanArtifact artifact() { return artifact; }
    public PlanArtifact.RuntimeTarget runtimeTarget() {
        return artifact.runtimeTarget();
    }
}
