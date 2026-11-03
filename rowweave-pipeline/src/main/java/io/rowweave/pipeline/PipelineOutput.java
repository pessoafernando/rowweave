package io.rowweave.pipeline;

import java.util.Objects;

/** Stable product-visible output selection for Pipeline compilation. */
public final class PipelineOutput {
    private final String outputId;
    private final String stepId;
    private final String sinkOperatorId;

    public PipelineOutput(String outputId, String stepId, String sinkOperatorId) {
        this.outputId = PipelineText.required(
                outputId, "output.outputId", PipelineLimits.MAX_ID_BYTES);
        this.stepId = PipelineText.required(
                stepId, "output.stepId", PipelineLimits.MAX_ID_BYTES);
        this.sinkOperatorId = PipelineText.required(
                sinkOperatorId, "output.sinkOperatorId", PipelineLimits.MAX_ID_BYTES);
    }

    public String outputId() { return outputId; }
    public String stepId() { return stepId; }
    public String sinkOperatorId() { return sinkOperatorId; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof PipelineOutput)) return false;
        PipelineOutput that = (PipelineOutput) other;
        return outputId.equals(that.outputId)
                && stepId.equals(that.stepId)
                && sinkOperatorId.equals(that.sinkOperatorId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputId, stepId, sinkOperatorId);
    }
}
