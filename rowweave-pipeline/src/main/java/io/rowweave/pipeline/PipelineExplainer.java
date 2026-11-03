package io.rowweave.pipeline;

/** Canonical metadata-only text explanation; never reads artifact payload bytes. */
public final class PipelineExplainer {
    private PipelineExplainer() { }

    public static String explain(PipelineExecutionPlan plan) {
        if (plan == null) {
            throw PipelineText.failure("INVALID_PLAN", "plan", "plan is required");
        }
        StringBuilder text = new StringBuilder(256);
        text.append("pipeline-v1\n")
                .append("identity\t").append(plan.pipelineId()).append('\t')
                .append(plan.revisionId()).append('\t')
                .append(plan.definitionDigest()).append('\n');
        for (PipelineExecutionUnit unit : plan.orderedUnits()) {
            text.append("unit\t").append(unit.stepId()).append('\t')
                    .append(unit.runtimeTarget()).append('\t')
                    .append(unit.artifact().artifactId()).append('\n');
        }
        for (PipelineEdge edge : plan.materializedEdges()) {
            text.append("edge\t").append(edge.edgeId()).append('\t')
                    .append(edge.producerStepId()).append('\t')
                    .append(edge.producerSinkOperatorId()).append('\t')
                    .append(edge.consumerStepId()).append('\t')
                    .append(edge.consumerSourceOperatorId()).append('\t')
                    .append(edge.materializationId()).append('\t')
                    .append(edge.schemaFingerprint()).append('\t')
                    .append(edge.boundaryKind()).append('\n');
        }
        for (PipelineOutput output : plan.selectedOutputs()) {
            text.append("output\t").append(output.outputId()).append('\t')
                    .append(output.stepId()).append('\t')
                    .append(output.sinkOperatorId()).append('\n');
        }
        return text.toString();
    }
}
