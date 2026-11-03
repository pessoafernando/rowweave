package io.rowweave.pipeline;

import java.util.Collection;

/** Public side-effect-free validation and compilation entry point for product clients. */
public final class PipelineValidator {
    private PipelineValidator() { }

    public static PipelineExecutionPlan validateAndCompile(
            PipelineDefinition definition, Collection<String> requestedOutputIds) {
        return PipelineCompiler.compile(definition, requestedOutputIds);
    }
}
