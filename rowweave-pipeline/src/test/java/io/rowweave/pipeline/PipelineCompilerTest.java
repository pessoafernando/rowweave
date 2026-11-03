package io.rowweave.pipeline;

import io.rowweave.artifact.OperatorDescriptor;
import io.rowweave.artifact.PlanArtifact;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineCompilerTest {
    private static final String SCHEMA = repeat('d', 64);

    @Test
    void requestedOutputPrunesUnreachableUnitsAndKeepsArtifactsAtomic() {
        PipelineDefinition definition = branchingDefinition();

        PipelineExecutionPlan plan = PipelineCompiler.compile(
                definition, Collections.singletonList("result-beta"));

        assertEquals(Arrays.asList("shared", "beta"), unitIds(plan));
        assertEquals(Collections.singletonList("shared-beta"), edgeIds(plan));
        assertEquals(Collections.singletonList("result-beta"), outputIds(plan));
        assertEquals(definition.definitionDigest(), plan.definitionDigest());
        assertSame(step(definition, "shared").artifact(),
                plan.orderedUnits().get(0).artifact());
        assertSame(step(definition, "beta").artifact(),
                plan.orderedUnits().get(1).artifact());
        assertEquals(2, plan.orderedUnits().get(1).artifact().operators().size());
        assertThrows(UnsupportedOperationException.class,
                () -> plan.orderedUnits().clear());
        assertThrows(UnsupportedOperationException.class,
                () -> plan.materializedEdges().clear());
    }

    @Test
    void callerOrderDoesNotChangeCanonicalTopoEdgesOrOutputs() {
        PipelineDefinition definition = branchingDefinition();

        PipelineExecutionPlan reverse = PipelineCompiler.compile(
                definition, Arrays.asList("result-beta", "result-alpha"));
        PipelineExecutionPlan forward = PipelineCompiler.compile(
                definition, Arrays.asList("result-alpha", "result-beta"));

        assertEquals(Arrays.asList("shared", "alpha", "beta"), unitIds(reverse));
        assertEquals(unitIds(forward), unitIds(reverse));
        assertEquals(Arrays.asList("shared-alpha", "shared-beta"), edgeIds(reverse));
        assertEquals(edgeIds(forward), edgeIds(reverse));
        assertEquals(Arrays.asList("result-alpha", "result-beta"), outputIds(reverse));
        assertEquals(outputIds(forward), outputIds(reverse));
        assertEquals(PlanArtifact.RuntimeTarget.BATCH,
                reverse.orderedUnits().get(1).runtimeTarget());
        assertEquals(PlanArtifact.RuntimeTarget.STREAM,
                reverse.orderedUnits().get(2).runtimeTarget());
        assertEquals(PipelineEdge.BoundaryKind.VERSIONED_DATASET,
                reverse.materializedEdges().get(1).boundaryKind());
    }

    @Test
    void requestedOutputsAreRequiredKnownAndUnique() {
        PipelineDefinition definition = branchingDefinition();
        assertFailure("MISSING_REQUESTED_OUTPUT", () ->
                PipelineCompiler.compile(definition, Collections.<String>emptyList()));
        assertFailure("MISSING_REQUESTED_OUTPUT", () ->
                PipelineCompiler.compile(definition, null));
        assertFailure("UNKNOWN_REQUESTED_OUTPUT", () ->
                PipelineCompiler.compile(definition, Collections.singletonList("missing")));
        assertFailure("DUPLICATE_REQUESTED_OUTPUT", () ->
                PipelineCompiler.compile(definition,
                        Arrays.asList("result-alpha", "result-alpha")));
    }

    private static PipelineDefinition branchingDefinition() {
        PipelineStep beta = step("beta", PlanArtifact.RuntimeTarget.STREAM);
        PipelineStep island = step("island", PlanArtifact.RuntimeTarget.BATCH);
        PipelineStep shared = step("shared", PlanArtifact.RuntimeTarget.BATCH);
        PipelineStep alpha = step("alpha", PlanArtifact.RuntimeTarget.BATCH);
        return new PipelineDefinition(
                PipelineDefinition.CURRENT_FORMAT_VERSION,
                PipelineDefinition.CURRENT_READER_VERSION,
                "branching", "r1",
                Collections.singletonList(PipelineDefinition.FEATURE_COMPOSITION_V1),
                Collections.<String>emptyList(),
                Arrays.asList(beta, island, shared, alpha),
                Arrays.asList(edge("shared-beta", shared, beta),
                        edge("shared-alpha", shared, alpha)),
                Arrays.asList(
                        new PipelineOutput("result-island", "island", "island-sink"),
                        new PipelineOutput("result-beta", "beta", "beta-sink"),
                        new PipelineOutput("result-alpha", "alpha", "alpha-sink")));
    }

    private static PipelineStep step(String id, PlanArtifact.RuntimeTarget runtime) {
        return new PipelineStep(id, new PlanArtifact(id,
                PlanArtifact.CURRENT_PLAN_SCHEMA_VERSION,
                PlanArtifact.CURRENT_READER_VERSION,
                runtime,
                runtime == PlanArtifact.RuntimeTarget.BATCH
                        ? PlanArtifact.Boundedness.BOUNDED
                        : PlanArtifact.Boundedness.UNBOUNDED,
                PlanArtifact.ChangelogMode.APPEND,
                runtime == PlanArtifact.RuntimeTarget.BATCH
                        ? PlanArtifact.TimeMode.NONE
                        : PlanArtifact.TimeMode.PROCESSING_TIME,
                runtime == PlanArtifact.RuntimeTarget.BATCH
                        ? Arrays.asList(PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS,
                                PlanArtifact.FEATURE_PORTABLE_BATCH_PLAN_V1)
                        : Arrays.asList(PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS,
                                PlanArtifact.FEATURE_PORTABLE_STREAM_JOB_V1,
                                PlanArtifact.FEATURE_RUNTIME_STREAM_V1),
                Collections.<String>emptyList(),
                Arrays.asList(
                        new OperatorDescriptor(id + "-source",
                                OperatorDescriptor.Kind.SOURCE, "test.source", SCHEMA,
                                Collections.<String>emptyList()),
                        new OperatorDescriptor(id + "-sink",
                                OperatorDescriptor.Kind.SINK, "test.sink", SCHEMA,
                                Collections.singletonList(id + "-source"))),
                runtime == PlanArtifact.RuntimeTarget.BATCH
                        ? PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1
                        : PlanArtifact.PayloadFormat.PORTABLE_STREAM_JOB_V1,
                new byte[] {1}));
    }

    private static PipelineEdge edge(
            String id, PipelineStep producer, PipelineStep consumer) {
        return new PipelineEdge(id, producer.stepId(),
                producer.stepId() + "-sink", consumer.stepId(),
                consumer.stepId() + "-source", id + "-dataset", SCHEMA,
                PipelineEdge.BoundaryKind.VERSIONED_DATASET);
    }

    private static PipelineStep step(PipelineDefinition definition, String id) {
        for (PipelineStep step : definition.steps()) {
            if (id.equals(step.stepId())) return step;
        }
        throw new AssertionError("missing step " + id);
    }

    private static java.util.List<String> unitIds(PipelineExecutionPlan plan) {
        java.util.List<String> ids = new java.util.ArrayList<>();
        for (PipelineExecutionUnit unit : plan.orderedUnits()) ids.add(unit.stepId());
        return ids;
    }

    private static java.util.List<String> edgeIds(PipelineExecutionPlan plan) {
        java.util.List<String> ids = new java.util.ArrayList<>();
        for (PipelineEdge edge : plan.materializedEdges()) ids.add(edge.edgeId());
        return ids;
    }

    private static java.util.List<String> outputIds(PipelineExecutionPlan plan) {
        java.util.List<String> ids = new java.util.ArrayList<>();
        for (PipelineOutput output : plan.selectedOutputs()) ids.add(output.outputId());
        return ids;
    }

    private static void assertFailure(String code, Runnable action) {
        PipelineValidationException failure = assertThrows(
                PipelineValidationException.class, action::run);
        assertEquals(code, failure.code());
    }

    private static String repeat(char value, int count) {
        StringBuilder text = new StringBuilder(count);
        for (int i = 0; i < count; i++) text.append(value);
        return text.toString();
    }
}
