package io.rowweave.pipeline;

import io.rowweave.artifact.OperatorDescriptor;
import io.rowweave.artifact.PlanArtifact;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineDefinitionTest {
    private static final String SCHEMA_A = repeat('a', 64);
    private static final String SCHEMA_B = repeat('b', 64);

    @Test
    void validDefinitionIsImmutableAndPreservesRuntimeArtifactIdentity() {
        PipelineStep producer = step("produce", artifact(
                "producer", PlanArtifact.RuntimeTarget.BATCH, SCHEMA_A));
        PipelineStep consumer = step("consume", artifact(
                "consumer", PlanArtifact.RuntimeTarget.STREAM, SCHEMA_A));
        PipelineEdge edge = edge("edge", "produce", "producer-sink",
                "consume", "consumer-source", SCHEMA_A);
        PipelineOutput output = new PipelineOutput(
                "published", "consume", "consumer-sink");

        List<PipelineStep> steps = new ArrayList<>(Arrays.asList(producer, consumer));
        PipelineDefinition definition = definition(steps,
                Collections.singletonList(edge), Collections.singletonList(output));
        steps.clear();

        assertEquals(Arrays.asList(producer, consumer), definition.steps());
        assertEquals(producer.artifact().artifactId(),
                definition.steps().get(0).artifact().artifactId());
        assertThrows(UnsupportedOperationException.class,
                () -> definition.steps().add(producer));
        assertThrows(UnsupportedOperationException.class,
                () -> definition.requiredFeatures().add("x"));
    }

    @Test
    void duplicateIdentityAndUnknownEndpointsFailClosed() {
        PipelineStep producer = step("produce", artifact(
                "producer", PlanArtifact.RuntimeTarget.BATCH, SCHEMA_A));
        PipelineStep duplicate = step("produce", artifact(
                "duplicate", PlanArtifact.RuntimeTarget.BATCH, SCHEMA_A));
        assertFailure("DUPLICATE_STEP_ID", () -> definition(
                Arrays.asList(producer, duplicate), Collections.<PipelineEdge>emptyList(),
                Collections.singletonList(new PipelineOutput(
                        "out", "produce", "producer-sink"))));

        assertFailure("UNKNOWN_STEP", () -> definition(
                Collections.singletonList(producer),
                Collections.singletonList(edge("edge", "produce", "producer-sink",
                        "missing", "missing-source", SCHEMA_A)),
                Collections.singletonList(new PipelineOutput(
                        "out", "produce", "producer-sink"))));

        assertFailure("UNKNOWN_OPERATOR", () -> definition(
                Collections.singletonList(producer), Collections.<PipelineEdge>emptyList(),
                Collections.singletonList(new PipelineOutput(
                        "out", "produce", "missing-sink"))));
    }

    @Test
    void materializedEdgesRequireSinkToSourceAndExactSchema() {
        PipelineStep producer = step("produce", artifact(
                "producer", PlanArtifact.RuntimeTarget.BATCH, SCHEMA_A));
        PipelineStep consumer = step("consume", artifact(
                "consumer", PlanArtifact.RuntimeTarget.STREAM, SCHEMA_A));
        PipelineOutput output = new PipelineOutput(
                "published", "consume", "consumer-sink");

        assertFailure("INVALID_EDGE_ENDPOINT_KIND", () -> definition(
                Arrays.asList(producer, consumer),
                Collections.singletonList(edge("edge", "produce", "producer-source",
                        "consume", "consumer-source", SCHEMA_A)),
                Collections.singletonList(output)));
        assertFailure("INVALID_EDGE_ENDPOINT_KIND", () -> definition(
                Arrays.asList(producer, consumer),
                Collections.singletonList(edge("edge", "produce", "producer-sink",
                        "consume", "consumer-sink", SCHEMA_A)),
                Collections.singletonList(output)));
        assertFailure("SCHEMA_MISMATCH", () -> definition(
                Arrays.asList(producer, consumer),
                Collections.singletonList(edge("edge", "produce", "producer-sink",
                        "consume", "consumer-source", SCHEMA_B)),
                Collections.singletonList(output)));
    }

    @Test
    void cyclesAndSelfEdgesFailClosed() {
        PipelineStep left = step("left", artifact(
                "left", PlanArtifact.RuntimeTarget.BATCH, SCHEMA_A));
        PipelineStep right = step("right", artifact(
                "right", PlanArtifact.RuntimeTarget.STREAM, SCHEMA_A));
        PipelineOutput output = new PipelineOutput("out", "right", "right-sink");

        assertFailure("SELF_EDGE", () -> definition(
                Collections.singletonList(left),
                Collections.singletonList(edge("self", "left", "left-sink",
                        "left", "left-source", SCHEMA_A)),
                Collections.singletonList(new PipelineOutput(
                        "out", "left", "left-sink"))));
        assertFailure("CYCLIC_PIPELINE", () -> definition(
                Arrays.asList(left, right),
                Arrays.asList(
                        edge("lr", "left", "left-sink",
                                "right", "right-source", SCHEMA_A),
                        edge("rl", "right", "right-sink",
                                "left", "left-source", SCHEMA_A)),
                Collections.singletonList(output)));
    }

    @Test
    void countsAndTextAreBoundedBeforePublication() {
        PipelineStep only = step("only", artifact(
                "only", PlanArtifact.RuntimeTarget.BATCH, SCHEMA_A));
        assertFailure("LIMIT_EXCEEDED", () -> new PipelineDefinition(
                PipelineDefinition.CURRENT_FORMAT_VERSION,
                PipelineDefinition.CURRENT_READER_VERSION,
                repeat('p', PipelineLimits.MAX_ID_BYTES + 1), "revision",
                Collections.singletonList(PipelineDefinition.FEATURE_COMPOSITION_V1),
                Collections.<String>emptyList(), Collections.singletonList(only),
                Collections.<PipelineEdge>emptyList(),
                Collections.singletonList(new PipelineOutput(
                        "out", "only", "only-sink"))));

        List<PipelineOutput> outputs = new ArrayList<>();
        for (int i = 0; i <= PipelineLimits.MAX_OUTPUTS; i++) {
            outputs.add(new PipelineOutput("out-" + i, "only", "only-sink"));
        }
        assertFailure("LIMIT_EXCEEDED", () -> definition(
                Collections.singletonList(only), Collections.<PipelineEdge>emptyList(), outputs));
    }

    @Test
    void edgeOutputFeatureAndMaterializationIdentityAreUnambiguous() {
        PipelineStep producer = step("produce", artifact(
                "producer", PlanArtifact.RuntimeTarget.BATCH, SCHEMA_A));
        PipelineStep left = step("left", artifact(
                "left", PlanArtifact.RuntimeTarget.BATCH, SCHEMA_A));
        PipelineStep right = step("right", artifact(
                "right", PlanArtifact.RuntimeTarget.STREAM, SCHEMA_A));

        PipelineEdge leftEdge = edge("same", "produce", "producer-sink",
                "left", "left-source", SCHEMA_A);
        assertFailure("DUPLICATE_EDGE_ID", () -> definition(
                Arrays.asList(producer, left, right),
                Arrays.asList(leftEdge, edge("same", "produce", "producer-sink",
                        "right", "right-source", SCHEMA_A)),
                Collections.singletonList(new PipelineOutput(
                        "out", "right", "right-sink"))));

        assertFailure("DUPLICATE_OUTPUT_ID", () -> definition(
                Collections.singletonList(producer), Collections.<PipelineEdge>emptyList(),
                Arrays.asList(new PipelineOutput("out", "produce", "producer-sink"),
                        new PipelineOutput("out", "produce", "producer-sink"))));

        PipelineEdge firstBinding = edge("first", "produce", "producer-sink",
                "left", "left-source", SCHEMA_A);
        assertFailure("DUPLICATE_SOURCE_BINDING", () -> definition(
                Arrays.asList(producer, left, right),
                Arrays.asList(firstBinding,
                        edge("second", "right", "right-sink",
                                "left", "left-source", SCHEMA_A)),
                Collections.singletonList(new PipelineOutput(
                        "out", "left", "left-sink"))));

        PipelineEdge conflicting = new PipelineEdge(
                "right-edge", "right", "right-sink", "produce", "producer-source",
                firstBinding.materializationId(), SCHEMA_A,
                PipelineEdge.BoundaryKind.VERSIONED_DATASET);
        assertFailure("MATERIALIZATION_CONFLICT", () -> definition(
                Arrays.asList(producer, left, right),
                Arrays.asList(firstBinding, conflicting),
                Collections.singletonList(new PipelineOutput(
                        "out", "left", "left-sink"))));

        assertFailure("DUPLICATE_FEATURE", () -> new PipelineDefinition(
                PipelineDefinition.CURRENT_FORMAT_VERSION,
                PipelineDefinition.CURRENT_READER_VERSION,
                "pipeline", "revision",
                Arrays.asList(PipelineDefinition.FEATURE_COMPOSITION_V1,
                        PipelineDefinition.FEATURE_COMPOSITION_V1),
                Collections.<String>emptyList(), Collections.singletonList(producer),
                Collections.<PipelineEdge>emptyList(),
                Collections.singletonList(new PipelineOutput(
                        "out", "produce", "producer-sink"))));
    }

    private static PipelineDefinition definition(
            List<PipelineStep> steps, List<PipelineEdge> edges,
            List<PipelineOutput> outputs) {
        return new PipelineDefinition(
                PipelineDefinition.CURRENT_FORMAT_VERSION,
                PipelineDefinition.CURRENT_READER_VERSION,
                "pipeline", "revision",
                Collections.singletonList(PipelineDefinition.FEATURE_COMPOSITION_V1),
                Collections.<String>emptyList(), steps, edges, outputs);
    }

    private static PipelineStep step(String id, PlanArtifact artifact) {
        return new PipelineStep(id, artifact);
    }

    private static PipelineEdge edge(
            String id, String producerStep, String producerOperator,
            String consumerStep, String consumerOperator, String schema) {
        return new PipelineEdge(id, producerStep, producerOperator,
                consumerStep, consumerOperator, "dataset-" + id, schema,
                PipelineEdge.BoundaryKind.VERSIONED_DATASET);
    }

    private static PlanArtifact artifact(
            String definitionId, PlanArtifact.RuntimeTarget target,
            String schema) {
        List<OperatorDescriptor> operators = Arrays.asList(
                new OperatorDescriptor(definitionId + "-source",
                        OperatorDescriptor.Kind.SOURCE, "test.source", schema,
                        Collections.<String>emptyList()),
                new OperatorDescriptor(definitionId + "-sink",
                        OperatorDescriptor.Kind.SINK, "test.sink", schema,
                        Collections.singletonList(definitionId + "-source")));
        return new PlanArtifact(definitionId,
                PlanArtifact.CURRENT_PLAN_SCHEMA_VERSION,
                PlanArtifact.CURRENT_READER_VERSION, target,
                target == PlanArtifact.RuntimeTarget.BATCH
                        ? PlanArtifact.Boundedness.BOUNDED
                        : PlanArtifact.Boundedness.UNBOUNDED,
                PlanArtifact.ChangelogMode.APPEND,
                target == PlanArtifact.RuntimeTarget.BATCH
                        ? PlanArtifact.TimeMode.NONE
                        : PlanArtifact.TimeMode.PROCESSING_TIME,
                Collections.<String>emptyList(), Collections.<String>emptyList(),
                operators,
                target == PlanArtifact.RuntimeTarget.BATCH
                        ? PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1
                        : PlanArtifact.PayloadFormat.PORTABLE_STREAM_JOB_V1,
                new byte[] {1});
    }

    private static void assertFailure(String code, Runnable action) {
        PipelineValidationException failure = assertThrows(
                PipelineValidationException.class, action::run);
        assertEquals(code, failure.code());
        assertTrue(failure.getMessage().contains(code));
    }

    private static String repeat(char value, int count) {
        StringBuilder text = new StringBuilder(count);
        for (int i = 0; i < count; i++) text.append(value);
        return text.toString();
    }
}
