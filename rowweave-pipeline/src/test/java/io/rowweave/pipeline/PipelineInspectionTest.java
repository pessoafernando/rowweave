package io.rowweave.pipeline;

import io.rowweave.artifact.OperatorDescriptor;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactValidator;
import io.rowweave.portable.PortablePlanException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineInspectionTest {
    private static final String SCHEMA = repeat('e', 64);
    private static final String PAYLOAD_MARKER = "SECRET-PAYLOAD-MUST-NOT-BE-READ";

    @Test
    void publicAdaptersValidateMetadataWithoutDecodingPayload() {
        PlanArtifact poisonBatch = poisonArtifact(
                "batch", PlanArtifact.RuntimeTarget.BATCH);
        assertThrows(PortablePlanException.class,
                () -> PlanArtifactValidator.validateReadable(poisonBatch));

        PipelineStep adapted = PipelineArtifactAdapter.forBatch(
                "batch-step", poisonBatch);

        assertSame(poisonBatch, adapted.artifact());
        assertEquals("batch-step", adapted.stepId());
        assertFailure("RUNTIME_TARGET_MISMATCH", () ->
                PipelineArtifactAdapter.forStream("wrong", poisonBatch));
        assertFailure("INVALID_ARTIFACT_METADATA", () ->
                PipelineArtifactAdapter.forBatch("legacy", legacyBatchArtifact()));
    }

    @Test
    void validateAndCompileSupportsBatchAndStreamWithoutPayloadLoad() {
        PipelineStep batch = PipelineArtifactAdapter.forBatch(
                "batch", poisonArtifact("batch", PlanArtifact.RuntimeTarget.BATCH));
        PipelineStep stream = PipelineArtifactAdapter.forStream(
                "stream", poisonArtifact("stream", PlanArtifact.RuntimeTarget.STREAM));
        PipelineDefinition definition = new PipelineDefinition(
                PipelineDefinition.CURRENT_FORMAT_VERSION,
                PipelineDefinition.CURRENT_READER_VERSION,
                "inspection", "r1",
                Collections.singletonList(PipelineDefinition.FEATURE_COMPOSITION_V1),
                Collections.<String>emptyList(), Arrays.asList(batch, stream),
                Collections.singletonList(new PipelineEdge(
                        "materialized", "batch", "batch-sink",
                        "stream", "stream-source", "dataset-v1", SCHEMA,
                        PipelineEdge.BoundaryKind.VERSIONED_DATASET)),
                Collections.singletonList(new PipelineOutput(
                        "published", "stream", "stream-sink")));

        PipelineExecutionPlan plan = PipelineValidator.validateAndCompile(
                definition, Collections.singletonList("published"));

        assertEquals(Arrays.asList("batch", "stream"), Arrays.asList(
                plan.orderedUnits().get(0).stepId(),
                plan.orderedUnits().get(1).stepId()));
    }

    @Test
    void explainIsCanonicalMetadataOnlyAndContainsNoPayload() {
        PipelineStep batch = PipelineArtifactAdapter.forBatch(
                "batch", poisonArtifact("batch", PlanArtifact.RuntimeTarget.BATCH));
        PipelineDefinition definition = new PipelineDefinition(
                PipelineDefinition.CURRENT_FORMAT_VERSION,
                PipelineDefinition.CURRENT_READER_VERSION,
                "explain", "r7",
                Collections.singletonList(PipelineDefinition.FEATURE_COMPOSITION_V1),
                Collections.<String>emptyList(), Collections.singletonList(batch),
                Collections.<PipelineEdge>emptyList(),
                Collections.singletonList(new PipelineOutput(
                        "published", "batch", "batch-sink")));
        PipelineExecutionPlan plan = PipelineValidator.validateAndCompile(
                definition, Collections.singletonList("published"));

        String first = PipelineExplainer.explain(plan);
        String second = PipelineExplainer.explain(plan);

        assertEquals(first, second);
        assertTrue(first.startsWith("pipeline-v1\n"));
        assertTrue(first.contains("unit\tbatch\tBATCH\t"));
        assertTrue(first.contains("output\tpublished\tbatch\tbatch-sink"));
        assertFalse(first.contains(PAYLOAD_MARKER));
        assertFalse(first.contains("test.connector.config"));
    }

    private static PlanArtifact poisonArtifact(
            String id, PlanArtifact.RuntimeTarget runtime) {
        boolean batch = runtime == PlanArtifact.RuntimeTarget.BATCH;
        return new PlanArtifact(id,
                PlanArtifact.CURRENT_PLAN_SCHEMA_VERSION,
                PlanArtifact.CURRENT_READER_VERSION,
                runtime,
                batch ? PlanArtifact.Boundedness.BOUNDED
                        : PlanArtifact.Boundedness.UNBOUNDED,
                PlanArtifact.ChangelogMode.APPEND,
                batch ? PlanArtifact.TimeMode.NONE
                        : PlanArtifact.TimeMode.PROCESSING_TIME,
                batch
                        ? Arrays.asList(PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS,
                                PlanArtifact.FEATURE_PORTABLE_BATCH_PLAN_V1)
                        : Arrays.asList(PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS,
                                PlanArtifact.FEATURE_PORTABLE_STREAM_JOB_V1,
                                PlanArtifact.FEATURE_RUNTIME_STREAM_V1),
                Collections.<String>emptyList(),
                Arrays.asList(
                        new OperatorDescriptor(id + "-source",
                                OperatorDescriptor.Kind.SOURCE, "test.connector.config",
                                SCHEMA, Collections.<String>emptyList()),
                        new OperatorDescriptor(id + "-sink",
                                OperatorDescriptor.Kind.SINK, "test.sink", SCHEMA,
                                Collections.singletonList(id + "-source"))),
                batch ? PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1
                        : PlanArtifact.PayloadFormat.PORTABLE_STREAM_JOB_V1,
                PAYLOAD_MARKER.getBytes(StandardCharsets.UTF_8));
    }

    private static PlanArtifact legacyBatchArtifact() {
        return new PlanArtifact("legacy",
                PlanArtifact.CURRENT_PLAN_SCHEMA_VERSION,
                PlanArtifact.CURRENT_READER_VERSION,
                PlanArtifact.RuntimeTarget.BATCH,
                PlanArtifact.Boundedness.BOUNDED,
                PlanArtifact.ChangelogMode.APPEND,
                PlanArtifact.TimeMode.NONE,
                Arrays.asList(PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS,
                        PlanArtifact.FEATURE_TRUSTED_LEGACY_PLAN_GRAPH),
                Collections.<String>emptyList(),
                Arrays.asList(
                        new OperatorDescriptor("legacy-source",
                                OperatorDescriptor.Kind.SOURCE, "test.source", SCHEMA,
                                Collections.<String>emptyList()),
                        new OperatorDescriptor("legacy-sink",
                                OperatorDescriptor.Kind.SINK, "test.sink", SCHEMA,
                                Collections.singletonList("legacy-source"))),
                PlanArtifact.PayloadFormat.TRUSTED_LEGACY_PLAN_GRAPH,
                new byte[] {1});
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
