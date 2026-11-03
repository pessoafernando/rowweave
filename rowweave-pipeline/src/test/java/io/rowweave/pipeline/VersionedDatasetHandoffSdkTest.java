package io.rowweave.pipeline;

import io.rowweave.artifact.OperatorDescriptor;
import io.rowweave.artifact.PlanArtifact;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VersionedDatasetHandoffSdkTest {
    private static final String SCHEMA = repeat('f', 64);
    private static final String CONTENT = repeat('c', 64);

    @Test
    void derivesProducerAndConsumerIdentitiesFromSelectedPipelineEdge() {
        PipelineDefinition definition = definition();
        PipelineExecutionPlan plan = PipelineValidator.validateAndCompile(
                definition, Collections.singletonList("published"));
        VersionedDatasetHandoffKey key = VersionedDatasetHandoffSdk.handoffKey(
                definition, plan, "batch-stream", "logical-run");

        assertEquals(definition.definitionDigest(), key.definitionDigest());
        assertEquals("batch", key.producerStepId());
        assertEquals("batch-sink", key.producerSinkOperatorId());
        assertEquals(plan.orderedUnits().get(0).artifact().artifactId(),
                key.producerArtifactId());
        VersionedDatasetProducerAttempt attempt =
                VersionedDatasetHandoffSdk.producerAttempt(
                        key, "producer-run", "producer-attempt", 1L, 10L);
        VersionedDatasetCommitEvidence evidence = new VersionedDatasetCommitEvidence(
                attempt, "provider", 1, "version", "reader", 1,
                new byte[] {1}, CONTENT, 1L, 10L, 100L);
        InMemoryVersionedDatasetHandoffAuthority authority =
                new InMemoryVersionedDatasetHandoffAuthority();
        authority.claim(attempt);
        VersionedDatasetVisibleWinner winner = authority.publish(evidence);

        VersionedDatasetConsumerRequest request =
                VersionedDatasetHandoffSdk.consumerRequest(
                        definition, plan, key, winner,
                        "consumer-run", "consumer-attempt");
        assertEquals("stream", request.consumerStepId());
        assertEquals("stream-source", request.consumerSourceOperatorId());
        assertEquals(plan.orderedUnits().get(1).artifact().artifactId(),
                request.consumerArtifactId());
        assertEquals(winner, authority.bind(request).winner());
    }

    @Test
    void rejectsPrunedUnknownAndCrossDefinitionEdges() {
        PipelineDefinition definition = branchingDefinition();
        PipelineExecutionPlan onlyLeft = PipelineValidator.validateAndCompile(
                definition, Collections.singletonList("left-output"));
        assertFailure("EDGE_NOT_SELECTED", () ->
                VersionedDatasetHandoffSdk.handoffKey(
                        definition, onlyLeft, "to-right", "run"));
        assertFailure("UNKNOWN_EDGE", () ->
                VersionedDatasetHandoffSdk.handoffKey(
                        definition, onlyLeft, "missing", "run"));

        PipelineDefinition otherRevision = PipelineDefinition.builder("pipeline", "r2")
                .addBatchStep("batch", artifact("batch", PlanArtifact.RuntimeTarget.BATCH))
                .addStreamStep("stream", artifact("stream", PlanArtifact.RuntimeTarget.STREAM))
                .addVersionedDatasetEdge("batch-stream", "batch", "batch-sink",
                        "stream", "stream-source", "dataset", SCHEMA)
                .addOutput("published", "stream", "stream-sink")
                .build();
        assertFailure("PLAN_DEFINITION_MISMATCH", () ->
                VersionedDatasetHandoffSdk.handoffKey(
                        otherRevision, onlyLeft, "to-left", "run"));
    }

    @Test
    void consumerRequestRejectsSpoofedProducerKeyOrWinner() {
        PipelineDefinition definition = definition();
        PipelineExecutionPlan plan = PipelineValidator.validateAndCompile(
                definition, Collections.singletonList("published"));
        VersionedDatasetHandoffKey key = VersionedDatasetHandoffSdk.handoffKey(
                definition, plan, "batch-stream", "logical-run");
        VersionedDatasetProducerAttempt attempt =
                VersionedDatasetHandoffSdk.producerAttempt(
                        key, "producer-run", "attempt", 1L, 1L);
        VersionedDatasetVisibleWinner winner = new VersionedDatasetVisibleWinner(
                new VersionedDatasetCommitEvidence(attempt, "provider", 1,
                        "version", "reader", 1, new byte[] {1}, CONTENT,
                        1L, 1L, 1L), 2L);
        VersionedDatasetHandoffKey spoofed = new VersionedDatasetHandoffKey(
                key.pipelineId(), key.revisionId(), key.definitionDigest(),
                key.edgeId(), key.materializationId(), key.schemaFingerprint(),
                key.producerStepId(), key.producerSinkOperatorId(),
                "other-artifact", key.logicalRunId());
        assertFailure("HANDOFF_KEY_MISMATCH", () ->
                VersionedDatasetHandoffSdk.consumerRequest(
                        definition, plan, spoofed, winner, "run", "attempt"));
        assertFailure("WINNER_KEY_MISMATCH", () ->
                VersionedDatasetHandoffSdk.consumerRequest(
                        definition, plan, key,
                        new VersionedDatasetVisibleWinner(
                                new VersionedDatasetCommitEvidence(
                                        new VersionedDatasetProducerAttempt(
                                                spoofed, "run", "attempt", 1L, 1L),
                                        "provider", 1, "version", "reader", 1,
                                        new byte[] {1}, CONTENT, 1L, 1L, 1L), 2L),
                        "run", "attempt"));
    }

    private static PipelineDefinition definition() {
        return PipelineDefinition.builder("pipeline", "r1")
                .addBatchStep("batch", artifact("batch", PlanArtifact.RuntimeTarget.BATCH))
                .addStreamStep("stream", artifact("stream", PlanArtifact.RuntimeTarget.STREAM))
                .addVersionedDatasetEdge("batch-stream", "batch", "batch-sink",
                        "stream", "stream-source", "dataset", SCHEMA)
                .addOutput("published", "stream", "stream-sink")
                .build();
    }

    private static PipelineDefinition branchingDefinition() {
        return PipelineDefinition.builder("pipeline", "r1")
                .addBatchStep("root", artifact("root", PlanArtifact.RuntimeTarget.BATCH))
                .addBatchStep("left", artifact("left", PlanArtifact.RuntimeTarget.BATCH))
                .addStreamStep("right", artifact("right", PlanArtifact.RuntimeTarget.STREAM))
                .addVersionedDatasetEdge("to-left", "root", "root-sink",
                        "left", "left-source", "left-data", SCHEMA)
                .addVersionedDatasetEdge("to-right", "root", "root-sink",
                        "right", "right-source", "right-data", SCHEMA)
                .addOutput("left-output", "left", "left-sink")
                .addOutput("right-output", "right", "right-sink")
                .build();
    }

    private static PlanArtifact artifact(
            String id, PlanArtifact.RuntimeTarget runtime) {
        return new PlanArtifact(id,
                PlanArtifact.CURRENT_PLAN_SCHEMA_VERSION,
                PlanArtifact.CURRENT_READER_VERSION, runtime,
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
