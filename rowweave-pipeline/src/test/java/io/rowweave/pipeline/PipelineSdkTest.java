package io.rowweave.pipeline;

import io.rowweave.artifact.OperatorDescriptor;
import io.rowweave.artifact.PlanArtifact;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class PipelineSdkTest {
    private static final String SCHEMA = repeat('f', 64);

    @Test
    void builderCoversBatchOnlyAndStreamOnlyDefinitions() {
        PipelineDefinition batch = PipelineDefinition.builder("batch-only", "r1")
                .addBatchStep("batch", artifact("batch", PlanArtifact.RuntimeTarget.BATCH))
                .addOutput("published", "batch", "batch-sink")
                .build();
        PipelineDefinition stream = PipelineDefinition.builder("stream-only", "r1")
                .addStreamStep("stream", artifact("stream", PlanArtifact.RuntimeTarget.STREAM))
                .addOutput("published", "stream", "stream-sink")
                .build();

        assertEquals(PlanArtifact.RuntimeTarget.BATCH,
                PipelineValidator.validateAndCompile(batch,
                        Collections.singletonList("published"))
                        .orderedUnits().get(0).runtimeTarget());
        assertEquals(PlanArtifact.RuntimeTarget.STREAM,
                PipelineValidator.validateAndCompile(stream,
                        Collections.singletonList("published"))
                        .orderedUnits().get(0).runtimeTarget());
    }

    @Test
    void builderCoversMaterializedMixedRuntimeSharedUpstreamAndMultiOutput() {
        PlanArtifact shared = artifact("shared", PlanArtifact.RuntimeTarget.BATCH);
        PlanArtifact batch = artifact("a-batch", PlanArtifact.RuntimeTarget.BATCH);
        PlanArtifact stream = artifact("b-stream", PlanArtifact.RuntimeTarget.STREAM);
        PipelineDefinition definition = PipelineDefinition.builder("mixed", "r1")
                .addBatchStep("shared", shared)
                .addBatchStep("a-batch", batch)
                .addStreamStep("b-stream", stream)
                .addVersionedDatasetEdge("shared-batch", "shared", "shared-sink",
                        "a-batch", "a-batch-source", "shared-batch-v1", SCHEMA)
                .addVersionedDatasetEdge("shared-stream", "shared", "shared-sink",
                        "b-stream", "b-stream-source", "shared-stream-v1", SCHEMA)
                .addOutput("batch-output", "a-batch", "a-batch-sink")
                .addOutput("stream-output", "b-stream", "b-stream-sink")
                .build();

        PipelineExecutionPlan plan = PipelineValidator.validateAndCompile(
                definition, Arrays.asList("stream-output", "batch-output"));

        assertEquals(Arrays.asList("shared", "a-batch", "b-stream"),
                Arrays.asList(plan.orderedUnits().get(0).stepId(),
                        plan.orderedUnits().get(1).stepId(),
                        plan.orderedUnits().get(2).stepId()));
        assertEquals(2, plan.materializedEdges().size());
        assertEquals(2, plan.selectedOutputs().size());
    }

    @Test
    void revisionIdentityIsVisibleInCanonicalDigest() {
        PlanArtifact artifact = artifact("batch", PlanArtifact.RuntimeTarget.BATCH);
        PipelineDefinition first = PipelineDefinition.builder("same-pipeline", "r1")
                .addBatchStep("batch", artifact)
                .addOutput("out", "batch", "batch-sink")
                .build();
        PipelineDefinition second = PipelineDefinition.builder("same-pipeline", "r2")
                .addBatchStep("batch", artifact)
                .addOutput("out", "batch", "batch-sink")
                .build();

        assertEquals(first.pipelineId(), second.pipelineId());
        assertNotEquals(first.revisionId(), second.revisionId());
        assertNotEquals(first.definitionDigest(), second.definitionDigest());
    }

    private static PlanArtifact artifact(
            String id, PlanArtifact.RuntimeTarget runtime) {
        return new PlanArtifact(id,
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
                new byte[] {1});
    }

    private static String repeat(char value, int count) {
        StringBuilder text = new StringBuilder(count);
        for (int i = 0; i < count; i++) text.append(value);
        return text.toString();
    }
}
