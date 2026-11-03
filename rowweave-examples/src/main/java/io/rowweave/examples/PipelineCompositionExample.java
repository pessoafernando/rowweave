package io.rowweave.examples;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSinkDescriptorCodec;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.BatchSourceSplitDescriptorCodec;
import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PortablePlanArtifact;
import io.rowweave.pipeline.PipelineDefinition;
import io.rowweave.pipeline.PipelineExecutionPlan;
import io.rowweave.pipeline.PipelineValidator;
import io.rowweave.pipeline.InMemoryVersionedDatasetHandoffAuthority;
import io.rowweave.pipeline.VersionedDatasetCommitEvidence;
import io.rowweave.pipeline.VersionedDatasetConsumerRequest;
import io.rowweave.pipeline.VersionedDatasetHandoffKey;
import io.rowweave.pipeline.VersionedDatasetHandoffSdk;
import io.rowweave.pipeline.VersionedDatasetProducerAttempt;
import io.rowweave.pipeline.VersionedDatasetVisibleWinner;
import io.rowweave.portable.PortableBatchPlan;
import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortableOperatorCode;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.portable.PortablePlanNode;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.portable.PortableSinkBinding;
import io.rowweave.portable.PortableSourceBinding;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamRestartPolicy;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamTimeMode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * External-platform-facing composition examples. A Pipeline step takes the portable carrier the
 * public compilers hand out: {@code PORTABLE_BATCH_PLAN_V1} for a Batch step,
 * {@code PORTABLE_STREAM_JOB_V1} for a Stream step. Pipeline combines that metadata and never
 * executes it. The Batch binding below carries canonical split and sink descriptor frames, the
 * form the Batch compiler expects of a source and sink binding, rather than placeholder bytes.
 */
public final class PipelineCompositionExample {
    private static final String SCHEMA = portableSchema().fingerprint();

    private PipelineCompositionExample() { }

    /** Runs Batch-only, Stream-only, mixed-runtime, shared-upstream and revision examples. */
    public static String run() {
        PlanArtifact batch = artifact("batch", PlanArtifact.RuntimeTarget.BATCH);
        PlanArtifact stream = artifact("stream", PlanArtifact.RuntimeTarget.STREAM);
        PipelineExecutionPlan batchOnly = PipelineValidator.validateAndCompile(
                single("batch-only", "r1", "batch", batch),
                Collections.singletonList("published"));
        PipelineExecutionPlan streamOnly = PipelineValidator.validateAndCompile(
                single("stream-only", "r1", "stream", stream),
                Collections.singletonList("published"));

        PipelineDefinition mixedDefinition = PipelineDefinition.builder("mixed", "r1")
                .addBatchStep("batch", batch)
                .addStreamStep("stream", stream)
                .addVersionedDatasetEdge("batch-stream", "batch", "batch-sink",
                        "stream", "stream-source", "dataset-v1", SCHEMA)
                .addOutput("published", "stream", "stream-sink")
                .build();
        PipelineExecutionPlan mixed = PipelineValidator.validateAndCompile(
                mixedDefinition, Collections.singletonList("published"));
        VersionedDatasetHandoffKey handoffKey =
                VersionedDatasetHandoffSdk.handoffKey(
                        mixedDefinition, mixed, "batch-stream", "logical-run-1");
        VersionedDatasetProducerAttempt producerAttempt =
                VersionedDatasetHandoffSdk.producerAttempt(
                        handoffKey, "batch-run", "batch-attempt", 1L, 1L);
        VersionedDatasetCommitEvidence evidence =
                new VersionedDatasetCommitEvidence(
                        producerAttempt, "example-store", 1, "version-1",
                        "example-reader", 1, new byte[] {1}, repeat('c', 64),
                        1L, 16L, 1L);
        InMemoryVersionedDatasetHandoffAuthority handoffAuthority =
                new InMemoryVersionedDatasetHandoffAuthority();
        handoffAuthority.claim(producerAttempt);
        VersionedDatasetVisibleWinner winner = handoffAuthority.publish(evidence);
        VersionedDatasetConsumerRequest consumerRequest =
                VersionedDatasetHandoffSdk.consumerRequest(
                        mixedDefinition, mixed, handoffKey, winner,
                        "stream-run", "stream-attempt");
        boolean handoffBound = handoffAuthority.bind(consumerRequest)
                .winner().equals(winner);

        PlanArtifact shared = artifact("shared", PlanArtifact.RuntimeTarget.BATCH);
        PlanArtifact branch = artifact("branch", PlanArtifact.RuntimeTarget.BATCH);
        PipelineDefinition sharedDefinition = PipelineDefinition.builder("shared", "r1")
                .addBatchStep("shared", shared)
                .addBatchStep("a-batch", branch)
                .addStreamStep("b-stream", stream)
                .addVersionedDatasetEdge("to-batch", "shared", "shared-sink",
                        "a-batch", "branch-source", "batch-v1", SCHEMA)
                .addVersionedDatasetEdge("to-stream", "shared", "shared-sink",
                        "b-stream", "stream-source", "stream-v1", SCHEMA)
                .addOutput("batch-output", "a-batch", "branch-sink")
                .addOutput("stream-output", "b-stream", "stream-sink")
                .build();
        PipelineExecutionPlan sharedPlan = PipelineValidator.validateAndCompile(
                sharedDefinition, Arrays.asList("stream-output", "batch-output"));

        PipelineDefinition nextRevision = PipelineDefinition.builder("mixed", "r2")
                .addBatchStep("batch", batch)
                .addStreamStep("stream", stream)
                .addVersionedDatasetEdge("batch-stream", "batch", "batch-sink",
                        "stream", "stream-source", "dataset-v1", SCHEMA)
                .addOutput("published", "stream", "stream-sink")
                .build();
        boolean revisionChanged = !mixedDefinition.definitionDigest().equals(
                nextRevision.definitionDigest());
        return "batch=" + batchOnly.orderedUnits().size()
                + ",stream=" + streamOnly.orderedUnits().size()
                + ",mixed=" + unitIds(mixed)
                + ",shared=" + unitIds(sharedPlan)
                + ",outputs=" + sharedPlan.selectedOutputs().size()
                + ",revisionChanged=" + revisionChanged
                + ",handoffBound=" + handoffBound;
    }

    private static PipelineDefinition single(
            String pipelineId, String revisionId,
            String stepId, PlanArtifact artifact) {
        if (artifact.runtimeTarget() == PlanArtifact.RuntimeTarget.BATCH) {
            return PipelineDefinition.builder(pipelineId, revisionId)
                    .addBatchStep(stepId, artifact)
                    .addOutput("published", stepId, stepId + "-sink")
                    .build();
        }
        return PipelineDefinition.builder(pipelineId, revisionId)
                .addStreamStep(stepId, artifact)
                .addOutput("published", stepId, stepId + "-sink")
                .build();
    }

    private static List<String> unitIds(PipelineExecutionPlan plan) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < plan.orderedUnits().size(); i++) {
            result.add(plan.orderedUnits().get(i).stepId());
        }
        return result;
    }

    private static PlanArtifact artifact(
            String id, PlanArtifact.RuntimeTarget runtime) {
        return runtime == PlanArtifact.RuntimeTarget.BATCH
                ? batchArtifact(id)
                : streamArtifact(id);
    }

    /** Package-private so the release gate test can run the same preflight
     *  the Batch compiler applies to published carriers. */
    static PlanArtifact batchArtifact(String id) {
        return PortablePlanArtifact.wrap(id, batchPlan(id));
    }

    static PortableBatchPlan batchPlan(String id) {
        PortableRowSchema schema = portableSchema();
        PortablePlanNode source = new PortablePlanNode(
                id + "-source", PortableOperatorCode.SOURCE,
                Collections.<String>emptyList(), Collections.<String>emptyList(),
                schema, Collections.<String, String>emptyMap(),
                Collections.<String, List<String>>emptyMap(),
                new PortableSourceBinding("example-source-v1", repeat('a', 64),
                        new byte[] {1}, Collections.singletonList(splitFrame(id, schema))));
        PortablePlanNode sink = new PortablePlanNode(
                id + "-sink", PortableOperatorCode.SINK,
                Collections.singletonList(id + "-source"),
                Collections.singletonList(schema.fingerprint()), schema,
                Collections.<String, String>emptyMap(),
                Collections.<String, List<String>>emptyMap(),
                new PortableSinkBinding("example-sink-v1", repeat('b', 64),
                        sinkFrame(id, schema), new byte[] {4}));
        PortableBatchPlan plan = new PortableBatchPlan(
                1, 1, Collections.singletonList(id + "-sink"),
                Arrays.asList(source, sink),
                Collections.singletonList("rowweave.runtime.batch.v1"));
        return plan;
    }

    /**
     * A split descriptor whose schema fingerprint is the runtime one, which is what
     * {@link PortablePlanCompiler#preflight} requires of a Batch source binding.
     */
    private static byte[] splitFrame(String id, PortableRowSchema schema) {
        return BatchSourceSplitDescriptorCodec.encode(new BatchSourceSplitDescriptor(
                1, "example-source-v1", 1, id + "-split-1",
                SchemaFingerprint.of(PortablePlanCompiler.toDataSchema(schema)), "snapshot-1",
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START));
    }

    private static byte[] sinkFrame(String id, PortableRowSchema schema) {
        return BatchSinkDescriptorCodec.encode(new BatchSinkDescriptor(1, "example-sink-v1", 1,
                SchemaFingerprint.of(PortablePlanCompiler.toDataSchema(schema)),
                id + "-destination", id + "-domain",
                BatchSinkDescriptor.CapabilityProfile.SIDE_EFFECT_FREE_V1));
    }

    private static PlanArtifact streamArtifact(String id) {
        StreamSchema schema = new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id", DataType.LongType, false)));
        StreamSession session = RowWeave.createStreamSession(id, id);
        session.setDefinitionId(id);
        session.setParallelism(1);
        session.setMaxParallelism(128);
        session.setDeliveryGuarantee(StreamDeliveryGuarantee.AT_LEAST_ONCE);
        session.setChangelogMode(StreamChangelogMode.APPEND);
        session.setTimeMode(StreamTimeMode.PROCESSING_TIME);
        session.setCheckpointConfig(StreamCheckpointConfig.manualOnly(30_000L, 2));
        session.setRestartPolicy(StreamRestartPolicy.fixedDelay(1, 0L));
        session.setResourceProfile(StreamResourceProfile.defaults());
        StreamSourceSpec source = StreamSourceSpec.exactOrdered(
                schema, "example-source-v1", repeat('c', 64), new byte[] {5},
                Collections.singletonList(new StreamSplitSpec(
                        repeat('d', 64), "sequence-v1", new byte[] {6})));
        StreamSinkSpec sink = StreamSinkSpec.eagerAtLeastOnce(
                "example-sink-v1", repeat('e', 64), new byte[] {7});
        session.fromSource(source).uid(id + "-source")
                .sinkTo(id + "-sink", sink);
        return session.toPlanArtifact();
    }

    private static PortableRowSchema portableSchema() {
        return new PortableRowSchema(Collections.singletonList(
                new PortableField("id", "id", "id",
                        PortableDataType.LONG, false)));
    }

    private static String repeat(char value, int count) {
        StringBuilder text = new StringBuilder(count);
        for (int i = 0; i < count; i++) text.append(value);
        return text.toString();
    }
}
