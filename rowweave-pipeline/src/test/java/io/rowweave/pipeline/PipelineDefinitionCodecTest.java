package io.rowweave.pipeline;

import io.rowweave.artifact.OperatorDescriptor;
import io.rowweave.artifact.PlanArtifact;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineDefinitionCodecTest {
    private static final String SCHEMA = repeat('c', 64);

    @Test
    void canonicalEncodingAndDigestIgnoreCallerCollectionOrder() {
        PipelineStep producer = step("a-producer", PlanArtifact.RuntimeTarget.BATCH);
        PipelineStep consumer = step("b-consumer", PlanArtifact.RuntimeTarget.STREAM);
        PipelineEdge edge = edge(producer, consumer);
        PipelineOutput output = new PipelineOutput(
                "result", consumer.stepId(), "b-consumer-sink");

        PipelineDefinition forward = definition(
                Arrays.asList(producer, consumer),
                Collections.singletonList(edge), Collections.singletonList(output),
                Arrays.asList(PipelineDefinition.FEATURE_COMPOSITION_V1),
                Arrays.asList("z.optional", "a.optional"));
        PipelineDefinition reversed = definition(
                Arrays.asList(consumer, producer),
                Collections.singletonList(edge), Collections.singletonList(output),
                Arrays.asList(PipelineDefinition.FEATURE_COMPOSITION_V1),
                Arrays.asList("a.optional", "z.optional"));

        assertArrayEquals(PipelineDefinitionCodec.encode(forward),
                PipelineDefinitionCodec.encode(reversed));
        assertEquals(forward.definitionDigest(), reversed.definitionDigest());
        assertEquals(64, forward.definitionDigest().length());

        PipelineDefinition decoded = PipelineDefinitionCodec.decode(
                PipelineDefinitionCodec.encode(forward));
        assertEquals(forward.pipelineId(), decoded.pipelineId());
        assertEquals(forward.revisionId(), decoded.revisionId());
        assertEquals(forward.definitionDigest(), decoded.definitionDigest());
        assertEquals(Arrays.asList("a-producer", "b-consumer"),
                Arrays.asList(decoded.steps().get(0).stepId(),
                        decoded.steps().get(1).stepId()));
        assertEquals(forward.steps().get(0).artifact().artifactId(),
                decoded.steps().get(0).artifact().artifactId());
        assertEquals("7627240b8c40245679b43ad5afbe0e2cae9f1776ae5c68eb7ae4f2c7330874b1",
                forward.definitionDigest());
    }

    @Test
    void definitionVersionAndMinimumReaderFailClosed() {
        PipelineStep only = step("only", PlanArtifact.RuntimeTarget.BATCH);
        assertFailure("UNSUPPORTED_VERSION", () -> new PipelineDefinition(
                PipelineDefinition.CURRENT_FORMAT_VERSION + 1,
                PipelineDefinition.CURRENT_READER_VERSION,
                "pipeline", "revision",
                Collections.singletonList(PipelineDefinition.FEATURE_COMPOSITION_V1),
                Collections.<String>emptyList(),
                Collections.singletonList(only),
                Collections.<PipelineEdge>emptyList(),
                Collections.singletonList(new PipelineOutput(
                        "out", "only", "only-sink"))));
        assertFailure("UNSUPPORTED_VERSION", () -> new PipelineDefinition(
                PipelineDefinition.CURRENT_FORMAT_VERSION,
                PipelineDefinition.CURRENT_READER_VERSION + 1,
                "pipeline", "revision",
                Collections.singletonList(PipelineDefinition.FEATURE_COMPOSITION_V1),
                Collections.<String>emptyList(),
                Collections.singletonList(only),
                Collections.<PipelineEdge>emptyList(),
                Collections.singletonList(new PipelineOutput(
                        "out", "only", "only-sink"))));
    }

    @Test
    void envelopeRejectsMagicVersionLengthChecksumAndTrailingBytes() {
        byte[] encoded = PipelineDefinitionCodec.encode(definition(
                Collections.singletonList(step("only", PlanArtifact.RuntimeTarget.BATCH)),
                Collections.<PipelineEdge>emptyList(),
                Collections.singletonList(new PipelineOutput(
                        "out", "only", "only-sink")),
                Collections.singletonList(PipelineDefinition.FEATURE_COMPOSITION_V1),
                Collections.<String>emptyList()));

        byte[] badMagic = encoded.clone();
        badMagic[0] ^= 1;
        assertDecodeFailure("INVALID_MAGIC", badMagic);

        byte[] badVersion = encoded.clone();
        writeInt(badVersion, 8, PipelineDefinitionCodec.ENVELOPE_VERSION + 1);
        assertDecodeFailure("UNSUPPORTED_ENVELOPE_VERSION", badVersion);

        byte[] badLength = encoded.clone();
        writeInt(badLength, 12, Integer.MAX_VALUE);
        assertDecodeFailure("LIMIT_EXCEEDED", badLength);

        byte[] corrupt = encoded.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertDecodeFailure("CHECKSUM_MISMATCH", corrupt);

        assertDecodeFailure("FRAME_LENGTH_MISMATCH",
                Arrays.copyOf(encoded, encoded.length - 1));
        assertDecodeFailure("FRAME_LENGTH_MISMATCH",
                Arrays.copyOf(encoded, encoded.length + 1));
    }

    @Test
    void unknownRequiredFeatureFailsWhileUnknownOptionalRoundTrips() {
        PipelineStep only = step("only", PlanArtifact.RuntimeTarget.BATCH);
        assertFailure("UNSUPPORTED_REQUIRED_FEATURE", () -> definition(
                Collections.singletonList(only), Collections.<PipelineEdge>emptyList(),
                Collections.singletonList(new PipelineOutput(
                        "out", "only", "only-sink")),
                Arrays.asList(PipelineDefinition.FEATURE_COMPOSITION_V1, "future.required"),
                Collections.<String>emptyList()));

        PipelineDefinition optional = definition(
                Collections.singletonList(only), Collections.<PipelineEdge>emptyList(),
                Collections.singletonList(new PipelineOutput(
                        "out", "only", "only-sink")),
                Collections.singletonList(PipelineDefinition.FEATURE_COMPOSITION_V1),
                Collections.singletonList("future.optional"));
        assertEquals(Collections.singletonList("future.optional"),
                PipelineDefinitionCodec.decode(
                        PipelineDefinitionCodec.encode(optional)).optionalFeatures());
    }

    private static PipelineDefinition definition(
            java.util.List<PipelineStep> steps,
            java.util.List<PipelineEdge> edges,
            java.util.List<PipelineOutput> outputs,
            java.util.List<String> required,
            java.util.List<String> optional) {
        return new PipelineDefinition(
                PipelineDefinition.CURRENT_FORMAT_VERSION,
                PipelineDefinition.CURRENT_READER_VERSION,
                "pipeline", "revision", required, optional, steps, edges, outputs);
    }

    private static PipelineStep step(
            String id, PlanArtifact.RuntimeTarget runtime) {
        return new PipelineStep(id, artifact(id, runtime));
    }

    private static PipelineEdge edge(PipelineStep producer, PipelineStep consumer) {
        return new PipelineEdge("edge", producer.stepId(),
                producer.stepId() + "-sink", consumer.stepId(),
                consumer.stepId() + "-source", "dataset", SCHEMA,
                PipelineEdge.BoundaryKind.VERSIONED_DATASET);
    }

    private static PlanArtifact artifact(
            String id, PlanArtifact.RuntimeTarget target) {
        return new PlanArtifact(id,
                PlanArtifact.CURRENT_PLAN_SCHEMA_VERSION,
                PlanArtifact.CURRENT_READER_VERSION,
                target,
                target == PlanArtifact.RuntimeTarget.BATCH
                        ? PlanArtifact.Boundedness.BOUNDED
                        : PlanArtifact.Boundedness.UNBOUNDED,
                PlanArtifact.ChangelogMode.APPEND,
                target == PlanArtifact.RuntimeTarget.BATCH
                        ? PlanArtifact.TimeMode.NONE
                        : PlanArtifact.TimeMode.PROCESSING_TIME,
                Arrays.asList(PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS,
                        PlanArtifact.FEATURE_TRUSTED_LEGACY_PLAN_GRAPH),
                Collections.<String>emptyList(),
                Arrays.asList(
                        new OperatorDescriptor(id + "-source",
                                OperatorDescriptor.Kind.SOURCE, "test.source", SCHEMA,
                                Collections.<String>emptyList()),
                        new OperatorDescriptor(id + "-sink",
                                OperatorDescriptor.Kind.SINK, "test.sink", SCHEMA,
                                Collections.singletonList(id + "-source"))),
                PlanArtifact.PayloadFormat.TRUSTED_LEGACY_PLAN_GRAPH,
                new byte[] {1, 2, 3});
    }

    private static void assertDecodeFailure(String code, byte[] encoded) {
        PipelineValidationException failure = assertThrows(
                PipelineValidationException.class,
                () -> PipelineDefinitionCodec.decode(encoded));
        assertEquals(code, failure.code());
    }

    private static void assertFailure(String code, Runnable action) {
        PipelineValidationException failure = assertThrows(
                PipelineValidationException.class, action::run);
        assertEquals(code, failure.code());
    }

    private static void writeInt(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) (value >>> 24);
        bytes[offset + 1] = (byte) (value >>> 16);
        bytes[offset + 2] = (byte) (value >>> 8);
        bytes[offset + 3] = (byte) value;
    }

    private static String repeat(char value, int count) {
        StringBuilder text = new StringBuilder(count);
        for (int i = 0; i < count; i++) text.append(value);
        return text.toString();
    }
}
