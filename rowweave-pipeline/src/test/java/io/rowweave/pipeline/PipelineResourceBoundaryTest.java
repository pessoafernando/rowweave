package io.rowweave.pipeline;

import io.rowweave.artifact.OperatorDescriptor;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactLimits;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineResourceBoundaryTest {
    private static final String SCHEMA = repeat('a', 64);

    /**
     * Counts element reads, so "refused before traversal" is measured rather
     * than assumed: a limit violation must cost nothing beyond the size check.
     */
    private static final class Counted<T> extends java.util.AbstractList<T> {
        private final List<T> delegate;
        private final AtomicLong reads = new AtomicLong();

        Counted(List<T> delegate) {
            this.delegate = delegate;
        }

        @Override public T get(int index) {
            reads.incrementAndGet();
            return delegate.get(index);
        }

        @Override public int size() { return delegate.size(); }

        long reads() { return reads.get(); }
    }

    @Test
    void collectionLimitPlusOneFailsBeforeElementTraversal() {
        PipelineStep only = step();
        Counted<String> features = new Counted<>(Collections.nCopies(
                PipelineLimits.MAX_FEATURES + 1,
                PipelineDefinition.FEATURE_COMPOSITION_V1));
        Counted<PipelineStep> steps = new Counted<>(Collections.nCopies(
                PipelineLimits.MAX_STEPS + 1, only));
        PipelineEdge edge = new PipelineEdge("edge", "only", "only-sink",
                "only", "only-source", "dataset", SCHEMA,
                PipelineEdge.BoundaryKind.VERSIONED_DATASET);
        Counted<PipelineEdge> edges = new Counted<>(Collections.nCopies(
                PipelineLimits.MAX_EDGES + 1, edge));
        Counted<PipelineOutput> outputs = new Counted<>(
                outputList(PipelineLimits.MAX_OUTPUTS + 1));

        assertFailure("LIMIT_EXCEEDED", () -> new PipelineDefinition(
                1, 1, "pipeline", "revision", features,
                Collections.<String>emptyList(), Collections.singletonList(only),
                Collections.<PipelineEdge>emptyList(), outputList(1)));
        assertFailure("LIMIT_EXCEEDED", () -> new PipelineDefinition(
                1, 1, "pipeline", "revision",
                Collections.singletonList(PipelineDefinition.FEATURE_COMPOSITION_V1),
                Collections.<String>emptyList(), steps,
                Collections.<PipelineEdge>emptyList(), outputList(1)));
        assertFailure("LIMIT_EXCEEDED", () -> new PipelineDefinition(
                1, 1, "pipeline", "revision",
                Collections.singletonList(PipelineDefinition.FEATURE_COMPOSITION_V1),
                Collections.<String>emptyList(), Collections.singletonList(only),
                edges, outputList(1)));
        assertFailure("LIMIT_EXCEEDED", () -> new PipelineDefinition(
                1, 1, "pipeline", "revision",
                Collections.singletonList(PipelineDefinition.FEATURE_COMPOSITION_V1),
                Collections.<String>emptyList(), Collections.singletonList(only),
                Collections.<PipelineEdge>emptyList(), outputs));

        assertEquals(0L, features.reads());
        assertEquals(0L, steps.reads());
        assertEquals(0L, edges.reads());
        assertEquals(0L, outputs.reads());
    }

    @Test
    void outputLimitAndUtf8ByteLimitHaveExactBoundaryBehavior() {
        PipelineDefinition atLimit = new PipelineDefinition(
                1, 1, repeat('p', PipelineLimits.MAX_ID_BYTES), "revision",
                Collections.singletonList(PipelineDefinition.FEATURE_COMPOSITION_V1),
                Collections.<String>emptyList(), Collections.singletonList(step()),
                Collections.<PipelineEdge>emptyList(),
                outputList(PipelineLimits.MAX_OUTPUTS));
        assertEquals(PipelineLimits.MAX_OUTPUTS, atLimit.outputs().size());

        String utf8AtLimit = repeat('\u754c', 85); // 255 UTF-8 bytes
        PipelineDefinition unicode = PipelineDefinition.builder(
                        utf8AtLimit, "revision-\uD83D\uDE80")
                .addStep(step()).addOutput("out", "only", "only-sink").build();
        assertEquals(unicode.definitionDigest(), PipelineDefinitionCodec.decode(
                PipelineDefinitionCodec.encode(unicode)).definitionDigest());
        assertFailure("LIMIT_EXCEEDED", () ->
                PipelineDefinition.builder(repeat('\u754c', 86), "revision"));
        assertFailure("INVALID_UTF8", () ->
                PipelineDefinition.builder("bad-\uD800", "revision"));
        assertFailure("INVALID_ID", () ->
                PipelineDefinition.builder("bad\nline", "revision"));
    }

    @Test
    void codecReportsReservationsForEncodeAndDecode() {
        PipelineDefinition definition = new PipelineDefinition(
                1, 1, "pipeline", "revision",
                Collections.singletonList(PipelineDefinition.FEATURE_COMPOSITION_V1),
                Collections.<String>emptyList(), Collections.singletonList(step()),
                Collections.<PipelineEdge>emptyList(), outputList(1));
        AtomicLong encodeReservation = new AtomicLong();
        byte[] encoded = PipelineDefinitionCodec.encode(
                definition, encodeReservation::addAndGet);
        AtomicLong decodeReservation = new AtomicLong();
        PipelineDefinition decoded = PipelineDefinitionCodec.decode(
                encoded, decodeReservation::addAndGet);

        assertTrue(encodeReservation.get() > encoded.length);
        assertTrue(decodeReservation.get() > 0L);
        assertEquals(definition.definitionDigest(), decoded.definitionDigest());
        assertTrue(encoded.length <= PipelineDefinitionCodec.MAX_FRAME_BYTES);
    }

    @Test
    void aggregateArtifactBytesRespectExactBodyBoundaryBeforeReservation() {
        int emptyBodyBytes = bodyLength(PipelineDefinitionCodec.encode(
                aggregatePayloadDefinition(0)));
        int payloadAtBodyLimit = PipelineLimits.MAX_BODY_BYTES - emptyBodyBytes;
        assertTrue(payloadAtBodyLimit > PlanArtifactLimits.MAX_PAYLOAD_BYTES);
        assertTrue(payloadAtBodyLimit <= 2 * PlanArtifactLimits.MAX_PAYLOAD_BYTES);

        assertEquals(PipelineLimits.MAX_BODY_BYTES - 1,
                bodyLength(PipelineDefinitionCodec.encode(
                        aggregatePayloadDefinition(payloadAtBodyLimit - 1))));
        assertEquals(PipelineLimits.MAX_BODY_BYTES,
                bodyLength(PipelineDefinitionCodec.encode(
                        aggregatePayloadDefinition(payloadAtBodyLimit))));

        AtomicLong reservation = new AtomicLong();
        PipelineValidationException failure = assertThrows(
                PipelineValidationException.class,
                () -> PipelineDefinitionCodec.encode(
                        aggregatePayloadDefinition(payloadAtBodyLimit + 1),
                        reservation::addAndGet));
        assertEquals("LIMIT_EXCEEDED", failure.code());
        assertEquals("envelope.bodyLength", failure.path());
        assertEquals(0L, reservation.get(),
                "aggregate overflow must fail before nested encoding or body allocation");
    }

    private static List<PipelineOutput> outputList(int count) {
        List<PipelineOutput> outputs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            outputs.add(new PipelineOutput("out-" + i, "only", "only-sink"));
        }
        return outputs;
    }

    private static PipelineStep step() {
        PlanArtifact artifact = new PlanArtifact("only",
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
                        new OperatorDescriptor("only-source",
                                OperatorDescriptor.Kind.SOURCE, "test.source", SCHEMA,
                                Collections.<String>emptyList()),
                        new OperatorDescriptor("only-sink",
                                OperatorDescriptor.Kind.SINK, "test.sink", SCHEMA,
                                Collections.singletonList("only-source"))),
                PlanArtifact.PayloadFormat.TRUSTED_LEGACY_PLAN_GRAPH,
                new byte[] {1});
        return new PipelineStep("only", artifact);
    }

    private static PipelineDefinition aggregatePayloadDefinition(int payloadBytes) {
        int leftBytes = Math.min(payloadBytes, PlanArtifactLimits.MAX_PAYLOAD_BYTES);
        int rightBytes = payloadBytes - leftBytes;
        PipelineStep left = payloadStep("left", leftBytes);
        PipelineStep right = payloadStep("right", rightBytes);
        return new PipelineDefinition(
                1, 1, "aggregate-pipeline", "aggregate-revision",
                Collections.singletonList(PipelineDefinition.FEATURE_COMPOSITION_V1),
                Collections.<String>emptyList(), Arrays.asList(left, right),
                Collections.<PipelineEdge>emptyList(),
                Collections.singletonList(new PipelineOutput(
                        "aggregate-out", "right", "right-sink")));
    }

    private static PipelineStep payloadStep(String id, int payloadBytes) {
        PlanArtifact artifact = new PlanArtifact(id,
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
                        new OperatorDescriptor(id + "-source",
                                OperatorDescriptor.Kind.SOURCE, "test.source", SCHEMA,
                                Collections.<String>emptyList()),
                        new OperatorDescriptor(id + "-sink",
                                OperatorDescriptor.Kind.SINK, "test.sink", SCHEMA,
                                Collections.singletonList(id + "-source"))),
                PlanArtifact.PayloadFormat.TRUSTED_LEGACY_PLAN_GRAPH,
                new byte[payloadBytes]);
        return new PipelineStep(id, artifact);
    }

    private static int bodyLength(byte[] encoded) {
        return ByteBuffer.wrap(encoded).getInt(12);
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
