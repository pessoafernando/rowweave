package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamWatermarkPolicy;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamContractException;
import io.rowweave.stream.spi.StreamSourceEvent;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Public bounded completion must be explicit and capability-gated. */
class StreamPublicBoundedSourceContractTest {

    @Test
    void boundedSourcePublishesTerminalCapabilityAndEvent() {
        StreamSession session = RowWeave.createStreamSession("bounded-source");
        session.fromSource(StreamSourceSpec.boundedExactOrdered(schema(),
                "source-v1", sha256("source-contract"), new byte[] {1},
                splits())).uid("source").sinkTo("sink", sink());

        PlanArtifact artifact = session.toPlanArtifact();
        PortableStreamPlan plan = PortableStreamJobArtifact
                .decodeValidated(artifact).plan();
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_SOURCE_END_OF_INPUT_V1));
        assertTrue(plan.requiredCapabilities().contains(
                PortableStreamPlan.SOURCE_END_OF_INPUT_CAPABILITY));
        assertEquals("EXPLICIT_END_OF_INPUT", plan.nodes().get(0)
                .scalarParameters().get("source.termination"));
        assertEquals(StreamSourceEvent.Kind.END_OF_INPUT,
                StreamSourceEvent.endOfInput().kind());

        java.util.List<String> missing = new ArrayList<>(
                plan.requiredCapabilities());
        missing.remove(PortableStreamPlan.SOURCE_END_OF_INPUT_CAPABILITY);
        StreamContractException mismatch = assertThrows(
                StreamContractException.class, () -> new PortableStreamPlan(
                        plan.planVersion(), plan.deliveryGuarantee(),
                        plan.parallelism(), plan.maxParallelism(),
                        plan.changelogMode(), plan.timeMode(),
                        plan.rootSinkOperatorUids(), plan.nodes(),
                        plan.edges(), missing));
        assertEquals("SOURCE_TERMINATION_CAPABILITY_MISMATCH",
                mismatch.code());
    }

    @Test
    void existingFixedSourceKeepsLegacyTerminationSemantics() {
        StreamSession session = RowWeave.createStreamSession("resident-source");
        session.fromSource(StreamSourceSpec.exactOrdered(schema(),
                "source-v1", sha256("source-contract"), new byte[] {1},
                splits())).uid("source").sinkTo("sink", sink());

        PlanArtifact artifact = session.toPlanArtifact();
        PortableStreamPlan plan = PortableStreamJobArtifact
                .decodeValidated(artifact).plan();
        assertFalse(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_SOURCE_END_OF_INPUT_V1));
        assertFalse(plan.requiredCapabilities().contains(
                PortableStreamPlan.SOURCE_END_OF_INPUT_CAPABILITY));
        assertFalse(plan.nodes().get(0).scalarParameters()
                .containsKey("source.termination"));
    }

    @Test
    void boundedEventTimeSourcePublishesTerminalCapability() {
        StreamSession session = RowWeave.createStreamSession(
                "bounded-event-time");
        session.setTimeMode(io.rowweave.stream.api.StreamTimeMode.EVENT_TIME);
        session.fromSource(StreamSourceSpec.boundedExactOrderedEventTime(
                schema(), "source-v1", sha256("source-contract"),
                new byte[] {1}, splits(),
                StreamWatermarkPolicy.providerEvents(1_000L)))
                .uid("source").sinkTo("sink", sink());

        PortableStreamPlan plan = PortableStreamJobArtifact.decodeValidated(
                session.toPlanArtifact()).plan();
        assertTrue(plan.requiredCapabilities().contains(
                PortableStreamPlan.SOURCE_END_OF_INPUT_CAPABILITY));
        assertEquals("EXPLICIT_END_OF_INPUT", plan.nodes().get(0)
                .scalarParameters().get("source.termination"));
    }

    private static StreamSchema schema() {
        return new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id", DataType.LongType, false)));
    }

    private static java.util.List<StreamSplitSpec> splits() {
        return Collections.singletonList(new StreamSplitSpec(
                sha256("split"), "position-v1", new byte[] {2}));
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", sha256("sink-contract"), new byte[] {3});
    }

    private static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(64);
            for (byte current : digest) {
                result.append(Character.forDigit((current >>> 4) & 0xf, 16));
                result.append(Character.forDigit(current & 0xf, 16));
            }
            return result.toString();
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
