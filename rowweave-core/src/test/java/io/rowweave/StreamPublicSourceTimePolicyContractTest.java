package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamApiException;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.stream.api.StreamWatermarkPolicy;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** event-time source progress is explicit and capability-gated. */
class StreamPublicSourceTimePolicyContractTest {

    @Test
    void eventTimeSourceCompilesProviderWatermarkAndIdleContract() {
        StreamSession session = RowWeave.createStreamSession("event-source");
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.fromSource(eventSource()).uid("source").sinkTo("sink", sink());

        PlanArtifact artifact = session.toPlanArtifact();
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_SOURCE_TIME_POLICY_V1));
        PortableStreamNode source = PortableStreamJobArtifact.decodeValidated(
                artifact).plan().nodes().get(0);
        assertEquals("PROVIDER_RECORD",
                source.scalarParameters().get("source.time.timestamp"));
        assertEquals("PROVIDER_EVENT",
                source.scalarParameters().get("source.time.watermark"));
        assertEquals("GENERATION_FENCED_ACTIVE_IDLE",
                source.scalarParameters().get("source.time.status"));
        assertEquals("30000",
                source.scalarParameters().get("source.time.idleTimeoutMillis"));
    }

    @Test
    void timeModeAndSourcePolicyMustCloseBeforeArtifact() {
        StreamSession missing = RowWeave.createStreamSession("missing-policy");
        missing.setTimeMode(StreamTimeMode.EVENT_TIME);
        missing.fromSource(processingSource()).uid("source").sinkTo("sink", sink());
        StreamApiException noPolicy = assertThrows(
                StreamApiException.class, missing::toPlanArtifact);
        assertEquals(StreamApiException.Code.MISSING_WATERMARK_POLICY,
                noPolicy.code());

        StreamSession processing = RowWeave.createStreamSession("unexpected-policy");
        processing.fromSource(eventSource()).uid("source").sinkTo("sink", sink());
        StreamApiException wrongMode = assertThrows(
                StreamApiException.class, processing::toPlanArtifact);
        assertEquals(StreamApiException.Code.TIME_MODE_MISMATCH,
                wrongMode.code());
    }

    private static StreamSourceSpec eventSource() {
        return StreamSourceSpec.exactOrderedEventTime(schema(), "source-v1",
                sha256("source-contract"), new byte[] {1}, splits(),
                StreamWatermarkPolicy.providerEvents(30_000L));
    }

    private static StreamSourceSpec processingSource() {
        return StreamSourceSpec.exactOrdered(schema(), "source-v1",
                sha256("source-contract"), new byte[] {1}, splits());
    }

    private static java.util.List<StreamSplitSpec> splits() {
        return Collections.singletonList(new StreamSplitSpec(
                sha256("split"), "sequence-v1", new byte[] {2}));
    }

    private static StreamSchema schema() {
        return new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id", DataType.StringType, false)));
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", sha256("sink-contract"), new byte[] {3});
    }

    private static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte current : digest) result.append(String.format("%02x", current & 0xff));
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
