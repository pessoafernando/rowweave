package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamApiException;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.stream.api.StreamWindowSpec;
import io.rowweave.stream.api.StreamWatermarkPolicy;
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamOperatorCode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** product windows bind all input/output fields by stable ID. */
class StreamPublicWindowCompilerContractTest {

    @Test
    void finalDecimalWindowCompilesExplicitFieldBindingsAndRequiredFeature() {
        StreamSession session = RowWeave.createStreamSession("window");
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.fromSource(source(inputSchema(false))).uid("source")
                .keyBy("adaptor-id").uid("key")
                .eventTimeDecimalWindow(window(), outputSchema(), windowState())
                .uid("window").sinkTo("sink", sink());

        PlanArtifact artifact = session.toPlanArtifact();
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_WINDOW_FIELD_BINDING_V1));
        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(artifact);
        assertTrue(job.plan().requiredCapabilities().contains(
                PortableStreamPlan.WINDOW_FIELD_BINDING_CAPABILITY));
        PortableStreamNode window = job.plan().nodes().get(2);
        assertEquals(StreamOperatorCode.EVENT_TIME_WINDOW, window.opcode());
        assertEquals(9, window.scalarParameters().size());
        assertEquals("event-time",
                window.scalarParameters().get("window.eventTimeField"));
        assertEquals("amount", window.scalarParameters().get("window.valueField"));
        assertEquals("window-start",
                window.scalarParameters().get("window.startOutputField"));
        assertEquals(Collections.singletonList("adaptor-id"),
                window.stableKeyFields());
    }

    @Test
    void slidingWindowKeepsFiniteSizeAndSlide() {
        StreamWindowSpec sliding = StreamWindowSpec.slidingDecimal(
                "event-time", "amount", 60_000L, 10_000L,
                "window-start", "window-end", "total");
        StreamSession session = RowWeave.createStreamSession("sliding-window");
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.fromSource(source(inputSchema(false))).uid("source")
                .keyBy("adaptor-id").uid("key")
                .eventTimeDecimalWindow(sliding, outputSchema(), windowState())
                .uid("window").sinkTo("sink", sink());
        PortableStreamNode node = PortableStreamJobArtifact.decodeValidated(
                session.toPlanArtifact()).plan().nodes().get(2);
        assertEquals("SLIDING", node.scalarParameters().get("window.type"));
        assertEquals("60000", node.scalarParameters().get("window.sizeMillis"));
        assertEquals("10000", node.scalarParameters().get("window.slideMillis"));
    }

    @Test
    void productWindowRejectsTimeAndSchemaGuessing() {
        StreamSession processing = RowWeave.createStreamSession("processing-window");
        addWindow(processing, inputSchema(false));
        StreamApiException wrongTime = assertThrows(
                StreamApiException.class, processing::toPlanArtifact);
        assertEquals(StreamApiException.Code.TIME_MODE_MISMATCH, wrongTime.code());

        StreamSession nullableTime = RowWeave.createStreamSession("nullable-time");
        nullableTime.setTimeMode(StreamTimeMode.EVENT_TIME);
        addWindow(nullableTime, inputSchema(true));
        StreamApiException badField = assertThrows(
                StreamApiException.class, nullableTime::toPlanArtifact);
        assertEquals(StreamApiException.Code.INVALID_WINDOW_FIELD_BINDING,
                badField.code());
    }

    private static void addWindow(StreamSession session, StreamSchema input) {
        session.fromSource(source(input)).uid("source")
                .keyBy("adaptor-id").uid("key")
                .eventTimeDecimalWindow(window(), outputSchema(), windowState())
                .uid("window").sinkTo("sink", sink());
    }

    private static StreamWindowSpec window() {
        return StreamWindowSpec.tumblingDecimal("event-time", "amount", 60_000L,
                "window-start", "window-end", "total");
    }

    private static StreamStateSpec windowState() {
        StreamStateSpec state = StreamStateSpec.decimalWindow("window-state");
        assertEquals(StreamStateSpec.Kind.KEYED_VALUE, state.kind());
        assertEquals("rowweave.decimal-unscaled-v1",
                state.serializerContractId());
        assertEquals(1, state.stateSchemaVersion());
        assertEquals(StreamStateSpec.TtlMode.NONE, state.ttlMode());
        assertEquals(0L, state.ttlMillis());
        return state;
    }

    private static StreamSchema inputSchema(boolean nullableTime) {
        return new StreamSchema(Arrays.asList(
                new StreamField("adaptor-id", "adaptorId", "adaptorId",
                        DataType.StringType, false),
                new StreamField("event-time", "eventTime", "eventTime",
                        DataType.TimestampType, nullableTime),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, true)));
    }

    private static StreamSchema outputSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("adaptor-id", "adaptorId", "adaptorId",
                        DataType.StringType, false),
                new StreamField("window-start", "windowStart", "windowStart",
                        DataType.LongType, false),
                new StreamField("window-end", "windowEnd", "windowEnd",
                        DataType.LongType, false),
                new StreamField("total", "total", "total",
                        DataType.BigDecimalType, false)));
    }

    private static StreamSourceSpec source(StreamSchema schema) {
        return StreamSourceSpec.exactOrderedEventTime(schema, "source-v1",
                sha256("source-contract"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        sha256("split"), "sequence-v1", new byte[] {2})),
                StreamWatermarkPolicy.providerEvents(30_000L));
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
