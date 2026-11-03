package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.portable.PortableDataType;
import io.rowweave.stream.api.StreamApiException;
import io.rowweave.stream.api.StreamEventTimeSpec;
import io.rowweave.stream.api.StreamExpression;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.stream.api.StreamWatermarkPolicy;
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamTimeBinding;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** event time assignment is an explicit atomic authority. */
class StreamPublicEventTimeCompilerContractTest {

    @Test
    void boundedAssignmentCompilesWithoutProviderTimeAuthority() {
        StreamSession session = eventSession("assigned-time");
        StreamEventTimeSpec spec = StreamEventTimeSpec.boundedOutOfOrderness(
                StreamExpression.input("event-time",
                        PortableDataType.TIMESTAMP, false),
                5_000L, 30_000L,
                StreamEventTimeSpec.InvalidTimestampPolicy.FAIL);
        session.fromSource(source(false)).uid("source")
                .assignEventTime(spec).uid("assign-time")
                .sinkTo("sink", sink());

        PlanArtifact artifact = session.toPlanArtifact();
        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(artifact);
        PortableStreamNode node = job.plan().nodes().get(1);
        StreamTimeBinding binding = (StreamTimeBinding) node.binding();
        assertEquals(StreamOperatorCode.ASSIGN_EVENT_TIME, node.opcode());
        assertEquals(StreamTimeBinding.WatermarkMode.BOUNDED_OUT_OF_ORDERNESS,
                binding.watermarkMode());
        assertEquals(5_000L, binding.maximumOutOfOrderMillis());
        assertEquals(30_000L, binding.idleTimeoutMillis());
        assertEquals(schema().toPortableSchema(), node.outputSchema());
        assertTrue(job.plan().requiredCapabilities().contains(
                PortableStreamPlan.EVENT_TIME_ASSIGNMENT_CAPABILITY));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_EVENT_TIME_ASSIGNMENT_V1));
    }

    @Test
    void timeAuthorityMustCoverEveryPathAndMustNotOverlapProviderAuthority() {
        StreamEventTimeSpec monotonic = StreamEventTimeSpec.monotonic(
                StreamExpression.input("event-time",
                        PortableDataType.TIMESTAMP, false), 30_000L,
                StreamEventTimeSpec.InvalidTimestampPolicy.FAIL);

        StreamSession uncovered = eventSession("uncovered-time");
        DataStream source = uncovered.fromSource(source(false)).uid("source");
        source.assignEventTime(monotonic).uid("assign").sinkTo("assigned", sink());
        source.sinkTo("unassigned", sink());
        assertCode(StreamApiException.Code.MISSING_WATERMARK_POLICY,
                uncovered::toPlanArtifact);

        StreamSession duplicate = eventSession("duplicate-time");
        duplicate.fromSource(source(true)).uid("source")
                .assignEventTime(monotonic).uid("assign")
                .sinkTo("sink", sink());
        assertCode(StreamApiException.Code.TIME_AUTHORITY_CONFLICT,
                duplicate::toPlanArtifact);

        StreamSession processing = RowWeave.createStreamSession("processing-time");
        processing.fromSource(source(false)).uid("source")
                .assignEventTime(monotonic).uid("assign")
                .sinkTo("sink", sink());
        assertCode(StreamApiException.Code.TIME_MODE_MISMATCH,
                processing::toPlanArtifact);
    }

    @Test
    void timestampTypeAndAllResourceBoundsFailClosed() {
        assertThrows(IllegalArgumentException.class, () ->
                StreamEventTimeSpec.monotonic(StreamExpression.input(
                        "id", PortableDataType.LONG, false), 1_000L,
                        StreamEventTimeSpec.InvalidTimestampPolicy.FAIL));
        assertThrows(IllegalArgumentException.class, () ->
                StreamEventTimeSpec.boundedOutOfOrderness(
                        StreamExpression.input("event-time",
                                PortableDataType.TIMESTAMP, false),
                        -1L, 1_000L,
                        StreamEventTimeSpec.InvalidTimestampPolicy.FAIL));
        assertThrows(IllegalArgumentException.class, () ->
                StreamEventTimeSpec.monotonic(StreamExpression.input(
                        "event-time", PortableDataType.TIMESTAMP, false),
                        0L, StreamEventTimeSpec.InvalidTimestampPolicy.FAIL));
    }

    private static StreamSession eventSession(String id) {
        StreamSession session = RowWeave.createStreamSession(id);
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        return session;
    }

    private static void assertCode(StreamApiException.Code code,
            Runnable action) {
        StreamApiException failure = assertThrows(
                StreamApiException.class, action::run);
        assertEquals(code, failure.code());
    }

    private static StreamSchema schema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("id", "id", "id", DataType.LongType, false),
                new StreamField("event-time", "eventTime", "eventTime",
                        DataType.TimestampType, false)));
    }

    private static StreamSourceSpec source(boolean nativeTime) {
        java.util.List<StreamSplitSpec> splits = Collections.singletonList(
                new StreamSplitSpec(sha256("split"), "sequence-v1",
                        new byte[] {2}));
        return nativeTime
                ? StreamSourceSpec.exactOrderedEventTime(schema(), "source-v1",
                sha256("source-contract"), new byte[] {1}, splits,
                StreamWatermarkPolicy.providerEvents(30_000L))
                : StreamSourceSpec.exactOrdered(schema(), "source-v1",
                sha256("source-contract"), new byte[] {1}, splits);
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
            for (byte current : digest) result.append(String.format(
                    java.util.Locale.ROOT, "%02x", current & 0xff));
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
