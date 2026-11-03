package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamAccumulationMode;
import io.rowweave.stream.api.StreamApiException;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamGenericWindowSpec;
import io.rowweave.stream.api.StreamOutputTag;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.stream.api.StreamWatermarkPolicy;
import io.rowweave.stream.api.StreamWindowAssignerSpec;
import io.rowweave.stream.api.StreamWindowResourceSpec;
import io.rowweave.stream.api.StreamWindowTriggerSpec;
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamWindowBinding;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** generic window is a typed, bounded portable composition. */
class StreamPublicGenericWindowCompilerContractTest {

    @Test
    void eventTimeSlidingAggregateRoundTripsItsCompletePortableContract() {
        StreamGenericWindowSpec specification = StreamGenericWindowSpec.aggregate(
                StreamWindowAssignerSpec.eventTimeSliding(
                        "event-time", 60_000L, 10_000L),
                StreamWindowTriggerSpec.eventTimeFinal().purging(),
                StreamAccumulationMode.DISCARDING, 30_000L,
                reference(FunctionAbi.STREAM_WINDOW_AGGREGATE_V1),
                outputSchema(), Collections.singletonList("adaptor-id"),
                windowState(), resources());
        StreamSession session = RowWeave.createStreamSession("generic-window");
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.fromSource(eventTimeSource()).uid("source")
                .keyBy("adaptor-id").uid("key")
                .genericWindow(specification).uid("window")
                .sinkTo("sink", sink());

        PlanArtifact artifact = session.toPlanArtifact();
        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(artifact);
        PortableStreamNode node = job.plan().nodes().get(2);
        assertEquals(StreamOperatorCode.GENERIC_WINDOW, node.opcode());
        StreamWindowBinding binding = (StreamWindowBinding) node.binding();
        assertEquals(StreamWindowBinding.AssignerKind.EVENT_TIME_SLIDING,
                binding.assignerKind());
        assertEquals(StreamWindowBinding.TriggerKind.EVENT_TIME_FINAL,
                binding.triggerKind());
        assertTrue(binding.purging());
        assertEquals(30_000L, binding.allowedLatenessMillis());
        assertEquals(8, binding.resources().maximumActiveWindowsPerKey());
        assertEquals(specification, specification);
        assertTrue(job.plan().requiredCapabilities().contains(
                PortableStreamPlan.GENERIC_WINDOW_CAPABILITY));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_GENERIC_WINDOW_V1));
    }

    @Test
    void processingTimeSessionProcessCarriesMergeAndCursorBounds() {
        StreamGenericWindowSpec specification = StreamGenericWindowSpec.process(
                StreamWindowAssignerSpec.processingTimeSession(5_000L),
                StreamWindowTriggerSpec.processingTime(1_000L),
                StreamAccumulationMode.ACCUMULATING, 0L,
                reference(FunctionAbi.STREAM_WINDOW_PROCESS_V1),
                outputSchema(), Collections.singletonList("adaptor-id"),
                windowState(), resources());
        StreamSession session = RowWeave.createStreamSession("session-window");
        session.setChangelogMode(StreamChangelogMode.UPSERT);
        session.fromSource(processingSource()).uid("source")
                .keyBy("adaptor-id").uid("key")
                .genericWindow(specification).uid("window")
                .sinkTo("sink", sink());

        StreamWindowBinding binding = (StreamWindowBinding)
                PortableStreamJobArtifact.decodeValidated(
                        session.toPlanArtifact()).plan().nodes().get(2).binding();
        assertEquals(StreamWindowBinding.AssignerKind.PROCESSING_TIME_SESSION,
                binding.assignerKind());
        assertEquals(4, binding.resources().maximumSessionMergeFanIn());
        assertEquals(1000L, binding.resources().maximumRowsPerWindow());
    }

    @Test
    void compilerRejectsUnkeyedWrongTimeAndWrongFunctionAbi() {
        StreamGenericWindowSpec eventWindow = StreamGenericWindowSpec.aggregate(
                StreamWindowAssignerSpec.eventTimeTumbling(
                        "event-time", 60_000L),
                StreamWindowTriggerSpec.eventTimeFinal(),
                StreamAccumulationMode.DISCARDING, 0L,
                reference(FunctionAbi.STREAM_WINDOW_AGGREGATE_V1),
                outputSchema(), Collections.singletonList("adaptor-id"),
                windowState(), resources());

        StreamSession unkeyed = RowWeave.createStreamSession("unkeyed-window");
        unkeyed.setTimeMode(StreamTimeMode.EVENT_TIME);
        unkeyed.fromSource(eventTimeSource()).uid("source")
                .genericWindow(eventWindow).uid("window").sinkTo("sink", sink());
        assertCode(StreamApiException.Code.MISSING_STABLE_KEY,
                unkeyed::toPlanArtifact);

        StreamSession wrongTime = RowWeave.createStreamSession("wrong-time-window");
        wrongTime.fromSource(processingSource()).uid("source")
                .keyBy("adaptor-id").uid("key")
                .genericWindow(eventWindow).uid("window").sinkTo("sink", sink());
        assertCode(StreamApiException.Code.TIME_MODE_MISMATCH,
                wrongTime::toPlanArtifact);

        assertThrows(IllegalArgumentException.class, () ->
                StreamGenericWindowSpec.aggregate(
                        StreamWindowAssignerSpec.countTumbling(100L),
                        StreamWindowTriggerSpec.count(100L),
                        StreamAccumulationMode.DISCARDING, 0L,
                        reference(FunctionAbi.MAP_V1), outputSchema(),
                        Collections.singletonList("adaptor-id"), windowState(),
                        resources()));
        assertThrows(IllegalArgumentException.class, () ->
                StreamWindowResourceSpec.of(0, 1000L, 1024L, 8192L, 4,
                        StreamEmissionLimit.of(1, 1024L)));
    }

    @Test
    void lateRowsMustUseAnExplicitConsumedTaggedPort() {
        StreamOutputTag late = StreamOutputTag.of("late", inputSchema(),
                StreamChangelogMode.APPEND,
                Collections.singletonList("adaptor-id"),
                StreamEmissionLimit.of(1, 4096L),
                StreamEmissionLimit.of(1, 4096L),
                StreamEmissionLimit.of(1, 4096L));
        StreamGenericWindowSpec rewindow = StreamGenericWindowSpec
                .aggregate(StreamWindowAssignerSpec.eventTimeTumbling(
                                "event-time", 60_000L),
                        StreamWindowTriggerSpec.eventTimeFinal(),
                        StreamAccumulationMode.DISCARDING, 30_000L,
                        reference(FunctionAbi.STREAM_WINDOW_AGGREGATE_V1),
                        outputSchema(), Collections.singletonList("adaptor-id"),
                        windowState(), resources());
        StreamGenericWindowSpec specification = rewindow.withLateOutput(late);
        StreamSession session = RowWeave.createStreamSession("late-window");
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        StreamWindowResult result = session.fromSource(eventTimeSource())
                .uid("source").keyBy("adaptor-id").uid("key")
                .genericWindowWithLateOutput(specification, "window");
        result.main().sinkTo("main-sink", sink());
        result.late().genericWindow(rewindow).uid("late-rewindow")
                .sinkTo("late-sink", sink());

        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(
                session.toPlanArtifact());
        StreamWindowBinding binding = (StreamWindowBinding)
                job.plan().nodes().get(2).binding();
        assertEquals("late", binding.lateOutputTag().tagId());
        assertEquals("late", edge(job, "late-rewindow").sourceOutputTagId());
        assertEquals(inputSchema().fingerprint(),
                node(job, "late-rewindow").inputSchemaFingerprints().get(0));
        assertTrue(job.plan().requiredCapabilities().contains(
                PortableStreamPlan.TAGGED_OUTPUT_CAPABILITY));

        StreamSession unused = RowWeave.createStreamSession("unused-late-window");
        unused.setTimeMode(StreamTimeMode.EVENT_TIME);
        StreamWindowResult unusedResult = unused.fromSource(eventTimeSource())
                .uid("source").keyBy("adaptor-id").uid("key")
                .genericWindowWithLateOutput(specification, "window");
        unusedResult.main().sinkTo("sink", sink());
        assertCode(StreamApiException.Code.UNUSED_OUTPUT_TAG,
                unused::toPlanArtifact);
    }

    private static StreamEdge edge(PortableStreamJob job, String target) {
        for (StreamEdge edge : job.plan().edges()) {
            if (edge.targetOperatorUid().equals(target)) return edge;
        }
        throw new AssertionError("missing edge");
    }

    private static PortableStreamNode node(PortableStreamJob job,
            String uid) {
        for (PortableStreamNode node : job.plan().nodes()) {
            if (node.operatorUid().equals(uid)) return node;
        }
        throw new AssertionError("missing node");
    }

    private static StreamWindowResourceSpec resources() {
        return StreamWindowResourceSpec.of(8, 1000L, 1024L * 1024L,
                8L * 1024L * 1024L, 4,
                StreamEmissionLimit.of(4, 16L * 1024L));
    }

    private static StreamStateSpec windowState() {
        return new StreamStateSpec("generic-window-state",
                StreamStateSpec.Kind.KEYED_AGGREGATING, "window-state-v1", 1,
                StreamStateSpec.TtlMode.NONE, 0L);
    }

    private static StreamSchema inputSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("adaptor-id", "adaptorId", "adaptorId",
                        DataType.StringType, false),
                new StreamField("event-time", "eventTime", "eventTime",
                        DataType.TimestampType, false),
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

    private static StreamSourceSpec eventTimeSource() {
        return StreamSourceSpec.exactOrderedEventTime(inputSchema(), "source-v1",
                sha256("source-contract"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        sha256("split"), "sequence-v1", new byte[] {2})),
                StreamWatermarkPolicy.providerEvents(30_000L));
    }

    private static StreamSourceSpec processingSource() {
        return StreamSourceSpec.exactOrdered(inputSchema(), "source-v1",
                sha256("source-contract"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        sha256("split"), "sequence-v1", new byte[] {2})));
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", sha256("sink-contract"), new byte[] {3});
    }

    private static FunctionDeploymentReference reference(FunctionAbi abi) {
        byte[] config = new byte[] {4};
        return new FunctionDeploymentReference(sha256("deployment-set"),
                "window-function", sha256("jar"), "example.WindowFunction",
                abi, "config-v1", sha256(config), config,
                new FunctionResourceProfile(1024L * 1024L, 0L, 8));
    }

    private static void assertCode(StreamApiException.Code code,
            Runnable action) {
        StreamApiException failure = assertThrows(
                StreamApiException.class, action::run);
        assertEquals(code, failure.code());
    }

    private static String sha256(String value) {
        return sha256(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String sha256(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(64);
            for (byte current : digest) result.append(String.format(
                    java.util.Locale.ROOT, "%02x", current & 0xff));
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
