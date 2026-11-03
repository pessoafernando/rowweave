package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamAsyncOrdering;
import io.rowweave.stream.api.StreamAsyncReplaySpec;
import io.rowweave.stream.api.StreamAsyncResourceSpec;
import io.rowweave.stream.api.StreamAsyncWaitSpec;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamManagedStateSpec;
import io.rowweave.stream.api.StreamProcessSpec;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.stream.api.StreamTimerSpec;
import io.rowweave.stream.api.StreamWatermarkPolicy;
import io.rowweave.stream.api.StreamWindowSpec;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.PortableStreamPlanCodec;
import io.rowweave.stream.contract.StreamContractException;
import io.rowweave.stream.contract.StreamSinkBinding;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** RETRACT is explicit on Source, edge, node and Sink. */
class StreamPublicRetractCompilerContractTest {

    @Test
    void retractSourceAndSinkCompileAsRequiredV2Capability() {
        StreamSession session = RowWeave.createStreamSession("retract-contract");
        session.setChangelogMode(StreamChangelogMode.RETRACT);
        session.fromSource(StreamSourceSpec.retractExactOrdered(
                schema(), Collections.singletonList("id"), "source-v2",
                sha256("source"), new byte[] {1},
                Collections.singletonList(split())))
                .uid("source")
                .sinkTo("sink", StreamSinkSpec.retractEagerAtLeastOnce(
                        "sink-v2", sha256("sink"), new byte[] {2}));

        PlanArtifact artifact = session.toPlanArtifact();
        PortableStreamPlan plan = PortableStreamJobArtifact
                .decodeValidated(artifact).plan();
        StreamSinkBinding sink = (StreamSinkBinding)
                plan.nodes().get(1).binding();

        assertEquals(PlanArtifact.ChangelogMode.RETRACT,
                artifact.changelogMode());
        assertEquals(PortableStreamPlan.ChangelogMode.RETRACT,
                plan.changelogMode());
        assertEquals(PortableStreamPlan.RETRACT_CHANGELOG_VERSION,
                plan.planVersion());
        assertTrue(plan.requiredCapabilities().contains(
                PortableStreamPlan.RETRACT_CHANGELOG_CAPABILITY));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_RETRACT_CHANGELOG_V2));
        assertEquals(PortableStreamPlan.ChangelogMode.RETRACT,
                plan.outputChangelogMode("source"));
        assertTrue(sink.accepts(
                PortableStreamPlan.ChangelogMode.RETRACT));
        byte[] encoded = PortableStreamPlanCodec.encode(plan);
        assertEquals(plan, PortableStreamPlanCodec.decode(encoded));
        StreamContractException rejected = assertThrows(
                StreamContractException.class,
                () -> PortableStreamPlanCodec.decodeV1(encoded));
        assertEquals("UNSUPPORTED_ENVELOPE_VERSION", rejected.code());
    }

    @Test
    void retractRequiresStableSourceKeyAndRetractAwareSink() {
        assertThrows(IllegalArgumentException.class,
                () -> StreamSourceSpec.retractExactOrdered(
                        schema(), Collections.<String>emptyList(), "source-v2",
                        sha256("source"), new byte[] {1},
                        Collections.singletonList(split())));

        StreamSession session = RowWeave.createStreamSession("legacy-sink");
        session.setChangelogMode(StreamChangelogMode.RETRACT);
        session.fromSource(StreamSourceSpec.retractExactOrdered(
                schema(), Collections.singletonList("id"), "source-v2",
                sha256("source"), new byte[] {1},
                Collections.singletonList(split())))
                .uid("source")
                .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        "sink-v1", sha256("sink"), new byte[] {2}));

        io.rowweave.stream.api.StreamApiException rejected = assertThrows(
                io.rowweave.stream.api.StreamApiException.class,
                session::toPlanArtifact);
        assertEquals(io.rowweave.stream.api.StreamApiException.Code
                .CHANGELOG_MODE_MISMATCH, rejected.code());
    }

    @Test
    void mapAndFlatMapRequireAndCarryDeclaredRetractResultIdentity() {
        StreamSession valid = RowWeave.createStreamSession("retract-functions");
        valid.setChangelogMode(StreamChangelogMode.RETRACT);
        valid.fromSource(source()).uid("source")
                .map(function(FunctionAbi.MAP_V1, "Map"), schema(), "id")
                .uid("map")
                .flatMap(function(FunctionAbi.FLAT_MAP_V1, "FlatMap"),
                        schema(), StreamEmissionLimit.of(4, 4_096L), "id")
                .uid("flat-map")
                .sinkTo("sink", retractSink());

        PortableStreamPlan plan = PortableStreamJobArtifact.decodeValidated(
                valid.toPlanArtifact()).plan();
        assertEquals(Collections.singletonList("id"),
                plan.nodes().get(1).stableKeyFields());
        assertEquals(Collections.singletonList("id"),
                plan.nodes().get(2).stableKeyFields());
        assertEquals(PortableStreamPlan.ChangelogMode.RETRACT,
                plan.outputChangelogMode("map"));
        assertEquals(PortableStreamPlan.ChangelogMode.RETRACT,
                plan.outputChangelogMode("flat-map"));

        StreamSession missingIdentity = RowWeave.createStreamSession(
                "retract-function-without-key");
        missingIdentity.setChangelogMode(StreamChangelogMode.RETRACT);
        missingIdentity.fromSource(source()).uid("source")
                .map(function(FunctionAbi.MAP_V1, "Map"), schema())
                .uid("map")
                .sinkTo("sink", retractSink());
        io.rowweave.stream.api.StreamApiException rejected = assertThrows(
                io.rowweave.stream.api.StreamApiException.class,
                missingIdentity::toPlanArtifact);
        assertEquals(io.rowweave.stream.api.StreamApiException.Code
                .CHANGELOG_MODE_MISMATCH, rejected.code());
    }

    @Test
    void irreversibleStatefulAbisAreRejectedLocallyInRetractPlans() {
        StreamSession process = RowWeave.createStreamSession(
                "retract-keyed-process-v1");
        process.setChangelogMode(StreamChangelogMode.RETRACT);
        process.fromSource(source()).uid("source")
                .keyBy("id").uid("key")
                .keyedProcess(processSpec()).uid("process")
                .sinkTo("sink", retractSink());
        assertRetractMismatch(process);

        StreamSession async = RowWeave.createStreamSession(
                "retract-async-v1");
        async.setChangelogMode(StreamChangelogMode.RETRACT);
        async.fromSource(source()).uid("source")
                .asyncWait(asyncSpec()).uid("async")
                .sinkTo("sink", retractSink());
        assertRetractMismatch(async);

        StreamSession legacyWindow = RowWeave.createStreamSession(
                "retract-decimal-window-v1");
        legacyWindow.setChangelogMode(StreamChangelogMode.RETRACT);
        legacyWindow.setTimeMode(StreamTimeMode.EVENT_TIME);
        legacyWindow.fromSource(eventTimeSource()).uid("source")
                .keyBy("id").uid("key")
                .eventTimeDecimalWindow(StreamWindowSpec.tumblingDecimal(
                                "event-time", "amount", 60_000L,
                                "window-start", "window-end", "total"),
                        windowSchema(),
                        StreamStateSpec.decimalWindow("window-state"))
                .uid("window")
                .sinkTo("sink", retractSink());
        assertRetractMismatch(legacyWindow);
    }

    private static void assertRetractMismatch(StreamSession session) {
        io.rowweave.stream.api.StreamApiException rejected = assertThrows(
                io.rowweave.stream.api.StreamApiException.class,
                session::toPlanArtifact);
        assertEquals(io.rowweave.stream.api.StreamApiException.Code
                .CHANGELOG_MODE_MISMATCH, rejected.code());
    }

    private static StreamProcessSpec processSpec() {
        StreamManagedStateSpec state = StreamManagedStateSpec.bounded(
                new StreamStateSpec("state",
                        StreamStateSpec.Kind.KEYED_VALUE, "row-v1", 1,
                        StreamStateSpec.TtlMode.PROCESSING_TIME, 60_000L),
                1, 1_000L, 1_024L * 1_024L, 16L * 1_024L);
        return StreamProcessSpec.keyed(
                function(FunctionAbi.KEYED_PROCESS_V1, "Process"), schema(),
                Collections.singletonList("id"),
                Collections.singletonList(state),
                Collections.singletonList(StreamTimerSpec.processingTime(
                        "expiry", 1, 1_000L, 16L * 1_024L)),
                StreamEmissionLimit.of(1, 16L * 1_024L),
                StreamEmissionLimit.of(1, 16L * 1_024L));
    }

    private static StreamAsyncWaitSpec asyncSpec() {
        return StreamAsyncWaitSpec.of(
                function(FunctionAbi.STREAM_ASYNC_V1, "Async"), schema(),
                Collections.singletonList("id"), StreamAsyncOrdering.ORDERED,
                StreamAsyncReplaySpec.identityKeyed(
                        "request-v1", sha256("request-identity")),
                StreamAsyncResourceSpec.of(4, 64L * 1_024L, 1_000L, 2,
                        10L, 100L,
                        StreamEmissionLimit.of(1, 16L * 1_024L)));
    }

    private static StreamSourceSpec source() {
        return StreamSourceSpec.retractExactOrdered(
                schema(), Collections.singletonList("id"), "source-v2",
                sha256("source"), new byte[] {1},
                Collections.singletonList(split()));
    }

    private static StreamSourceSpec eventTimeSource() {
        return StreamSourceSpec.retractExactOrderedEventTime(
                eventTimeSchema(), Collections.singletonList("id"),
                "source-v2", sha256("source"), new byte[] {1},
                Collections.singletonList(split()),
                StreamWatermarkPolicy.providerEvents(30_000L));
    }

    private static StreamSinkSpec retractSink() {
        return StreamSinkSpec.retractEagerAtLeastOnce(
                "sink-v2", sha256("sink"), new byte[] {2});
    }

    private static FunctionDeploymentReference function(
            FunctionAbi abi, String className) {
        byte[] configuration = new byte[] {4};
        return new FunctionDeploymentReference(sha256("deployment"),
                "stream-functions", sha256("jar"),
                "example." + className, abi, "stream-function-config-v1",
                sha256(configuration), configuration,
                new FunctionResourceProfile(1_024L, 0L, 16));
    }

    private static StreamSchema schema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("id", "id", "id",
                        DataType.StringType, false),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, true)));
    }

    private static StreamSchema eventTimeSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("id", "id", "id",
                        DataType.StringType, false),
                new StreamField("event-time", "eventTime", "eventTime",
                        DataType.TimestampType, false),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, true)));
    }

    private static StreamSchema windowSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("id", "id", "id",
                        DataType.StringType, false),
                new StreamField("window-start", "windowStart",
                        "windowStart", DataType.LongType, false),
                new StreamField("window-end", "windowEnd",
                        "windowEnd", DataType.LongType, false),
                new StreamField("total", "total", "total",
                        DataType.BigDecimalType, false)));
    }

    private static StreamSplitSpec split() {
        return new StreamSplitSpec(
                sha256("split"), "position-v1", new byte[] {3});
    }

    private static String sha256(String value) {
        return sha256(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String sha256(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    value);
            StringBuilder result = new StringBuilder(64);
            for (byte current : digest) {
                result.append(Character.forDigit(
                        (current >>> 4) & 0xf, 16));
                result.append(Character.forDigit(current & 0xf, 16));
            }
            return result.toString();
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
