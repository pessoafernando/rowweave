package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamApiException;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamManagedStateSpec;
import io.rowweave.stream.api.StreamProcessSpec;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamTimerSpec;
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamProcessBinding;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** keyed process declares every state, timer and output bound. */
class StreamPublicKeyedProcessCompilerContractTest {

    @Test
    void keyedProcessCompilesCompleteBoundedContract() {
        StreamManagedStateSpec state = StreamManagedStateSpec.bounded(
                new StreamStateSpec("latest", StreamStateSpec.Kind.KEYED_VALUE,
                        "row-v1", 1, StreamStateSpec.TtlMode.PROCESSING_TIME,
                        60_000L), 1, 100_000L, 64L * 1024L * 1024L,
                64L * 1024L);
        StreamTimerSpec timer = StreamTimerSpec.processingTime(
                "expiry", 2, 200_000L, 16L * 1024L * 1024L);
        StreamProcessSpec process = StreamProcessSpec.keyed(
                reference(FunctionAbi.KEYED_PROCESS_V1, "LatestProcess"),
                schema(), Collections.singletonList("id"),
                Collections.singletonList(state),
                Collections.singletonList(timer),
                StreamEmissionLimit.of(2, 128L * 1024L),
                StreamEmissionLimit.of(1, 64L * 1024L));

        StreamSession session = RowWeave.createStreamSession("keyed-process");
        session.fromSource(source()).uid("source")
                .keyBy("id").uid("key")
                .keyedProcess(process).uid("process")
                .sinkTo("sink", sink());

        PlanArtifact artifact = session.toPlanArtifact();
        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(artifact);
        PortableStreamNode node = job.plan().nodes().get(2);
        StreamProcessBinding binding = (StreamProcessBinding) node.binding();
        assertEquals(StreamOperatorCode.KEYED_PROCESS, node.opcode());
        assertEquals(StreamProcessBinding.FunctionKind.KEYED_PROCESS,
                binding.functionKind());
        assertEquals(FunctionAbi.KEYED_PROCESS_V1,
                binding.deploymentReference().functionAbi());
        assertEquals(1, binding.managedStates().size());
        assertEquals(100_000L,
                binding.managedStates().get(0).maximumTotalEntries());
        assertEquals(1, binding.timerFamilies().size());
        assertEquals(2, binding.timerFamilies().get(0).maximumTimersPerKey());
        assertEquals(2, binding.maximumElementOutputs());
        assertEquals(1, binding.maximumTimerOutputs());
        assertEquals(Collections.singletonList("id"), node.stableKeyFields());
        assertTrue(job.plan().requiredCapabilities().contains(
                PortableStreamPlan.KEYED_PROCESS_CAPABILITY));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_KEYED_PROCESS_V1));
    }

    @Test
    void processRequiresPriorKeyAndExactAbi() {
        StreamProcessSpec valid = process(reference(
                FunctionAbi.KEYED_PROCESS_V1, "Process"));

        StreamSession unkeyed = RowWeave.createStreamSession("unkeyed-process");
        unkeyed.fromSource(source()).uid("source")
                .keyedProcess(valid).uid("process").sinkTo("sink", sink());
        assertCode(StreamApiException.Code.MISSING_STABLE_KEY,
                unkeyed::toPlanArtifact);

        StreamSession wrongAbi = RowWeave.createStreamSession("wrong-process-abi");
        wrongAbi.fromSource(source()).uid("source")
                .keyBy("id").uid("key")
                .keyedProcess(process(reference(FunctionAbi.MAP_V1, "Map")))
                .uid("process").sinkTo("sink", sink());
        assertCode(StreamApiException.Code.FUNCTION_ABI_MISMATCH,
                wrongAbi::toPlanArtifact);
    }

    @Test
    void residentCdcTombstonesAreRejectedBeforeAStatefulCallback() {
        StreamSession session = RowWeave.createStreamSession(
                "cdc-stateful-delete");
        session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
        session.setChangelogMode(StreamChangelogMode.UPSERT);
        session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                1_000L, 5_000L, 2));
        session.fromSource(StreamSourceSpec.residentCdc(
                        schema(), Collections.singletonList("id"),
                        "cdc-source-v1", sha256("cdc-source-contract"),
                        new byte[] {1}, sha256("snapshot"), 10L,
                        Collections.singletonList("snapshot-0")))
                .uid("source")
                .keyBy("id").uid("key")
                .keyedProcess(process(reference(
                        FunctionAbi.KEYED_PROCESS_V1, "Process")))
                .uid("process")
                .sinkTo("sink", StreamSinkSpec.checkpointTransactional(
                        "sink-v1", sha256("sink-contract"),
                        new byte[] {3}));

        assertCode(StreamApiException.Code.CHANGELOG_MODE_MISMATCH,
                session::toPlanArtifact);
    }

    @Test
    void unboundedStateTimerAndOutputDeclarationsAreRejected() {
        StreamStateSpec noTtl = new StreamStateSpec(
                "state", StreamStateSpec.Kind.KEYED_MAP, "map-v1", 1,
                StreamStateSpec.TtlMode.NONE, 0L);
        assertThrows(IllegalArgumentException.class, () ->
                StreamManagedStateSpec.bounded(noTtl, 10, 100L,
                        1024L, 128L));
        assertThrows(IllegalArgumentException.class, () ->
                StreamManagedStateSpec.bounded(state().state(), 0, 100L,
                        1024L, 128L));
        assertThrows(IllegalArgumentException.class, () ->
                StreamTimerSpec.processingTime("timer", 0, 10L, 1024L));

        assertThrows(IllegalArgumentException.class, () ->
                StreamProcessSpec.keyed(
                        reference(FunctionAbi.KEYED_PROCESS_V1, "Process"),
                        schema(), Collections.singletonList("id"),
                        Arrays.asList(state(), state()),
                        Collections.singletonList(timer()),
                        StreamEmissionLimit.of(1, 1024L),
                        StreamEmissionLimit.of(1, 1024L)));
        assertThrows(IllegalArgumentException.class, () ->
                StreamProcessSpec.keyed(
                        reference(FunctionAbi.KEYED_PROCESS_V1, "Process"),
                        schema(), Collections.singletonList("id"),
                        Collections.singletonList(state()),
                        Arrays.asList(timer(), timer()),
                        StreamEmissionLimit.of(1, 1024L),
                StreamEmissionLimit.of(1, 1024L)));

        StreamProcessSpec eventTimer = StreamProcessSpec.keyed(
                reference(FunctionAbi.KEYED_PROCESS_V1, "EventTimer"),
                schema(), Collections.singletonList("id"),
                Collections.singletonList(state()),
                Collections.singletonList(StreamTimerSpec.eventTime(
                        "event-expiry", 1, 100L, 1024L)),
                StreamEmissionLimit.of(1, 1024L),
                StreamEmissionLimit.of(1, 1024L));
        StreamSession processing = RowWeave.createStreamSession(
                "event-timer-in-processing-time");
        processing.fromSource(source()).uid("source")
                .keyBy("id").uid("key")
                .keyedProcess(eventTimer).uid("process")
                .sinkTo("sink", sink());
        assertCode(StreamApiException.Code.TIME_MODE_MISMATCH,
                processing::toPlanArtifact);
    }

    private static StreamProcessSpec process(
            FunctionDeploymentReference function) {
        return StreamProcessSpec.keyed(function, schema(),
                Collections.singletonList("id"),
                Collections.singletonList(state()),
                Collections.singletonList(timer()),
                StreamEmissionLimit.of(1, 1024L),
                StreamEmissionLimit.of(1, 1024L));
    }

    private static StreamManagedStateSpec state() {
        return StreamManagedStateSpec.bounded(new StreamStateSpec(
                "state", StreamStateSpec.Kind.KEYED_MAP, "map-v1", 1,
                StreamStateSpec.TtlMode.PROCESSING_TIME, 60_000L),
                10, 100L, 1024L, 128L);
    }

    private static StreamTimerSpec timer() {
        return StreamTimerSpec.processingTime("timer", 2, 100L, 1024L);
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
                new StreamField("value", "value", "value",
                        DataType.StringType, true)));
    }

    private static StreamSourceSpec source() {
        return StreamSourceSpec.exactOrdered(schema(), "source-v1",
                sha256("source-contract"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        sha256("split"), "sequence-v1", new byte[] {2})));
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", sha256("sink-contract"), new byte[] {3});
    }

    private static FunctionDeploymentReference reference(
            FunctionAbi abi, String name) {
        byte[] config = new byte[] {4};
        return new FunctionDeploymentReference(sha256("deployment-set"),
                "stream-functions", sha256("jar"), "example." + name, abi,
                "stream-function-config-v1", sha256(config), config,
                new FunctionResourceProfile(1_024L, 0L, 8));
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
