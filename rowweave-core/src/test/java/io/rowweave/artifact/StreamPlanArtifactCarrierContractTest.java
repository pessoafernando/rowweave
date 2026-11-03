package io.rowweave.artifact;

import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.PortableStreamPlanArtifact;
import io.rowweave.stream.contract.StreamContractException;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamSinkBinding;
import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamSourceSplit;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamRestartPolicy;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Stream payload is semantically readable but no Batch runtime executes it. */
class StreamPlanArtifactCarrierContractTest {

    @Test
    void streamCarrierHasStableFeatureAndSurvivesTheFrozenArtifactEnvelope() {
        PlanArtifact artifact = validStreamArtifact();

        PlanArtifact decoded = PlanArtifactCodec.decode(PlanArtifactCodec.encode(artifact));

        assertEquals(PlanArtifact.PayloadFormat.PORTABLE_STREAM_PLAN_V1,
                decoded.payloadFormat());
        assertTrue(decoded.requiredFeatures().contains(
                PlanArtifact.FEATURE_PORTABLE_STREAM_PLAN_V1));
        assertTrue(decoded.requiredFeatures().contains(
                PlanArtifact.FEATURE_RUNTIME_STREAM_V1));
        assertArrayEquals(artifact.payload(), decoded.payload());
        assertEquals(artifact.artifactId(), decoded.artifactId());
    }

    @Test
    void innerPlanChecksumIsValidatedByEveryReadablePath() {
        PlanArtifact valid = validStreamArtifact();
        byte[] corruptPayload = valid.payload();
        corruptPayload[corruptPayload.length - 1] ^= 1;
        PlanArtifact corrupt = new PlanArtifact(
                valid.definitionId(), valid.planSchemaVersion(),
                valid.minimumReaderVersion(), valid.runtimeTarget(),
                valid.boundedness(), valid.changelogMode(), valid.timeMode(),
                valid.requiredFeatures(), valid.optionalFeatures(), valid.operators(),
                valid.payloadFormat(), corruptPayload);

        assertStreamFailure("CHECKSUM_MISMATCH",
                () -> PlanArtifactValidator.validateReadable(corrupt));
        byte[] reSignedOuter = PlanArtifactCodec.encode(corrupt);
        assertStreamFailure("CHECKSUM_MISMATCH",
                () -> PlanArtifactCodec.decode(reSignedOuter));
    }

    @Test
    void payloadFeatureAndOuterStreamSemanticsAreClosedBeforeRuntimeUse() {
        assertFailure("PAYLOAD_FEATURE_MISMATCH", streamArtifact(
                Collections.singletonList(PlanArtifact.FEATURE_PORTABLE_BATCH_PLAN_V1),
                PlanArtifact.RuntimeTarget.STREAM,
                PlanArtifact.Boundedness.UNBOUNDED,
                PlanArtifact.ChangelogMode.APPEND,
                PlanArtifact.TimeMode.EVENT_TIME,
                new byte[]{1}));

        assertFailure("STREAM_SEMANTIC_MISMATCH", streamArtifact(
                Collections.singletonList(PlanArtifact.FEATURE_PORTABLE_STREAM_PLAN_V1),
                PlanArtifact.RuntimeTarget.BATCH,
                PlanArtifact.Boundedness.UNBOUNDED,
                PlanArtifact.ChangelogMode.APPEND,
                PlanArtifact.TimeMode.EVENT_TIME,
                new byte[]{1}));
        assertFailure("STREAM_SEMANTIC_MISMATCH", streamArtifact(
                Collections.singletonList(PlanArtifact.FEATURE_PORTABLE_STREAM_PLAN_V1),
                PlanArtifact.RuntimeTarget.STREAM,
                PlanArtifact.Boundedness.BOUNDED,
                PlanArtifact.ChangelogMode.APPEND,
                PlanArtifact.TimeMode.EVENT_TIME,
                new byte[]{1}));
        assertFailure("MISSING_STREAM_CAPABILITY", streamArtifact(
                Collections.singletonList(PlanArtifact.FEATURE_PORTABLE_STREAM_PLAN_V1),
                PlanArtifact.RuntimeTarget.STREAM,
                PlanArtifact.Boundedness.UNBOUNDED,
                PlanArtifact.ChangelogMode.RETRACT,
                PlanArtifact.TimeMode.EVENT_TIME,
                new byte[]{1}));
        assertFailure("STREAM_SEMANTIC_MISMATCH", streamArtifact(
                Collections.singletonList(PlanArtifact.FEATURE_PORTABLE_STREAM_PLAN_V1),
                PlanArtifact.RuntimeTarget.STREAM,
                PlanArtifact.Boundedness.UNBOUNDED,
                PlanArtifact.ChangelogMode.APPEND,
                PlanArtifact.TimeMode.NONE,
                new byte[]{1}));
    }

    @Test
    void streamRuntimeCapabilityIsRequiredIndependentlyOfPayloadFormat() {
        PlanArtifact valid = validStreamArtifact();
        java.util.List<String> withoutRuntime =
                new java.util.ArrayList<>(valid.requiredFeatures());
        withoutRuntime.remove(PlanArtifact.FEATURE_RUNTIME_STREAM_V1);
        PlanArtifact missing = new PlanArtifact(
                valid.definitionId(), valid.planSchemaVersion(),
                valid.minimumReaderVersion(), valid.runtimeTarget(),
                valid.boundedness(), valid.changelogMode(), valid.timeMode(),
                withoutRuntime, valid.optionalFeatures(), valid.operators(),
                valid.payloadFormat(), valid.payload());

        assertFailure("MISSING_STREAM_CAPABILITY", missing);
    }

    @Test
    void everyExistingExecutionAdmissionKeepsStreamLocked() {
        PlanArtifact artifact = validStreamArtifact();

        assertRuntimeLocked(() -> PlanArtifactValidator.validateForCurrentRuntime(artifact));
        assertRuntimeLocked(() -> PlanArtifactValidator.validateForPublicV1Execution(artifact));
    }

    @Test
    void productAdmissionAcceptsOnlyProductEnvelopeAndProductClosedPlan() {
        PlanArtifact legacyArtifact = validStreamArtifact();
        PlanArtifactValidationException legacy = assertThrows(
                PlanArtifactValidationException.class,
                () -> PlanArtifactValidator.validateForStreamProductExecution(
                        legacyArtifact));
        assertEquals("STREAM_PRODUCT_JOB_REQUIRED", legacy.code());

        PortableStreamPlan legacyEventTime =
                PortableStreamPlanArtifact.decodeValidated(legacyArtifact);
        StreamContractException missingPolicy = assertThrows(
                StreamContractException.class,
                () -> productJob(legacyEventTime));
        assertEquals("MISSING_SOURCE_TIME_POLICY", missingPolicy.code());

        PortableStreamPlan processingTime = new PortableStreamPlan(
                legacyEventTime.planVersion(),
                legacyEventTime.deliveryGuarantee(),
                legacyEventTime.parallelism(),
                legacyEventTime.maxParallelism(),
                legacyEventTime.changelogMode(),
                PortableStreamPlan.TimeMode.PROCESSING_TIME,
                legacyEventTime.rootSinkOperatorUids(),
                legacyEventTime.nodes(), legacyEventTime.edges(),
                legacyEventTime.requiredCapabilities());
        PlanArtifact product = PortableStreamJobArtifact.wrap(
                "stream-product", productJob(processingTime));
        PlanArtifactValidator.validateForStreamProductExecution(product);
    }

    private static PortableStreamJob productJob(PortableStreamPlan plan) {
        return new PortableStreamJob(plan,
                StreamCheckpointConfig.periodic(1_000L, 30_000L, 2),
                StreamRestartPolicy.fixedDelay(3, 1_000L),
                StreamResourceProfile.defaults());
    }

    @Test
    void genericClientsRejectBeforeGraphDispatchOrMasterLookup() {
        PlanArtifact artifact = validStreamArtifact();
        java.util.concurrent.atomic.AtomicInteger graphDispatches =
                new java.util.concurrent.atomic.AtomicInteger();
        io.rowweave.cluster.JobClient legacyClient =
                new io.rowweave.cluster.JobClient() {
                    @Override
                    public long submitToSink(io.rowweave.plan.PlanGraph graph) {
                        graphDispatches.incrementAndGet();
                        return 0L;
                    }
                };
        assertRuntimeLocked(() -> legacyClient.submit(artifact));
        assertEquals(0, graphDispatches.get());

        assertRuntimeLocked(() ->
                new io.rowweave.local.LocalJobClient().submit(artifact));

        java.util.concurrent.atomic.AtomicInteger masterLookups =
                new java.util.concurrent.atomic.AtomicInteger();
        try (io.rowweave.cluster.ClusterJobClient client =
                     new io.rowweave.cluster.ClusterJobClient(() -> {
                         masterLookups.incrementAndGet();
                         throw new AssertionError("Stream reached Master lookup");
                     })) {
            assertRuntimeLocked(() -> client.submit(artifact));
        }
        assertEquals(0, masterLookups.get());
    }

    private static PlanArtifact streamArtifact(
            java.util.List<String> payloadFeatures,
            PlanArtifact.RuntimeTarget runtimeTarget,
            PlanArtifact.Boundedness boundedness,
            PlanArtifact.ChangelogMode changelogMode,
            PlanArtifact.TimeMode timeMode,
            byte[] payload) {
        java.util.List<String> required = new java.util.ArrayList<>(payloadFeatures);
        required.add(PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS);
        return new PlanArtifact(
                "stream-ledger", PlanArtifact.CURRENT_PLAN_SCHEMA_VERSION,
                PlanArtifact.CURRENT_READER_VERSION, runtimeTarget, boundedness,
                changelogMode, timeMode, required, Collections.emptyList(),
                Arrays.asList(
                        new OperatorDescriptor("source", OperatorDescriptor.Kind.SOURCE,
                                "rowweave.stream.opcode.1", fingerprint(), Collections.emptyList()),
                        new OperatorDescriptor("sink", OperatorDescriptor.Kind.SINK,
                                "rowweave.stream.opcode.8", fingerprint(),
                                Collections.singletonList("source"))),
                PlanArtifact.PayloadFormat.PORTABLE_STREAM_PLAN_V1, payload);
    }

    private static PlanArtifact validStreamArtifact() {
        PortableRowSchema schema = new PortableRowSchema(Arrays.asList(
                new PortableField("id", "id", "id", PortableDataType.LONG, false),
                new PortableField("amount", "amount", "amount",
                        PortableDataType.BIG_DECIMAL, true)));
        PortableStreamNode source = new PortableStreamNode(
                "source", StreamOperatorCode.SOURCE,
                Collections.emptyList(), Collections.emptyList(), schema,
                Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), new StreamSourceBinding(
                        "log-v1", repeat('a'), sha256Hex(new byte[]{1}), new byte[]{1},
                        Collections.singletonList(new StreamSourceSplit(
                                repeat('c'), "stream-position-v1", new byte[]{2})),
                        StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED));
        PortableStreamNode sink = new PortableStreamNode(
                "sink", StreamOperatorCode.SINK,
                Collections.singletonList("source"),
                Collections.singletonList(schema.fingerprint()), schema,
                Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), new StreamSinkBinding(
                        "manifest-stream-v1", repeat('b'),
                        sha256Hex(new byte[]{3}), new byte[]{3},
                        StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL));
        PortableStreamPlan plan = new PortableStreamPlan(
                PortableStreamPlan.CURRENT_VERSION,
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE,
                2, 128, PortableStreamPlan.ChangelogMode.APPEND,
                PortableStreamPlan.TimeMode.EVENT_TIME,
                Collections.singletonList("sink"), Arrays.asList(source, sink),
                Collections.singletonList(new StreamEdge(
                        "source", "sink", 0, "source-sink-0",
                        StreamEdge.Partitioning.FORWARD, 1024, 4096)),
                Collections.singletonList(PortableStreamPlan.STREAM_CAPABILITY));
        return PortableStreamPlanArtifact.wrap("stream-ledger", plan);
    }

    private static void assertFailure(String code, PlanArtifact artifact) {
        PlanArtifactValidationException failure = assertThrows(
                PlanArtifactValidationException.class,
                () -> PlanArtifactValidator.validateReadable(artifact));
        assertEquals(code, failure.code(), failure.getMessage());
    }

    private static void assertRuntimeLocked(org.junit.jupiter.api.function.Executable action) {
        PlanArtifactValidationException failure = assertThrows(
                PlanArtifactValidationException.class, action);
        assertEquals("UNSUPPORTED_RUNTIME", failure.code(), failure.getMessage());
        assertEquals("plan.runtimeTarget", failure.path());
    }

    private static void assertStreamFailure(
            String code, org.junit.jupiter.api.function.Executable action) {
        StreamContractException failure = assertThrows(StreamContractException.class, action);
        assertEquals(code, failure.code(), failure.getMessage());
    }

    private static String repeat(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static String sha256Hex(byte[] value) {
        try {
            byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(value);
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte current : digest) {
                result.append(String.format("%02x", current & 0xff));
            }
            return result.toString();
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String fingerprint() {
        return "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    }
}
