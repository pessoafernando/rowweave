package io.rowweave.incremental;

import io.rowweave.artifact.OperatorDescriptor;
import io.rowweave.artifact.PlanArtifact;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** client submission is canonical intent, never authority tokens. */
class IncrementalRunSubmissionCodecTest {

    @Test
    void normalSubmissionRoundTripsEveryStaticAndTriggerBinding()
            throws Exception {
        Fixture fixture = fixture();
        IncrementalRunSubmission submission = IncrementalRunSubmission.normal(
                "ledger-feed", "trigger-17", IncrementalRunMode.INCREMENTAL_V1,
                "attempt-1", "publication-1", 3, 2_000L, 1_000L,
                fixture.definition, fixture.template, fixture.contract,
                fixture.configuration);
        AtomicLong charged = new AtomicLong();

        byte[] frame = IncrementalRunSubmissionCodec.encode(submission);
        IncrementalRunSubmission decoded =
                IncrementalRunSubmissionCodec.decode(frame, charged::addAndGet);

        assertEquals(submission, decoded);
        assertArrayEquals(frame, IncrementalRunSubmissionCodec.encode(decoded));
        assertArrayEquals(digest(frame),
                IncrementalRunSubmissionCodec.digest(submission));
        assertTrue(charged.get() > frame.length,
                "nested component copies and decoded objects must be reserved");
    }

    @Test
    void backfillRetainsExactOrdinalAndBounds() throws Exception {
        Fixture fixture = fixture();
        IncrementalFrontier from = IncrementalFrontier.int64Sequence(10L);
        IncrementalFrontier target = IncrementalFrontier.int64Sequence(20L);
        IncrementalRunSubmission submission = IncrementalRunSubmission.backfill(
                "ledger-feed", 7L, from, target, "backfill-q3",
                "attempt-backfill", "publication-backfill", 2,
                0L, 1_000L, fixture.definition, fixture.template,
                fixture.contract, fixture.configuration);

        IncrementalRunSubmission decoded = IncrementalRunSubmissionCodec.decode(
                IncrementalRunSubmissionCodec.encode(submission));

        assertEquals(IncrementalRunMode.BACKFILL_V1, decoded.mode());
        assertEquals(7L, decoded.expectedBackfillOrdinal());
        assertEquals(from, decoded.backfillFrom());
        assertEquals(target, decoded.backfillTarget());
        assertEquals("backfill-q3", decoded.requestIdentity());
    }

    @Test
    void malformedOuterFrameAndStaticBindingDriftFailClosed()
            throws Exception {
        Fixture fixture = fixture();
        IncrementalRunSubmission submission = IncrementalRunSubmission.normal(
                "ledger-feed", "trigger", IncrementalRunMode.SNAPSHOT_V1,
                "attempt", "publication", 1, 0L, 10L,
                fixture.definition, fixture.template, fixture.contract,
                fixture.configuration);
        byte[] frame = IncrementalRunSubmissionCodec.encode(submission);
        byte[] checksum = frame.clone();
        checksum[checksum.length - 1] ^= 1;
        byte[] trailing = Arrays.copyOf(frame, frame.length + 1);
        byte[] impossibleLength = frame.clone();
        ByteBuffer.wrap(impossibleLength).putInt(8, Integer.MAX_VALUE);

        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRunSubmissionCodec.decode(checksum));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRunSubmissionCodec.decode(trailing));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRunSubmissionCodec.decode(impossibleLength));
        IncrementalSourceConfig drift = new IncrementalSourceConfig(
                fixture.contract.providerKey(), digest("other-contract"),
                bytes("config"));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalRunSubmission.normal(
                        "ledger-feed", "trigger", IncrementalRunMode.SNAPSHOT_V1,
                        "attempt", "publication", 1, 0L, 10L,
                        fixture.definition, fixture.template, fixture.contract,
                        drift));
    }

    private static Fixture fixture() throws Exception {
        byte[] frontierContract = digest("frontier-contract");
        byte[] schema = digest("schema");
        IncrementalPlanDescriptor descriptor = new IncrementalPlanDescriptor(
                "source", frontierContract, Collections.singletonList(0), 1,
                Collections.singletonList(2), schema, false,
                digest("partitions"),
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
        IncrementalDefinition definition = new IncrementalDefinition(
                "ledger-definition", descriptor,
                Arrays.asList(
                        new IncrementalDefinition.OperatorBinding(
                                "source", IncrementalDefinition.OperatorKind.SOURCE_V1,
                                "source", schema, Collections.emptyList(),
                                digest("source-impl")),
                        new IncrementalDefinition.OperatorBinding(
                                "sink", IncrementalDefinition.OperatorKind.SINK_V1,
                                "sink", schema, Collections.singletonList("source"),
                                digest("sink-impl"))),
                Collections.singletonList(digest("sink-binding")));
        PlanArtifact template = new PlanArtifact(
                definition.definitionId(), 1, 1,
                PlanArtifact.RuntimeTarget.BATCH,
                PlanArtifact.Boundedness.BOUNDED,
                PlanArtifact.ChangelogMode.UPSERT,
                PlanArtifact.TimeMode.NONE,
                Arrays.asList(
                        PlanArtifact.FEATURE_TRUSTED_LEGACY_PLAN_GRAPH,
                        PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS,
                        PlanArtifact.FEATURE_BOUNDED_INCREMENTAL_FRONTIER),
                Collections.emptyList(),
                Arrays.asList(
                        new OperatorDescriptor(
                                "source", OperatorDescriptor.Kind.SOURCE,
                                "source", hex(schema), Collections.emptyList()),
                        new OperatorDescriptor(
                                "sink", OperatorDescriptor.Kind.SINK,
                                "sink", hex(schema),
                                Collections.singletonList("source"))),
                PlanArtifact.PayloadFormat.TRUSTED_LEGACY_PLAN_GRAPH,
                bytes("inert-template"));
        IncrementalSourceProviderContract contract =
                new IncrementalSourceProviderContract(
                        1, new IncrementalSourceProviderKey("embedded.cdc", 1),
                        new IncrementalSourceCodecDescriptor("config", 1, 1024),
                        new IncrementalSourceCodecDescriptor(
                                IncrementalSourceActionExecutor.CAPTURE_CODEC_ID,
                                1, IncrementalCaptureResponseCodec.MAX_FRAME_BYTES),
                        new IncrementalSourceCodecDescriptor(
                                IncrementalSourceActionExecutor.MATERIALIZATION_CODEC_ID,
                                1, IncrementalMaterializationResponseCodec.MAX_FRAME_BYTES),
                        IncrementalFrontierCodec.MAX_FRAME_BYTES,
                        IncrementalFrontier.Kind.INT64_SEQUENCE_V1,
                        IncrementalSourceProviderContract.OrderingGuarantee
                                .TOTAL_ORDER_NO_LATE_LEFT_V1,
                        IncrementalSourceProviderContract.SnapshotGuarantee
                                .EXACT_REOPENABLE_V1,
                        false, new IncrementalSourceActionBudget(
                                1024L * 1024L, 24L * 1024L * 1024L,
                                1, 60_000L),
                        IncrementalSourceProviderContract.DeploymentCapability
                                .SIGNED_TEST_CONFORMANCE_V1);
        IncrementalSourceConfig configuration = new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract),
                bytes("config"));
        return new Fixture(definition, template, contract, configuration);
    }

    private static byte[] digest(String value) throws Exception {
        return digest(bytes(value));
    }

    private static byte[] digest(byte[] value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(value);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String hex(byte[] value) {
        StringBuilder encoded = new StringBuilder(value.length * 2);
        for (byte next : value) {
            encoded.append(String.format("%02x", next & 0xff));
        }
        return encoded.toString();
    }

    private static final class Fixture {
        final IncrementalDefinition definition;
        final PlanArtifact template;
        final IncrementalSourceProviderContract contract;
        final IncrementalSourceConfig configuration;

        Fixture(IncrementalDefinition definition, PlanArtifact template,
                IncrementalSourceProviderContract contract,
                IncrementalSourceConfig configuration) {
            this.definition = definition;
            this.template = template;
            this.contract = contract;
            this.configuration = configuration;
        }
    }
}
