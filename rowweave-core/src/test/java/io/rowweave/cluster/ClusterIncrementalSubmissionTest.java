package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.ManifestFileSink;
import io.rowweave.DataSchema;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcServer;
import io.rowweave.deployment.SignedIncrementalSourceProviderFixture;
import io.rowweave.incremental.IncrementalCaptureResponseCodec;
import io.rowweave.incremental.IncrementalDefinition;
import io.rowweave.incremental.IncrementalFrontier;
import io.rowweave.incremental.IncrementalFrontierCodec;
import io.rowweave.incremental.IncrementalFrontierView;
import io.rowweave.incremental.IncrementalFrontierViewCodec;
import io.rowweave.incremental.IncrementalInitializeRequest;
import io.rowweave.incremental.IncrementalInitializeRequestCodec;
import io.rowweave.incremental.IncrementalMaterializationResponseCodec;
import io.rowweave.incremental.IncrementalPlanDescriptor;
import io.rowweave.incremental.IncrementalPlanTemplateFingerprint;
import io.rowweave.incremental.IncrementalRunMode;
import io.rowweave.incremental.IncrementalRunSubmission;
import io.rowweave.incremental.IncrementalRunSubmissionCodec;
import io.rowweave.incremental.IncrementalSourceActionBudget;
import io.rowweave.incremental.IncrementalSourceActionExecutor;
import io.rowweave.incremental.IncrementalSourceCodecDescriptor;
import io.rowweave.incremental.IncrementalSourceConfig;
import io.rowweave.incremental.IncrementalSourceProviderContract;
import io.rowweave.incremental.IncrementalSourceProviderContractCodec;
import io.rowweave.incremental.IncrementalSourceProviderKey;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Real client submits canonical intent on its dedicated route. */
class ClusterIncrementalSubmissionTest {

    @Test
    void clientRejectsSourceContractAbsentFromSignedDeployment(@TempDir Path root)
            throws Exception {
        AtomicReference<RpcMessage> observed = new AtomicReference<>();
        try (RpcServer server = new RpcServer(request -> {
            observed.set(request);
            return RpcMessage.jobResult(true, 1L, null);
        }); ClusterJobClient client =
                     new ClusterJobClient("127.0.0.1", server.port())) {
            assertThrows(io.rowweave.deployment.DeploymentValidationException.class,
                    () -> client.submitIncrementalRun(submission(root)));
            assertNull(observed.get());
        }
    }

    @Test
    void realClientPreflightsAndSendsIncrementalEnvelope(@TempDir Path root)
            throws Exception {
        AtomicReference<RpcMessage> observed = new AtomicReference<>();
        try (SignedIncrementalSourceProviderFixture.Session deployments =
                     SignedIncrementalSourceProviderFixture.open(root);
             RpcServer server = new RpcServer(request -> {
            observed.set(request);
            return RpcMessage.jobResult(true, 17L, null);
        }); ClusterJobClient client = new ClusterJobClient(
                     new FixedMasterLocator(
                             new Endpoint("127.0.0.1", server.port())),
                     deployments.staticRuntime())) {
            IncrementalRunSubmission submission = submission(
                    root, SignedIncrementalSourceProviderFixture.contract());

            assertEquals(17L,
                    client.submitIncrementalRun(submission).rowsOut());
            assertEquals(RpcMessage.Type.SUBMIT_INCREMENTAL_RUN,
                    observed.get().type());
            assertEquals(submission, IncrementalRunSubmissionCodec.decode(
                    observed.get().incrementalSubmissionBytes()));
        }
    }

    @Test
    void realClientDerivesAndSendsCanonicalFeedInitialization(@TempDir Path root)
            throws Exception {
        AtomicReference<RpcMessage> observed = new AtomicReference<>();
        IncrementalFrontierView expected = new IncrementalFrontierView(
                "feed", 7L, 0L,
                IncrementalFrontier.Kind.INT64_SEQUENCE_V1,
                "0011223344556677", "8899aabbccddeeff", null, "READY");
        try (SignedIncrementalSourceProviderFixture.Session deployments =
                     SignedIncrementalSourceProviderFixture.open(root);
             RpcServer server = new RpcServer(request -> {
                 observed.set(request);
                 return RpcMessage.incrementalFrontierView(
                         request.runId(),
                         IncrementalFrontierViewCodec.encode(expected));
             }); ClusterJobClient client = new ClusterJobClient(
                     new FixedMasterLocator(
                             new Endpoint("127.0.0.1", server.port())),
                     deployments.staticRuntime())) {
            IncrementalRunSubmission submission = submission(
                    root, SignedIncrementalSourceProviderFixture.contract());

            assertEquals(expected, client.initializeIncrementalFeed(
                    "initialize-feed", 7L, submission));

            RpcMessage request = observed.get();
            assertEquals(RpcMessage.Type.INITIALIZE_INCREMENTAL_FEED,
                    request.type());
            assertEquals("feed", request.runId());
            assertEquals("initialize-feed", request.attemptId());
            IncrementalInitializeRequest initialize =
                    IncrementalInitializeRequestCodec.decode(
                            request.incrementalInitializeBytes());
            assertEquals(7L, initialize.feedIncarnation());
            assertArrayEquals(submission.definition().fingerprint(),
                    initialize.definitionDigest());
            assertArrayEquals(IncrementalSourceProviderContractCodec.digest(
                    submission.sourceContract()),
                    initialize.sourceContractDigest());
            assertArrayEquals(submission.definition().planDescriptor()
                            .frontierContractDigest(),
                    initialize.frontierContractDigest());
            assertEquals(submission.sourceContract().frontierKind(),
                    initialize.frontierKind());
        }
    }

    private static IncrementalRunSubmission submission(Path root)
            throws Exception {
        return submission(root, contract());
    }

    private static IncrementalRunSubmission submission(
            Path root, IncrementalSourceProviderContract contract)
            throws Exception {
        DataSchema meta = new DataSchema(
                new Field("id", DataType.LongType),
                new Field("op", DataType.StringType),
                new Field("sequence", DataType.LongType));
        byte[] schema = hexBytes(io.rowweave.commit.SchemaFingerprint.of(meta));
        IncrementalPlanDescriptor descriptor = new IncrementalPlanDescriptor(
                "source", digest("frontier-contract"),
                Collections.singletonList(0), 1,
                Collections.singletonList(2), schema, false,
                digest("partitions"),
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
        PlanNode source = PlanNode.placeholderSource(meta)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "source")
                .withIncrementalPlanDescriptor(descriptor);
        ManifestFileSink output = new ManifestFileSink(
                meta, root.resolve("manifest").toString(), "changes");
        PlanNode sink = PlanNode.sink(meta, 0, output.toProviderSink())
                .param(PlanNode.OPERATOR_UID_PARAM, "sink").build();
        PlanGraph template = new PlanGraph(Arrays.asList(source, sink), 1);
        ObjectTrustContext trust = ObjectTrustContext.localTrusted();
        IncrementalDefinition definition =
                IncrementalPlanTemplateFingerprint.derive(
                        "definition", template, trust, ignored -> { });
        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrapDurable(
                definition.definitionId(), template, trust, ignored -> { });
        IncrementalSourceConfig configuration = new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract),
                bytes("configuration"));
        return IncrementalRunSubmission.normal(
                "feed", "trigger", IncrementalRunMode.SNAPSHOT_V1,
                "attempt-1", "publication-1", 3, 0L, 10L,
                definition, artifact, contract, configuration);
    }

    private static IncrementalSourceProviderContract contract() {
        return new IncrementalSourceProviderContract(
                1, new IncrementalSourceProviderKey("embedded.cdc", 1),
                new IncrementalSourceCodecDescriptor("config", 1, 1024),
                new IncrementalSourceCodecDescriptor(
                        IncrementalSourceActionExecutor.CAPTURE_CODEC_ID, 1,
                        IncrementalCaptureResponseCodec.MAX_FRAME_BYTES),
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
    }

    private static byte[] digest(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(bytes(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] hexBytes(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int index = 0; index < bytes.length; index++) {
            bytes[index] = (byte) Integer.parseInt(
                    value.substring(index * 2, index * 2 + 2), 16);
        }
        return bytes;
    }
}
