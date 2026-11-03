package io.rowweave.deployment;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.StreamSession;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamMasterRuntime;
import io.rowweave.StreamWorkerRuntime;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.internal.ProviderActionSupervisor;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamConnectorConfiguration;
import io.rowweave.stream.api.StreamControlException;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceDiscoverySpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.api.StreamWorkerEndpoint;
import io.rowweave.stream.api.StreamWorkerConfig;
import io.rowweave.stream.spi.StreamSecretResolver;
import io.rowweave.stream.cluster.RemoteStreamWorkerEndpoint;
import io.rowweave.stream.cluster.StreamRemoteWorkerServer;
import io.rowweave.stream.cluster.StreamWorkerCapabilityProfile;
import io.rowweave.stream.cluster.StreamProductJobMasterServer;
import io.rowweave.stream.cluster.StreamProductRun;
import io.rowweave.stream.contract.StreamConnectorProviderContract;
import io.rowweave.stream.contract.StreamConnectorProviderContractCodec;
import io.rowweave.stream.runtime.ResidentCdcStreamSourceProvider;
import io.rowweave.stream.runtime.SignedStreamConnectorAdapters;
import io.rowweave.stream.spi.StreamCdcCheckpointCompletion;
import io.rowweave.stream.spi.StreamCdcSourceContext;
import io.rowweave.stream.spi.StreamCdcSourceReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.Signature;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamConnectorProviderRegistryTest {
    private static final String SOURCE_CLASS = "example.SignedStreamSource";
    private static final String SINK_CLASS = "example.SignedStreamSink";
    private static final String SOURCE_RESOURCE =
            "META-INF/rowweave/stream-connector/contracts/source.bin";
    private static final String SINK_RESOURCE =
            "META-INF/rowweave/stream-connector/contracts/sink.bin";
    private static final String INITIALIZED = "rowweave.test.stream-connector.initialized";
    private static final String RESULT = "rowweave.test.stream-connector.result";
    private static final String OPEN_COUNT = "rowweave.test.stream-connector.opens";
    private static final String PENDING_WRITE =
            "rowweave.test.stream-connector.pending-write";
    private static final String SINK_OPEN_COUNT =
            "rowweave.test.stream-connector.sink-opens";
    private static final String SOURCE_INITIALIZED =
            "rowweave.test.stream-connector.source-initialized";
    private static final String SINK_INITIALIZED =
            "rowweave.test.stream-connector.sink-initialized";
    private static final String SECRET_SEEN =
            "rowweave.test.stream-connector.secret-seen";
    private static final String CDC_COMPLETION =
            "rowweave.test.stream-connector.cdc-completion";

    @AfterEach void clear() {
        System.clearProperty(INITIALIZED);
        System.clearProperty(RESULT);
        System.clearProperty(OPEN_COUNT);
        System.clearProperty(PENDING_WRITE);
        System.clearProperty(SINK_OPEN_COUNT);
        System.clearProperty(SOURCE_INITIALIZED);
        System.clearProperty(SINK_INITIALIZED);
        System.clearProperty(SECRET_SEEN);
        System.clearProperty(CDC_COMPLETION);
    }

    @Test
    void staticScanUsesOnlySignedBytesAndWorkerLoadsOwnerLater(@TempDir Path temp)
            throws Exception {
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, source(), sink(), "signed-stream"))) {
            StreamConnectorProviderRegistry staticRegistry =
                    StreamConnectorProviderRegistry.scan(deployments,
                            StreamConnectorProviderRegistry.Role.STATIC_ONLY);
            assertEquals(source(), staticRegistry.requireContract(
                    StreamConnectorProviderContract.Kind.SOURCE,
                    "example.connector"));
            assertEquals(sink(), staticRegistry.requireContract(
                    StreamConnectorProviderContract.Kind.SINK,
                    "example.connector"));
            assertNull(System.getProperty(INITIALIZED));
            assertThrows(IllegalStateException.class, () ->
                    staticRegistry.loadSource("example.connector",
                            ProviderActionSupervisor.processWide(2),
                            System.nanoTime() + 5_000_000_000L));
            assertNull(System.getProperty(INITIALIZED));

            StreamConnectorProviderRegistry workerRegistry =
                    StreamConnectorProviderRegistry.scan(deployments,
                            StreamConnectorProviderRegistry.Role.WORKER);
            assertNotNull(workerRegistry.loadSource("example.connector",
                    ProviderActionSupervisor.processWide(2),
                    System.nanoTime() + 5_000_000_000L));
            assertEquals("source", System.getProperty(INITIALIZED));
            assertNotNull(workerRegistry.loadSink("example.connector",
                    ProviderActionSupervisor.processWide(2),
                    System.nanoTime() + 5_000_000_000L));
            assertEquals("sink", System.getProperty(INITIALIZED));
        }
    }

    @Test
    void manifestIdentityMismatchFailsWithoutInitializingProvider(@TempDir Path temp)
            throws Exception {
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, source(), sink(), "other-deployment"))) {
            DeploymentValidationException failure = assertThrows(
                    DeploymentValidationException.class,
                    () -> StreamConnectorProviderRegistry.scan(deployments,
                            StreamConnectorProviderRegistry.Role.WORKER));
            assertEquals("STREAM_CONNECTOR_CONTRACT_INVALID", failure.code());
            assertNull(System.getProperty(INITIALIZED));
        }
    }

    @Test
    void boundPlanMustBeASubsetOfSignedContractsBeforeLoading(@TempDir Path temp)
            throws Exception {
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, source(), sink(), "signed-stream"))) {
            StreamConnectorProviderRegistry registry =
                    StreamConnectorProviderRegistry.scan(deployments,
                            StreamConnectorProviderRegistry.Role.STATIC_ONLY);
            assertDoesNotThrow(() -> registry.verifyArtifact(
                    plan(deployments.deploymentSetId(),
                            StreamConnectorProviderContractCodec.digestHex(source()),
                            StreamConnectorProviderContractCodec.digestHex(sink()),
                            true, configuration(1), false)));
            assertEquals("STREAM_CONNECTOR_DEPLOYMENT_MISMATCH",
                    assertThrows(DeploymentValidationException.class,
                            () -> registry.verifyArtifact(plan(
                                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                                    StreamConnectorProviderContractCodec.digestHex(source()),
                                    StreamConnectorProviderContractCodec.digestHex(sink()),
                                    true, configuration(1), false))).code());
            assertEquals("STREAM_CONNECTOR_CONTRACT_DRIFT",
                    assertThrows(DeploymentValidationException.class,
                            () -> registry.verifyArtifact(plan(
                                    deployments.deploymentSetId(),
                                    StreamConnectorProviderContractCodec.digestHex(sink()),
                                    StreamConnectorProviderContractCodec.digestHex(sink()),
                                    true, configuration(1), false))).code());
            assertEquals("STREAM_CONNECTOR_CAPABILITY_MISMATCH",
                    assertThrows(DeploymentValidationException.class,
                            () -> registry.verifyArtifact(plan(
                                    deployments.deploymentSetId(),
                                    StreamConnectorProviderContractCodec.digestHex(source()),
                                    StreamConnectorProviderContractCodec.digestHex(sink()),
                                    false, configuration(1), true))).code());
            assertEquals("STREAM_CONNECTOR_CONFIGURATION_LIMIT",
                    assertThrows(DeploymentValidationException.class,
                            () -> registry.verifyArtifact(plan(
                                    deployments.deploymentSetId(),
                                    StreamConnectorProviderContractCodec.digestHex(source()),
                                    StreamConnectorProviderContractCodec.digestHex(sink()),
                                    true, new byte[4097], false))).code());
            assertNull(System.getProperty(INITIALIZED));
        }
    }

    @Test
    void cdcV2RequiresSignedExternalCheckpointAcknowledgement(
            @TempDir Path temp) throws Exception {
        StreamConnectorProviderContract legacy = cdcSource(1);
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, legacy, transactionalSink(),
                        "signed-stream"))) {
            StreamConnectorProviderRegistry registry =
                    StreamConnectorProviderRegistry.scan(deployments,
                            StreamConnectorProviderRegistry.Role.STATIC_ONLY);
            assertEquals("STREAM_CONNECTOR_CAPABILITY_MISMATCH",
                    assertThrows(DeploymentValidationException.class,
                            () -> registry.verifyArtifact(cdcV2Plan(
                                    deployments.deploymentSetId(),
                                    StreamConnectorProviderContractCodec
                                            .digestHex(legacy),
                                    StreamConnectorProviderContractCodec
                                            .digestHex(transactionalSink()),
                                    configuration(1)))).code());
            assertNull(System.getProperty(INITIALIZED));
        }

        StreamConnectorProviderContract acknowledged =
                StreamConnectorProviderContract.source(
                        "signed-stream", "example.connector",
                        StreamConnectorConfiguration.CODEC_ID,
                        4096, 1, 30_000L,
                        false, false, true, true);
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, acknowledged, transactionalSink(),
                        "signed-stream"))) {
            StreamConnectorProviderRegistry registry =
                    StreamConnectorProviderRegistry.scan(deployments,
                            StreamConnectorProviderRegistry.Role.STATIC_ONLY);
            assertDoesNotThrow(() -> registry.verifyArtifact(cdcV2Plan(
                    deployments.deploymentSetId(),
                    StreamConnectorProviderContractCodec
                            .digestHex(acknowledged),
                    StreamConnectorProviderContractCodec
                            .digestHex(transactionalSink()),
                    configuration(1))));
            assertNull(System.getProperty(INITIALIZED));
        }
    }

    @Test
    void signedCdcAdapterForwardsCheckpointCompletion(
            @TempDir Path temp) throws Exception {
        StreamConnectorProviderContract source =
                StreamConnectorProviderContract.source(
                        "signed-stream", "example.connector",
                        StreamConnectorConfiguration.CODEC_ID,
                        4096, 1, 30_000L,
                        false, false, true, true);
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, source, transactionalSink(),
                        "signed-stream"))) {
            StreamConnectorProviderRegistry registry =
                    StreamConnectorProviderRegistry.scan(deployments,
                            StreamConnectorProviderRegistry.Role.WORKER);
            SignedStreamConnectorAdapters adapters =
                    new SignedStreamConnectorAdapters(registry,
                            ProviderActionSupervisor.processWide(2));
            ResidentCdcStreamSourceProvider provider =
                    (ResidentCdcStreamSourceProvider) adapters.sources()
                            .get("example.connector");
            try (StreamCdcSourceReader reader = provider.openCdc(
                    new StreamCdcSourceContext(
                            "source", emptyConfiguration(), "snapshot", 0L,
                            Collections.singletonList("snapshot-all"),
                            Collections.<String>emptyList(),
                            StreamCdcSourceContext.Phase.SNAPSHOT, 0L))) {
                reader.notifyCheckpointComplete(
                        new StreamCdcCheckpointCompletion(
                                3L, 7L, new byte[] {9}));
                assertEquals("3:7:9", System.getProperty(CDC_COMPLETION));
            }
        }
    }

    @Test
    void opaqueSignedConfigurationIsRejectedBeforeProviderCodeLoads(
            @TempDir Path temp) throws Exception {
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, source(), sink(), "signed-stream"))) {
            StreamConnectorProviderRegistry registry =
                    StreamConnectorProviderRegistry.scan(deployments,
                            StreamConnectorProviderRegistry.Role.STATIC_ONLY);
            assertEquals("STREAM_CONNECTOR_CONFIGURATION_INVALID",
                    assertThrows(DeploymentValidationException.class,
                            () -> registry.verifyArtifact(plan(
                                    deployments.deploymentSetId(),
                                    StreamConnectorProviderContractCodec
                                            .digestHex(source()),
                                    StreamConnectorProviderContractCodec
                                            .digestHex(sink()),
                                    true, new byte[] {1}, false))).code());
            assertNull(System.getProperty(INITIALIZED));
        }
    }

    @Test
    void masterChecksSignedPlanBeforeDurablePublish(@TempDir Path temp)
            throws Exception {
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, source(), sink(), "signed-stream"))) {
            StreamConnectorProviderRegistry registry =
                    StreamConnectorProviderRegistry.scan(deployments,
                            StreamConnectorProviderRegistry.Role.STATIC_ONLY);
            PlanArtifact valid = plan(deployments.deploymentSetId(),
                    StreamConnectorProviderContractCodec.digestHex(source()),
                    StreamConnectorProviderContractCodec.digestHex(sink()),
                    true, configuration(1), false);
            AtomicInteger starts = new AtomicInteger();
            try (StreamProductJobMasterServer unsignedMaster =
                         new StreamProductJobMasterServer(
                                 temp.resolve("unsigned-jobs"),
                                 snapshot -> run(starts));
                 StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                         "127.0.0.1", unsignedMaster.port())) {
                StreamControlException failure = assertThrows(
                        StreamControlException.class,
                        () -> client.submit(StreamSubmissionId.of("unsigned"),
                                valid));
                assertEquals(StreamControlException.Code.INVALID_RESPONSE,
                        failure.code());
                assertEquals(0, starts.get());
                assertEquals(StreamControlException.Code.JOB_NOT_FOUND,
                        assertThrows(StreamControlException.class,
                                () -> client.attachSubmission(
                                        StreamSubmissionId.of("unsigned")))
                                .code());
            }
            try (StreamProductJobMasterServer signedMaster =
                         new StreamProductJobMasterServer(
                                 temp.resolve("signed-jobs"),
                                 snapshot -> run(starts), registry);
                 StreamJobClient client = RowWeave.createRealClusterStreamJobClient(
                         "127.0.0.1", signedMaster.port())) {
                assertEquals(StreamJobStatus.RUNNING,
                        client.submit(StreamSubmissionId.of("valid"), valid)
                                .status());
                assertEquals(1, starts.get());
                assertNull(System.getProperty(INITIALIZED));
                StreamControlException drift = assertThrows(
                        StreamControlException.class,
                        () -> client.submit(StreamSubmissionId.of("drift"),
                                plan(deployments.deploymentSetId(),
                                        StreamConnectorProviderContractCodec
                                                .digestHex(sink()),
                                        StreamConnectorProviderContractCodec
                                                .digestHex(sink()),
                                        true, configuration(1), false)));
                assertEquals(StreamControlException.Code.INVALID_RESPONSE,
                        drift.code());
                assertEquals(1, starts.get());
                assertEquals(StreamControlException.Code.JOB_NOT_FOUND,
                        assertThrows(StreamControlException.class,
                                () -> client.attachSubmission(
                                        StreamSubmissionId.of("drift")))
                                .code());
            }
        }
    }

    @Test
    void publicMasterBootstrapUsesTheVerifiedDeploymentSet(@TempDir Path temp)
            throws Exception {
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, source(), sink(), "signed-stream"));
             StreamRemoteWorkerServer worker = new StreamRemoteWorkerServer(
                     new StreamWorkerCapabilityProfile(
                             "legacy-worker", "incarnation", 2, 1,
                             Arrays.asList(
                                     StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                                     StreamWorkerCapabilityProfile.DATA_CAPABILITY),
                             2, 8, 4096, 1024 * 1024L, 8),
                     Collections.emptyMap(), Collections.emptyMap());
             StreamMasterRuntime master = RowWeave.startStreamMaster(
                     temp.resolve("public-master"),
                     Collections.singletonList(new StreamWorkerEndpoint(
                             "127.0.0.1", worker.port())),
                     io.rowweave.cluster.rpc.ControlSecurityConfig
                             .fromProperties(new Properties()), 0,
                     deployments);
             StreamJobClient client = master.createJobClient()) {
            PlanArtifact drift = plan(deployments.deploymentSetId(),
                    StreamConnectorProviderContractCodec.digestHex(sink()),
                    StreamConnectorProviderContractCodec.digestHex(sink()),
                    true, configuration(1), false);
            assertEquals(StreamControlException.Code.INVALID_RESPONSE,
                    assertThrows(StreamControlException.class,
                            () -> client.submit(StreamSubmissionId.of(
                                    "public-master-drift"), drift)).code());
            assertEquals(StreamControlException.Code.JOB_NOT_FOUND,
                    assertThrows(StreamControlException.class,
                            () -> client.attachSubmission(StreamSubmissionId.of(
                                    "public-master-drift"))).code());
            assertNull(System.getProperty(INITIALIZED));
        }
    }

    @Test
    void publicWorkerBootstrapAdvertisesOnlyVerifiedMetadataWithoutLoadingCode(
            @TempDir Path temp) throws Exception {
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, source(), sink(), "signed-stream"));
             StreamWorkerRuntime worker = RowWeave.startStreamWorker(
                     StreamWorkerConfig.defaults("signed-worker",
                             temp.resolve("signed-worker")), deployments)) {
            RemoteStreamWorkerEndpoint endpoint =
                    RemoteStreamWorkerEndpoint.connect(
                            worker.endpoint().host(), worker.endpoint().port());
            assertTrue(endpoint.profile().capabilities().contains(
                    PlanArtifact.FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1));
            assertTrue(endpoint.profile().capabilities().contains(
                    PlanArtifact.DEPLOYMENT_FEATURE_PREFIX
                            + deployments.deploymentSetId()));
            assertNull(System.getProperty(INITIALIZED));
        }
    }

    @Test
    void signedPublicWorkerExecutesBoundedSourceThroughRealMasterAndSink(
            @TempDir Path temp) throws Exception {
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, source(), sink(), "signed-stream"));
             StreamWorkerRuntime worker = RowWeave.startStreamWorker(
                     StreamWorkerConfig.defaults("signed-worker",
                             temp.resolve("signed-worker")), deployments);
             StreamMasterRuntime master = RowWeave.startStreamMaster(
                     temp.resolve("signed-master"),
                     Collections.singletonList(worker.endpoint()),
                     io.rowweave.cluster.rpc.ControlSecurityConfig
                             .fromProperties(new Properties()), 0,
                     deployments);
             StreamJobClient client = master.createJobClient()) {
            assertNull(System.getProperty(INITIALIZED));
            StreamJobHandle job = client.submit(
                    StreamSubmissionId.of("signed-end-to-end"),
                    plan(deployments.deploymentSetId(),
                            StreamConnectorProviderContractCodec
                                    .digestHex(source()),
                            StreamConnectorProviderContractCodec
                                    .digestHex(sink()),
                            true, configuration(1), false));
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(10L);
            while (System.getProperty(RESULT) == null
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertEquals("42", System.getProperty(RESULT));
            assertEquals(StreamJobStatus.FINISHED,
                    job.awaitTermination(10_000L));
        }
    }

    @Test
    void signedSourceOpenLimitIsHeldUntilReaderCloses(
            @TempDir Path temp) throws Exception {
        StreamConnectorProviderContract limited =
                StreamConnectorProviderContract.source(
                        "signed-stream", "example.connector",
                        StreamConnectorConfiguration.CODEC_ID,
                        4096, 1, 30_000L,
                        true, false, false);
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, limited, sink(), "signed-stream"));
             StreamWorkerRuntime worker = RowWeave.startStreamWorker(
                     StreamWorkerConfig.defaults("limited-worker",
                             temp.resolve("limited-worker")), deployments);
             StreamMasterRuntime master = RowWeave.startStreamMaster(
                     temp.resolve("limited-master"),
                     Collections.singletonList(worker.endpoint()),
                     io.rowweave.cluster.rpc.ControlSecurityConfig
                             .fromProperties(new Properties()), 0,
                     deployments);
             StreamJobClient client = master.createJobClient()) {
            String sourceDigest = StreamConnectorProviderContractCodec
                    .digestHex(limited);
            String sinkDigest = StreamConnectorProviderContractCodec
                    .digestHex(sink());
            StreamJobHandle first = client.submit(
                    StreamSubmissionId.of("limited-first"),
                    plan(deployments.deploymentSetId(), sourceDigest,
                            sinkDigest, true, configuration(9), false));
            assertEquals(StreamJobStatus.RUNNING,
                    first.awaitRunning(10_000L));
            awaitProperty(OPEN_COUNT, "1", 10_000L);
            StreamJobHandle second = client.submit(
                    StreamSubmissionId.of("limited-second"),
                    plan(deployments.deploymentSetId(), sourceDigest,
                            sinkDigest, true, configuration(10), false));
            assertEquals(StreamJobStatus.FAILED,
                    second.awaitTermination(10_000L));
            assertEquals("1", System.getProperty(OPEN_COUNT));
            first.cancel(StreamCommandId.of("limited-first-cancel"))
                    .awaitCompletion(10_000L);
            StreamJobHandle third = client.submit(
                    StreamSubmissionId.of("limited-third"),
                    plan(deployments.deploymentSetId(), sourceDigest,
                            sinkDigest, true, configuration(11), false));
            assertEquals(StreamJobStatus.RUNNING,
                    third.awaitRunning(10_000L));
            awaitProperty(OPEN_COUNT, "2", 10_000L);
            third.cancel(StreamCommandId.of("limited-third-cancel"))
                    .awaitCompletion(10_000L);
        }
    }

    @Test
    void unresolvedSecretReferenceFailsBeforeSignedSourceLoads(
            @TempDir Path temp) throws Exception {
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, source(), sink(), "signed-stream"));
             StreamWorkerRuntime worker = RowWeave.startStreamWorker(
                     StreamWorkerConfig.defaults("secret-worker",
                             temp.resolve("secret-worker")), deployments);
             StreamMasterRuntime master = RowWeave.startStreamMaster(
                     temp.resolve("secret-master"),
                     Collections.singletonList(worker.endpoint()),
                     io.rowweave.cluster.rpc.ControlSecurityConfig
                             .fromProperties(new Properties()), 0,
                     deployments);
             StreamJobClient client = master.createJobClient()) {
            byte[] config = StreamConnectorConfiguration.of(
                    Collections.singletonMap("mode", "1"),
                    Collections.singletonMap("password",
                            "vault://production/postgres")).encode();
            StreamJobHandle job = client.submit(
                    StreamSubmissionId.of("missing-secret-resolver"),
                    plan(deployments.deploymentSetId(),
                            StreamConnectorProviderContractCodec
                                    .digestHex(source()),
                            StreamConnectorProviderContractCodec
                                    .digestHex(sink()),
                            true, config, false));
            assertEquals(StreamJobStatus.FAILED,
                    job.awaitTermination(10_000L));
            assertNull(System.getProperty(SOURCE_INITIALIZED));
            assertNull(System.getProperty(OPEN_COUNT));
        }
    }

    @Test
    void workerResolvesSecretOnlyForOpenAndClearsItAfterward(
            @TempDir Path temp) throws Exception {
        AtomicReference<byte[]> issued = new AtomicReference<>();
        StreamSecretResolver resolver = reference -> {
            assertEquals("vault://production/postgres", reference);
            byte[] secret = "temporary".getBytes(StandardCharsets.UTF_8);
            issued.set(secret);
            return secret;
        };
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, source(), sink(), "signed-stream"));
             StreamWorkerRuntime worker = RowWeave.startStreamWorker(
                     StreamWorkerConfig.defaults("resolved-worker",
                             temp.resolve("resolved-worker")), deployments,
                     resolver);
             StreamMasterRuntime master = RowWeave.startStreamMaster(
                     temp.resolve("resolved-master"),
                     Collections.singletonList(worker.endpoint()),
                     io.rowweave.cluster.rpc.ControlSecurityConfig
                             .fromProperties(new Properties()), 0,
                     deployments);
             StreamJobClient client = master.createJobClient()) {
            byte[] config = StreamConnectorConfiguration.of(
                    Collections.singletonMap("mode", "1"),
                    Collections.singletonMap("password",
                            "vault://production/postgres")).encode();
            StreamJobHandle job = client.submit(
                    StreamSubmissionId.of("resolved-secret"),
                    plan(deployments.deploymentSetId(),
                            StreamConnectorProviderContractCodec
                                    .digestHex(source()),
                            StreamConnectorProviderContractCodec
                                    .digestHex(sink()),
                            true, config, false));
            awaitProperty(SECRET_SEEN, "true", 10_000L);
            assertNotNull(issued.get());
            awaitZeroed(issued.get(), 10_000L);
            assertEquals(StreamJobStatus.FINISHED,
                    job.awaitTermination(10_000L));
        }
    }

    @Test
    void signedTransactionalSinkBecomesVisibleOnlyAfterCheckpointDecision(
            @TempDir Path temp) throws Exception {
        StreamConnectorProviderContract transactional =
                transactionalSink();
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, source(), transactional,
                        "signed-stream"));
             StreamWorkerRuntime worker = RowWeave.startStreamWorker(
                     StreamWorkerConfig.defaults("transaction-worker",
                             temp.resolve("transaction-worker")), deployments);
             StreamMasterRuntime master = RowWeave.startStreamMaster(
                     temp.resolve("transaction-master"),
                     Collections.singletonList(worker.endpoint()),
                     io.rowweave.cluster.rpc.ControlSecurityConfig
                             .fromProperties(new Properties()), 0,
                     deployments);
             StreamJobClient client = master.createJobClient()) {
            StreamJobHandle job = client.submit(
                    StreamSubmissionId.of("signed-transactional"),
                    plan(deployments.deploymentSetId(),
                            StreamConnectorProviderContractCodec
                                    .digestHex(source()),
                            StreamConnectorProviderContractCodec
                                    .digestHex(transactional),
                            false, configuration(9), true));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(10_000L));
            awaitProperty(PENDING_WRITE, "true", 10_000L);
            assertNull(System.getProperty(RESULT));
            StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                    StreamCommandId.of("signed-transaction-cut"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    checkpoint.awaitCompletion(10_000L));
            awaitProperty(RESULT, "42", 10_000L);
            assertEquals(1L, job.progress().completedCheckpoints());
            job.cancel(StreamCommandId.of("signed-transaction-cancel"))
                    .awaitCompletion(10_000L);
        }
    }

    @Test
    void signedBoundedTransactionalJobFinishesAfterAutomaticTerminalCut(
            @TempDir Path temp) throws Exception {
        StreamConnectorProviderContract transactional = transactionalSink();
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, source(), transactional,
                        "signed-stream"));
             StreamWorkerRuntime worker = RowWeave.startStreamWorker(
                     StreamWorkerConfig.defaults("terminal-worker",
                             temp.resolve("terminal-worker")), deployments);
             StreamMasterRuntime master = RowWeave.startStreamMaster(
                     temp.resolve("terminal-master"),
                     Collections.singletonList(worker.endpoint()),
                     io.rowweave.cluster.rpc.ControlSecurityConfig
                             .fromProperties(new Properties()), 0,
                     deployments);
             StreamJobClient client = master.createJobClient()) {
            StreamJobHandle job = client.submit(
                    StreamSubmissionId.of("signed-terminal-transaction"),
                    plan(deployments.deploymentSetId(),
                            StreamConnectorProviderContractCodec
                                    .digestHex(source()),
                            StreamConnectorProviderContractCodec
                                    .digestHex(transactional),
                            true, configuration(1), true));
            assertEquals(StreamJobStatus.FINISHED,
                    job.awaitTermination(10_000L));
            assertEquals("42", System.getProperty(RESULT));
            assertEquals(1L, job.progress().completedCheckpoints());
        }
    }

    @Test
    void signedTransactionalSinkOpenLimitIsHeldUntilWriterCloses(
            @TempDir Path temp) throws Exception {
        StreamConnectorProviderContract limited = transactionalSink(1);
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, source(), limited, "signed-stream"));
             StreamWorkerRuntime worker = RowWeave.startStreamWorker(
                     StreamWorkerConfig.defaults("limited-sink-worker",
                             temp.resolve("limited-sink-worker")), deployments);
             StreamMasterRuntime master = RowWeave.startStreamMaster(
                     temp.resolve("limited-sink-master"),
                     Collections.singletonList(worker.endpoint()),
                     io.rowweave.cluster.rpc.ControlSecurityConfig
                             .fromProperties(new Properties()), 0,
                     deployments);
             StreamJobClient client = master.createJobClient()) {
            String sourceDigest = StreamConnectorProviderContractCodec
                    .digestHex(source());
            String sinkDigest = StreamConnectorProviderContractCodec
                    .digestHex(limited);
            StreamJobHandle first = client.submit(
                    StreamSubmissionId.of("limited-sink-first"),
                    plan(deployments.deploymentSetId(), sourceDigest,
                            sinkDigest, false, configuration(9), true));
            assertEquals(StreamJobStatus.RUNNING,
                    first.awaitRunning(10_000L));
            awaitProperty(SINK_OPEN_COUNT, "1", 10_000L);
            StreamJobHandle second = client.submit(
                    StreamSubmissionId.of("limited-sink-second"),
                    plan(deployments.deploymentSetId(), sourceDigest,
                            sinkDigest, false, configuration(10), true));
            assertEquals(StreamJobStatus.FAILED,
                    second.awaitTermination(10_000L));
            assertEquals("1", System.getProperty(SINK_OPEN_COUNT));
            first.cancel(StreamCommandId.of("limited-sink-first-cancel"))
                    .awaitCompletion(10_000L);
            StreamJobHandle third = client.submit(
                    StreamSubmissionId.of("limited-sink-third"),
                    plan(deployments.deploymentSetId(), sourceDigest,
                            sinkDigest, false, configuration(11), true));
            assertEquals(StreamJobStatus.RUNNING,
                    third.awaitRunning(10_000L));
            awaitProperty(SINK_OPEN_COUNT, "2", 10_000L);
            third.cancel(StreamCommandId.of("limited-sink-third-cancel"))
                    .awaitCompletion(10_000L);
        }
    }

    @Test
    void signedTransactionalSinkSecretsExistOnlyDuringOpen(
            @TempDir Path temp) throws Exception {
        AtomicReference<byte[]> issued = new AtomicReference<>();
        AtomicInteger resolutions = new AtomicInteger();
        StreamSecretResolver resolver = reference -> {
            assertEquals("vault://production/transaction", reference);
            byte[] secret = "transaction-secret".getBytes(
                    StandardCharsets.UTF_8);
            issued.set(secret);
            resolutions.incrementAndGet();
            return secret;
        };
        StreamConnectorProviderContract transactional = transactionalSink();
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, source(), transactional, "signed-stream"));
             StreamWorkerRuntime worker = RowWeave.startStreamWorker(
                     StreamWorkerConfig.defaults("secret-sink-worker",
                             temp.resolve("secret-sink-worker")), deployments,
                     resolver);
             StreamMasterRuntime master = RowWeave.startStreamMaster(
                     temp.resolve("secret-sink-master"),
                     Collections.singletonList(worker.endpoint()),
                     io.rowweave.cluster.rpc.ControlSecurityConfig
                             .fromProperties(new Properties()), 0,
                     deployments);
             StreamJobClient client = master.createJobClient()) {
            byte[] sinkConfiguration = StreamConnectorConfiguration.of(
                    Collections.emptyMap(), Collections.singletonMap(
                            "password",
                            "vault://production/transaction")).encode();
            StreamJobHandle job = client.submit(
                    StreamSubmissionId.of("secret-transactional-sink"),
                    plan(deployments.deploymentSetId(),
                            StreamConnectorProviderContractCodec
                                    .digestHex(source()),
                            StreamConnectorProviderContractCodec
                                    .digestHex(transactional),
                            false, configuration(9), sinkConfiguration, true));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(10_000L));
            awaitProperty(PENDING_WRITE, "true", 10_000L);
            assertEquals(1, resolutions.get());
            assertNotNull(issued.get());
            awaitZeroed(issued.get(), 10_000L);
            job.cancel(StreamCommandId.of("secret-sink-cancel"))
                    .awaitCompletion(10_000L);
        }
    }

    @Test
    void unresolvedTransactionalSinkSecretFailsBeforeSinkClassLoads(
            @TempDir Path temp) throws Exception {
        StreamConnectorProviderContract transactional = transactionalSink();
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, source(), transactional, "signed-stream"));
             StreamWorkerRuntime worker = RowWeave.startStreamWorker(
                     StreamWorkerConfig.defaults("missing-sink-secret-worker",
                             temp.resolve("missing-sink-secret-worker")),
                     deployments);
             StreamMasterRuntime master = RowWeave.startStreamMaster(
                     temp.resolve("missing-sink-secret-master"),
                     Collections.singletonList(worker.endpoint()),
                     io.rowweave.cluster.rpc.ControlSecurityConfig
                             .fromProperties(new Properties()), 0,
                     deployments);
             StreamJobClient client = master.createJobClient()) {
            byte[] sinkConfiguration = StreamConnectorConfiguration.of(
                    Collections.emptyMap(), Collections.singletonMap(
                            "password", "vault://missing/transaction"))
                    .encode();
            StreamJobHandle job = client.submit(
                    StreamSubmissionId.of("missing-transactional-secret"),
                    plan(deployments.deploymentSetId(),
                            StreamConnectorProviderContractCodec
                                    .digestHex(source()),
                            StreamConnectorProviderContractCodec
                                    .digestHex(transactional),
                            false, configuration(9), sinkConfiguration, true));
            assertEquals(StreamJobStatus.FAILED,
                    job.awaitTermination(10_000L));
            assertNull(System.getProperty(SINK_INITIALIZED));
            assertNull(System.getProperty(SINK_OPEN_COUNT));
        }
    }

    @Test
    void signedDynamicEnumeratorOpenLimitIsHeldUntilJobCloses(
            @TempDir Path temp) throws Exception {
        StreamConnectorProviderContract dynamic = dynamicSource(1);
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, dynamic, sink(), "signed-stream"));
             StreamWorkerRuntime worker = RowWeave.startStreamWorker(
                     StreamWorkerConfig.defaults("dynamic-limit-worker",
                             temp.resolve("dynamic-limit-worker")), deployments);
             StreamMasterRuntime master = RowWeave.startStreamMaster(
                     temp.resolve("dynamic-limit-master"),
                     Collections.singletonList(worker.endpoint()),
                     io.rowweave.cluster.rpc.ControlSecurityConfig
                             .fromProperties(new Properties()), 0,
                     deployments);
             StreamJobClient client = master.createJobClient()) {
            String sourceDigest = StreamConnectorProviderContractCodec
                    .digestHex(dynamic);
            String sinkDigest = StreamConnectorProviderContractCodec
                    .digestHex(sink());
            byte[] config = emptyConfiguration();
            StreamJobHandle first = client.submit(
                    StreamSubmissionId.of("dynamic-limit-first"),
                    dynamicPlan(deployments.deploymentSetId(), sourceDigest,
                            sinkDigest, config));
            assertEquals(StreamJobStatus.RUNNING,
                    first.awaitRunning(10_000L));
            awaitProperty(OPEN_COUNT, "1", 10_000L);
            StreamControlException exhausted = assertThrows(
                    StreamControlException.class, () -> client.submit(
                            StreamSubmissionId.of("dynamic-limit-second"),
                            dynamicPlan(deployments.deploymentSetId(),
                                    sourceDigest, sinkDigest, config)));
            assertEquals(StreamControlException.Code.INVALID_RESPONSE,
                    exhausted.code());
            assertEquals("1", System.getProperty(OPEN_COUNT));
            first.cancel(StreamCommandId.of("dynamic-limit-first-cancel"))
                    .awaitCompletion(10_000L);
            StreamJobHandle third = client.submit(
                    StreamSubmissionId.of("dynamic-limit-third"),
                    dynamicPlan(deployments.deploymentSetId(), sourceDigest,
                            sinkDigest, config));
            assertEquals(StreamJobStatus.RUNNING,
                    third.awaitRunning(10_000L));
            awaitProperty(OPEN_COUNT, "2", 10_000L);
            third.cancel(StreamCommandId.of("dynamic-limit-third-cancel"))
                    .awaitCompletion(10_000L);
        }
    }

    @Test
    void signedDynamicEnumeratorSecretIsScopedAndRequired(
            @TempDir Path temp) throws Exception {
        StreamConnectorProviderContract dynamic = dynamicSource(1);
        byte[] config = StreamConnectorConfiguration.of(
                Collections.emptyMap(), Collections.singletonMap(
                        "password", "vault://production/enumerator"))
                .encode();
        AtomicReference<byte[]> issued = new AtomicReference<>();
        StreamSecretResolver resolver = reference -> {
            assertEquals("vault://production/enumerator", reference);
            byte[] secret = "enumerator-secret".getBytes(
                    StandardCharsets.UTF_8);
            issued.set(secret);
            return secret;
        };
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, dynamic, sink(), "signed-stream"));
             StreamWorkerRuntime unresolved = RowWeave.startStreamWorker(
                     StreamWorkerConfig.defaults("dynamic-missing-worker",
                             temp.resolve("dynamic-missing-worker")),
                     deployments);
             StreamMasterRuntime unresolvedMaster = RowWeave.startStreamMaster(
                     temp.resolve("dynamic-missing-master"),
                     Collections.singletonList(unresolved.endpoint()),
                     io.rowweave.cluster.rpc.ControlSecurityConfig
                             .fromProperties(new Properties()), 0,
                     deployments);
             StreamJobClient unresolvedClient =
                     unresolvedMaster.createJobClient()) {
            String sourceDigest = StreamConnectorProviderContractCodec
                    .digestHex(dynamic);
            String sinkDigest = StreamConnectorProviderContractCodec
                    .digestHex(sink());
            StreamControlException unresolvedSecret = assertThrows(
                    StreamControlException.class, () ->
                            unresolvedClient.submit(StreamSubmissionId.of(
                                            "dynamic-secret-missing"),
                                    dynamicPlan(deployments.deploymentSetId(),
                                            sourceDigest, sinkDigest, config)));
            assertEquals(StreamControlException.Code.INVALID_RESPONSE,
                    unresolvedSecret.code());
            assertNull(System.getProperty(SOURCE_INITIALIZED));
        }
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, dynamic, sink(), "signed-stream"));
             StreamWorkerRuntime worker = RowWeave.startStreamWorker(
                     StreamWorkerConfig.defaults("dynamic-secret-worker",
                             temp.resolve("dynamic-secret-worker")),
                     deployments, resolver);
             StreamMasterRuntime master = RowWeave.startStreamMaster(
                     temp.resolve("dynamic-secret-master"),
                     Collections.singletonList(worker.endpoint()),
                     io.rowweave.cluster.rpc.ControlSecurityConfig
                             .fromProperties(new Properties()), 0,
                     deployments);
             StreamJobClient client = master.createJobClient()) {
            StreamJobHandle job = client.submit(
                    StreamSubmissionId.of("dynamic-secret-resolved"),
                    dynamicPlan(deployments.deploymentSetId(),
                            StreamConnectorProviderContractCodec
                                    .digestHex(dynamic),
                            StreamConnectorProviderContractCodec
                                    .digestHex(sink()), config));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(10_000L));
            awaitProperty(SECRET_SEEN, "true", 10_000L);
            assertNotNull(issued.get());
            awaitZeroed(issued.get(), 10_000L);
            job.cancel(StreamCommandId.of("dynamic-secret-cancel"))
                    .awaitCompletion(10_000L);
        }
    }

    @Test
    void signedCdcOpenLimitIsHeldUntilJobCloses(
            @TempDir Path temp) throws Exception {
        StreamConnectorProviderContract cdc = cdcSource(1);
        StreamConnectorProviderContract transactional = transactionalSink();
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, cdc, transactional, "signed-stream"));
             StreamWorkerRuntime worker = RowWeave.startStreamWorker(
                     StreamWorkerConfig.defaults("cdc-limit-worker",
                             temp.resolve("cdc-limit-worker")), deployments);
             StreamMasterRuntime master = RowWeave.startStreamMaster(
                     temp.resolve("cdc-limit-master"),
                     Collections.singletonList(worker.endpoint()),
                     io.rowweave.cluster.rpc.ControlSecurityConfig
                             .fromProperties(new Properties()), 0,
                     deployments);
             StreamJobClient client = master.createJobClient()) {
            String sourceDigest = StreamConnectorProviderContractCodec
                    .digestHex(cdc);
            String sinkDigest = StreamConnectorProviderContractCodec
                    .digestHex(transactional);
            byte[] config = emptyConfiguration();
            StreamJobHandle first = client.submit(
                    StreamSubmissionId.of("cdc-limit-first"),
                    cdcPlan(deployments.deploymentSetId(), sourceDigest,
                            sinkDigest, config));
            assertEquals(StreamJobStatus.RUNNING,
                    first.awaitRunning(10_000L));
            awaitProperty(OPEN_COUNT, "1", 10_000L);
            StreamJobHandle second = client.submit(
                    StreamSubmissionId.of("cdc-limit-second"),
                    cdcPlan(deployments.deploymentSetId(), sourceDigest,
                            sinkDigest, config));
            assertEquals(StreamJobStatus.FAILED,
                    second.awaitTermination(10_000L));
            assertEquals("1", System.getProperty(OPEN_COUNT));
            first.cancel(StreamCommandId.of("cdc-limit-first-cancel"))
                    .awaitCompletion(10_000L);
            StreamJobHandle third = client.submit(
                    StreamSubmissionId.of("cdc-limit-third"),
                    cdcPlan(deployments.deploymentSetId(), sourceDigest,
                            sinkDigest, config));
            assertEquals(StreamJobStatus.RUNNING,
                    third.awaitRunning(10_000L));
            awaitProperty(OPEN_COUNT, "2", 10_000L);
            third.cancel(StreamCommandId.of("cdc-limit-third-cancel"))
                    .awaitCompletion(10_000L);
        }
    }

    @Test
    void signedCdcSecretIsScopedAndRequired(
            @TempDir Path temp) throws Exception {
        StreamConnectorProviderContract cdc = cdcSource(1);
        StreamConnectorProviderContract transactional = transactionalSink();
        byte[] config = StreamConnectorConfiguration.of(
                Collections.emptyMap(), Collections.singletonMap(
                        "password", "vault://production/cdc"))
                .encode();
        AtomicReference<byte[]> issued = new AtomicReference<>();
        StreamSecretResolver resolver = reference -> {
            assertEquals("vault://production/cdc", reference);
            byte[] secret = "cdc-secret".getBytes(StandardCharsets.UTF_8);
            issued.set(secret);
            return secret;
        };
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, cdc, transactional, "signed-stream"));
             StreamWorkerRuntime unresolved = RowWeave.startStreamWorker(
                     StreamWorkerConfig.defaults("cdc-missing-worker",
                             temp.resolve("cdc-missing-worker")),
                     deployments);
             StreamMasterRuntime unresolvedMaster = RowWeave.startStreamMaster(
                     temp.resolve("cdc-missing-master"),
                     Collections.singletonList(unresolved.endpoint()),
                     io.rowweave.cluster.rpc.ControlSecurityConfig
                             .fromProperties(new Properties()), 0,
                     deployments);
             StreamJobClient unresolvedClient =
                     unresolvedMaster.createJobClient()) {
            String sourceDigest = StreamConnectorProviderContractCodec
                    .digestHex(cdc);
            String sinkDigest = StreamConnectorProviderContractCodec
                    .digestHex(transactional);
            StreamJobHandle failed = unresolvedClient.submit(
                    StreamSubmissionId.of("cdc-secret-missing"),
                    cdcPlan(deployments.deploymentSetId(), sourceDigest,
                            sinkDigest, config));
            assertEquals(StreamJobStatus.FAILED,
                    failed.awaitTermination(10_000L));
            assertNull(System.getProperty(SOURCE_INITIALIZED));
        }
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, cdc, transactional, "signed-stream"));
             StreamWorkerRuntime worker = RowWeave.startStreamWorker(
                     StreamWorkerConfig.defaults("cdc-secret-worker",
                             temp.resolve("cdc-secret-worker")),
                     deployments, resolver);
             StreamMasterRuntime master = RowWeave.startStreamMaster(
                     temp.resolve("cdc-secret-master"),
                     Collections.singletonList(worker.endpoint()),
                     io.rowweave.cluster.rpc.ControlSecurityConfig
                             .fromProperties(new Properties()), 0,
                     deployments);
             StreamJobClient client = master.createJobClient()) {
            StreamJobHandle job = client.submit(
                    StreamSubmissionId.of("cdc-secret-resolved"),
                    cdcPlan(deployments.deploymentSetId(),
                            StreamConnectorProviderContractCodec
                                    .digestHex(cdc),
                            StreamConnectorProviderContractCodec
                                    .digestHex(transactional), config));
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(10_000L));
            awaitProperty(SECRET_SEEN, "true", 10_000L);
            assertNotNull(issued.get());
            awaitZeroed(issued.get(), 10_000L);
            job.cancel(StreamCommandId.of("cdc-secret-cancel"))
                    .awaitCompletion(10_000L);
        }
    }

    @Test
    void boundedBranchCannotFinishUnionWithUnboundedBranch(
            @TempDir Path temp) throws Exception {
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, source(), sink(), "signed-stream"));
             StreamWorkerRuntime worker = RowWeave.startStreamWorker(
                     StreamWorkerConfig.defaults("mixed-worker",
                             temp.resolve("mixed-worker")), deployments);
             StreamMasterRuntime master = RowWeave.startStreamMaster(
                     temp.resolve("mixed-master"),
                     Collections.singletonList(worker.endpoint()),
                     io.rowweave.cluster.rpc.ControlSecurityConfig
                             .fromProperties(new Properties()), 0,
                     deployments);
             StreamJobClient client = master.createJobClient()) {
            StreamSchema schema = new StreamSchema(Collections.singletonList(
                    new StreamField("id", "id", "id", DataType.LongType,
                            false)));
            StreamSplitSpec split = new StreamSplitSpec(
                    StreamConnectorProviderContractCodec.digestHex(source()),
                    "position-v1", new byte[] {2});
            String digest = StreamConnectorProviderContractCodec
                    .digestHex(source());
            StreamSession session = RowWeave.createStreamSession("mixed-end");
            session.setDeploymentSetId(deployments.deploymentSetId());
            session.setResourceProfile(new StreamResourceProfile(
                    8, 4_096, 8_192, 0L, 0, 0,
                    32_768L, 8, 32, 0L));
            session.fromSource(StreamSourceSpec.boundedExactOrdered(
                            schema, "example.connector", digest,
                            configuration(1), Collections.singletonList(split)))
                    .uid("bounded")
                    .union(session.fromSource(StreamSourceSpec.exactOrdered(
                            schema, "example.connector", digest,
                            configuration(9), Collections.singletonList(split)))
                            .uid("unbounded"))
                    .uid("union")
                    .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                            "example.connector",
                            StreamConnectorProviderContractCodec
                                    .digestHex(sink()),
                            emptyConfiguration()));
            StreamJobHandle job = client.submit(
                    StreamSubmissionId.of("mixed-end-submit"),
                    session.toPlanArtifact());
            assertEquals(StreamJobStatus.RUNNING,
                    job.awaitRunning(10_000L));
            awaitProperty(RESULT, "42", 10_000L);
            Thread.sleep(1_500L);
            assertEquals(StreamJobStatus.RUNNING, job.status());
            job.cancel(StreamCommandId.of("mixed-end-cancel"))
                    .awaitCompletion(10_000L);
            assertEquals(StreamJobStatus.CANCELED,
                    job.awaitTermination(10_000L));
        }
    }

    @Test
    void signedBoundedCompletionRemainsFinishedAfterMasterReplacement(
            @TempDir Path temp) throws Exception {
        Path masterRoot = temp.resolve("finished-master");
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, source(), sink(), "signed-stream"));
             StreamWorkerRuntime worker = RowWeave.startStreamWorker(
                     StreamWorkerConfig.defaults("finished-worker",
                             temp.resolve("finished-worker")), deployments)) {
            StreamJobId jobId;
            try (StreamMasterRuntime first = RowWeave.startStreamMaster(
                    masterRoot, Collections.singletonList(worker.endpoint()),
                    io.rowweave.cluster.rpc.ControlSecurityConfig
                            .fromProperties(new Properties()), 0,
                    deployments);
                 StreamJobClient client = first.createJobClient()) {
                StreamJobHandle job = client.submit(
                        StreamSubmissionId.of("finished-before-replacement"),
                        plan(deployments.deploymentSetId(),
                                StreamConnectorProviderContractCodec
                                        .digestHex(source()),
                                StreamConnectorProviderContractCodec
                                        .digestHex(sink()),
                                true, configuration(1), false));
                assertEquals(StreamJobStatus.FINISHED,
                        job.awaitTermination(10_000L));
                jobId = job.id();
                assertEquals("1", System.getProperty(OPEN_COUNT));
            }
            try (StreamMasterRuntime replacement = RowWeave.startStreamMaster(
                    masterRoot, Collections.singletonList(worker.endpoint()),
                    io.rowweave.cluster.rpc.ControlSecurityConfig
                            .fromProperties(new Properties()), 0,
                    deployments);
                 StreamJobClient client = replacement.createJobClient()) {
                assertEquals(StreamJobStatus.FINISHED,
                        client.attach(jobId).status());
                assertEquals("1", System.getProperty(OPEN_COUNT),
                        "terminal authority must not relaunch the Source");
            }
        }
    }

    @Test
    void signedBoundedSourceFailureCannotPublishFinished(
            @TempDir Path temp) throws Exception {
        try (VerifiedDeploymentSet deployments = deployment(temp,
                providerJar(temp, source(), sink(), "signed-stream"));
             StreamWorkerRuntime worker = RowWeave.startStreamWorker(
                     StreamWorkerConfig.defaults("failed-worker",
                             temp.resolve("failed-worker")), deployments);
             StreamMasterRuntime master = RowWeave.startStreamMaster(
                     temp.resolve("failed-master"),
                     Collections.singletonList(worker.endpoint()),
                     io.rowweave.cluster.rpc.ControlSecurityConfig
                             .fromProperties(new Properties()), 0,
                     deployments);
             StreamJobClient client = master.createJobClient()) {
            StreamJobHandle job = client.submit(
                    StreamSubmissionId.of("failed-bounded-source"),
                    plan(deployments.deploymentSetId(),
                            StreamConnectorProviderContractCodec
                                    .digestHex(source()),
                            StreamConnectorProviderContractCodec
                                    .digestHex(sink()),
                            true, configuration(7), false));
            assertEquals(StreamJobStatus.FAILED,
                    job.awaitTermination(10_000L));
        }
    }

    private static void awaitProperty(String key, String expected,
            long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (!expected.equals(System.getProperty(key))
                && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertEquals(expected, System.getProperty(key));
    }

    private static void awaitZeroed(byte[] bytes, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        byte[] zeroed = new byte[bytes.length];
        while (!Arrays.equals(zeroed, bytes)
                && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertArrayEquals(zeroed, bytes);
    }

    private static StreamProductRun run(AtomicInteger starts) {
        return new StreamProductRun() {
            @Override public void start() { starts.incrementAndGet(); }
            @Override public void cancel() { }
            @Override public void close() { }
        };
    }

    private static PlanArtifact plan(String deploymentSetId,
            String sourceDigest, String sinkDigest, boolean bounded,
            byte[] configuration, boolean transactionalSink) {
        return plan(deploymentSetId, sourceDigest, sinkDigest, bounded,
                configuration, emptyConfiguration(), transactionalSink);
    }

    private static PlanArtifact plan(String deploymentSetId,
            String sourceDigest, String sinkDigest, boolean bounded,
            byte[] sourceConfiguration, byte[] sinkConfiguration,
            boolean transactionalSink) {
        StreamSchema schema = new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id", DataType.LongType, false)));
        StreamSplitSpec split = new StreamSplitSpec(
                StreamConnectorProviderContractCodec.digestHex(source()),
                "position-v1", new byte[] {2});
        StreamSourceSpec sourceSpec = bounded
                ? StreamSourceSpec.boundedExactOrdered(schema,
                "example.connector", sourceDigest, sourceConfiguration,
                Collections.singletonList(split))
                : StreamSourceSpec.exactOrdered(schema,
                "example.connector", sourceDigest, sourceConfiguration,
                Collections.singletonList(split));
        StreamSinkSpec sinkSpec = transactionalSink
                ? StreamSinkSpec.checkpointTransactional(
                "example.connector", sinkDigest, sinkConfiguration)
                : StreamSinkSpec.eagerAtLeastOnce(
                "example.connector", sinkDigest, sinkConfiguration);
        StreamSession session = RowWeave.createStreamSession("signed-connector");
        session.setDeploymentSetId(deploymentSetId);
        if (transactionalSink) {
            session.setDeliveryGuarantee(
                    StreamDeliveryGuarantee.EXACTLY_ONCE);
            session.setCheckpointConfig(
                    StreamCheckpointConfig.periodic(60_000L, 10_000L, 2));
        }
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 0L, 0, 0,
                32_768L, 8, 32, 0L));
        session.fromSource(sourceSpec).uid("source").sinkTo("sink", sinkSpec);
        return session.toPlanArtifact();
    }

    private static StreamConnectorProviderContract source() {
        return StreamConnectorProviderContract.source(
                "signed-stream", "example.connector",
                StreamConnectorConfiguration.CODEC_ID,
                4096, 2, 30_000L, true, false, false);
    }

    private static StreamConnectorProviderContract dynamicSource(
            int maximumConcurrentOpens) {
        return StreamConnectorProviderContract.source(
                "signed-stream", "example.connector",
                StreamConnectorConfiguration.CODEC_ID,
                4096, maximumConcurrentOpens, 30_000L,
                false, true, false);
    }

    private static StreamConnectorProviderContract cdcSource(
            int maximumConcurrentOpens) {
        return StreamConnectorProviderContract.source(
                "signed-stream", "example.connector",
                StreamConnectorConfiguration.CODEC_ID,
                4096, maximumConcurrentOpens, 30_000L,
                false, false, true);
    }

    private static PlanArtifact dynamicPlan(String deploymentSetId,
            String sourceDigest, String sinkDigest, byte[] configuration) {
        StreamSchema schema = new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id", DataType.LongType, false)));
        StreamSession session = RowWeave.createStreamSession("signed-dynamic");
        session.setDeploymentSetId(deploymentSetId);
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 0L, 0, 0,
                32_768L, 8, 32, 0L));
        session.fromSource(StreamSourceSpec.dynamicExactOrdered(
                        schema, "example.connector", sourceDigest,
                        configuration, new StreamSourceDiscoverySpec(
                                10L, 1_000L, 8, 8, 8, 64, 64)))
                .uid("source")
                .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        "example.connector", sinkDigest,
                        emptyConfiguration()));
        return session.toPlanArtifact();
    }

    private static PlanArtifact cdcPlan(String deploymentSetId,
            String sourceDigest, String sinkDigest, byte[] configuration) {
        StreamSchema schema = new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id", DataType.LongType, false)));
        StreamSession session = RowWeave.createStreamSession("signed-cdc");
        session.setDeploymentSetId(deploymentSetId);
        session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
        session.setChangelogMode(StreamChangelogMode.UPSERT);
        session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                60_000L, 5_000L, 2));
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 0L, 0, 0,
                32_768L, 8, 32, 0L));
        session.fromSource(StreamSourceSpec.residentCdc(
                        schema, Collections.singletonList("id"),
                        "example.connector", sourceDigest, configuration,
                        sourceDigest, 10L,
                        Collections.singletonList("snapshot-0")))
                .uid("source")
                .sinkTo("sink", StreamSinkSpec.checkpointTransactional(
                        "example.connector", sinkDigest,
                        emptyConfiguration()));
        return session.toPlanArtifact();
    }

    private static PlanArtifact cdcV2Plan(String deploymentSetId,
            String sourceDigest, String sinkDigest, byte[] configuration) {
        StreamSchema schema = new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id",
                        DataType.LongType, false)));
        StreamSession session = RowWeave.createStreamSession("signed-cdc-v2");
        session.setDeploymentSetId(deploymentSetId);
        session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
        session.setChangelogMode(StreamChangelogMode.UPSERT);
        session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                60_000L, 5_000L, 2));
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 0L, 0, 0,
                32_768L, 8, 32, 0L));
        session.fromSource(StreamSourceSpec.residentCdcV2(
                        schema, Collections.singletonList("id"),
                        "example.connector", sourceDigest, configuration,
                        sourceDigest, 0L,
                        Collections.singletonList("snapshot-all")))
                .uid("source")
                .sinkTo("sink", StreamSinkSpec.checkpointTransactional(
                        "example.connector", sinkDigest,
                        emptyConfiguration()));
        return session.toPlanArtifact();
    }

    private static byte[] configuration(int mode) {
        return StreamConnectorConfiguration.of(
                Collections.singletonMap("mode", Integer.toString(mode)),
                Collections.emptyMap()).encode();
    }

    private static byte[] emptyConfiguration() {
        return StreamConnectorConfiguration.of(Collections.emptyMap(),
                Collections.emptyMap()).encode();
    }

    private static StreamConnectorProviderContract sink() {
        return StreamConnectorProviderContract.sink(
                "signed-stream", "example.connector",
                StreamConnectorConfiguration.CODEC_ID,
                4096, 2, 30_000L,
                StreamConnectorProviderContract.SinkVisibility.EAGER_AT_LEAST_ONCE);
    }

    private static StreamConnectorProviderContract transactionalSink() {
        return transactionalSink(2);
    }

    private static StreamConnectorProviderContract transactionalSink(
            int maximumConcurrentOpens) {
        return StreamConnectorProviderContract.sink(
                "signed-stream", "example.connector",
                StreamConnectorConfiguration.CODEC_ID,
                4096, maximumConcurrentOpens, 30_000L,
                StreamConnectorProviderContract.SinkVisibility
                        .CHECKPOINT_TRANSACTIONAL);
    }

    private static byte[] providerJar(Path temp,
            StreamConnectorProviderContract source,
            StreamConnectorProviderContract sink,
            String resourceDeploymentId) throws Exception {
        Path sources = temp.resolve("stream-connector-src");
        Path classes = temp.resolve("stream-connector-classes");
        Files.createDirectories(sources.resolve("example"));
        Files.createDirectories(classes);
        Path sourceFile = sources.resolve("example/SignedStreamSource.java");
        Path sinkFile = sources.resolve("example/SignedStreamSink.java");
        String sourceProviderType = source.dynamicDiscovery()
                ? "io.rowweave.stream.spi.StreamDynamicSourceProvider"
                : source.residentCdc()
                ? "io.rowweave.stream.spi.StreamCdcSourceProvider"
                : "io.rowweave.stream.spi.StreamSourceProvider";
        String sourceExtra = source.dynamicDiscovery()
                ? dynamicEnumeratorSource()
                : source.residentCdc() ? cdcReaderSource() : "";
        String sourceOpen = source.residentCdc()
                ? ""
                : standardReaderSource();
        String sourceImplementation =
                "package example; public final class SignedStreamSource "
                + "implements " + sourceProviderType + " {"
                + "static { System.setProperty(\"" + INITIALIZED + "\", \"source\"); }"
                + "static { System.setProperty(\"" + SOURCE_INITIALIZED
                + "\", \"true\"); }"
                + "public String providerKey(){return \"example.connector\";}"
                + "public String contractDigest(){return \""
                + StreamConnectorProviderContractCodec.digestHex(source) + "\";}"
                + sourceOpen
                + sourceExtra
                + "}";
        Files.write(sourceFile,
                sourceImplementation.getBytes(StandardCharsets.UTF_8));
        boolean transactional = sink.sinkVisibility()
                == StreamConnectorProviderContract.SinkVisibility
                .CHECKPOINT_TRANSACTIONAL;
        String sinkSource = transactional
                ? transactionalSinkSource(sink)
                : "package example; public final class SignedStreamSink "
                + "implements io.rowweave.stream.spi.StreamSinkProvider {"
                + "static { System.setProperty(\"" + INITIALIZED + "\", \"sink\"); }"
                + "static { System.setProperty(\"" + SINK_INITIALIZED
                + "\", \"true\"); }"
                + "public String providerKey(){return \"example.connector\";}"
                + "public String contractDigest(){return \""
                + StreamConnectorProviderContractCodec.digestHex(sink) + "\";}"
                + "public io.rowweave.stream.spi.StreamSinkWriter open("
                + "io.rowweave.stream.spi.StreamSinkContext c){"
                + "return new io.rowweave.stream.spi.StreamSinkWriter(){"
                + "public void write(byte[] frame){"
                + "long value=new io.rowweave.stream.api.StreamDataRowCodec("
                + "new io.rowweave.stream.api.StreamSchema("
                + "java.util.Collections.singletonList(new "
                + "io.rowweave.stream.api.StreamField(\"id\",\"id\",\"id\","
                + "io.rowweave.DataType.LongType,false))))"
                + ".decode(frame).getLong(0);"
                + "System.setProperty(\"" + RESULT + "\",Long.toString(value));}"
                + "};}"
                + "}";
        Files.write(sinkFile, sinkSource.getBytes(StandardCharsets.UTF_8));
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null || compiler.run(null, null, null,
                "-source", "8", "-target", "8",
                "-classpath", System.getProperty("java.class.path"),
                "-d", classes.toString(), sourceFile.toString(),
                sinkFile.toString()) != 0) {
            throw new AssertionError("signed Stream connector compilation failed");
        }
        Map<String, byte[]> entries = new LinkedHashMap<>();
        java.util.List<String> classNames = new java.util.ArrayList<>(
                Arrays.asList(SOURCE_CLASS, SOURCE_CLASS + "$1",
                        SINK_CLASS, SINK_CLASS + "$1"));
        if (source.dynamicDiscovery()) {
            classNames.add(SOURCE_CLASS + "$2");
        }
        for (String name : classNames) {
            String resource = name.replace('.', '/') + ".class";
            entries.put(resource, Files.readAllBytes(classes.resolve(resource)));
        }
        entries.put(StreamConnectorProviderRegistry.SOURCE_SERVICE_RESOURCE,
                (SOURCE_CLASS + '=' + SOURCE_RESOURCE + '\n')
                        .getBytes(StandardCharsets.UTF_8));
        entries.put(transactional
                        ? StreamConnectorProviderRegistry
                                .TRANSACTIONAL_SINK_SERVICE_RESOURCE
                        : StreamConnectorProviderRegistry.SINK_SERVICE_RESOURCE,
                (SINK_CLASS + '=' + SINK_RESOURCE + '\n')
                        .getBytes(StandardCharsets.UTF_8));
        entries.put(SOURCE_RESOURCE,
                StreamConnectorProviderContractCodec.encode(
                        StreamConnectorProviderContract.source(
                                resourceDeploymentId, source.providerKey(),
                                source.configurationCodecId(),
                                source.maximumConfigurationBytes(),
                                source.maximumConcurrentOpens(),
                                source.openTimeoutMillis(),
                                source.boundedEndOfInput(),
                                source.dynamicDiscovery(), source.residentCdc(),
                                source.externalCheckpointAcknowledgement())));
        entries.put(SINK_RESOURCE,
                StreamConnectorProviderContractCodec.encode(
                        StreamConnectorProviderContract.sink(
                                resourceDeploymentId, sink.providerKey(),
                                sink.configurationCodecId(),
                                sink.maximumConfigurationBytes(),
                                sink.maximumConcurrentOpens(),
                                sink.openTimeoutMillis(), sink.sinkVisibility())));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JarOutputStream output = new JarOutputStream(bytes)) {
            for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
                JarEntry jarEntry = new JarEntry(entry.getKey());
                jarEntry.setTime(0L);
                output.putNextEntry(jarEntry);
                output.write(entry.getValue());
                output.closeEntry();
            }
        }
        return bytes.toByteArray();
    }

    private static String dynamicEnumeratorSource() {
        return "public io.rowweave.stream.spi.StreamSourceEnumerator "
                + "openEnumerator(io.rowweave.stream.spi."
                + "StreamSourceEnumeratorContext c) throws Exception{"
                + "if(!io.rowweave.stream.api.StreamConnectorConfiguration"
                + ".decode(c.configurationFrame()).secretReferences().isEmpty())"
                + "c.secrets().withSecret(\"password\",secret->{"
                + "System.setProperty(\"" + SECRET_SEEN + "\",\"true\");"
                + "return null;});"
                + "System.setProperty(\"" + OPEN_COUNT + "\",Integer.toString(1+"
                + "Integer.parseInt(System.getProperty(\"" + OPEN_COUNT
                + "\",\"0\"))));"
                + "return new io.rowweave.stream.spi.StreamSourceEnumerator(){"
                + "public io.rowweave.stream.spi.StreamSplitDiscovery discover("
                + "io.rowweave.stream.spi.StreamDiscoveryCursor cursor,int limit){"
                + "return new io.rowweave.stream.spi.StreamSplitDiscovery(1L,"
                + "cursor,java.util.Collections.emptyList());}"
                + "public void close(){}"
                + "};}";
    }

    private static String standardReaderSource() {
        return "public io.rowweave.stream.spi.StreamSourceReader open("
                + "io.rowweave.stream.spi.StreamSourceContext c) throws Exception{"
                + "if(!io.rowweave.stream.api.StreamConnectorConfiguration"
                + ".decode(c.configurationFrame()).secretReferences().isEmpty())"
                + "c.secrets().withSecret(\"password\",secret->{"
                + "System.setProperty(\"" + SECRET_SEEN + "\","
                + "Boolean.toString(java.util.Arrays.equals(secret,"
                + "\"temporary\".getBytes(java.nio.charset.StandardCharsets.UTF_8))));"
                + "return null;});"
                + "System.setProperty(\"" + OPEN_COUNT + "\",Integer.toString(1+"
                + "Integer.parseInt(System.getProperty(\"" + OPEN_COUNT
                + "\",\"0\"))));"
                + "return new io.rowweave.stream.spi.StreamSourceReader(){"
                + "int position=c.hasRestoredPosition()?c.restoredPositionFrame()[0]:0;"
                + "public io.rowweave.stream.spi.StreamSourceRecord next(){"
                + "if(position!=0)return null;position=1;"
                + "io.rowweave.DataRow row=new io.rowweave.DataRow(1);row.set(0,42L);"
                + "return io.rowweave.stream.spi.StreamSourceRecord.of("
                + "new io.rowweave.stream.api.StreamDataRowCodec("
                + "new io.rowweave.stream.api.StreamSchema("
                + "java.util.Collections.singletonList(new "
                + "io.rowweave.stream.api.StreamField(\"id\",\"id\",\"id\","
                + "io.rowweave.DataType.LongType,false)))).encode(row),"
                + "new byte[]{1});}"
                + "public io.rowweave.stream.spi.StreamSourceEvent nextEvent(){"
                + "if(position==0)return io.rowweave.stream.spi.StreamSourceEvent"
                + ".record(next());int mode=Integer.parseInt("
                + "io.rowweave.stream.api.StreamConnectorConfiguration"
                + ".decode(c.configurationFrame()).options().get(\"mode\"));"
                + "if(mode==7)throw new IllegalStateException(\"injected failure\");"
                + "return mode>=9?null:io.rowweave.stream.spi.StreamSourceEvent"
                + ".endOfInput();}"
                + "public byte[] checkpointPositionFrame(){"
                + "return new byte[]{(byte)position};}"
                + "public void cancel(){}public void close(){}"
                + "};}";
    }

    private static String cdcReaderSource() {
        return "public io.rowweave.stream.spi.StreamCdcSourceReader "
                + "openCdc(io.rowweave.stream.spi.StreamCdcSourceContext c)"
                + " throws Exception{"
                + "if(!io.rowweave.stream.api.StreamConnectorConfiguration"
                + ".decode(c.configurationFrame()).secretReferences().isEmpty())"
                + "c.secrets().withSecret(\"password\",secret->{"
                + "System.setProperty(\"" + SECRET_SEEN + "\",\"true\");"
                + "return null;});"
                + "System.setProperty(\"" + OPEN_COUNT + "\",Integer.toString(1+"
                + "Integer.parseInt(System.getProperty(\"" + OPEN_COUNT
                + "\",\"0\"))));"
                + "return new io.rowweave.stream.spi.StreamCdcSourceReader(){"
                + "private final java.util.concurrent.CountDownLatch stop="
                + "new java.util.concurrent.CountDownLatch(1);"
                + "public io.rowweave.stream.spi.StreamCdcSourceEvent next()"
                + " throws Exception{stop.await();return null;}"
                + "public void notifyCheckpointComplete("
                + "io.rowweave.stream.spi.StreamCdcCheckpointCompletion c){"
                + "System.setProperty(\"" + CDC_COMPLETION + "\","
                + "c.checkpointSequence()+\":\"+c.lastLogSequence()+\":\"+"
                + "c.externalPositionFrame()[0]);}"
                + "public void cancel(){stop.countDown();}"
                + "public void close(){stop.countDown();}"
                + "};}";
    }

    private static String transactionalSinkSource(
            StreamConnectorProviderContract sink) {
        return "package example; public final class SignedStreamSink "
                + "implements io.rowweave.stream.spi.StreamTransactionalSinkProvider {"
                + "static { System.setProperty(\"" + SINK_INITIALIZED
                + "\",\"true\"); }"
                + "static final java.util.concurrent.ConcurrentHashMap<String,byte[]>"
                + " PREPARED=new java.util.concurrent.ConcurrentHashMap<>();"
                + "static final java.util.Set<String> VISIBLE=java.util.Collections"
                + ".newSetFromMap(new java.util.concurrent.ConcurrentHashMap"
                + "<String,Boolean>());"
                + "static final java.util.Set<String> ABORTED=java.util.Collections"
                + ".newSetFromMap(new java.util.concurrent.ConcurrentHashMap"
                + "<String,Boolean>());"
                + "public String providerKey(){return \"example.connector\";}"
                + "public String contractDigest(){return \""
                + StreamConnectorProviderContractCodec.digestHex(sink) + "\";}"
                + "public io.rowweave.stream.spi.StreamTransactionalSinkWriter open("
                + "io.rowweave.stream.spi.StreamTransactionalSinkContext c){"
                + "System.setProperty(\"" + SINK_OPEN_COUNT + "\",Integer.toString(1+"
                + "Integer.parseInt(System.getProperty(\"" + SINK_OPEN_COUNT
                + "\",\"0\"))));"
                + "return new io.rowweave.stream.spi.StreamTransactionalSinkWriter(){"
                + "byte[] pending;"
                + "public void write(byte[] frame){pending=frame.clone();"
                + "System.setProperty(\"" + PENDING_WRITE + "\",\"true\");}"
                + "public byte[] prepare(String id){"
                + "if(pending==null)throw new IllegalStateException(\"no write\");"
                + "PREPARED.putIfAbsent(id,pending.clone());pending=null;"
                + "return id.getBytes(java.nio.charset.StandardCharsets.UTF_8);}"
                + "public io.rowweave.stream.spi.StreamTransactionStatus query("
                + "String id){return VISIBLE.contains(id)?"
                + "io.rowweave.stream.spi.StreamTransactionStatus.VISIBLE:"
                + "ABORTED.contains(id)?"
                + "io.rowweave.stream.spi.StreamTransactionStatus.ABORTED:"
                + "PREPARED.containsKey(id)?"
                + "io.rowweave.stream.spi.StreamTransactionStatus.PREPARED:"
                + "io.rowweave.stream.spi.StreamTransactionStatus.UNKNOWN;}"
                + "public void commit(String id){if(VISIBLE.contains(id))return;"
                + "byte[] frame=PREPARED.get(id);"
                + "if(frame==null)throw new IllegalStateException(\"not prepared\");"
                + "long value=new io.rowweave.stream.api.StreamDataRowCodec("
                + "new io.rowweave.stream.api.StreamSchema("
                + "java.util.Collections.singletonList(new "
                + "io.rowweave.stream.api.StreamField(\"id\",\"id\",\"id\","
                + "io.rowweave.DataType.LongType,false))))"
                + ".decode(frame).getLong(0);"
                + "System.setProperty(\"" + RESULT + "\",Long.toString(value));"
                + "VISIBLE.add(id);}"
                + "public void finalizeTransaction(String id){"
                + "if(VISIBLE.contains(id))PREPARED.remove(id);}"
                + "public void abort(String id){if(!VISIBLE.contains(id)){"
                + "PREPARED.remove(id);ABORTED.add(id);}}"
                + "public void close(){}"
                + "};}"
                + "}";
    }

    private static VerifiedDeploymentSet deployment(Path temp, byte[] jar)
            throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(3072);
        KeyPair signer = generator.generateKeyPair();
        Path path = temp.resolve("signed-stream.jar").toAbsolutePath();
        Files.write(path, jar);
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(jar);
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(signer.getPrivate());
        signature.update(DeploymentManifest.signingPayload(
                "signed-stream", jar.length, digest));
        DeploymentManifest manifest = new DeploymentManifest(
                "signed-stream", path, jar.length, digest,
                "test-signer", signature.sign());
        return VerifiedDeploymentSet.load(Collections.singletonList(manifest),
                Collections.singletonMap("test-signer", signer.getPublic()),
                DeploymentMemoryBudget.forTesting(16L * 1024L * 1024L));
    }
}
