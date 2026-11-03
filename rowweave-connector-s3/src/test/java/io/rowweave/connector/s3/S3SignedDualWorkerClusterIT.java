package io.rowweave.connector.s3;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.DataRow;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamClusterMain;
import io.rowweave.StreamClusterRuntime;
import io.rowweave.StreamMasterRuntime;
import io.rowweave.StreamSession;
import io.rowweave.StreamWorkerRuntime;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.deployment.DeploymentManifest;
import io.rowweave.deployment.DeploymentManifestCodec;
import io.rowweave.deployment.DeploymentMemoryBudget;
import io.rowweave.deployment.DeploymentRuntime;
import io.rowweave.deployment.DeploymentRuntimeConfig;
import io.rowweave.deployment.PortableFunctionProviderRegistry;
import io.rowweave.deployment.StreamConnectorProviderRegistry;
import io.rowweave.deployment.VerifiedDeploymentSet;
import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.portable.PortableFunctionProviderContract;
import io.rowweave.portable.PortableFunctionProviderContractCodec;
import io.rowweave.stream.api.StreamCalcSpec;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamConnectorConfiguration;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamExpression;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobProgress;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamDataRowCodec;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.api.StreamWorkerConfig;
import io.rowweave.stream.contract.StreamConnectorProviderContract;
import io.rowweave.stream.contract.StreamConnectorProviderContractCodec;
import io.rowweave.stream.spi.StreamSecretResolver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.Signature;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.VersioningConfiguration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Actual signed S3 image executed by an independent Master and two Workers. */
class S3SignedDualWorkerClusterIT {
    private static final String TLS_PASSWORD = "changeit";
    private static final String SINK_DEPLOYMENT = "rowweave-test-file-sink";
    private static final String SINK_PROVIDER = "rowweave.test.file.sink";
    private static final String TRANSACTIONAL_SINK_PROVIDER =
            "rowweave.test.file.transactional-sink";
    private static final String SINK_CLASS = "test.connector.SignedSink";
    private static final String SINK_CONTRACT_RESOURCE =
            "META-INF/rowweave/stream-connector/contracts/test-sink.bin";
    private static final String TRANSACTIONAL_SINK_CLASS =
            SINK_CLASS + "$TransactionalSink";
    private static final String TRANSACTIONAL_SINK_CONTRACT_RESOURCE =
            "META-INF/rowweave/stream-connector/contracts/"
                    + "test-transactional-sink.bin";
    private static final String AGGREGATE_CLASS =
            SINK_CLASS + "$PassAggregate";
    private static final String AGGREGATE_PROVIDER_CLASS =
            SINK_CLASS + "$AggregateProvider";
    private static final String AGGREGATE_CONTRACT_RESOURCE =
            "META-INF/rowweave/functions/s3-test-aggregate.bin";
    private static final String RESULT_PROPERTY =
            "rowweave.test.s3.signed-cluster.rows";
    private static final String ACCESS_KEY = "rowweave-cluster-access";
    private static final String SECRET_KEY = "rowweave-cluster-secret";
    private static final String BUCKET = "ledger-snapshots";
    private static final StreamSchema SCHEMA = new StreamSchema(
            Collections.singletonList(new StreamField(
                    "id", "id", "id", DataType.LongType, false)));
    private static final StreamConnectorProviderContract SINK_CONTRACT =
            StreamConnectorProviderContract.sink(
                    SINK_DEPLOYMENT, SINK_PROVIDER,
                    StreamConnectorConfiguration.CODEC_ID,
                    4096, 8, 30_000L,
                    StreamConnectorProviderContract.SinkVisibility
                            .EAGER_AT_LEAST_ONCE);
    private static final StreamConnectorProviderContract
            TRANSACTIONAL_SINK_CONTRACT =
            StreamConnectorProviderContract.sink(
                    SINK_DEPLOYMENT, TRANSACTIONAL_SINK_PROVIDER,
                    StreamConnectorConfiguration.CODEC_ID,
                    4096, 8, 30_000L,
                    StreamConnectorProviderContract.SinkVisibility
                            .CHECKPOINT_TRANSACTIONAL);
    private static final FunctionResourceProfile AGGREGATE_RESOURCES =
            new FunctionResourceProfile(2_048L, 0L, 1);
    private static final PortableFunctionProviderContract AGGREGATE_CONTRACT =
            new PortableFunctionProviderContract(
                    PortableFunctionProviderContract.CURRENT_VERSION,
                    AGGREGATE_CLASS, FunctionAbi.STREAM_AGGREGATE_V1,
                    PortableFunctionProviderRegistry.OPAQUE_CONFIGURATION_CODEC,
                    1, AGGREGATE_RESOURCES);

    @AfterEach
    void clearResult() {
        System.clearProperty(RESULT_PROPERTY);
        S3TestSecretResolver.reset();
    }

    @Test
    void configuredClusterLoadsSignedImagesAndWorkerLocalResolver(
            @TempDir Path temp) throws Exception {
        Path s3Image = Paths.get(System.getProperty("s3.deployment.jar"))
                .toAbsolutePath().normalize();
        Path sinkImage = createSinkImage(temp);
        KeyPair signer = signer();
        List<DeploymentManifest> manifests = Arrays.asList(
                sign(s3Image, S3ConnectorContract.DEPLOYMENT_ID, signer),
                sign(sinkImage, SINK_DEPLOYMENT, signer));
        Map<String, java.security.PublicKey> trust =
                Collections.singletonMap("s3-cluster-test",
                        signer.getPublic());
        String deploymentSetId;
        try (VerifiedDeploymentSet expected = load(manifests, trust)) {
            deploymentSetId = expected.deploymentSetId();
        }

        Path trustRoot = Files.createDirectory(
                temp.resolve("deployment-trust")).toAbsolutePath();
        Path signerPath = trustRoot.resolve("s3-cluster-test.der");
        Files.write(signerPath, signer.getPublic().getEncoded());
        List<Path> manifestPaths = new ArrayList<>();
        for (int index = 0; index < manifests.size(); index++) {
            Path path = trustRoot.resolve("connector-" + index + ".rwdep");
            Files.write(path, DeploymentManifestCodec.encode(
                    manifests.get(index)));
            manifestPaths.add(path);
        }
        byte[] credentials = credentialFrame();
        try {
            readOnly(signerPath, false);
            for (Path path : manifestPaths) readOnly(path, false);
            readOnly(trustRoot, true);
            S3TestSecretResolver.configure(credentials);
            List<String> requests = new CopyOnWriteArrayList<>();
            try (ProtocolEndpoint endpoint = new ProtocolEndpoint(requests);
                 StreamClusterRuntime runtime = StreamClusterMain.start(
                         processProperties(temp.resolve("configured-cluster"),
                                 trustRoot, signerPath, manifestPaths));
                 StreamJobClient client = runtime.createLoopbackClient()) {
                S3ConnectorConfiguration configuration =
                        configuration(endpoint.port());
                S3Manifest manifest = S3Manifest.of(SCHEMA, Arrays.asList(
                        descriptor("frozen/a.ndjson", "version-a",
                                "\"etag-a\"", ProtocolEndpoint.FIRST),
                        descriptor("frozen/b.ndjson", "version-b",
                                "\"etag-b\"", ProtocolEndpoint.SECOND)));
                StreamSinkSpec sink = StreamSinkSpec.eagerAtLeastOnce(
                        SINK_PROVIDER,
                        StreamConnectorProviderContractCodec.digestHex(
                                SINK_CONTRACT),
                        StreamConnectorConfiguration.of(
                                Collections.<String, String>emptyMap(),
                                Collections.<String, String>emptyMap())
                                .encode());
                StreamSession session = RowWeave.createStreamSession(
                        "configured-signed-s3");
                session.setDeploymentSetId(deploymentSetId);
                session.setChangelogMode(StreamChangelogMode.UPSERT);
                session.setResourceProfile(new StreamResourceProfile(
                        8, 4_096, 8_192, 8_192L, 0, 0,
                        32_768L, 8, 32, 8_192L));
                session.fromSource(manifest.toSourceSpec(configuration))
                        .uid("s3-source")
                        .calc(identityCalc(), SCHEMA).uid("identity-calc")
                        .keyBy("id").uid("id-key")
                        .keyedAggregate(aggregate(deploymentSetId, sinkImage),
                                SCHEMA, aggregateState())
                        .uid("id-aggregate")
                        .sinkTo("signed-sink", sink);
                StreamJobHandle handle = client.submit(
                        StreamSubmissionId.of("configured-signed-s3"),
                        session.toPlanArtifact());
                assertEquals(StreamJobStatus.FINISHED,
                        handle.awaitTermination(30_000L));
                assertTrue(S3TestSecretResolver.resolutions() > 0);
                assertEquals(2, requests.size(), requests.toString());
            }
        } finally {
            S3TestSecretResolver.reset();
            Arrays.fill(credentials, (byte) 0);
            writable(trustRoot, true);
            writable(signerPath, false);
            for (Path path : manifestPaths) writable(path, false);
        }
    }

    @Test
    void signedImageRunsAcrossIndependentDualWorkers(@TempDir Path temp)
            throws Exception {
        Path s3Image = Paths.get(System.getProperty("s3.deployment.jar"))
                .toAbsolutePath().normalize();
        assertTrue(Files.isRegularFile(s3Image), s3Image.toString());
        Path sinkImage = createSinkImage(temp);
        KeyPair signer = signer();
        List<DeploymentManifest> manifests = Arrays.asList(
                sign(s3Image, S3ConnectorContract.DEPLOYMENT_ID, signer),
                sign(sinkImage, SINK_DEPLOYMENT, signer));
        Map<String, java.security.PublicKey> trust =
                Collections.singletonMap("s3-cluster-test",
                        signer.getPublic());

        byte[] credentials = credentialFrame();
        AtomicInteger firstResolutions = new AtomicInteger();
        AtomicInteger secondResolutions = new AtomicInteger();
        StreamSecretResolver firstResolver = resolver(
                credentials, firstResolutions);
        StreamSecretResolver secondResolver = resolver(
                credentials, secondResolutions);
        List<String> requests = new CopyOnWriteArrayList<>();
        try (ProtocolEndpoint endpoint = new ProtocolEndpoint(requests);
             VerifiedDeploymentSet masterDeployments = load(manifests, trust);
             VerifiedDeploymentSet firstDeployments = load(manifests, trust);
             VerifiedDeploymentSet secondDeployments = load(manifests, trust);
             StreamWorkerRuntime first = RowWeave.startStreamWorker(
                     worker("s3-worker-a", temp.resolve("worker-a")),
                     firstDeployments, firstResolver);
             StreamWorkerRuntime second = RowWeave.startStreamWorker(
                     worker("s3-worker-b", temp.resolve("worker-b")),
                     secondDeployments, secondResolver);
             StreamMasterRuntime master = RowWeave.startStreamMaster(
                     temp.resolve("master"),
                     Arrays.asList(first.endpoint(), second.endpoint()),
                     ControlSecurityConfig.fromProperties(new Properties()),
                     0, masterDeployments);
             StreamJobClient client = master.createJobClient()) {
            assertEquals(masterDeployments.deploymentSetId(),
                    firstDeployments.deploymentSetId());
            assertEquals(masterDeployments.deploymentSetId(),
                    secondDeployments.deploymentSetId());
            S3ConnectorConfiguration configuration =
                    configuration(endpoint.port());
            S3Manifest manifest = S3Manifest.of(SCHEMA, Arrays.asList(
                    descriptor("frozen/a.ndjson", "version-a",
                            "\"etag-a\"", ProtocolEndpoint.FIRST),
                    descriptor("frozen/b.ndjson", "version-b",
                            "\"etag-b\"", ProtocolEndpoint.SECOND)));
            StreamSourceSpec source = manifest.toSourceSpec(configuration);
            StreamSinkSpec sink = StreamSinkSpec.eagerAtLeastOnce(
                    SINK_PROVIDER,
                    StreamConnectorProviderContractCodec.digestHex(
                            SINK_CONTRACT),
                    StreamConnectorConfiguration.of(
                            Collections.<String, String>emptyMap(),
                            Collections.<String, String>emptyMap()).encode());

            StreamSession session = RowWeave.createStreamSession(
                    "signed-s3-dual-worker");
            session.setDeploymentSetId(
                    masterDeployments.deploymentSetId());
            session.setParallelism(2);
            session.setChangelogMode(StreamChangelogMode.UPSERT);
            session.setResourceProfile(new StreamResourceProfile(
                    8, 4_096, 8_192, 8_192L, 0, 0,
                    32_768L, 8, 32, 8_192L));
            session.fromSource(source).uid("s3-source")
                    .calc(identityCalc(), SCHEMA).uid("identity-calc")
                    .keyBy("id").uid("id-key")
                    .keyedAggregate(aggregate(
                                    masterDeployments.deploymentSetId(),
                                    sinkImage), SCHEMA, aggregateState())
                    .uid("id-aggregate")
                    .sinkTo("signed-sink", sink);
            StreamJobHandle handle = client.submit(
                    StreamSubmissionId.of("signed-s3-dual-worker"),
                    session.toPlanArtifact());
            StreamJobStatus terminal = handle.awaitTermination(30_000L);
            assertEquals(StreamJobStatus.FINISHED, terminal,
                    "progress=" + handle.progress()
                            + ", firstResolutions=" + firstResolutions.get()
                            + ", secondResolutions=" + secondResolutions.get()
                            + ", requests=" + requests
                            + ", rows=" + System.getProperty(
                            RESULT_PROPERTY, ""));

            List<String> rows = new ArrayList<>(Arrays.asList(
                    System.getProperty(RESULT_PROPERTY, "").split(",")));
            rows.removeAll(Collections.singleton(""));
            Collections.sort(rows);
            assertEquals(Arrays.asList("1", "2"), rows);
            assertTrue(firstResolutions.get() > 0,
                    "first Worker did not open an S3 split");
            assertTrue(secondResolutions.get() > 0,
                    "second Worker did not open an S3 split");
            assertEquals(2, requests.size(), requests.toString());
        } finally {
            Arrays.fill(credentials, (byte) 0);
        }
    }

    @Test
    void signedBoundedSourceCommitsTransactionalSinkAtTerminalCheckpoint(
            @TempDir Path temp) throws Exception {
        Path s3Image = Paths.get(System.getProperty("s3.deployment.jar"))
                .toAbsolutePath().normalize();
        Path sinkImage = createSinkImage(temp);
        KeyPair signer = signer();
        List<DeploymentManifest> manifests = Arrays.asList(
                sign(s3Image, S3ConnectorContract.DEPLOYMENT_ID, signer),
                sign(sinkImage, SINK_DEPLOYMENT, signer));
        Map<String, java.security.PublicKey> trust =
                Collections.singletonMap("s3-cluster-test",
                        signer.getPublic());

        byte[] credentials = credentialFrame();
        AtomicInteger firstResolutions = new AtomicInteger();
        AtomicInteger secondResolutions = new AtomicInteger();
        Path visible = Files.createDirectory(temp.resolve("visible"));
        List<String> requests = new CopyOnWriteArrayList<>();
        try (ProtocolEndpoint endpoint = new ProtocolEndpoint(requests);
             VerifiedDeploymentSet masterDeployments = load(manifests, trust);
             VerifiedDeploymentSet firstDeployments = load(manifests, trust);
             VerifiedDeploymentSet secondDeployments = load(manifests, trust);
             StreamWorkerRuntime first = RowWeave.startStreamWorker(
                     worker("s3-transactional-worker-a",
                             temp.resolve("transactional-worker-a")),
                     firstDeployments, resolver(credentials, firstResolutions));
             StreamWorkerRuntime second = RowWeave.startStreamWorker(
                     worker("s3-transactional-worker-b",
                             temp.resolve("transactional-worker-b")),
                     secondDeployments,
                     resolver(credentials, secondResolutions));
             StreamMasterRuntime master = RowWeave.startStreamMaster(
                     temp.resolve("transactional-master"),
                     Arrays.asList(first.endpoint(), second.endpoint()),
                     ControlSecurityConfig.fromProperties(new Properties()),
                     0, masterDeployments);
             StreamJobClient client = master.createJobClient()) {
            S3ConnectorConfiguration configuration =
                    configuration(endpoint.port());
            S3Manifest manifest = S3Manifest.of(SCHEMA, Arrays.asList(
                    descriptor("frozen/a.ndjson", "version-a",
                            "\"etag-a\"", ProtocolEndpoint.FIRST),
                    descriptor("frozen/b.ndjson", "version-b",
                            "\"etag-b\"", ProtocolEndpoint.SECOND)));
            StreamSinkSpec sink = StreamSinkSpec.checkpointTransactional(
                    TRANSACTIONAL_SINK_PROVIDER,
                    StreamConnectorProviderContractCodec.digestHex(
                            TRANSACTIONAL_SINK_CONTRACT),
                    StreamConnectorConfiguration.of(
                            Collections.singletonMap("output.path",
                                    visible.toString()),
                            Collections.<String, String>emptyMap()).encode());

            StreamSession session = RowWeave.createStreamSession(
                    "signed-s3-transactional");
            session.setDeploymentSetId(
                    masterDeployments.deploymentSetId());
            session.setParallelism(2);
            session.setDeliveryGuarantee(
                    StreamDeliveryGuarantee.EXACTLY_ONCE);
            session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                    60_000L, 10_000L, 2));
            session.setResourceProfile(new StreamResourceProfile(
                    8, 4_096, 8_192, 0L, 0, 0,
                    32_768L, 8, 32, 0L));
            session.fromSource(manifest.toSourceSpec(configuration))
                    .uid("s3-transactional-source")
                    .calc(identityCalc(), SCHEMA)
                    .uid("s3-transactional-calc")
                    .sinkTo("signed-transactional-sink", sink);

            StreamJobHandle handle = client.submit(
                    StreamSubmissionId.of("signed-s3-transactional"),
                    session.toPlanArtifact());
            assertEquals(StreamJobStatus.FINISHED,
                    handle.awaitTermination(30_000L));
            assertEquals(1L, handle.progress().completedCheckpoints());
            assertEquals(Arrays.asList("1", "2"),
                    transactionalRows(visible));
            assertTrue(firstResolutions.get() > 0);
            assertTrue(secondResolutions.get() > 0);
            assertEquals(2, requests.size(), requests.toString());
        } finally {
            Arrays.fill(credentials, (byte) 0);
        }
    }

    @Test
    @Timeout(120)
    void configuredSignedImagesRunInIndependentMasterAndDualWorkerProcesses(
            @TempDir Path temp) throws Exception {
        try (S3QualificationService service =
                     S3QualificationService.protocol()) {
            runConfiguredSignedImagesInIndependentProcesses(temp, service);
        }
    }

    @Test
    @Timeout(120)
    void realMinioRunsSignedImagesInIndependentMasterAndDualWorkerProcesses(
            @TempDir Path temp) throws Exception {
        String configuredBinary = System.getProperty("minio.binary");
        Assumptions.assumeTrue(configuredBinary != null,
                "set -Dminio.binary to run real MinIO cluster qualification");
        Path binary = Paths.get(configuredBinary).toAbsolutePath().normalize();
        Assumptions.assumeTrue(Files.isExecutable(binary),
                "MinIO binary is not executable: " + binary);
        try (S3QualificationService service =
                     S3QualificationService.minio(
                             binary, temp.resolve("minio"))) {
            runConfiguredSignedImagesInIndependentProcesses(temp, service);
        }
    }

    private void runConfiguredSignedImagesInIndependentProcesses(
            Path temp, S3QualificationService service) throws Exception {
        Path s3Image = Paths.get(System.getProperty("s3.deployment.jar"))
                .toAbsolutePath().normalize();
        Path sinkImage = createSinkImage(temp);
        KeyPair signer = signer();
        List<DeploymentManifest> manifests = Arrays.asList(
                sign(s3Image, S3ConnectorContract.DEPLOYMENT_ID, signer),
                sign(sinkImage, SINK_DEPLOYMENT, signer));
        Map<String, java.security.PublicKey> trust =
                Collections.singletonMap("s3-cluster-test",
                        signer.getPublic());
        String deploymentSetId;
        try (VerifiedDeploymentSet expected = load(manifests, trust)) {
            deploymentSetId = expected.deploymentSetId();
        }

        Path trustRoot = Files.createDirectory(
                temp.resolve("process-deployment-trust")).toAbsolutePath();
        Path signerPath = trustRoot.resolve("s3-cluster-test.der");
        Files.write(signerPath, signer.getPublic().getEncoded());
        List<Path> manifestPaths = new ArrayList<>();
        for (int index = 0; index < manifests.size(); index++) {
            Path path = trustRoot.resolve("connector-" + index + ".rwdep");
            Files.write(path, DeploymentManifestCodec.encode(
                    manifests.get(index)));
            manifestPaths.add(path);
        }

        Identity masterIdentity = identity(temp, "signed-stream-master");
        Identity workerAIdentity = identity(temp, "signed-stream-worker-a");
        Identity workerBIdentity = identity(temp, "signed-stream-worker-b");
        Identity clientIdentity = identity(temp, "signed-stream-client");
        Path trustStore = temp.resolve("signed-stream-trust.p12");
        importCertificate(trustStore, "master", masterIdentity.certificate);
        importCertificate(trustStore, "worker-a", workerAIdentity.certificate);
        importCertificate(trustStore, "worker-b", workerBIdentity.certificate);
        importCertificate(trustStore, "client", clientIdentity.certificate);
        Path authz = temp.resolve("signed-stream-authz.txt");
        Files.write(authz, (masterIdentity.fingerprint + "=MASTER\n"
                + workerAIdentity.fingerprint + "=WORKER\n"
                + workerBIdentity.fingerprint + "=WORKER\n"
                + clientIdentity.fingerprint + "=CLIENT\n")
                .getBytes(StandardCharsets.US_ASCII));

        byte[] credentials = credentialFrame();
        Path secretPath = temp.resolve("worker-secret.bin");
        Files.write(secretPath, credentials);
        Path markerA = temp.resolve("worker-a-secret.marker");
        Path markerB = temp.resolve("worker-b-secret.marker");
        Path resultPath = Files.createDirectories(
                temp.resolve("signed-process-result"));
        Path workerALog = temp.resolve("worker-a.log");
        Path workerBLog = temp.resolve("worker-b.log");
        Path masterLog = temp.resolve("master.log");
        Process workerA = null;
        Process workerB = null;
        Process master = null;
        try {
            readOnly(signerPath, false);
            for (Path path : manifestPaths) readOnly(path, false);
            readOnly(trustRoot, true);
            workerA = launchProcess(processCommand(
                    temp.resolve("worker-a-state"), workerAIdentity,
                    trustStore, authz, trustRoot, signerPath, manifestPaths,
                    false, "signed-worker-a", null, secretPath, markerA),
                    workerALog);
            workerB = launchProcess(processCommand(
                    temp.resolve("worker-b-state"), workerBIdentity,
                    trustStore, authz, trustRoot, signerPath, manifestPaths,
                    false, "signed-worker-b", null, secretPath, markerB),
                    workerBLog);
            String endpointA = awaitReady(workerALog,
                    "STREAM_WORKER_READY=", workerA, 20_000L);
            String endpointB = awaitReady(workerBLog,
                    "STREAM_WORKER_READY=", workerB, 20_000L);
            master = launchProcess(processCommand(
                    temp.resolve("master-state"), masterIdentity,
                    trustStore, authz, trustRoot, signerPath, manifestPaths,
                    true, null, endpointA + ',' + endpointB,
                    null, null), masterLog);
            String masterEndpoint = awaitReady(masterLog,
                    "STREAM_MASTER_READY=", master, 20_000L);
            int separator = masterEndpoint.lastIndexOf(':');
            ControlSecurityConfig clientSecurity = security(
                    clientIdentity, trustStore, authz)
                    .withDeploymentSetId(deploymentSetId);
            try (StreamJobClient client =
                         RowWeave.createClusterStreamJobClient(
                                 masterEndpoint.substring(0, separator),
                                 Integer.parseInt(masterEndpoint.substring(
                                         separator + 1)), clientSecurity)) {
                S3ConnectorConfiguration configuration =
                        service.configuration();
                S3Manifest manifest = service.manifest();
                StreamSinkSpec sink = StreamSinkSpec.checkpointTransactional(
                        TRANSACTIONAL_SINK_PROVIDER,
                        StreamConnectorProviderContractCodec.digestHex(
                                TRANSACTIONAL_SINK_CONTRACT),
                        StreamConnectorConfiguration.of(
                                Collections.singletonMap("output.path",
                                        resultPath.toString()),
                                Collections.<String, String>emptyMap())
                                .encode());
                StreamSession session = RowWeave.createStreamSession(
                        "signed-s3-process-cluster");
                session.setDeploymentSetId(deploymentSetId);
                session.setParallelism(2);
                session.setDeliveryGuarantee(
                        StreamDeliveryGuarantee.EXACTLY_ONCE);
                session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                        60_000L, 10_000L, 2));
                session.setResourceProfile(new StreamResourceProfile(
                        8, 4_096, 8_192, 8_192L, 0, 0,
                        32_768L, 8, 32, 8_192L));
                session.fromSource(manifest.toSourceSpec(configuration))
                        .uid("s3-source")
                        .calc(identityCalc(), SCHEMA).uid("identity-calc")
                        .sinkTo("signed-sink", sink);
                StreamJobHandle handle = client.submit(
                        StreamSubmissionId.of("signed-s3-process-cluster"),
                        session.toPlanArtifact());
                assertEquals(StreamJobStatus.FINISHED,
                        handle.awaitTermination(30_000L),
                        () -> processLogs(masterLog, workerALog, workerBLog));
                awaitNonEmpty(markerA, 5_000L);
                awaitNonEmpty(markerB, 5_000L);
                List<String> rows = awaitTransactionalRows(
                        resultPath, 2, 5_000L);
                assertEquals(Arrays.asList("1", "2"), rows);
                service.assertConsumedExactlyOnce();
                assertQualifiedEvidence(handle, 2L,
                        expectedSourceBytes(1L, 2L),
                        expectedSourceBytes(1L, 2L));
            }
            assertTrue(workerA.isAlive(), () -> text(workerALog));
            assertTrue(workerB.isAlive(), () -> text(workerBLog));
            assertTrue(master.isAlive(), () -> text(masterLog));
        } finally {
            stop(master);
            stop(workerB);
            stop(workerA);
            Arrays.fill(credentials, (byte) 0);
            writable(trustRoot, true);
            writable(signerPath, false);
            for (Path path : manifestPaths) writable(path, false);
        }
    }

    private static StreamWorkerConfig worker(String id, Path root) {
        return new StreamWorkerConfig(id, root,
                8, 16, 1024 * 1024,
                64L * 1024L * 1024L, 16);
    }

    private static Properties processProperties(Path stateRoot,
            Path trustRoot, Path signerPath, List<Path> manifests) {
        Properties properties = new Properties();
        properties.setProperty("rowweave.stream.master.enable", "true");
        properties.setProperty("rowweave.stream.worker.enable", "true");
        properties.setProperty("rowweave.stream.worker.id", "signed-process");
        properties.setProperty("rowweave.stream.worker.taskSlots", "8");
        properties.setProperty("rowweave.stream.state.dir",
                stateRoot.toAbsolutePath().normalize().toString());
        properties.setProperty(DeploymentRuntime.DEPLOYMENT_COUNT_PROPERTY,
                Integer.toString(manifests.size()));
        for (int index = 0; index < manifests.size(); index++) {
            properties.setProperty(
                    DeploymentRuntime.MANIFEST_PROPERTY_PREFIX + index,
                    manifests.get(index).toString());
        }
        properties.setProperty(DeploymentRuntime.SIGNER_COUNT_PROPERTY, "1");
        properties.setProperty(
                DeploymentRuntime.SIGNER_ID_PROPERTY_PREFIX + "0",
                "s3-cluster-test");
        properties.setProperty(
                DeploymentRuntime.SIGNER_PATH_PROPERTY_PREFIX + "0",
                signerPath.toString());
        properties.setProperty(DeploymentRuntime.TRUST_ROOT_PROPERTY,
                trustRoot.toString());
        properties.setProperty(DeploymentRuntimeConfig.MEMORY_BYTES_PROPERTY,
                Long.toString(128L * 1024L * 1024L));
        return properties;
    }

    private static List<String> processCommand(Path stateRoot,
            Identity identity, Path trustStore, Path authz,
            Path trustRoot, Path signerPath, List<Path> manifests,
            boolean master, String workerId, String fixedWorkers,
            Path secretPath, Path markerPath) throws IOException {
        Path processTmp = Files.createDirectories(stateRoot.resolve("tmp"));
        List<String> command = new ArrayList<>();
        command.add(new File(System.getProperty("java.home"), "bin/java")
                .getAbsolutePath());
        Collections.addAll(command,
                "-Xms64m", "-Xmx2048m",
                "-XX:MaxDirectMemorySize=256m",
                "-XX:MaxMetaspaceSize=256m",
                "-XX:ReservedCodeCacheSize=128m",
                "-Xss1m", "-XX:ActiveProcessorCount=4",
                "-Drowweave.process.memoryLimitBytes=8589934592",
                "-Djava.io.tmpdir=" + processTmp,
                "-Djdk.tls.maxHandshakeMessageSize=65536",
                "-Djdk.tls.maxCertificateChainLength=10",
                "-Drowweave.control.securityProfile=MTLS",
                "-Drowweave.control.host=127.0.0.1",
                "-Drowweave.control.tls.keyStore=" + identity.keyStore,
                "-Drowweave.control.tls.keyStorePassword=" + TLS_PASSWORD,
                "-Drowweave.control.tls.trustStore=" + trustStore,
                "-Drowweave.control.tls.trustStorePassword=" + TLS_PASSWORD,
                "-Drowweave.control.tls.authzFile=" + authz,
                "-Drowweave.control.residentBytes=234881024",
                "-Drowweave.deployment.memoryBytes=134217728",
                "-Drowweave.shuffle.ingressResidentBytes=268435456",
                "-Drowweave.stream.state.dir=" + stateRoot.toAbsolutePath(),
                "-Drowweave.deployment.count=" + manifests.size(),
                "-Drowweave.deployment.signer.count=1",
                "-Drowweave.deployment.signer.id.0=s3-cluster-test",
                "-Drowweave.deployment.signer.path.0=" + signerPath,
                "-Drowweave.deployment.trustRoot=" + trustRoot);
        for (int index = 0; index < manifests.size(); index++) {
            command.add("-Drowweave.deployment.manifest." + index + '='
                    + manifests.get(index));
        }
        if (master) {
            command.add("-Drowweave.stream.master.enable=true");
            command.add("-Drowweave.stream.master.port=0");
            command.add("-Drowweave.stream.master.workers=" + fixedWorkers);
        } else {
            command.add("-Drowweave.stream.worker.enable=true");
            command.add("-Drowweave.stream.worker.id=" + workerId);
            command.add("-Drowweave.stream.worker.port=0");
            command.add("-Drowweave.stream.worker.taskSlots=8");
            command.add("-Drowweave.stream.worker.channelCapacity=16");
            command.add("-Drowweave.stream.worker.maximumFrameBytes=1048576");
            command.add("-Drowweave.stream.worker.maximumResidentBytes=67108864");
            command.add("-Drowweave.stream.worker.maximumThreads=16");
            command.add("-D" + S3TestSecretResolver.SECRET_PATH_PROPERTY
                    + '=' + secretPath);
            command.add("-D" + S3TestSecretResolver.MARKER_PATH_PROPERTY
                    + '=' + markerPath);
        }
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(StreamClusterMain.class.getName());
        return command;
    }

    private static Process launchProcess(List<String> command, Path log)
            throws IOException {
        return new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(log.toFile()).start();
    }

    private static String awaitReady(Path log, String prefix,
            Process process, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (Files.isRegularFile(log)) {
                for (String line : Files.readAllLines(
                        log, StandardCharsets.UTF_8)) {
                    if (line.startsWith(prefix)) {
                        return line.substring(prefix.length()).trim();
                    }
                }
            }
            if (!process.isAlive()) {
                throw new AssertionError("cluster process exited before ready: "
                        + text(log));
            }
            Thread.sleep(20L);
        }
        throw new AssertionError("cluster process did not become ready: "
                + text(log));
    }

    private static void awaitNonEmpty(Path path, long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (Files.isRegularFile(path) && Files.size(path) > 0L) return;
            Thread.sleep(20L);
        }
        throw new AssertionError("expected non-empty file " + path);
    }

    private static List<String> transactionalRows(Path root)
            throws Exception {
        List<String> rows = new ArrayList<>();
        try (java.nio.file.DirectoryStream<Path> files =
                     Files.newDirectoryStream(root, "visible-*")) {
            for (Path file : files) {
                for (String row : Files.readAllLines(file,
                        StandardCharsets.US_ASCII)) {
                    if (!row.isEmpty()) rows.add(row);
                }
            }
        }
        Collections.sort(rows);
        return rows;
    }

    private static List<String> awaitTransactionalRows(Path root,
            int expected, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        List<String> rows = Collections.emptyList();
        while (System.nanoTime() < deadline) {
            rows = transactionalRows(root);
            if (rows.size() == expected) return rows;
            Thread.sleep(20L);
        }
        throw new AssertionError("expected " + expected
                + " transactional rows but found " + rows);
    }

    private static void assertQualifiedEvidence(StreamJobHandle handle,
            long expectedRecords, long expectedReadBytes,
            long expectedWrittenBytes) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        StreamJobEvidence evidence = handle.evidence();
        while (System.nanoTime() < deadline
                && (!evidence.complete()
                || evidence.lifetimeProgress().recordsWritten()
                < expectedRecords
                || evidence.latestCompletedCheckpointId() == null)) {
            Thread.sleep(20L);
            evidence = handle.evidence();
        }
        assertTrue(evidence.available());
        assertTrue(evidence.complete());
        assertEquals(evidence.placementTaskCount(),
                evidence.reportedTaskCount());
        StreamJobProgress progress = evidence.lifetimeProgress();
        assertEquals(expectedRecords, progress.recordsRead());
        assertEquals(expectedRecords, progress.recordsWritten());
        assertEquals(expectedReadBytes, progress.bytesRead());
        assertEquals(expectedWrittenBytes, progress.bytesWritten());
        assertEquals(Long.MIN_VALUE, progress.operatorWatermark(),
                "the bounded S3 source emits no watermark");
        assertEquals(0L, progress.backlogEvents());
        assertEquals(0L, progress.backlogBytes());
        assertTrue(progress.channelHighWaterBytes() > 0L);
        assertEquals(0L, progress.managedStateBytes());
        assertEquals(0, progress.timerHighWaterCount());
        assertEquals(0, progress.windowHighWaterCount());
        assertTrue(progress.completedCheckpoints() > 0L);
        assertNotNull(evidence.latestCompletedCheckpointId());
        assertTrue(evidence.latestCompletedCheckpointSequence() > 0L);
        assertTrue(evidence.latestCheckpointBytes() > 0L);
        String publicIdentity = evidence.runId() + evidence.attemptId()
                + evidence.latestCompletedCheckpointId();
        assertFalse(publicIdentity.contains(BUCKET));
        assertFalse(publicIdentity.contains(ACCESS_KEY));
        assertFalse(publicIdentity.contains(SECRET_KEY));
        assertFalse(publicIdentity.contains("vault/s3"));
    }

    private static long expectedSourceBytes(long... ids) {
        StreamDataRowCodec codec = new StreamDataRowCodec(SCHEMA);
        long bytes = 0L;
        for (long id : ids) bytes += codec.encode(row(id)).length;
        return bytes;
    }

    private static DataRow row(long id) {
        DataRow row = new DataRow(1);
        row.set(0, id);
        return row;
    }

    private static String text(Path path) {
        try {
            return Files.isRegularFile(path)
                    ? new String(Files.readAllBytes(path),
                    StandardCharsets.UTF_8) : "<missing>";
        } catch (IOException failure) {
            return "<unreadable: " + failure.getClass().getSimpleName() + '>';
        }
    }

    private static String processLogs(Path master, Path workerA,
            Path workerB) {
        return "MASTER:\n" + text(master) + "\nWORKER A:\n"
                + text(workerA) + "\nWORKER B:\n" + text(workerB);
    }

    private static void stop(Process process) {
        if (process == null) return;
        process.destroy();
        try {
            if (!process.waitFor(5L, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                process.waitFor(5L, TimeUnit.SECONDS);
            }
        } catch (InterruptedException interrupted) {
            process.destroyForcibly();
            Thread.currentThread().interrupt();
        }
    }

    private static void readOnly(Path target, boolean directory)
            throws IOException {
        Files.setPosixFilePermissions(target, directory
                ? EnumSet.of(PosixFilePermission.OWNER_READ,
                PosixFilePermission.OWNER_EXECUTE,
                PosixFilePermission.GROUP_READ,
                PosixFilePermission.GROUP_EXECUTE,
                PosixFilePermission.OTHERS_READ,
                PosixFilePermission.OTHERS_EXECUTE)
                : EnumSet.of(PosixFilePermission.OWNER_READ,
                PosixFilePermission.GROUP_READ,
                PosixFilePermission.OTHERS_READ));
    }

    private static void writable(Path target, boolean directory)
            throws IOException {
        if (!Files.exists(target)) return;
        Files.setPosixFilePermissions(target, directory
                ? EnumSet.of(PosixFilePermission.OWNER_READ,
                PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.OWNER_EXECUTE)
                : EnumSet.of(PosixFilePermission.OWNER_READ,
                PosixFilePermission.OWNER_WRITE));
    }

    private static VerifiedDeploymentSet load(
            List<DeploymentManifest> manifests,
            Map<String, java.security.PublicKey> trust) {
        return VerifiedDeploymentSet.load(manifests, trust,
                DeploymentMemoryBudget.forTesting(
                        128L * 1024L * 1024L));
    }

    private static StreamSecretResolver resolver(byte[] frame,
            AtomicInteger resolutions) {
        return reference -> {
            if (!"vault/s3".equals(reference)) {
                throw new IllegalArgumentException("unknown secret");
            }
            resolutions.incrementAndGet();
            return frame.clone();
        };
    }

    private static S3ConnectorConfiguration configuration(int port) {
        return S3ConnectorConfiguration.builder()
                .endpoint(URI.create("http://127.0.0.1:" + port))
                .region("us-east-1").bucket(BUCKET)
                .format(S3ObjectFormat.NDJSON_UTF8)
                .credentialsReference("vault/s3")
                .maximumObjectBytes(1024 * 1024)
                .maximumRowBytes(64 * 1024)
                .temporaryDiskBudgetBytes(1024 * 1024)
                .connectTimeoutMillis(2_000).readTimeoutMillis(5_000)
                .maximumAttempts(2).pathStyleAccess(true)
                .allowInsecureEndpoint(true).build();
    }

    private static StreamCalcSpec identityCalc() {
        return StreamCalcSpec.of(null, Collections.singletonList(
                StreamCalcSpec.projection("id", StreamExpression.input(
                        "id", PortableDataType.LONG, false))));
    }

    private static FunctionDeploymentReference aggregate(
            String deploymentSetId, Path image) throws Exception {
        byte[] configuration = new byte[] {7};
        return new FunctionDeploymentReference(deploymentSetId,
                SINK_DEPLOYMENT, sha256(Files.readAllBytes(image)),
                AGGREGATE_CLASS,
                FunctionAbi.STREAM_AGGREGATE_V1,
                PortableFunctionProviderRegistry.OPAQUE_CONFIGURATION_CODEC,
                sha256(configuration), configuration, AGGREGATE_RESOURCES);
    }

    private static StreamStateSpec aggregateState() {
        return new StreamStateSpec("s3-id-aggregate",
                StreamStateSpec.Kind.KEYED_AGGREGATING,
                "s3-test-long-state-v1", 1,
                StreamStateSpec.TtlMode.PROCESSING_TIME, 60_000L);
    }

    private static S3ObjectDescriptor descriptor(String key,
            String version, String etag, byte[] content) throws Exception {
        return S3ObjectDescriptor.versioned(BUCKET, key, version, etag,
                content.length, sha256(content),
                S3ObjectFormat.NDJSON_UTF8, SCHEMA);
    }

    private static byte[] credentialFrame() {
        S3Credentials value = S3Credentials.basic(
                ACCESS_KEY.getBytes(StandardCharsets.UTF_8),
                SECRET_KEY.getBytes(StandardCharsets.UTF_8));
        try {
            return S3CredentialCodec.encode(value);
        } finally {
            value.destroy();
        }
    }

    private static ControlSecurityConfig security(Identity identity,
            Path trustStore, Path authz) {
        Properties properties = new Properties();
        properties.setProperty(ControlSecurityConfig.PROFILE_PROPERTY,
                "MTLS");
        properties.setProperty(ControlSecurityConfig.HOST_PROPERTY,
                "127.0.0.1");
        properties.setProperty("rowweave.control.tls.keyStore",
                identity.keyStore.toString());
        properties.setProperty("rowweave.control.tls.keyStorePassword",
                TLS_PASSWORD);
        properties.setProperty("rowweave.control.tls.trustStore",
                trustStore.toString());
        properties.setProperty("rowweave.control.tls.trustStorePassword",
                TLS_PASSWORD);
        properties.setProperty("rowweave.control.tls.authzFile",
                authz.toString());
        return ControlSecurityConfig.fromProperties(properties);
    }

    private static Identity identity(Path root, String name)
            throws Exception {
        Path keyStore = root.resolve(name + ".p12");
        Path certificate = root.resolve(name + ".cer");
        keytool("-genkeypair", "-alias", "identity", "-keyalg", "RSA",
                "-storetype", "PKCS12", "-keystore", keyStore.toString(),
                "-storepass", TLS_PASSWORD, "-keypass", TLS_PASSWORD,
                "-dname", "CN=" + name, "-validity", "30",
                "-ext", "SAN=ip:127.0.0.1,dns:localhost",
                "-ext", "EKU=serverAuth,clientAuth");
        keytool("-exportcert", "-alias", "identity", "-keystore",
                keyStore.toString(), "-storepass", TLS_PASSWORD,
                "-file", certificate.toString());
        X509Certificate leaf;
        try (InputStream input = Files.newInputStream(certificate)) {
            leaf = (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(input);
        }
        return new Identity(keyStore, certificate,
                sha256(leaf.getEncoded()));
    }

    private static void importCertificate(Path trustStore, String alias,
            Path certificate) throws Exception {
        keytool("-importcert", "-noprompt", "-alias", alias,
                "-file", certificate.toString(), "-storetype", "PKCS12",
                "-keystore", trustStore.toString(), "-storepass",
                TLS_PASSWORD);
    }

    private static void keytool(String... arguments) throws Exception {
        List<String> command = new ArrayList<>();
        command.add(new File(System.getProperty("java.home"), "bin/keytool")
                .getAbsolutePath());
        Collections.addAll(command,
                "-J-Xms64m", "-J-Xmx512m",
                "-J-XX:MaxDirectMemorySize=64m",
                "-J-XX:MaxMetaspaceSize=128m",
                "-J-XX:ReservedCodeCacheSize=64m",
                "-J-Xss1m", "-J-XX:ActiveProcessorCount=2");
        command.addAll(Arrays.asList(arguments));
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true).start();
        byte[] output = readAll(process.getInputStream());
        int exit = process.waitFor();
        if (exit != 0) {
            throw new AssertionError("keytool failed: "
                    + new String(output, StandardCharsets.UTF_8));
        }
    }

    private static byte[] readAll(InputStream input) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[4_096];
        for (int read; (read = input.read(buffer)) >= 0; ) {
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }

    private static KeyPair signer() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(3072);
        return generator.generateKeyPair();
    }

    private static DeploymentManifest sign(Path image, String deploymentId,
            KeyPair signer) throws Exception {
        byte[] bytes = Files.readAllBytes(image);
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(signer.getPrivate());
        signature.update(DeploymentManifest.signingPayload(
                deploymentId, bytes.length, digest));
        return new DeploymentManifest(deploymentId,
                image.toAbsolutePath().normalize(), bytes.length, digest,
                "s3-cluster-test", signature.sign());
    }

    private static Path createSinkImage(Path temp) throws Exception {
        Path source = temp.resolve("sink-src/test/connector/SignedSink.java");
        Path classes = Files.createDirectories(temp.resolve("sink-classes"));
        Files.createDirectories(source.getParent());
        Files.write(source, sinkSource().getBytes(StandardCharsets.UTF_8));
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null || compiler.run(null, null, null,
                "-source", "8", "-target", "8",
                "-classpath", System.getProperty("java.class.path"),
                "-d", classes.toString(), source.toString()) != 0) {
            throw new AssertionError("signed Sink compilation failed");
        }
        Map<String, byte[]> entries = new LinkedHashMap<>();
        for (String name : Arrays.asList(SINK_CLASS, SINK_CLASS + "$1",
                TRANSACTIONAL_SINK_CLASS,
                TRANSACTIONAL_SINK_CLASS + "$1",
                AGGREGATE_CLASS, AGGREGATE_PROVIDER_CLASS)) {
            String resource = name.replace('.', '/') + ".class";
            entries.put(resource,
                    Files.readAllBytes(classes.resolve(resource)));
        }
        entries.put(StreamConnectorProviderRegistry.SINK_SERVICE_RESOURCE,
                (SINK_CLASS + '=' + SINK_CONTRACT_RESOURCE + '\n')
                        .getBytes(StandardCharsets.UTF_8));
        entries.put(SINK_CONTRACT_RESOURCE,
                StreamConnectorProviderContractCodec.encode(SINK_CONTRACT));
        entries.put(StreamConnectorProviderRegistry
                        .TRANSACTIONAL_SINK_SERVICE_RESOURCE,
                (TRANSACTIONAL_SINK_CLASS + '='
                        + TRANSACTIONAL_SINK_CONTRACT_RESOURCE + '\n')
                        .getBytes(StandardCharsets.UTF_8));
        entries.put(TRANSACTIONAL_SINK_CONTRACT_RESOURCE,
                StreamConnectorProviderContractCodec.encode(
                        TRANSACTIONAL_SINK_CONTRACT));
        entries.put(PortableFunctionProviderRegistry.SERVICE_RESOURCE,
                (AGGREGATE_PROVIDER_CLASS + '='
                        + AGGREGATE_CONTRACT_RESOURCE + '\n')
                        .getBytes(StandardCharsets.UTF_8));
        entries.put(AGGREGATE_CONTRACT_RESOURCE,
                PortableFunctionProviderContractCodec.encode(
                        AGGREGATE_CONTRACT));
        Path image = temp.resolve("signed-test-sink.jar");
        try (OutputStream file = Files.newOutputStream(image);
             JarOutputStream output = new JarOutputStream(file)) {
            for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
                JarEntry jarEntry = new JarEntry(entry.getKey());
                jarEntry.setTime(0L);
                output.putNextEntry(jarEntry);
                output.write(entry.getValue());
                output.closeEntry();
            }
        }
        return image;
    }

    private static String sinkSource() {
        String digest = StreamConnectorProviderContractCodec.digestHex(
                SINK_CONTRACT);
        String transactionalDigest =
                StreamConnectorProviderContractCodec.digestHex(
                        TRANSACTIONAL_SINK_CONTRACT);
        return "package test.connector;"
                + "public final class SignedSink implements "
                + "io.rowweave.stream.spi.StreamSinkProvider {"
                + "public String providerKey(){return \"" + SINK_PROVIDER
                + "\";}"
                + "public String contractDigest(){return \"" + digest
                + "\";}"
                + "public io.rowweave.stream.spi.StreamSinkWriter open("
                + "io.rowweave.stream.spi.StreamSinkContext context){"
                + "io.rowweave.stream.api.StreamConnectorConfiguration c="
                + "io.rowweave.stream.api.StreamConnectorConfiguration"
                + ".decode(context.configurationFrame());"
                + "if(c.options().size()>1||"
                + "(c.options().size()==1&&!c.options().containsKey("
                + "\"output.path\"))||!c.secretReferences().isEmpty())"
                + "throw new IllegalArgumentException(\"sink config\");"
                + "final String path=c.options().get(\"output.path\");"
                + "return new io.rowweave.stream.spi.StreamSinkWriter(){"
                + "public void write(byte[] frame)throws Exception{"
                + "io.rowweave.stream.contract.StreamUpsertRecord change="
                + "io.rowweave.stream.contract.StreamUpsertRecordCodec"
                + ".decode(frame,4194304,4194304);"
                + "if(!change.hasAfterImage())return;"
                + "io.rowweave.stream.api.StreamSchema schema=new "
                + "io.rowweave.stream.api.StreamSchema(java.util.Collections"
                + ".singletonList(new io.rowweave.stream.api.StreamField("
                + "\"id\",\"id\",\"id\",io.rowweave.DataType.LongType,false)));"
                + "long value=new io.rowweave.stream.api.StreamDataRowCodec(schema)"
                + ".decode(change.afterImageFrame()).getLong(0);"
                + "if(path==null){synchronized(System.getProperties()){String key=\""
                + RESULT_PROPERTY + "\";String old=System.getProperty(key,\"\");"
                + "System.setProperty(key,old+value+\",\");}}else{"
                + "java.nio.file.Path target=java.nio.file.Paths.get(path);"
                + "try(java.nio.channels.FileChannel channel="
                + "java.nio.channels.FileChannel.open(target,"
                + "java.nio.file.StandardOpenOption.CREATE,"
                + "java.nio.file.StandardOpenOption.WRITE,"
                + "java.nio.file.StandardOpenOption.APPEND);"
                + "java.nio.channels.FileLock lock=channel.lock()){"
                + "byte[] bytes=(Long.toString(value)+\"\\n\").getBytes("
                + "java.nio.charset.StandardCharsets.US_ASCII);"
                + "channel.write(java.nio.ByteBuffer.wrap(bytes));}}"
                + "}};}"
                + "public static final class TransactionalSink implements "
                + "io.rowweave.stream.spi.StreamTransactionalSinkProvider{"
                + "public String providerKey(){return \""
                + TRANSACTIONAL_SINK_PROVIDER + "\";}"
                + "public String contractDigest(){return \""
                + transactionalDigest + "\";}"
                + "public io.rowweave.stream.spi.StreamTransactionalSinkWriter "
                + "open(io.rowweave.stream.spi.StreamTransactionalSinkContext "
                + "context){io.rowweave.stream.api."
                + "StreamConnectorConfiguration c=io.rowweave.stream.api."
                + "StreamConnectorConfiguration.decode("
                + "context.configurationFrame());"
                + "if(c.options().size()!=1||!c.options().containsKey("
                + "\"output.path\")||!c.secretReferences().isEmpty())"
                + "throw new IllegalArgumentException("
                + "\"transactional sink config\");"
                + "final java.nio.file.Path root=java.nio.file.Paths.get("
                + "c.options().get(\"output.path\"));"
                + "return new io.rowweave.stream.spi."
                + "StreamTransactionalSinkWriter(){"
                + "private final java.io.ByteArrayOutputStream pending="
                + "new java.io.ByteArrayOutputStream();"
                + "public void write(byte[] frame)throws Exception{"
                + "io.rowweave.stream.api.StreamSchema schema=new "
                + "io.rowweave.stream.api.StreamSchema(java.util.Collections"
                + ".singletonList(new io.rowweave.stream.api.StreamField("
                + "\"id\",\"id\",\"id\",io.rowweave.DataType.LongType,"
                + "false)));long value=new io.rowweave.stream.api."
                + "StreamDataRowCodec(schema).decode(frame).getLong(0);"
                + "pending.write((Long.toString(value)+\"\\n\").getBytes("
                + "java.nio.charset.StandardCharsets.US_ASCII));}"
                + "public byte[] prepare(String id)throws Exception{"
                + "java.nio.file.Path prepared=file(\"prepared-\",id);"
                + "if(!java.nio.file.Files.exists(prepared))"
                + "java.nio.file.Files.write(prepared,pending.toByteArray(),"
                + "java.nio.file.StandardOpenOption.CREATE_NEW);"
                + "pending.reset();return hash(id).getBytes("
                + "java.nio.charset.StandardCharsets.US_ASCII);}"
                + "public io.rowweave.stream.spi.StreamTransactionStatus "
                + "query(String id){if(java.nio.file.Files.exists("
                + "file(\"visible-\",id)))return io.rowweave.stream.spi."
                + "StreamTransactionStatus.VISIBLE;if(java.nio.file.Files."
                + "exists(file(\"prepared-\",id)))return io.rowweave."
                + "stream.spi.StreamTransactionStatus.PREPARED;return io.rowweave.stream.spi.StreamTransactionStatus.UNKNOWN;}"
                + "public void commit(String id)throws Exception{"
                + "java.nio.file.Path visible=file(\"visible-\",id);"
                + "if(java.nio.file.Files.exists(visible))return;"
                + "java.nio.file.Path prepared=file(\"prepared-\",id);"
                + "if(!java.nio.file.Files.exists(prepared))throw new "
                + "IllegalStateException(\"transaction not prepared\");"
                + "java.nio.file.Files.move(prepared,visible,"
                + "java.nio.file.StandardCopyOption.ATOMIC_MOVE);}"
                + "public void finalizeTransaction(String id){}"
                + "public void abort(String id)throws Exception{"
                + "if(!java.nio.file.Files.exists(file(\"visible-\",id)))"
                + "java.nio.file.Files.deleteIfExists("
                + "file(\"prepared-\",id));}public void close(){}"
                + "private java.nio.file.Path file(String prefix,String id){"
                + "return root.resolve(prefix+hash(id));}"
                + "private String hash(String id){try{byte[] bytes="
                + "java.security.MessageDigest.getInstance(\"SHA-256\")"
                + ".digest(id.getBytes(java.nio.charset.StandardCharsets."
                + "UTF_8));StringBuilder value=new StringBuilder(64);"
                + "for(byte item:bytes)value.append(String.format("
                + "\"%02x\",item&255));return value.toString();}"
                + "catch(java.security.NoSuchAlgorithmException failure){"
                + "throw new IllegalStateException(failure);}}"
                + "};}}"
                + "public static final class AggregateProvider implements "
                + "io.rowweave.portable.PortableFunctionProvider{"
                + "public AggregateProvider(){}"
                + "public io.rowweave.portable.PortableFunctionProviderContract "
                + "contract(){return new io.rowweave.portable."
                + "PortableFunctionProviderContract(1,\""
                + AGGREGATE_CLASS + "\",io.rowweave.portable.FunctionAbi."
                + "STREAM_AGGREGATE_V1,\""
                + PortableFunctionProviderRegistry.OPAQUE_CONFIGURATION_CODEC
                + "\",1,new io.rowweave.portable.FunctionResourceProfile("
                + "2048L,0L,1));}"
                + "public java.io.Serializable create(byte[] configuration){"
                + "if(configuration==null||configuration.length!=1||"
                + "configuration[0]!=7)throw new IllegalArgumentException("
                + "\"aggregate config\");return new PassAggregate();}"
                + "}"
                + "public static final class PassAggregate implements "
                + "io.rowweave.StreamAggregateFunction{"
                + "public byte[] createAccumulator(){return new byte[8];}"
                + "public byte[] add(io.rowweave.DataRow row,byte[] current){"
                + "return java.nio.ByteBuffer.allocate(8)"
                + ".putLong(row.getLong(0)).array();}"
                + "public io.rowweave.DataRow getResult(byte[] state){"
                + "io.rowweave.DataRow row=new io.rowweave.DataRow(1);"
                + "row.set(0,java.nio.ByteBuffer.wrap(state).getLong());"
                + "return row;}"
                + "}"
                + "}";
    }

    private static String sha256(byte[] bytes) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
        StringBuilder value = new StringBuilder(64);
        for (byte item : digest) value.append(String.format("%02x", item));
        return value.toString();
    }

    private static final class S3QualificationService
            implements AutoCloseable {
        private final ProtocolEndpoint protocol;
        private final Process minio;
        private final S3ConnectorConfiguration configuration;
        private final S3Manifest manifest;
        private final List<String> requests;

        private S3QualificationService(ProtocolEndpoint protocol,
                Process minio,
                S3ConnectorConfiguration configuration,
                S3Manifest manifest, List<String> requests) {
            this.protocol = protocol;
            this.minio = minio;
            this.configuration = configuration;
            this.manifest = manifest;
            this.requests = requests;
        }

        private static S3QualificationService protocol() throws Exception {
            List<String> requests = new CopyOnWriteArrayList<>();
            ProtocolEndpoint endpoint = new ProtocolEndpoint(requests);
            S3ConnectorConfiguration configuration =
                    S3SignedDualWorkerClusterIT.configuration(
                            endpoint.port());
            S3Manifest manifest = S3Manifest.of(SCHEMA, Arrays.asList(
                    descriptor("frozen/a.ndjson", "version-a",
                            "\"etag-a\"", ProtocolEndpoint.FIRST),
                    descriptor("frozen/b.ndjson", "version-b",
                            "\"etag-b\"", ProtocolEndpoint.SECOND)));
            return new S3QualificationService(endpoint, null,
                    configuration, manifest, requests);
        }

        private static S3QualificationService minio(Path binary, Path root)
                throws Exception {
            int apiPort = freePort();
            int consolePort = freePort();
            Path data = Files.createDirectories(root.resolve("data"));
            Path log = root.resolve("minio.log");
            ProcessBuilder builder = new ProcessBuilder(binary.toString(),
                    "server", "--address", "127.0.0.1:" + apiPort,
                    "--console-address", "127.0.0.1:" + consolePort,
                    data.toString());
            builder.environment().put("MINIO_ROOT_USER", ACCESS_KEY);
            builder.environment().put("MINIO_ROOT_PASSWORD", SECRET_KEY);
            builder.environment().put("MINIO_BROWSER", "off");
            Process process = builder.redirectErrorStream(true)
                    .redirectOutput(log.toFile()).start();
            byte[] credentials = credentialFrame();
            try {
                awaitMinio(process, log, apiPort);
                S3ConnectorConfiguration configuration =
                        S3SignedDualWorkerClusterIT.configuration(apiPort);
                S3Credentials secret = S3CredentialCodec.decode(credentials);
                try (S3Client client = AwsS3ClientFactory.create(
                        configuration, secret)) {
                    client.createBucket(CreateBucketRequest.builder()
                            .bucket(BUCKET).build());
                    client.putBucketVersioning(PutBucketVersioningRequest
                            .builder().bucket(BUCKET)
                            .versioningConfiguration(
                                    VersioningConfiguration.builder()
                                            .status(BucketVersioningStatus
                                                    .ENABLED)
                                            .build()).build());
                    put(client, "frozen/a.ndjson", ProtocolEndpoint.FIRST);
                    put(client, "frozen/b.ndjson", ProtocolEndpoint.SECOND);
                } finally {
                    secret.destroy();
                }
                S3Manifest manifest = S3ManifestPlanner.planPrefix(
                        configuration, SCHEMA, "frozen/", credentials);
                assertEquals(2, manifest.objects().size());
                assertTrue(manifest.objects().get(0).versioned());
                assertTrue(manifest.objects().get(1).versioned());
                return new S3QualificationService(null, process,
                        configuration, manifest, null);
            } catch (Throwable failure) {
                stop(process);
                throw failure;
            } finally {
                Arrays.fill(credentials, (byte) 0);
            }
        }

        private static void put(S3Client client, String key, byte[] content) {
            client.putObject(PutObjectRequest.builder().bucket(BUCKET)
                            .key(key).contentType("application/x-ndjson")
                            .build(), RequestBody.fromBytes(content));
        }

        private S3ConnectorConfiguration configuration() {
            return configuration;
        }

        private S3Manifest manifest() { return manifest; }

        private void assertConsumedExactlyOnce() {
            if (requests != null) {
                assertEquals(2, requests.size(), requests.toString());
            }
        }

        @Override public void close() throws Exception {
            if (protocol != null) protocol.close();
            if (minio != null) stop(minio);
        }

        private static int freePort() throws IOException {
            try (ServerSocket socket = new ServerSocket(
                    0, 1, InetAddress.getByName("127.0.0.1"))) {
                return socket.getLocalPort();
            }
        }

        private static void awaitMinio(Process process, Path log, int port)
                throws Exception {
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(30L);
            URL health = new URL("http://127.0.0.1:" + port
                    + "/minio/health/ready");
            while (System.nanoTime() < deadline) {
                if (!process.isAlive()) {
                    throw new AssertionError("MinIO exited before ready:\n"
                            + text(log));
                }
                HttpURLConnection connection = null;
                try {
                    connection = (HttpURLConnection)
                            health.openConnection();
                    connection.setConnectTimeout(250);
                    connection.setReadTimeout(250);
                    if (connection.getResponseCode() == 200) return;
                } catch (IOException notReady) {
                    // Startup is bounded by the outer deadline.
                } finally {
                    if (connection != null) connection.disconnect();
                }
                Thread.sleep(50L);
            }
            throw new AssertionError("MinIO did not become ready:\n"
                    + text(log));
        }
    }

    private static final class ProtocolEndpoint implements AutoCloseable {
        private static final byte[] FIRST =
                "{\"id\":1}\n".getBytes(StandardCharsets.UTF_8);
        private static final byte[] SECOND =
                "{\"id\":2}\n".getBytes(StandardCharsets.UTF_8);
        private final HttpServer server;
        private final ExecutorService executor;
        private final List<String> requests;

        private ProtocolEndpoint(List<String> requests) throws IOException {
            this.requests = requests;
            server = HttpServer.create(
                    new InetSocketAddress("127.0.0.1", 0), 0);
            executor = Executors.newCachedThreadPool(runnable -> {
                Thread thread = new Thread(runnable,
                        "s3-signed-cluster-http");
                thread.setDaemon(true);
                return thread;
            });
            server.setExecutor(executor);
            server.createContext("/", this::handle);
            server.start();
        }

        private int port() { return server.getAddress().getPort(); }

        private void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            String query = exchange.getRequestURI().getRawQuery();
            String authorization = exchange.getRequestHeaders()
                    .getFirst("Authorization");
            requests.add(exchange.getRequestMethod() + " "
                    + exchange.getRequestURI());
            byte[] body;
            String version;
            String etag;
            if (("/" + BUCKET + "/frozen/a.ndjson").equals(path)) {
                body = FIRST; version = "version-a"; etag = "\"etag-a\"";
            } else if (("/" + BUCKET + "/frozen/b.ndjson").equals(path)) {
                body = SECOND; version = "version-b"; etag = "\"etag-b\"";
            } else {
                exchange.sendResponseHeaders(404, -1L);
                exchange.close();
                return;
            }
            if (!"GET".equals(exchange.getRequestMethod())
                    || query == null
                    || !query.contains("versionId=" + version)
                    || authorization == null
                    || !authorization.contains(
                    "Credential=" + ACCESS_KEY + "/")) {
                exchange.sendResponseHeaders(403, -1L);
                exchange.close();
                return;
            }
            exchange.getResponseHeaders().add("ETag", etag);
            exchange.getResponseHeaders().add("x-amz-version-id", version);
            exchange.getResponseHeaders().add("x-amz-request-id",
                    "rowweave-cluster-test");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
        }

        @Override public void close() {
            server.stop(0);
            executor.shutdownNow();
        }
    }

    private static final class Identity {
        private final Path keyStore;
        private final Path certificate;
        private final String fingerprint;

        private Identity(Path keyStore, Path certificate,
                String fingerprint) {
            this.keyStore = keyStore;
            this.certificate = certificate;
            this.fingerprint = fingerprint;
        }
    }
}
