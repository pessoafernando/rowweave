package io.rowweave.connector.postgres;

import io.rowweave.RowWeave;
import io.rowweave.DataRow;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamMasterRuntime;
import io.rowweave.StreamSession;
import io.rowweave.StreamWorkerRuntime;
import io.rowweave.StreamClusterMain;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.deployment.DeploymentManifest;
import io.rowweave.deployment.DeploymentManifestCodec;
import io.rowweave.deployment.DeploymentMemoryBudget;
import io.rowweave.deployment.DeploymentRuntime;
import io.rowweave.deployment.StreamConnectorProviderRegistry;
import io.rowweave.deployment.VerifiedDeploymentSet;
import io.rowweave.portable.PortableDataType;
import io.rowweave.stream.api.StreamCalcSpec;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamConnectorConfiguration;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamExpression;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobProgress;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.api.StreamUpsertCodec;
import io.rowweave.stream.api.StreamWorkerConfig;
import io.rowweave.stream.contract.StreamConnectorProviderContract;
import io.rowweave.stream.contract.StreamConnectorProviderContractCodec;
import io.rowweave.stream.spi.StreamSecretResolver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.math.BigDecimal;
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Signed PostgreSQL CDC image on an independent Master and two Workers. */
class PostgresSignedDualWorkerClusterIT {
    private static final String TLS_PASSWORD = "changeit";
    private static final String DATABASE = "rowweave_cluster_cdc";
    private static final String SLOT = "rowweave_cluster_slot";
    private static final String PUBLICATION = "rowweave_cluster_publication";
    private static final String CREDENTIAL_REFERENCE = "vault/postgres";
    private static final String SINK_SECRET_REFERENCE = "vault/sink";
    private static final String SINK_DEPLOYMENT = "rowweave-test-upsert-sink";
    private static final String SINK_PROVIDER = "rowweave.test.upsert.sink";
    private static final String TRANSACTIONAL_SINK_PROVIDER =
            "rowweave.test.transactional-upsert.sink";
    private static final String SINK_CLASS = "test.connector.UpsertSink";
    private static final String SINK_CONTRACT_RESOURCE =
            "META-INF/rowweave/stream-connector/contracts/test-upsert-sink.bin";
    private static final String TRANSACTIONAL_SINK_CLASS =
            SINK_CLASS + "$TransactionalSink";
    private static final String TRANSACTIONAL_SINK_CONTRACT_RESOURCE =
            "META-INF/rowweave/stream-connector/contracts/"
                    + "test-transactional-upsert-sink.bin";
    private static final String RESULT_PROPERTY =
            "rowweave.test.postgres.signed-cluster.rows";
    private static final String TRANSACTIONAL_PENDING_PROPERTY =
            "rowweave.test.postgres.signed-cluster.transactional-pending";
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

    @AfterEach
    void clearResult() {
        System.clearProperty(RESULT_PROPERTY);
        System.clearProperty(TRANSACTIONAL_PENDING_PROPERTY);
    }

    @Test
    void signedSourceAndSinkRunAcrossDualWorkersAndCheckpointSlot(
            @TempDir Path temp) throws Exception {
        Path binaries = binaries();
        int postgresPort = freePort();
        try (Database database = Database.start(
                binaries, temp.resolve("postgres"), postgresPort)) {
            database.provision();
            PostgresConnectorConfiguration configuration =
                    database.configuration();
            Path postgresImage = Paths.get(System.getProperty(
                    "postgres.deployment.jar")).toAbsolutePath().normalize();
            assertTrue(Files.isRegularFile(postgresImage));
            Path sinkImage = createSinkImage(temp);
            KeyPair signer = signer();
            List<DeploymentManifest> manifests = Arrays.asList(
                    sign(postgresImage,
                            PostgresConnectorContract.DEPLOYMENT_ID, signer),
                    sign(sinkImage, SINK_DEPLOYMENT, signer));
            Map<String, java.security.PublicKey> trust =
                    Collections.singletonMap(
                            "postgres-cluster-test", signer.getPublic());
            byte[] credentialFrame = credentials();
            AtomicInteger firstSecrets = new AtomicInteger();
            AtomicInteger secondSecrets = new AtomicInteger();
            try (VerifiedDeploymentSet masterDeployments =
                         load(manifests, trust);
                 VerifiedDeploymentSet firstDeployments =
                         load(manifests, trust);
                 VerifiedDeploymentSet secondDeployments =
                         load(manifests, trust);
                 StreamWorkerRuntime first = RowWeave.startStreamWorker(
                         worker("postgres-worker-a", temp.resolve("worker-a")),
                         firstDeployments,
                         resolver(credentialFrame, firstSecrets));
                 StreamWorkerRuntime second = RowWeave.startStreamWorker(
                         worker("postgres-worker-b", temp.resolve("worker-b")),
                         secondDeployments,
                         resolver(credentialFrame, secondSecrets));
                 StreamMasterRuntime master = RowWeave.startStreamMaster(
                         temp.resolve("master"),
                         Arrays.asList(first.endpoint(), second.endpoint()),
                         ControlSecurityConfig.fromProperties(
                                 new Properties()), 0, masterDeployments);
                 StreamJobClient client = master.createJobClient()) {
                assertEquals(masterDeployments.deploymentSetId(),
                        firstDeployments.deploymentSetId());
                assertEquals(masterDeployments.deploymentSetId(),
                        secondDeployments.deploymentSetId());
                StreamSinkSpec sink = StreamSinkSpec.eagerAtLeastOnce(
                        SINK_PROVIDER,
                        StreamConnectorProviderContractCodec.digestHex(
                                SINK_CONTRACT),
                        StreamConnectorConfiguration.of(
                                Collections.<String, String>emptyMap(),
                                Collections.singletonMap(
                                        "marker", SINK_SECRET_REFERENCE))
                                .encode());
                StreamSession session = RowWeave.createStreamSession(
                        "signed-postgres-dual-worker");
                session.setDeploymentSetId(
                        masterDeployments.deploymentSetId());
                session.setParallelism(2);
                session.setChangelogMode(StreamChangelogMode.UPSERT);
                session.setDeliveryGuarantee(
                        StreamDeliveryGuarantee.AT_LEAST_ONCE);
                session.setCheckpointConfig(
                        StreamCheckpointConfig.manualOnly(10_000L, 2));
                session.setResourceProfile(new StreamResourceProfile(
                        8, 4_096, 8_192, 0L, 0, 0,
                        32_768L, 8, 32, 0L));
                session.fromSource(configuration.toSourceSpec())
                        .uid("postgres-source")
                        .calc(identityCalc(), configuration.streamSchema())
                        .uid("identity-calc")
                        .sinkTo("signed-upsert-sink", sink);
                StreamJobHandle handle = client.submit(
                        StreamSubmissionId.of(
                                "signed-postgres-dual-worker"),
                        session.toPlanArtifact());
                assertEquals(StreamJobStatus.RUNNING,
                        handle.awaitRunning(20_000L),
                        () -> "attempt=" + handle.progress().attempt()
                                + ",restarts="
                                + handle.progress().restarts()
                                + ",cdc=" + handle.progress().cdcPhase()
                                + ",read="
                                + handle.progress().recordsRead()
                                + ",written="
                                + handle.progress().recordsWritten()
                                + ", firstSecrets=" + firstSecrets.get()
                                + ", secondSecrets=" + secondSecrets.get());
                awaitRows(1);
                String before = database.confirmedFlush();
                database.insert(2L, "20.00");
                awaitRows(2);
                assertEquals(before, database.confirmedFlush(),
                        "sink visibility must not acknowledge the slot");
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        handle.triggerCheckpoint(StreamCommandId.of(
                                "postgres-cluster-cut"))
                                .awaitCompletion(15_000L));
                database.awaitConfirmedAfter(before);
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        handle.cancel(StreamCommandId.of(
                                "postgres-cluster-cancel"))
                                .awaitCompletion(10_000L));
                assertEquals(StreamJobStatus.CANCELED,
                        handle.awaitTermination(10_000L));
                assertTrue(firstSecrets.get() > 0,
                        "first Worker did not resolve a connector secret");
                assertTrue(secondSecrets.get() > 0,
                        "second Worker did not resolve a connector secret");
            } finally {
                Arrays.fill(credentialFrame, (byte) 0);
            }
        }
    }

    @Test
    void signedCdcAndTransactionalSinkPublishOneCompletedCut(
            @TempDir Path temp) throws Exception {
        Path binaries = binaries();
        int postgresPort = freePort();
        try (Database database = Database.start(
                binaries, temp.resolve("transactional-postgres"),
                postgresPort)) {
            database.provision();
            PostgresConnectorConfiguration configuration =
                    database.configuration();
            Path postgresImage = Paths.get(System.getProperty(
                    "postgres.deployment.jar")).toAbsolutePath().normalize();
            Path sinkImage = createSinkImage(
                    temp.resolve("transactional-sink"));
            KeyPair signer = signer();
            List<DeploymentManifest> manifests = Arrays.asList(
                    sign(postgresImage,
                            PostgresConnectorContract.DEPLOYMENT_ID, signer),
                    sign(sinkImage, SINK_DEPLOYMENT, signer));
            Map<String, java.security.PublicKey> trust =
                    Collections.singletonMap(
                            "postgres-cluster-test", signer.getPublic());
            byte[] credentialFrame = credentials();
            AtomicInteger firstSecrets = new AtomicInteger();
            AtomicInteger secondSecrets = new AtomicInteger();
            Path visible = Files.createDirectories(temp.resolve(
                    "transactional-visible"));
            try (VerifiedDeploymentSet masterDeployments =
                         load(manifests, trust);
                 VerifiedDeploymentSet firstDeployments =
                         load(manifests, trust);
                 VerifiedDeploymentSet secondDeployments =
                         load(manifests, trust);
                 StreamWorkerRuntime first = RowWeave.startStreamWorker(
                         worker("postgres-transactional-worker-a",
                                 temp.resolve("transactional-worker-a")),
                         firstDeployments,
                         resolver(credentialFrame, firstSecrets));
                 StreamWorkerRuntime second = RowWeave.startStreamWorker(
                         worker("postgres-transactional-worker-b",
                                 temp.resolve("transactional-worker-b")),
                         secondDeployments,
                         resolver(credentialFrame, secondSecrets));
                 StreamMasterRuntime master = RowWeave.startStreamMaster(
                         temp.resolve("transactional-master"),
                         Arrays.asList(first.endpoint(), second.endpoint()),
                         ControlSecurityConfig.fromProperties(
                                 new Properties()), 0, masterDeployments);
                 StreamJobClient client = master.createJobClient()) {
                StreamSinkSpec sink = StreamSinkSpec
                        .checkpointTransactional(
                                TRANSACTIONAL_SINK_PROVIDER,
                                StreamConnectorProviderContractCodec
                                        .digestHex(
                                                TRANSACTIONAL_SINK_CONTRACT),
                                StreamConnectorConfiguration.of(
                                        Collections.singletonMap(
                                                "output.path",
                                                visible.toString()),
                                        Collections.singletonMap(
                                                "marker",
                                                SINK_SECRET_REFERENCE))
                                        .encode());
                StreamSession session = RowWeave.createStreamSession(
                        "signed-postgres-transactional");
                session.setDeploymentSetId(
                        masterDeployments.deploymentSetId());
                session.setParallelism(2);
                session.setChangelogMode(StreamChangelogMode.UPSERT);
                session.setDeliveryGuarantee(
                        StreamDeliveryGuarantee.EXACTLY_ONCE);
                session.setCheckpointConfig(
                        StreamCheckpointConfig.periodic(
                                60_000L, 10_000L, 2));
                session.setResourceProfile(new StreamResourceProfile(
                        8, 4_096, 8_192, 0L, 0, 0,
                        32_768L, 8, 32, 0L));
                session.fromSource(configuration.toSourceSpec())
                        .uid("postgres-transactional-source")
                        .calc(identityCalc(), configuration.streamSchema())
                        .uid("postgres-transactional-calc")
                        .sinkTo("signed-transactional-upsert-sink", sink);
                StreamJobHandle handle = client.submit(
                        StreamSubmissionId.of(
                                "signed-postgres-transactional"),
                        session.toPlanArtifact());
                assertEquals(StreamJobStatus.RUNNING,
                        handle.awaitRunning(20_000L));
                awaitTransactionalPending(1);
                String before = database.confirmedFlush();
                database.insert(2L, "20.00");
                awaitTransactionalPending(2);

                assertEquals(Collections.emptyList(),
                        transactionalRows(visible),
                        "transactional rows must remain private before cut");
                assertEquals(before, database.confirmedFlush(),
                        "uncompleted CDC cut must not advance the slot");
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        handle.triggerCheckpoint(StreamCommandId.of(
                                "postgres-transactional-cut"))
                                .awaitCompletion(15_000L));
                database.awaitConfirmedAfter(before);
                assertEquals(Arrays.asList("1", "2"),
                        transactionalRows(visible));
                assertEquals(1L,
                        handle.progress().completedCheckpoints());
                assertEquals(StreamOperationStatus.SUCCEEDED,
                        handle.cancel(StreamCommandId.of(
                                "postgres-transactional-cancel"))
                                .awaitCompletion(10_000L));
                assertEquals(StreamJobStatus.CANCELED,
                        handle.awaitTermination(10_000L));
                assertTrue(firstSecrets.get() > 0);
                assertTrue(secondSecrets.get() > 0);
            } finally {
                Arrays.fill(credentialFrame, (byte) 0);
            }
        }
    }

    @Test
    @Timeout(120)
    void signedImagesRunInIndependentMtlsMasterAndWorkerProcesses(
            @TempDir Path temp) throws Exception {
        Path binaries = binaries();
        int postgresPort = freePort();
        try (Database database = Database.start(
                binaries, temp.resolve("process-postgres"), postgresPort)) {
            database.provision();
            PostgresConnectorConfiguration configuration =
                    database.configuration();
            Path postgresImage = Paths.get(System.getProperty(
                    "postgres.deployment.jar")).toAbsolutePath().normalize();
            Path sinkImage = createSinkImage(temp.resolve("process-sink"));
            KeyPair signer = signer();
            List<DeploymentManifest> manifests = Arrays.asList(
                    sign(postgresImage,
                            PostgresConnectorContract.DEPLOYMENT_ID, signer),
                    sign(sinkImage, SINK_DEPLOYMENT, signer));
            Map<String, java.security.PublicKey> trust =
                    Collections.singletonMap(
                            "postgres-cluster-test", signer.getPublic());
            String deploymentSetId;
            try (VerifiedDeploymentSet expected = load(manifests, trust)) {
                deploymentSetId = expected.deploymentSetId();
            }

            Path trustRoot = Files.createDirectory(
                    temp.resolve("deployment-trust")).toAbsolutePath();
            Path signerPath = trustRoot.resolve(
                    "postgres-cluster-test.der");
            Files.write(signerPath, signer.getPublic().getEncoded());
            List<Path> manifestPaths = new ArrayList<>();
            for (int index = 0; index < manifests.size(); index++) {
                Path path = trustRoot.resolve(
                        "connector-" + index + ".rwdep");
                Files.write(path, DeploymentManifestCodec.encode(
                        manifests.get(index)));
                manifestPaths.add(path);
            }

            Identity masterIdentity = identity(temp, "postgres-master");
            Identity workerAIdentity = identity(temp, "postgres-worker-a");
            Identity workerBIdentity = identity(temp, "postgres-worker-b");
            Identity clientIdentity = identity(temp, "postgres-client");
            Path trustStore = temp.resolve("postgres-trust.p12");
            importCertificate(trustStore, "master",
                    masterIdentity.certificate);
            importCertificate(trustStore, "worker-a",
                    workerAIdentity.certificate);
            importCertificate(trustStore, "worker-b",
                    workerBIdentity.certificate);
            importCertificate(trustStore, "client",
                    clientIdentity.certificate);
            Path authz = temp.resolve("postgres-authz.txt");
            Files.write(authz, (masterIdentity.fingerprint + "=MASTER\n"
                    + workerAIdentity.fingerprint + "=WORKER\n"
                    + workerBIdentity.fingerprint + "=WORKER\n"
                    + clientIdentity.fingerprint + "=CLIENT\n")
                    .getBytes(StandardCharsets.US_ASCII));

            byte[] credentialFrame = credentials();
            Path secretPath = temp.resolve("postgres-secret.bin");
            Files.write(secretPath, credentialFrame);
            Path markerA = temp.resolve("worker-a-secret.marker");
            Path markerB = temp.resolve("worker-b-secret.marker");
            Path resultPath = temp.resolve("postgres-result.txt");
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
                        trustStore, authz, trustRoot, signerPath,
                        manifestPaths, false, "postgres-worker-a", null,
                        secretPath, markerA), workerALog);
                workerB = launchProcess(processCommand(
                        temp.resolve("worker-b-state"), workerBIdentity,
                        trustStore, authz, trustRoot, signerPath,
                        manifestPaths, false, "postgres-worker-b", null,
                        secretPath, markerB), workerBLog);
                String endpointA = awaitReady(workerALog,
                        "STREAM_WORKER_READY=", workerA, 20_000L);
                String endpointB = awaitReady(workerBLog,
                        "STREAM_WORKER_READY=", workerB, 20_000L);
                master = launchProcess(processCommand(
                        temp.resolve("master-state"), masterIdentity,
                        trustStore, authz, trustRoot, signerPath,
                        manifestPaths, true, null,
                        endpointA + ',' + endpointB, null, null), masterLog);
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
                                             separator + 1)),
                                     clientSecurity)) {
                    StreamSinkSpec sink = StreamSinkSpec.eagerAtLeastOnce(
                            SINK_PROVIDER,
                            StreamConnectorProviderContractCodec.digestHex(
                                    SINK_CONTRACT),
                            StreamConnectorConfiguration.of(
                                    Collections.singletonMap("output.path",
                                            resultPath.toString()),
                                    Collections.singletonMap(
                                            "marker",
                                            SINK_SECRET_REFERENCE)).encode());
                    StreamSession session = RowWeave.createStreamSession(
                            "signed-postgres-process-cluster");
                    session.setDeploymentSetId(deploymentSetId);
                    session.setParallelism(2);
                    session.setChangelogMode(StreamChangelogMode.UPSERT);
                    session.setDeliveryGuarantee(
                            StreamDeliveryGuarantee.AT_LEAST_ONCE);
                    session.setCheckpointConfig(
                            StreamCheckpointConfig.manualOnly(15_000L, 2));
                    session.setResourceProfile(new StreamResourceProfile(
                            8, 4_096, 8_192, 0L, 0, 0,
                            32_768L, 8, 32, 0L));
                    session.fromSource(configuration.toSourceSpec())
                            .uid("postgres-source")
                            .calc(identityCalc(),
                                    configuration.streamSchema())
                            .uid("identity-calc")
                            .sinkTo("signed-upsert-sink", sink);
                    StreamJobHandle handle = client.submit(
                            StreamSubmissionId.of(
                                    "signed-postgres-process-cluster"),
                            session.toPlanArtifact());
                    assertEquals(StreamJobStatus.RUNNING,
                            handle.awaitRunning(30_000L),
                            () -> processLogs(
                                    masterLog, workerALog, workerBLog));
                    awaitLines(resultPath, 1, 20_000L);
                    String before = database.confirmedFlush();
                    database.insertUpdateDelete();
                    awaitLines(resultPath, 3, 20_000L);
                    assertEquals(before, database.confirmedFlush());
                    assertEquals(StreamOperationStatus.SUCCEEDED,
                            handle.triggerCheckpoint(StreamCommandId.of(
                                    "postgres-process-cut"))
                                    .awaitCompletion(20_000L),
                            () -> processLogs(
                                    masterLog, workerALog, workerBLog));
                    database.awaitConfirmedAfter(before);
                    assertEquals(StreamOperationStatus.SUCCEEDED,
                            handle.cancel(StreamCommandId.of(
                                    "postgres-process-cancel"))
                                    .awaitCompletion(15_000L));
                    assertEquals(StreamJobStatus.CANCELED,
                            handle.awaitTermination(15_000L));
                    List<String> rows = Files.readAllLines(
                            resultPath, StandardCharsets.US_ASCII);
                    Collections.sort(rows);
                    assertEquals(Arrays.asList("1", "2", "3"), rows);
                    assertQualifiedEvidence(handle, configuration);
                }
                awaitNonEmpty(markerA, 5_000L);
                awaitNonEmpty(markerB, 5_000L);
                assertTrue(workerA.isAlive(), () -> readQuiet(workerALog));
                assertTrue(workerB.isAlive(), () -> readQuiet(workerBLog));
                assertTrue(master.isAlive(), () -> readQuiet(masterLog));
            } finally {
                stop(master);
                stop(workerB);
                stop(workerA);
                Arrays.fill(credentialFrame, (byte) 0);
                writable(trustRoot, true);
                writable(signerPath, false);
                for (Path path : manifestPaths) writable(path, false);
            }
        }
    }

    private static Path binaries() {
        String configured = System.getProperty("postgres.bin.dir");
        Assumptions.assumeTrue(configured != null,
                "set -Dpostgres.bin.dir to run real PostgreSQL cluster gate");
        Path result = Paths.get(configured).toAbsolutePath().normalize();
        Assumptions.assumeTrue(Files.isExecutable(result.resolve("initdb"))
                        && Files.isExecutable(result.resolve("postgres")),
                "PostgreSQL binaries are not executable: " + result);
        return result;
    }

    private static StreamWorkerConfig worker(String id, Path root) {
        return new StreamWorkerConfig(id, root,
                4, 16, 1024 * 1024,
                64L * 1024L * 1024L, 16);
    }

    private static VerifiedDeploymentSet load(
            List<DeploymentManifest> manifests,
            Map<String, java.security.PublicKey> trust) {
        return VerifiedDeploymentSet.load(manifests, trust,
                DeploymentMemoryBudget.forTesting(
                        128L * 1024L * 1024L));
    }

    private static StreamSecretResolver resolver(byte[] credentials,
            AtomicInteger resolutions) {
        return reference -> {
            resolutions.incrementAndGet();
            if (CREDENTIAL_REFERENCE.equals(reference)) {
                return credentials.clone();
            }
            if (SINK_SECRET_REFERENCE.equals(reference)) {
                return new byte[] {1};
            }
            throw new IllegalArgumentException("unknown secret reference");
        };
    }

    private static void awaitRows(int expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20L);
        while (System.nanoTime() < deadline) {
            String rows = System.getProperty(RESULT_PROPERTY, "");
            int count = 0;
            for (String row : rows.split(",")) {
                if (!row.isEmpty()) count++;
            }
            if (count >= expected) return;
            Thread.sleep(20L);
        }
        throw new AssertionError("missing sink rows: "
                + System.getProperty(RESULT_PROPERTY, ""));
    }

    private static void awaitTransactionalPending(int expected)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20L);
        while (System.nanoTime() < deadline) {
            int actual = Integer.parseInt(System.getProperty(
                    TRANSACTIONAL_PENDING_PROPERTY, "0"));
            if (actual >= expected) return;
            Thread.sleep(20L);
        }
        throw new AssertionError("expected at least " + expected
                + " pending transactional rows but saw "
                + System.getProperty(TRANSACTIONAL_PENDING_PROPERTY, "0"));
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

    private static void assertQualifiedEvidence(StreamJobHandle handle,
            PostgresConnectorConfiguration configuration) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        StreamJobEvidence evidence = handle.evidence();
        while (System.nanoTime() < deadline
                && (!evidence.complete()
                || evidence.lifetimeProgress().recordsWritten() < 5L
                || evidence.latestCompletedCheckpointId() == null)) {
            Thread.sleep(20L);
            evidence = handle.evidence();
        }
        assertTrue(evidence.available());
        assertTrue(evidence.complete());
        assertEquals(evidence.placementTaskCount(),
                evidence.reportedTaskCount());
        StreamJobProgress progress = evidence.lifetimeProgress();
        long expectedBytes = expectedCdcBytes(configuration);
        assertEquals(5L, progress.recordsRead());
        assertEquals(5L, progress.recordsWritten());
        assertEquals(expectedBytes, progress.bytesRead());
        assertEquals(expectedBytes, progress.bytesWritten());
        assertEquals(Long.MIN_VALUE, progress.operatorWatermark(),
                "the PostgreSQL CDC source emits no watermark");
        assertEquals(0L, progress.backlogEvents());
        assertEquals(0L, progress.backlogBytes());
        assertTrue(progress.channelHighWaterBytes() > 0L);
        assertEquals(0L, progress.managedStateBytes());
        assertEquals(0, progress.timerHighWaterCount());
        assertEquals(0, progress.windowHighWaterCount());
        assertEquals(1L, progress.completedCheckpoints());
        assertNotNull(evidence.latestCompletedCheckpointId());
        assertTrue(evidence.latestCompletedCheckpointSequence() > 0L);
        assertTrue(evidence.latestCheckpointBytes() > 0L);
        String publicIdentity = evidence.runId() + evidence.attemptId()
                + evidence.latestCompletedCheckpointId();
        assertFalse(publicIdentity.contains(DATABASE));
        assertFalse(publicIdentity.contains(SLOT));
        assertFalse(publicIdentity.contains(PUBLICATION));
        assertFalse(publicIdentity.contains("ledger"));
        assertFalse(publicIdentity.contains(CREDENTIAL_REFERENCE));
    }

    private static long expectedCdcBytes(
            PostgresConnectorConfiguration configuration) {
        StreamUpsertCodec codec = new StreamUpsertCodec(
                configuration.stableKeySchema(),
                configuration.streamSchema());
        return codec.encodeAfterImage(key(1L), value(1L, "10.00")).length
                + codec.encodeAfterImage(key(2L),
                value(2L, "20.00")).length
                + codec.encodeTombstone(key(2L)).length
                + codec.encodeAfterImage(key(3L),
                value(3L, "30.00")).length
                + codec.encodeTombstone(key(3L)).length;
    }

    private static DataRow key(long id) {
        DataRow row = new DataRow(1);
        row.set(0, id);
        return row;
    }

    private static DataRow value(long id, String amount) {
        DataRow row = new DataRow(2);
        row.set(0, id);
        row.set(1, new BigDecimal(amount));
        return row;
    }

    private static StreamCalcSpec identityCalc() {
        return StreamCalcSpec.of(null, Arrays.asList(
                StreamCalcSpec.projection("id", StreamExpression.input(
                        "id", PortableDataType.LONG, false)),
                StreamCalcSpec.projection("amount", StreamExpression.input(
                        "amount", PortableDataType.BIG_DECIMAL, true))));
    }

    private static byte[] credentials() {
        PostgresCredentials value = PostgresCredentials.of(
                System.getProperty("user.name")
                        .getBytes(StandardCharsets.UTF_8),
                "unused".getBytes(StandardCharsets.UTF_8));
        try { return PostgresCredentialCodec.encode(value); }
        finally { value.destroy(); }
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
                "postgres-cluster-test", signature.sign());
    }

    private static Path createSinkImage(Path temp) throws Exception {
        Path source = temp.resolve("sink-src/test/connector/UpsertSink.java");
        Path classes = Files.createDirectories(temp.resolve("sink-classes"));
        Files.createDirectories(source.getParent());
        Files.write(source, sinkSource().getBytes(StandardCharsets.UTF_8));
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null || compiler.run(null, null, null,
                "-source", "8", "-target", "8",
                "-classpath", System.getProperty("java.class.path"),
                "-d", classes.toString(), source.toString()) != 0) {
            throw new AssertionError("signed UPSERT Sink compilation failed");
        }
        Map<String, byte[]> entries = new LinkedHashMap<>();
        for (String name : Arrays.asList(SINK_CLASS, SINK_CLASS + "$1",
                TRANSACTIONAL_SINK_CLASS,
                TRANSACTIONAL_SINK_CLASS + "$1")) {
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
        Path image = temp.resolve("signed-test-upsert-sink.jar");
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
                + "public final class UpsertSink implements "
                + "io.rowweave.stream.spi.StreamSinkProvider {"
                + "public String providerKey(){return \"" + SINK_PROVIDER
                + "\";}"
                + "public String contractDigest(){return \"" + digest
                + "\";}"
                + "public io.rowweave.stream.spi.StreamSinkWriter open("
                + "io.rowweave.stream.spi.StreamSinkContext context)"
                + "throws Exception{"
                + "io.rowweave.stream.api.StreamConnectorConfiguration c="
                + "io.rowweave.stream.api.StreamConnectorConfiguration"
                + ".decode(context.configurationFrame());"
                + "if(c.options().size()>1||"
                + "(c.options().size()==1&&!c.options().containsKey("
                + "\"output.path\"))||c.secretReferences().size()!=1)"
                + "throw new IllegalArgumentException(\"sink config\");"
                + "final String path=c.options().get(\"output.path\");"
                + "context.secrets().withSecret(\"marker\",bytes->{"
                + "if(bytes.length!=1)throw new IllegalArgumentException("
                + "\"sink secret\");return null;});"
                + "return new io.rowweave.stream.spi.StreamSinkWriter(){"
                + "public void write(byte[] frame)throws Exception{"
                + "io.rowweave.stream.contract.StreamUpsertRecord r="
                + "io.rowweave.stream.contract.StreamUpsertRecordCodec"
                + ".decode(frame,4194304,4194304);"
                + "if(!r.hasAfterImage())return;"
                + "java.util.List<io.rowweave.stream.api.StreamField> f="
                + "java.util.Arrays.asList("
                + "new io.rowweave.stream.api.StreamField(\"id\",\"id\","
                + "\"id\",io.rowweave.DataType.LongType,false),"
                + "new io.rowweave.stream.api.StreamField(\"amount\","
                + "\"amount\",\"amount\","
                + "io.rowweave.DataType.BigDecimalType,true));"
                + "long value=new io.rowweave.stream.api.StreamDataRowCodec("
                + "new io.rowweave.stream.api.StreamSchema(f))"
                + ".decode(r.afterImageFrame()).getLong(0);"
                + "if(path==null){synchronized(System.getProperties()){"
                + "String key=\"" + RESULT_PROPERTY
                + "\";String old=System.getProperty(key,\"\");"
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
                + "context)throws Exception{io.rowweave.stream.api."
                + "StreamConnectorConfiguration c=io.rowweave.stream.api."
                + "StreamConnectorConfiguration.decode("
                + "context.configurationFrame());"
                + "if(c.options().size()!=1||!c.options().containsKey("
                + "\"output.path\")||c.secretReferences().size()!=1)"
                + "throw new IllegalArgumentException("
                + "\"transactional sink config\");"
                + "context.secrets().withSecret(\"marker\",bytes->{"
                + "if(bytes.length!=1)throw new IllegalArgumentException("
                + "\"transactional sink secret\");return null;});"
                + "final java.nio.file.Path root=java.nio.file.Paths.get("
                + "c.options().get(\"output.path\"));"
                + "return new io.rowweave.stream.spi."
                + "StreamTransactionalSinkWriter(){"
                + "private final java.io.ByteArrayOutputStream pending="
                + "new java.io.ByteArrayOutputStream();"
                + "public void write(byte[] frame)throws Exception{"
                + "io.rowweave.stream.contract.StreamUpsertRecord change="
                + "io.rowweave.stream.contract.StreamUpsertRecordCodec"
                + ".decode(frame,4194304,4194304);"
                + "if(!change.hasAfterImage())return;"
                + "java.util.List<io.rowweave.stream.api.StreamField> fields="
                + "java.util.Arrays.asList(new io.rowweave.stream.api."
                + "StreamField(\"id\",\"id\",\"id\",io.rowweave."
                + "DataType.LongType,false),new io.rowweave.stream.api."
                + "StreamField(\"amount\",\"amount\",\"amount\","
                + "io.rowweave.DataType.BigDecimalType,true));"
                + "long value=new io.rowweave.stream.api.StreamDataRowCodec("
                + "new io.rowweave.stream.api.StreamSchema(fields)).decode("
                + "change.afterImageFrame()).getLong(0);pending.write("
                + "(Long.toString(value)+\"\\n\").getBytes("
                + "java.nio.charset.StandardCharsets.US_ASCII));"
                + "synchronized(System.getProperties()){String key=\""
                + TRANSACTIONAL_PENDING_PROPERTY
                + "\";System.setProperty(key,Integer.toString(1+"
                + "Integer.parseInt(System.getProperty(key,\"0\"))));}}"
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
                + "}";
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(
                0, 1, InetAddress.getByName("127.0.0.1"))) {
            return socket.getLocalPort();
        }
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
                "-Xms64m", "-Xmx1024m",
                "-XX:MaxDirectMemorySize=256m",
                "-XX:MaxMetaspaceSize=256m",
                "-XX:ReservedCodeCacheSize=128m",
                "-Xss1m", "-XX:ActiveProcessorCount=4",
                "-Drowweave.process.memoryLimitBytes=4294967296",
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
                "-Drowweave.deployment.signer.id.0=postgres-cluster-test",
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
            command.add("-Drowweave.stream.worker.taskSlots=4");
            command.add("-Drowweave.stream.worker.channelCapacity=16");
            command.add("-Drowweave.stream.worker.maximumFrameBytes=1048576");
            command.add("-Drowweave.stream.worker.maximumResidentBytes=67108864");
            command.add("-Drowweave.stream.worker.maximumThreads=16");
            command.add("-D" + PostgresTestSecretResolver
                    .SECRET_PATH_PROPERTY + '=' + secretPath);
            command.add("-D" + PostgresTestSecretResolver
                    .MARKER_PATH_PROPERTY + '=' + markerPath);
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
                throw new AssertionError("cluster process exited: "
                        + readQuiet(log));
            }
            Thread.sleep(20L);
        }
        throw new AssertionError("cluster process not ready: "
                + readQuiet(log));
    }

    private static void awaitLines(Path path, int expected,
            long timeoutMillis) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (Files.isRegularFile(path)
                    && Files.readAllLines(path,
                    StandardCharsets.US_ASCII).size() >= expected) return;
            Thread.sleep(20L);
        }
        throw new AssertionError("expected " + expected + " lines in "
                + path + ": " + readQuiet(path));
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
                hexSha256(leaf.getEncoded()));
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
        byte[] buffer = new byte[4096];
        for (int read; (read = input.read(buffer)) >= 0; ) {
            if (read > 0) output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }

    private static String hexSha256(byte[] bytes) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
        StringBuilder value = new StringBuilder(64);
        for (byte item : digest) value.append(String.format("%02x", item));
        return value.toString();
    }

    private static String processLogs(Path master, Path workerA,
            Path workerB) {
        return "MASTER:\n" + readQuiet(master) + "\nWORKER A:\n"
                + readQuiet(workerA) + "\nWORKER B:\n"
                + readQuiet(workerB);
    }

    private static String readQuiet(Path path) {
        try { return read(path); }
        catch (IOException failure) { return "<unreadable>"; }
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

    private static final class Database implements AutoCloseable {
        private final Process process;
        private final Path log;
        private final int port;

        private Database(Process process, Path log, int port) {
            this.process = process;
            this.log = log;
            this.port = port;
        }

        private static Database start(Path binaries, Path root, int port)
                throws Exception {
            Files.createDirectories(root);
            Path data = root.resolve("data");
            Path initLog = root.resolve("initdb.log");
            Process init = new ProcessBuilder(
                    binaries.resolve("initdb").toString(),
                    "--pgdata", data.toString(), "--auth=trust",
                    "--encoding=UTF8", "--no-locale")
                    .redirectErrorStream(true)
                    .redirectOutput(initLog.toFile()).start();
            if (!init.waitFor(30L, TimeUnit.SECONDS)
                    || init.exitValue() != 0) {
                throw new AssertionError("initdb failed: " + read(initLog));
            }
            Path log = root.resolve("postgres.log");
            Process process = new ProcessBuilder(
                    binaries.resolve("postgres").toString(),
                    "-D", data.toString(), "-h", "127.0.0.1",
                    "-p", Integer.toString(port),
                    "-c", "wal_level=logical",
                    "-c", "max_replication_slots=4",
                    "-c", "max_wal_senders=4", "-c", "fsync=off")
                    .redirectErrorStream(true)
                    .redirectOutput(log.toFile()).start();
            Database result = new Database(process, log, port);
            try { result.awaitReady(); return result; }
            catch (Throwable failure) { result.close(); throw failure; }
        }

        private void awaitReady() throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20L);
            while (System.nanoTime() < deadline) {
                if (!process.isAlive()) {
                    throw new AssertionError("PostgreSQL exited: " + read(log));
                }
                try (Connection ignored = connect("postgres")) { return; }
                catch (Exception notReady) { Thread.sleep(25L); }
            }
            throw new AssertionError("PostgreSQL not ready: " + read(log));
        }

        private void provision() throws Exception {
            try (Connection root = connect("postgres");
                 Statement statement = root.createStatement()) {
                statement.execute("CREATE DATABASE " + DATABASE);
            }
            try (Connection connection = connect(DATABASE);
                 Statement statement = connection.createStatement()) {
                statement.execute("CREATE SCHEMA ledger");
                statement.execute("CREATE TABLE ledger.entries ("
                        + "id bigint PRIMARY KEY, amount numeric(18,2))");
                statement.execute("INSERT INTO ledger.entries "
                        + "VALUES (1, 10.00)");
                statement.execute("CREATE PUBLICATION " + PUBLICATION
                        + " FOR TABLE ledger.entries");
            }
        }

        private PostgresConnectorConfiguration configuration()
                throws Exception {
            try (Connection connection = connect(DATABASE)) {
                String system;
                long databaseOid;
                try (Statement statement = connection.createStatement();
                     ResultSet result = statement.executeQuery(
                             "SELECT system_identifier::text, "
                                     + "(SELECT oid FROM pg_database WHERE "
                                     + "datname=current_database()) "
                                     + "FROM pg_control_system()")) {
                    assertTrue(result.next());
                    system = result.getString(1);
                    databaseOid = result.getLong(2);
                }
                long tableOid;
                List<PostgresColumnBinding> columns = new ArrayList<>();
                try (Statement statement = connection.createStatement();
                     ResultSet result = statement.executeQuery(
                             "SELECT c.oid,a.attnum,a.attname,a.atttypid,"
                                     + "a.atttypmod,NOT a.attnotnull FROM "
                                     + "pg_class c JOIN pg_namespace n ON "
                                     + "n.oid=c.relnamespace JOIN "
                                     + "pg_attribute a ON a.attrelid=c.oid "
                                     + "WHERE n.nspname='ledger' AND "
                                     + "c.relname='entries' AND a.attnum>0 "
                                     + "AND NOT a.attisdropped ORDER BY "
                                     + "a.attnum")) {
                    assertTrue(result.next());
                    tableOid = result.getLong(1);
                    do {
                        columns.add(new PostgresColumnBinding(
                                result.getString(3), result.getString(3),
                                result.getInt(2), result.getLong(4),
                                result.getInt(5), result.getBoolean(6)));
                    } while (result.next());
                }
                return PostgresConnectorConfiguration.builder()
                        .host("127.0.0.1").port(port).database(DATABASE)
                        .slot(SLOT).publication(PUBLICATION)
                        .databaseSystemIdentifier(system)
                        .databaseOid(databaseOid)
                        .tlsMode(PostgresTlsMode.DISABLED)
                        .credentialsReference(CREDENTIAL_REFERENCE)
                        .tables(Collections.singletonList(
                                new PostgresTableBinding(
                                        "ledger", "entries", tableOid,
                                        PostgresReplicaIdentity.DEFAULT, 0L,
                                        columns,
                                        Collections.singletonList(1))))
                        .maximumTransactionChanges(32)
                        .maximumTransactionBytes(1024 * 1024)
                        .maximumRetainedWalBytes(32L * 1024 * 1024)
                        .connectTimeoutMillis(2_000)
                        .socketTimeoutMillis(2_000)
                        .statusIntervalMillis(500).build();
            }
        }

        private void insert(long id, String amount) throws Exception {
            try (Connection connection = connect(DATABASE);
                 PreparedStatement statement = connection.prepareStatement(
                         "INSERT INTO ledger.entries VALUES (?, ?::numeric)")) {
                statement.setLong(1, id);
                statement.setString(2, amount);
                statement.executeUpdate();
            }
        }

        private void insertUpdateDelete() throws Exception {
            try (Connection connection = connect(DATABASE);
                 Statement statement = connection.createStatement()) {
                connection.setAutoCommit(false);
                statement.executeUpdate("INSERT INTO ledger.entries "
                        + "VALUES (2, 20.00)");
                statement.executeUpdate("UPDATE ledger.entries SET "
                        + "id = 3, amount = 30.00 WHERE id = 2");
                statement.executeUpdate("DELETE FROM ledger.entries "
                        + "WHERE id = 3");
                connection.commit();
            }
        }

        private String confirmedFlush() throws Exception {
            try (Connection connection = connect(DATABASE);
                 PreparedStatement statement = connection.prepareStatement(
                         "SELECT confirmed_flush_lsn::text FROM "
                                 + "pg_replication_slots WHERE slot_name=?")) {
                statement.setString(1, SLOT);
                try (ResultSet result = statement.executeQuery()) {
                    assertTrue(result.next());
                    return result.getString(1);
                }
            }
        }

        private void awaitConfirmedAfter(String before) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
            while (System.nanoTime() < deadline) {
                try (Connection connection = connect(DATABASE);
                     PreparedStatement statement = connection.prepareStatement(
                             "SELECT pg_wal_lsn_diff(confirmed_flush_lsn,"
                                     + "?::pg_lsn)>0 FROM "
                                     + "pg_replication_slots WHERE "
                                     + "slot_name=?")) {
                    statement.setString(1, before);
                    statement.setString(2, SLOT);
                    try (ResultSet result = statement.executeQuery()) {
                        if (result.next() && result.getBoolean(1)) return;
                    }
                }
                Thread.sleep(20L);
            }
            throw new AssertionError("slot did not advance after checkpoint");
        }

        private Connection connect(String database) throws Exception {
            Properties properties = new Properties();
            properties.setProperty("user", System.getProperty("user.name"));
            properties.setProperty("password", "unused");
            properties.setProperty("sslmode", "disable");
            return DriverManager.getConnection(
                    "jdbc:postgresql://127.0.0.1:" + port + '/' + database,
                    properties);
        }

        @Override public void close() throws Exception {
            process.destroy();
            if (!process.waitFor(10L, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                assertTrue(process.waitFor(5L, TimeUnit.SECONDS));
            }
        }
    }

    private static String read(Path path) throws IOException {
        if (!Files.exists(path)) return "<missing>";
        byte[] bytes = Files.readAllBytes(path);
        int start = Math.max(0, bytes.length - 16 * 1024);
        return new String(bytes, start, bytes.length - start,
                StandardCharsets.UTF_8);
    }
}
