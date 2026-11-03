package io.rowweave.cluster;

import io.rowweave.IncrementalSourceProvider;
import io.rowweave.ManifestFileSource;
import io.rowweave.ManifestFileSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.artifact.FileArtifactRepository;
import io.rowweave.artifact.InMemoryArtifactRepository;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.cluster.registry.zk.ZkServiceRegistry;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.deployment.DeploymentRuntime;
import io.rowweave.deployment.SignedIncrementalSourceProviderFixture;
import io.rowweave.incremental.IncrementalAuthorityDecision;
import io.rowweave.incremental.IncrementalAuthorityGateway;
import io.rowweave.incremental.IncrementalAuthorityGatewayProvider;
import io.rowweave.incremental.IncrementalControlMember;
import io.rowweave.incremental.IncrementalControlMemberCodec;
import io.rowweave.incremental.IncrementalDefinition;
import io.rowweave.incremental.IncrementalFrontier;
import io.rowweave.incremental.IncrementalFrontierAuthorityState;
import io.rowweave.incremental.IncrementalInitializeRequest;
import io.rowweave.incremental.IncrementalJobClient;
import io.rowweave.incremental.IncrementalJobResult;
import io.rowweave.incremental.IncrementalPlanDescriptor;
import io.rowweave.incremental.IncrementalPlanTemplateFingerprint;
import io.rowweave.incremental.IncrementalRunMode;
import io.rowweave.incremental.IncrementalRunSubmission;
import io.rowweave.incremental.IncrementalSourceConfig;
import io.rowweave.incremental.IncrementalSourceProviderContract;
import io.rowweave.incremental.IncrementalSourceProviderContractCodec;
import io.rowweave.incremental.InMemoryIncrementalAuthorityStore;
import io.rowweave.incremental.FrontierRecoveryToken;
import io.rowweave.internal.ProviderActionSupervisor;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RunStatus;
import io.rowweave.run.RunCancellationResult;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** F5 deployment proof: real ZK sessions + RPC + Master + two Workers. */
class ZkIncrementalClusterExecutionTest {

    @RepeatedTest(16)
    void workerLossRetriesTheFrozenInterval(@TempDir Path root)
            throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry masterRegistry =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry workerRegistry0 =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry workerRegistry1 =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry clientRegistry =
                     new ZkServiceRegistry(zk.getConnectString());
             SignedIncrementalSourceProviderFixture.Session deployments =
                     SignedIncrementalSourceProviderFixture.open(root);
             DeploymentRuntime masterRuntime = deployments.staticRuntime();
             DeploymentRuntime workerRuntime = deployments.workerRuntime();
             MasterServer master = new MasterServer(
                     60_000L, 0, masterRegistry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     new FileArtifactRepository(root.resolve("artifacts")),
                     masterRuntime);
             MasterElection election = new MasterElection(
                     masterRegistry,
                     new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();
            awaitRecoveryInventory(master);
            File spill0 = root.resolve("retry-worker-0").toFile();
            File spill1 = root.resolve("retry-worker-1").toFile();
            assertTrue(spill0.mkdirs());
            assertTrue(spill1.mkdirs());
            try (WorkerServer worker0 = WorkerServer.newUnguarded(
                         "retry-worker-0", 8,
                         new RegistryMasterLocator(workerRegistry0), spill0,
                         workerRuntime);
                 WorkerServer worker1 = WorkerServer.newUnguarded(
                         "retry-worker-1", 8,
                         new RegistryMasterLocator(workerRegistry1), spill1,
                         workerRuntime);
                 ClusterJobClient client = new ClusterJobClient(
                         new RegistryMasterLocator(clientRegistry), masterRuntime)) {
                awaitWorkers(master, 2);
                IncrementalRunSubmission submission = submission(
                        root, "block-once");
                IncrementalAuthorityGateway gateway =
                        ((IncrementalAuthorityGatewayProvider) masterRegistry)
                                .incrementalAuthorityGateway();
                initialize(gateway, submission);
                ExecutorService executor = Executors.newSingleThreadExecutor();
                try {
                    Future<io.rowweave.plan.ExecutionReport> submitted =
                            executor.submit(
                                    () -> client.submitIncrementalRun(
                                            submission));
                    String runId = awaitRunningRun(
                            gateway, submission.feedId());
                    awaitBlockOnceConsumed(deployments);
                    String artifactId = gateway.runSnapshot(runId).artifactId();
                    String sourceWorker = sourceWorker(master.lastPhysicalPlan());
                    if ("retry-worker-0".equals(sourceWorker)) {
                        worker1.close();
                    } else {
                        assertEquals("retry-worker-1", sourceWorker);
                        worker0.close();
                    }

                    assertEquals(2L, submitted.get(
                            20L, TimeUnit.SECONDS).rowsOut());
                    assertEquals(IncrementalFrontierAuthorityState.READY,
                            gateway.record(submission.feedId()).state());
                    assertEquals(IncrementalFrontier.int64Sequence(10L),
                            gateway.record(submission.feedId())
                                    .committedFrontier());
                    assertEquals(RunStatus.SUCCEEDED,
                            gateway.runSnapshot(runId).status());
                    assertEquals(2,
                            gateway.runSnapshot(runId).attemptCount());
                    assertEquals(artifactId,
                            gateway.runSnapshot(runId).artifactId());
                } finally {
                    executor.shutdownNow();
                }
            }
        }
    }

    @RepeatedTest(16)
    void runningCancellationClosesRunAndFrontierTogether(@TempDir Path root)
            throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry masterRegistry =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry workerRegistry0 =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry workerRegistry1 =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry clientRegistry =
                     new ZkServiceRegistry(zk.getConnectString());
             SignedIncrementalSourceProviderFixture.Session deployments =
                     SignedIncrementalSourceProviderFixture.open(root);
             DeploymentRuntime masterRuntime = deployments.staticRuntime();
             DeploymentRuntime workerRuntime = deployments.workerRuntime();
             MasterServer master = new MasterServer(
                     60_000L, 0, masterRegistry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     new FileArtifactRepository(root.resolve("artifacts")),
                     masterRuntime);
             MasterElection election = new MasterElection(
                     masterRegistry,
                     new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();
            awaitRecoveryInventory(master);
            File spill0 = root.resolve("cancel-worker-0").toFile();
            File spill1 = root.resolve("cancel-worker-1").toFile();
            assertTrue(spill0.mkdirs());
            assertTrue(spill1.mkdirs());
            try (WorkerServer worker0 = WorkerServer.newUnguarded(
                         "cancel-worker-0", 8,
                         new RegistryMasterLocator(workerRegistry0), spill0,
                         workerRuntime);
                 WorkerServer worker1 = WorkerServer.newUnguarded(
                         "cancel-worker-1", 8,
                         new RegistryMasterLocator(workerRegistry1), spill1,
                         workerRuntime);
                 ClusterJobClient client = new ClusterJobClient(
                         new RegistryMasterLocator(clientRegistry), masterRuntime)) {
                awaitWorkers(master, 2);
                IncrementalRunSubmission submission = submission(
                        root, "block-read");
                IncrementalAuthorityGateway gateway =
                        ((IncrementalAuthorityGatewayProvider) masterRegistry)
                                .incrementalAuthorityGateway();
                initialize(gateway, submission);
                ExecutorService executor = Executors.newSingleThreadExecutor();
                try {
                    Future<?> submitted = executor.submit(
                            () -> client.submitIncrementalRun(submission));
                    String runId = awaitRunningRun(
                            gateway, submission.feedId());
                    RunCancellationResult canceled = client.cancelRun(
                            runId, "cancel-running-incremental");
                    assertEquals(
                            RunCancellationResult.Disposition.ACCEPTED,
                            canceled.disposition());
                    assertThrows(ExecutionException.class,
                            () -> submitted.get(10L, TimeUnit.SECONDS));
                    awaitTerminalFrontier(gateway, submission.feedId());

                    assertEquals(IncrementalFrontierAuthorityState.READY,
                            gateway.record(submission.feedId()).state());
                    assertEquals(IncrementalFrontier.begin(),
                            gateway.record(submission.feedId())
                                    .committedFrontier());
                    assertEquals(RunStatus.CANCELED,
                            gateway.runSnapshot(runId).status());
                    assertNull(
                            new io.rowweave.commit.ManifestFileStore(
                                    root.resolve("manifest")).latest());
                } finally {
                    executor.shutdownNow();
                }
            }
        }
    }

    @RepeatedTest(16)
    void replacementMasterFinishesVisiblePublication(@TempDir Path root)
            throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry registryA =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry registryB =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry workerRegistry0 =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry workerRegistry1 =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry clientRegistry =
                     new ZkServiceRegistry(zk.getConnectString());
             SignedIncrementalSourceProviderFixture.Session deployments =
                     SignedIncrementalSourceProviderFixture.open(root);
             DeploymentRuntime masterRuntime = deployments.staticRuntime();
             DeploymentRuntime workerRuntime = deployments.workerRuntime();
             MasterServer masterA = new MasterServer(
                     60_000L, 0, registryA,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     new FileArtifactRepository(root.resolve("artifacts")),
                     masterRuntime);
             MasterServer masterB = new MasterServer(
                     60_000L, 0, registryB,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     new FileArtifactRepository(root.resolve("artifacts")),
                     masterRuntime);
             MasterElection electionA = new MasterElection(
                     registryA, new Endpoint("127.0.0.1", masterA.port()));
             MasterElection electionB = new MasterElection(
                     registryB, new Endpoint("127.0.0.1", masterB.port()))) {
            masterA.bindLeadership(electionA);
            masterB.bindLeadership(electionB);
            electionA.start();
            awaitLeader(electionA);
            awaitRecoveryInventory(masterA);
            electionB.start();
            File spill0 = root.resolve("replacement-worker-0").toFile();
            File spill1 = root.resolve("replacement-worker-1").toFile();
            assertTrue(spill0.mkdirs());
            assertTrue(spill1.mkdirs());
            try (WorkerServer worker0 = WorkerServer.newUnguarded(
                         "replacement-worker-0", 8,
                         new RegistryMasterLocator(workerRegistry0), spill0,
                         workerRuntime);
                 WorkerServer worker1 = WorkerServer.newUnguarded(
                         "replacement-worker-1", 8,
                         new RegistryMasterLocator(workerRegistry1), spill1,
                         workerRuntime);
                 ClusterJobClient client = new ClusterJobClient(
                         new RegistryMasterLocator(clientRegistry), masterRuntime)) {
                awaitWorkers(masterA, 2);
                IncrementalRunSubmission submission = submission(root);
                IncrementalAuthorityGateway gateway =
                        ((IncrementalAuthorityGatewayProvider) registryA)
                                .incrementalAuthorityGateway();
                initialize(gateway, submission);
                AtomicReference<String> runId = new AtomicReference<>();
                AtomicBoolean cut = new AtomicBoolean();
                masterA.setManifestPublishHooks(null, () -> {
                    if (cut.compareAndSet(false, true)) {
                        runId.set(gateway.record(submission.feedId())
                                .activeInterval().fence().runId());
                        electionA.close();
                        throw new IllegalStateException(
                                "injected leader loss after publication");
                    }
                });

                assertThrows(RuntimeException.class,
                        () -> client.submitIncrementalRun(submission));
                assertTrue(cut.get());
                assertNotNull(runId.get());
                awaitLeader(electionB);
                awaitTerminalFrontier(gateway, submission.feedId());

                assertEquals(IncrementalFrontierAuthorityState.READY,
                        gateway.record(submission.feedId()).state());
                assertEquals(IncrementalFrontier.int64Sequence(10L),
                        gateway.record(submission.feedId()).committedFrontier());
                assertEquals(RunStatus.SUCCEEDED,
                        gateway.runSnapshot(runId.get()).status());
                assertNotNull(new io.rowweave.commit.ManifestFileStore(
                        root.resolve("manifest")).latest());
            }
        }
    }

    @RepeatedTest(16)
    void twoWorkersConvergeAfterDurableIntentAckLoss(@TempDir Path root)
            throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry masterRegistry =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry workerRegistry0 =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry workerRegistry1 =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry clientRegistry =
                     new ZkServiceRegistry(zk.getConnectString());
             SignedIncrementalSourceProviderFixture.Session deployments =
                     SignedIncrementalSourceProviderFixture.open(root);
             DeploymentRuntime masterRuntime = deployments.staticRuntime();
             DeploymentRuntime workerRuntime = deployments.workerRuntime();
             MasterServer master = new MasterServer(
                     60_000L, 0, masterRegistry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     new FileArtifactRepository(root.resolve("artifacts")),
                     masterRuntime);
             MasterElection election = new MasterElection(
                     masterRegistry,
                     new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();
            awaitRecoveryInventory(master);
            File spill0 = root.resolve("worker-0").toFile();
            File spill1 = root.resolve("worker-1").toFile();
            assertTrue(spill0.mkdirs());
            assertTrue(spill1.mkdirs());
            try (WorkerServer worker0 = WorkerServer.newUnguarded(
                         "incremental-worker-0", 8,
                         new RegistryMasterLocator(workerRegistry0), spill0,
                         workerRuntime);
                 WorkerServer worker1 = WorkerServer.newUnguarded(
                         "incremental-worker-1", 8,
                         new RegistryMasterLocator(workerRegistry1), spill1,
                         workerRuntime);
                 ClusterJobClient client = new ClusterJobClient(
                         new RegistryMasterLocator(clientRegistry), masterRuntime)) {
                awaitWorkers(master, 2);
                IncrementalRunSubmission submission = submission(root);
                IncrementalAuthorityGateway gateway =
                        ((IncrementalAuthorityGatewayProvider) masterRegistry)
                                .incrementalAuthorityGateway();
                initialize(gateway, submission);
                AtomicBoolean lostAck = new AtomicBoolean();
                master.setDurableDecisionHooksForTesting(null, () -> {
                    if (lostAck.compareAndSet(false, true)) {
                        throw new IllegalStateException(
                                "injected durable intent ACK loss");
                    }
                });

                assertEquals(4L,
                        client.submitIncrementalRun(submission).rowsOut());
                assertTrue(lostAck.get());
                assertEquals(IncrementalFrontierAuthorityState.READY,
                        gateway.record(submission.feedId()).state());
                assertEquals(IncrementalFrontier.int64Sequence(10L),
                        gateway.record(submission.feedId()).committedFrontier());
                String runId = gateway.record(submission.feedId())
                        .lastCommitReceipt().fence().runId();
                assertEquals(RunStatus.SUCCEEDED,
                        gateway.runSnapshot(runId).status());
                assertNotNull(new io.rowweave.commit.ManifestFileStore(
                        root.resolve("manifest")).latest());
                assertTrue(successfulAssignments(worker0, runId) > 0,
                        "first Worker must execute the incremental DAG");
                assertTrue(successfulAssignments(worker1, runId) > 0,
                        "second Worker must execute the incremental DAG");
            }
        }
    }

    @Test
    void localAndZkTwoWorkerModesAndTwoToFiveSinksStayInParity(
            @TempDir Path root) throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry masterRegistry =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry workerRegistry0 =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry workerRegistry1 =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry clientRegistry =
                     new ZkServiceRegistry(zk.getConnectString());
             SignedIncrementalSourceProviderFixture.Session deployments =
                     SignedIncrementalSourceProviderFixture.open(root);
             DeploymentRuntime masterRuntime = deployments.staticRuntime();
             DeploymentRuntime workerRuntime = deployments.workerRuntime();
             MasterServer master = new MasterServer(
                     60_000L, 0, masterRegistry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     new FileArtifactRepository(root.resolve("parity-artifacts")),
                     masterRuntime);
             MasterElection election = new MasterElection(
                     masterRegistry,
                     new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();
            awaitRecoveryInventory(master);
            File spill0 = root.resolve("parity-worker-0").toFile();
            File spill1 = root.resolve("parity-worker-1").toFile();
            assertTrue(spill0.mkdirs());
            assertTrue(spill1.mkdirs());
            try (WorkerServer worker0 = WorkerServer.newUnguarded(
                         "parity-worker-0", 8,
                         new RegistryMasterLocator(workerRegistry0), spill0,
                         workerRuntime);
                 WorkerServer worker1 = WorkerServer.newUnguarded(
                         "parity-worker-1", 8,
                         new RegistryMasterLocator(workerRegistry1), spill1,
                         workerRuntime);
                 ClusterJobClient clusterClient = new ClusterJobClient(
                         new RegistryMasterLocator(clientRegistry), masterRuntime)) {
                awaitWorkers(master, 2);
                IncrementalSourceProvider localProvider = workerRuntime
                        .incrementalSourceProviderRegistry().loadProvider(
                                SignedIncrementalSourceProviderFixture.contract()
                                        .providerKey(),
                                ProviderActionSupervisor.processWide(4),
                                System.nanoTime()
                                        + TimeUnit.SECONDS.toNanos(10L));
                IncrementalAuthorityGateway clusterAuthority =
                        ((IncrementalAuthorityGatewayProvider) masterRegistry)
                                .incrementalAuthorityGateway();

                for (int sinkCount = 2; sinkCount <= 5; sinkCount++) {
                    ParityScenario local = parityScenario(
                            root.resolve("local-" + sinkCount), sinkCount);
                    ParityScenario cluster = parityScenario(
                            root.resolve("cluster-" + sinkCount), sinkCount);
                    Map<String, String> localResult = runLocalParity(
                            local, localProvider);
                    Map<String, String> clusterResult = runClusterParity(
                            cluster, clusterClient, clusterAuthority);

                    assertEquals(localResult, clusterResult,
                            "local and ZK M+2W CDC observations must match for "
                                    + sinkCount + " user Sinks");
                }
            }
        }
    }

    private static IncrementalRunSubmission submission(Path root)
            throws Exception {
        return submission(root, "two-splits");
    }

    private static IncrementalRunSubmission submission(
            Path root, String sourceConfiguration) throws Exception {
        DataSchema meta = SignedIncrementalSourceProviderFixture.sourceDataSchema();
        IncrementalPlanDescriptor descriptor = new IncrementalPlanDescriptor(
                "source", digest("frontier-contract"),
                Collections.singletonList(0), 1,
                Collections.singletonList(2),
                hexBytes(io.rowweave.commit.SchemaFingerprint.of(meta)),
                false, digest("partitions"),
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
        PlanNode source = PlanNode.placeholderSource(meta)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "source")
                .withIncrementalPlanDescriptor(descriptor);
        ManifestFileSink output = new ManifestFileSink(
                meta, root.resolve("manifest").toString(), "changes");
        PlanNode sink = PlanNode.sink(meta, 0, output.toProviderSink())
                .param(PlanNode.OPERATOR_UID_PARAM, "sink").build();
        PlanGraph template = new PlanGraph(Arrays.asList(source, sink), 2);
        ObjectTrustContext trust = ObjectTrustContext.mtlsBuiltIns();
        IncrementalDefinition definition =
                IncrementalPlanTemplateFingerprint.derive(
                        "zk-incremental-definition", template, trust,
                        ignored -> { });
        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrapDurable(
                definition.definitionId(), template, trust, ignored -> { });
        IncrementalSourceProviderContract contract =
                SignedIncrementalSourceProviderFixture.contract();
        IncrementalSourceConfig configuration = new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract),
                sourceConfiguration.getBytes(StandardCharsets.UTF_8));
        return IncrementalRunSubmission.normal(
                "zk-feed", "zk-trigger", IncrementalRunMode.SNAPSHOT_V1,
                "attempt-1", "publication-1", 3, 0L, 10L,
                definition, artifact, contract, configuration);
    }

    private static ParityScenario parityScenario(Path root, int sinkCount)
            throws Exception {
        DataSchema meta = SignedIncrementalSourceProviderFixture.sourceDataSchema();
        IncrementalPlanDescriptor descriptor = new IncrementalPlanDescriptor(
                "source", digest("frontier-contract"),
                Collections.singletonList(0), 1,
                Collections.singletonList(2),
                hexBytes(io.rowweave.commit.SchemaFingerprint.of(meta)),
                false, digest("partitions"),
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
        List<PlanNode> nodes = new ArrayList<>();
        nodes.add(PlanNode.placeholderSource(meta)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "source")
                .withIncrementalPlanDescriptor(descriptor));
        Path output = root.resolve("manifest");
        for (int sink = 0; sink < sinkCount; sink++) {
            ManifestFileSink destination = new ManifestFileSink(
                    meta, output.toString(), "changes-" + sink);
            nodes.add(PlanNode.sink(meta, 0, destination.toProviderSink())
                    .param(PlanNode.OPERATOR_UID_PARAM, "sink-" + sink).build());
        }
        PlanGraph template = new PlanGraph(nodes, 2);
        ObjectTrustContext trust = ObjectTrustContext.mtlsBuiltIns();
        IncrementalDefinition definition =
                IncrementalPlanTemplateFingerprint.derive(
                        "parity-definition-" + sinkCount, template, trust,
                        ignored -> { });
        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrapDurable(
                definition.definitionId(), template, trust, ignored -> { });
        IncrementalSourceProviderContract contract =
                SignedIncrementalSourceProviderFixture.contract();
        IncrementalSourceConfig configuration = new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract),
                "two-splits".getBytes(StandardCharsets.UTF_8));
        return new ParityScenario(
                "parity-feed-" + sinkCount, output, sinkCount,
                template, definition, artifact, contract, configuration, trust);
    }

    private static Map<String, String> runLocalParity(
            ParityScenario scenario, IncrementalSourceProvider provider)
            throws Exception {
        InMemoryIncrementalAuthorityStore authority =
                new InMemoryIncrementalAuthorityStore();
        IncrementalRunSubmission snapshot = scenario.normal(
                IncrementalRunMode.SNAPSHOT_V1, "snapshot-trigger",
                "snapshot-attempt", "snapshot-publication");
        initialize(authority, snapshot);
        FrontierRecoveryToken token = authority.claimOwnership(
                scenario.feedId, 0L, 0L,
                new RecoveryOwner("local-parity-master", 1L)).token();
        IncrementalJobClient client = new IncrementalJobClient(
                scenario.definition, scenario.template, scenario.contract,
                scenario.configuration, provider,
                new InMemoryArtifactRepository(64, 64L * 1024L * 1024L),
                authority, scenario.trust,
                ProviderActionSupervisor.processWide(4));
        Map<String, String> result = new LinkedHashMap<>();

        IncrementalJobResult snapshotResult = client.submitNormal(
                scenario.feedId, token, "snapshot-trigger",
                IncrementalRunMode.SNAPSHOT_V1,
                "snapshot-attempt", "snapshot-publication", 3, 0L, 10L,
                System.nanoTime() + TimeUnit.SECONDS.toNanos(10L),
                ignored -> { });
        assertEquals(IncrementalJobResult.Code.SUCCEEDED,
                snapshotResult.code());
        observe(result, "snapshot", snapshotResult.executionReport(),
                authority, scenario);

        IncrementalJobResult incrementalResult = client.submitNormal(
                scenario.feedId, token, "incremental-trigger",
                IncrementalRunMode.INCREMENTAL_V1,
                "incremental-attempt", "incremental-publication", 3, 0L, 20L,
                System.nanoTime() + TimeUnit.SECONDS.toNanos(10L),
                ignored -> { });
        assertEquals(IncrementalJobResult.Code.SUCCEEDED,
                incrementalResult.code());
        observe(result, "incremental", incrementalResult.executionReport(),
                authority, scenario);

        IncrementalJobResult backfillResult = client.submitBackfill(
                scenario.feedId, token, 1L,
                IncrementalFrontier.int64Sequence(0L),
                IncrementalFrontier.int64Sequence(2L),
                "backfill-request", "backfill-attempt",
                "backfill-publication", 3, 0L, 30L,
                System.nanoTime() + TimeUnit.SECONDS.toNanos(10L),
                ignored -> { });
        assertEquals(IncrementalJobResult.Code.SUCCEEDED,
                backfillResult.code());
        observe(result, "backfill", backfillResult.executionReport(),
                authority, scenario);
        finishObservation(result, authority, scenario.feedId);
        return result;
    }

    private static Map<String, String> runClusterParity(
            ParityScenario scenario, ClusterJobClient client,
            IncrementalAuthorityGateway authority) throws Exception {
        IncrementalRunSubmission snapshot = scenario.normal(
                IncrementalRunMode.SNAPSHOT_V1, "snapshot-trigger",
                "snapshot-attempt", "snapshot-publication");
        client.initializeIncrementalFeed(
                "initialize-" + scenario.feedId, 1L, snapshot);
        Map<String, String> result = new LinkedHashMap<>();

        observe(result, "snapshot", client.submitIncrementalRun(snapshot),
                authority, scenario);
        observe(result, "incremental", client.submitIncrementalRun(
                        scenario.normal(IncrementalRunMode.INCREMENTAL_V1,
                                "incremental-trigger", "incremental-attempt",
                                "incremental-publication")),
                authority, scenario);
        observe(result, "backfill", client.submitIncrementalRun(
                        scenario.backfill()), authority, scenario);
        finishObservation(result, authority, scenario.feedId);
        return result;
    }

    private static void observe(
            Map<String, String> observation, String phase,
            ExecutionReport report, IncrementalAuthorityGateway authority,
            ParityScenario scenario) throws Exception {
        CommitManifest manifest = new ManifestFileStore(
                scenario.output).latest();
        assertNotNull(manifest);
        CommitManifest.Member controlMember = manifest.members().stream()
                .filter(member -> IncrementalControlMemberCodec.MEMBER_NAME
                        .equals(member.name()))
                .findFirst().orElseThrow(AssertionError::new);
        IncrementalControlMember control = IncrementalControlMemberCodec.decode(
                Files.readAllBytes(scenario.output.resolve(
                        controlMember.relativeDataPath())));
        List<String> users = new ArrayList<>();
        for (CommitManifest.Member member : manifest.members()) {
            if (!IncrementalControlMemberCodec.MEMBER_NAME.equals(member.name())) {
                users.add(member.name());
            }
        }
        Collections.sort(users);
        assertEquals(scenario.sinkCount, users.size());
        assertEquals(scenario.sinkCount + 1, manifest.members().size());
        List<String> rows = new ArrayList<>();
        for (String user : users) {
            try (io.rowweave.FileSource.Reader reader = new ManifestFileSource(
                    SignedIncrementalSourceProviderFixture.sourceDataSchema(),
                    scenario.output.toString(), user).openReader()) {
                while (reader.hasNext()) {
                    DataRow row = reader.next();
                    rows.add(user + ':' + row.get(0) + ':' + row.get(1)
                            + ':' + row.get(2));
                }
            }
        }
        Collections.sort(rows);
        observation.put(phase + ".rows", Long.toString(report.rowsOut()));
        observation.put(phase + ".data", rows.toString());
        observation.put(phase + ".members", users.toString());
        observation.put(phase + ".memberCount",
                Integer.toString(manifest.members().size()));
        observation.put(phase + ".mode", control.mode().name());
        observation.put(phase + ".publication", control.publicationId());
        observation.put(phase + ".generation",
                Long.toString(control.generation()));
        observation.put(phase + ".runStatus",
                authority.runSnapshot(control.runId()).status().name());
    }

    private static void finishObservation(
            Map<String, String> observation,
            IncrementalAuthorityGateway authority, String feedId) {
        observation.put("frontier", Long.toString(
                authority.record(feedId).committedFrontier().sequence()));
        observation.put("backfillOrdinal", Long.toString(
                authority.record(feedId).lastCompletedBackfillOrdinal()));
        observation.put("state", authority.record(feedId).state().name());
    }

    private static void initialize(
            IncrementalAuthorityGateway gateway,
            IncrementalRunSubmission submission) {
        IncrementalInitializeRequest request = new IncrementalInitializeRequest(
                submission.feedId(), 1L,
                submission.definition().fingerprint(),
                IncrementalSourceProviderContractCodec.digest(
                        submission.sourceContract()),
                submission.definition().planDescriptor()
                        .frontierContractDigest(),
                submission.sourceContract().frontierKind());
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                gateway.initialize("initialize-zk-feed", request).code());
    }

    private static void awaitWorkers(MasterServer master, int count)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        while (master.workers().size() < count
                && System.nanoTime() < deadline) {
            Thread.sleep(20L);
        }
        assertEquals(count, master.workers().size());
    }

    private static void awaitBlockOnceConsumed(
            SignedIncrementalSourceProviderFixture.Session deployments)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        while (!deployments.blockOnceConsumed()
                && System.nanoTime() < deadline) {
            Thread.sleep(20L);
        }
        assertTrue(deployments.blockOnceConsumed(),
                "source must enter the injected blocking read before Worker loss");
    }

    private static void awaitLeader(MasterElection election)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        while (!election.isLeader() && System.nanoTime() < deadline) {
            Thread.sleep(20L);
        }
        assertTrue(election.isLeader());
    }

    private static void awaitTerminalFrontier(
            IncrementalAuthorityGateway gateway, String feedId)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15L);
        while (gateway.record(feedId).state()
                != IncrementalFrontierAuthorityState.READY
                && System.nanoTime() < deadline) {
            Thread.sleep(20L);
        }
    }

    private static String awaitRunningRun(
            IncrementalAuthorityGateway gateway, String feedId)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        String runId = null;
        while (System.nanoTime() < deadline) {
            io.rowweave.incremental.IncrementalAuthorityRecord record =
                    gateway.record(feedId);
            if (record != null && record.activeInterval() != null) {
                runId = record.activeInterval().fence().runId();
                if (gateway.runSnapshot(runId).currentAttempt().status()
                        == io.rowweave.run.AttemptStatus.RUNNING) {
                    return runId;
                }
            }
            Thread.sleep(20L);
        }
        assertNotNull(runId);
        assertEquals(io.rowweave.run.AttemptStatus.RUNNING,
                gateway.runSnapshot(runId).currentAttempt().status());
        return runId;
    }

    private static void awaitRecoveryInventory(MasterServer master)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        while (master.recoveryInventoryGeneration() == 0L
                && System.nanoTime() < deadline) {
            Thread.sleep(20L);
        }
        long generation = master.recoveryInventoryGeneration();
        assertTrue(generation > 0L);
        assertTrue(master.awaitRecoveryInventory(
                generation, 10L, TimeUnit.SECONDS));
    }

    private static int successfulAssignments(
            WorkerServer worker, String runId) throws Exception {
        java.lang.reflect.Field assignmentsField = WorkerServer.class
                .getDeclaredField("assignments");
        assignmentsField.setAccessible(true);
        Object ledger = assignmentsField.get(worker);
        java.lang.reflect.Field slotsField = ledger.getClass()
                .getDeclaredField("slots");
        slotsField.setAccessible(true);
        int successful = 0;
        synchronized (ledger) {
            @SuppressWarnings("unchecked")
            java.util.Map<Long, Object> slots =
                    (java.util.Map<Long, Object>) slotsField.get(ledger);
            for (Object slot : slots.values()) {
                java.lang.reflect.Field fenceField = slot.getClass()
                        .getDeclaredField("runAttemptFence");
                java.lang.reflect.Field receiptField = slot.getClass()
                        .getDeclaredField("terminalReceipt");
                fenceField.setAccessible(true);
                receiptField.setAccessible(true);
                io.rowweave.run.RunAttemptFence fence =
                        (io.rowweave.run.RunAttemptFence) fenceField.get(slot);
                io.rowweave.run.StageTerminalReceipt receipt =
                        (io.rowweave.run.StageTerminalReceipt)
                                receiptField.get(slot);
                if (runId.equals(fence.runId()) && receipt != null
                        && receipt.succeeded()) {
                    successful++;
                }
            }
        }
        return successful;
    }

    private static String sourceWorker(PhysicalPlan plan) {
        assertNotNull(plan);
        int sourceStage = Integer.MAX_VALUE;
        for (PhysicalPlan.Instance instance : plan.instances()) {
            sourceStage = Math.min(sourceStage, instance.logicalStage());
        }
        for (PhysicalPlan.Instance instance : plan.instances()) {
            if (instance.logicalStage() == sourceStage
                    && instance.instanceIndex() == 0) {
                return instance.workerId();
            }
        }
        throw new AssertionError("source instance zero is absent");
    }

    private static byte[] digest(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(
                value.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] hexBytes(String value) {
        byte[] result = new byte[value.length() / 2];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) Integer.parseInt(
                    value.substring(i * 2, i * 2 + 2), 16);
        }
        return result;
    }

    private static final class ParityScenario {
        private final String feedId;
        private final Path output;
        private final int sinkCount;
        private final PlanGraph template;
        private final IncrementalDefinition definition;
        private final PlanArtifact artifact;
        private final IncrementalSourceProviderContract contract;
        private final IncrementalSourceConfig configuration;
        private final ObjectTrustContext trust;

        private ParityScenario(
                String feedId, Path output, int sinkCount,
                PlanGraph template, IncrementalDefinition definition,
                PlanArtifact artifact,
                IncrementalSourceProviderContract contract,
                IncrementalSourceConfig configuration,
                ObjectTrustContext trust) {
            this.feedId = feedId;
            this.output = output;
            this.sinkCount = sinkCount;
            this.template = template;
            this.definition = definition;
            this.artifact = artifact;
            this.contract = contract;
            this.configuration = configuration;
            this.trust = trust;
        }

        private IncrementalRunSubmission normal(
                IncrementalRunMode mode, String trigger,
                String attempt, String publication) {
            return IncrementalRunSubmission.normal(
                    feedId, trigger, mode, attempt, publication,
                    3, 0L, mode == IncrementalRunMode.SNAPSHOT_V1 ? 10L : 20L,
                    definition, artifact, contract, configuration);
        }

        private IncrementalRunSubmission backfill() {
            return IncrementalRunSubmission.backfill(
                    feedId, 1L,
                    IncrementalFrontier.int64Sequence(0L),
                    IncrementalFrontier.int64Sequence(2L),
                    "backfill-request", "backfill-attempt",
                    "backfill-publication", 3, 0L, 30L,
                    definition, artifact, contract, configuration);
        }
    }
}
