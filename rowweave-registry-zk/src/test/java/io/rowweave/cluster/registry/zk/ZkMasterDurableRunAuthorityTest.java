package io.rowweave.cluster.registry.zk;

import io.rowweave.DataType;
import io.rowweave.CollectionSource;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.ArtifactRepository;
import io.rowweave.artifact.ArtifactReferenceAuthorityProvider;
import io.rowweave.artifact.FileArtifactRepository;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.cluster.MasterServer;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.StageExecutionTelemetry;
import io.rowweave.plan.TelemetryCoverage;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunExecutionEventPage;
import io.rowweave.run.RunExecutionEventPageCodec;
import io.rowweave.run.RunStateCodec;
import io.rowweave.run.RunStatus;
import io.rowweave.run.RunSubmission;
import io.rowweave.run.RunSubmissionCodec;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class ZkMasterDurableRunAuthorityTest {
    private static final String HOST = "zk-durable-worker";
    private static final int PORT = 61_169;
    private static final String INCARNATION = "inc-zk-durable";

    @TempDir
    Path artifactDirectory;

    @Test
    void twoIndependentMasterSessionsHaveOneDurableDispatchWinner() throws Exception {
        ArtifactRepository sharedArtifacts = new FileArtifactRepository(artifactDirectory);
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry registryA = new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry registryB = new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry workerRegistry = new ZkServiceRegistry(zk.getConnectString());
             MasterServer masterA = new MasterServer(
                     60_000L, 0, registryA,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP, sharedArtifacts);
             MasterServer masterB = new MasterServer(
                     60_000L, 0, registryB,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP, sharedArtifacts)) {
            workerRegistry.register(new RegistryWorker(
                    "w", HOST, PORT, 8, -1, INCARNATION));
            masterA.handler().apply(RpcMessage.registerWorker(
                    "w", HOST, PORT, 8, -1, INCARNATION));
            masterB.handler().apply(RpcMessage.registerWorker(
                    "w", HOST, PORT, 8, -1, INCARNATION));

            AtomicInteger assignments = new AtomicInteger();
            AtomicInteger outputControls = new AtomicInteger();
            AtomicReference<AttemptStatus> controlStatus = new AtomicReference<>();
            CountDownLatch assigned = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            InProcessRpcRegistry.Registration endpoint = InProcessRpcRegistry.register(HOST, PORT, request -> {
                if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                    assignments.incrementAndGet();
                    assigned.countDown();
                    try {
                        assertTrue(release.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return RpcMessage.jobResult(false, 0, "interrupted");
                    }
                    acceptAndReport(masterA, masterB, request);
                } else if (request.type() == RpcMessage.Type.COMMIT_PREPARED_OUTPUT
                        || request.type() == RpcMessage.Type.FINALIZE_PREPARED_OUTPUT) {
                    outputControls.incrementAndGet();
                    controlStatus.set(workerRegistry.runStateStore().get("run-zk-master")
                            .currentAttempt().status());
                }
                return RpcMessage.ack();
            });
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                byte[] envelope = envelope();
                Future<RpcMessage> first = executor.submit(
                        () -> masterA.handler().apply(RpcMessage.submitRun(envelope)));
                assertTrue(assigned.await(5, TimeUnit.SECONDS));
                Future<RpcMessage> second = executor.submit(
                        () -> masterB.handler().apply(RpcMessage.submitRun(envelope)));
                RpcMessage contender = second.get(5, TimeUnit.SECONDS);
                assertFalse(contender.success());
                assertEquals(1, assignments.get());

                release.countDown();
                assertTrue(first.get(5, TimeUnit.SECONDS).success());
                assertEquals(2, assignments.get(),
                        "durable single sink must be split into source + prepared sink stages");
                assertEquals(2, outputControls.get());
                assertEquals(AttemptStatus.COMMITTING, controlStatus.get());

                RunSnapshot shared = registryB.runStateStore().get("run-zk-master");
                assertEquals(RunStatus.SUCCEEDED, shared.status());
                RpcMessage queried = masterB.handler().apply(RpcMessage.queryRun("run-zk-master"));
                assertTrue(queried.success());
                assertEquals(RunStatus.SUCCEEDED,
                        RunStateCodec.decodeSnapshot(queried.planGraphBytes()).status());
                assertEquals(TelemetryCoverage.COMPLETE,
                        queried.executionTelemetrySummary().resourceCoverage());
                RpcMessage eventResponse = masterB.handler().apply(
                        RpcMessage.queryRunEvents("run-zk-master", 0L, 256));
                assertTrue(eventResponse.success(), eventResponse.errorMessage());
                RunExecutionEventPage eventPage = RunExecutionEventPageCodec.decode(
                        eventResponse.runEventPageBytes());
                assertEquals(RunExecutionEventPage.State.FOUND, eventPage.state());
                assertEquals(shared.revision(), eventPage.nextAfterSequence());
                assertEquals(shared.revision(), eventPage.events().size());
                assertFalse(eventPage.hasMore());
                byte[] artifactBytes = RunSubmissionCodec.decode(envelope()).artifactBytes();
                assertEquals(1L,
                        ((ArtifactReferenceAuthorityProvider) registryB)
                                .artifactReferenceAuthority()
                                .inspect(PlanArtifactCodec.artifactId(artifactBytes))
                                .referenceCount());
                assertArrayEquals(artifactBytes,
                        new FileArtifactRepository(artifactDirectory).get(
                                PlanArtifactCodec.artifactId(artifactBytes)));
            } finally {
                release.countDown();
                executor.shutdownNow();
                endpoint.close();
            }
        }
    }

    private static void acceptAndReport(
            MasterServer first, MasterServer second, RpcMessage assignment) {
        try {
            io.rowweave.run.StageAssignmentToken token =
                    io.rowweave.run.StageAssignmentManifestCodec.decodeToken(
                            assignment.stageAssignmentTokenBytes());
            byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(assignment.stageCommandBlob());
            RpcMessage acceptance = RpcMessage.assignmentAccepted(
                    assignment.workerId(), assignment.stageId(),
                    assignment.stageAssignmentTokenBytes(), digest,
                    INCARNATION, assignment.runAttemptFence());
            MasterServer owner = first;
            RpcMessage accepted = first.handler().apply(acceptance);
            if (accepted.type() != RpcMessage.Type.ACK) {
                owner = second;
                accepted = second.handler().apply(acceptance);
            }
            assertEquals(RpcMessage.Type.ACK, accepted.type(), accepted.errorMessage());
            RpcMessage terminal = owner.handler().apply(RpcMessage.durableStageReport(
                    assignment.workerId(), io.rowweave.run.StageTerminalReceipt.succeeded(
                            token, digest, 2L, 2L, System.currentTimeMillis(),
                            StageExecutionTelemetry.complete(
                                    1L, 2L, 3L, 4L, 5L, 6L))));
            assertEquals(RpcMessage.Type.ACK, terminal.type(), terminal.errorMessage());
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    @Test
    void unavailableRunAuthorityRejectsBeforeWorkerDispatch() throws Exception {
        ArtifactRepository sharedArtifacts = new FileArtifactRepository(artifactDirectory);
        int stoppedPort;
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry registry = new ZkServiceRegistry(zk.getConnectString());
             MasterServer master = new MasterServer(
                     60_000L, 0, registry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP, sharedArtifacts)) {
            stoppedPort = zk.getPort();
            master.handler().apply(RpcMessage.registerWorker(
                    "w", HOST, PORT, 8, -1, INCARNATION));
            AtomicInteger assignments = new AtomicInteger();
            InProcessRpcRegistry.Registration endpoint = InProcessRpcRegistry.register(
                    HOST, PORT, request -> {
                        if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                            assignments.incrementAndGet();
                        }
                        return RpcMessage.ack();
                    });
            try {
                zk.stop();

                RpcMessage rejected = master.handler().apply(RpcMessage.submitRun(envelope()));

                assertFalse(rejected.success());
                assertTrue(rejected.errorMessage().contains("submit failed"),
                        rejected.errorMessage());
                assertEquals(0, assignments.get());
            } finally {
                endpoint.close();
            }
        }
        assertTrue(awaitNoZooKeeperSendThread(stoppedPort, 5_000L),
                "stopped backend test leaked ZooKeeper reconnect thread on port "
                        + stoppedPort);
    }

    private static boolean awaitNoZooKeeperSendThread(int port, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        String portMarker = ":" + port + ")";
        do {
            boolean found = false;
            for (Thread thread : Thread.getAllStackTraces().keySet()) {
                if (thread.isAlive()
                        && thread.getName().contains("SendThread")
                        && thread.getName().contains(portMarker)) {
                    found = true;
                    break;
                }
            }
            if (!found) return true;
            Thread.sleep(25L);
        } while (System.nanoTime() < deadline);
        return false;
    }

    @Test
    void zkMasterWithoutArtifactRepositoryRejectsBeforeRunMutationOrDispatch() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry registry = new ZkServiceRegistry(zk.getConnectString());
             MasterServer master = new MasterServer(60_000L, 0, registry)) {
            master.handler().apply(RpcMessage.registerWorker(
                    "w", HOST, PORT, 8, -1, INCARNATION));
            AtomicInteger assignments = new AtomicInteger();
            InProcessRpcRegistry.Registration endpoint = InProcessRpcRegistry.register(
                    HOST, PORT, request -> {
                        if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                            assignments.incrementAndGet();
                        }
                        return RpcMessage.ack();
                    });
            try {
                RpcMessage rejected = master.handler().apply(RpcMessage.submitRun(envelope()));

                assertFalse(rejected.success());
                assertTrue(rejected.errorMessage().contains(
                        "artifact lifecycle capability unavailable"),
                        rejected.errorMessage());
                assertEquals(0, assignments.get());
                assertEquals(null, registry.runStateStore().get("run-zk-master"));
            } finally {
                endpoint.close();
            }
        }
    }

    private static byte[] envelope() {
        DataSchema meta = new DataSchema(new Field("id", DataType.IntegerType));
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(meta, Collections.singletonList(
                        new CollectionSource(meta, "source", Collections.emptyList())))
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(meta, 0, new DiscardSink(meta)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
        RunSubmission submission = new RunSubmission(
                "run-zk-master", "attempt-zk-master", "publication-zk-master",
                1, 0L, 10L);
        return RunSubmissionCodec.encode(submission, PlanArtifactCodec.encode(
                LegacyPlanArtifactAdapter.wrap("zk-durable-master", graph)));
    }
}
