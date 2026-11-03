package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.ManifestFileSource;
import io.rowweave.ObjectStorageReadHandle;
import io.rowweave.ObjectStorageReadRequest;
import io.rowweave.ObjectStorageSnapshotSource;
import io.rowweave.ObjectStorageSourceProvider;
import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.TypedQualityManifest;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.ControlResidentBudget;
import io.rowweave.cluster.rpc.ControlResidentLease;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.monitor.InMemoryJobMonitor;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStatus;
import io.rowweave.run.RunSubmission;
import io.rowweave.run.RunSubmissionCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MasterDurableRunDispatchTest {
    private static final String HOST = "durable-run-worker";
    private static final int PORT = 61_167;
    private static final String INCARNATION = "inc-durable-run";
    private static final DataSchema META = new DataSchema(new Field("id", DataType.IntegerType));
    private static volatile CountDownLatch blockingInputOpened = new CountDownLatch(1);
    private static volatile CountDownLatch releaseBlockingInput = new CountDownLatch(1);

    private static final class BlockingInput implements BatchSource {
        private static final long serialVersionUID = 1L;
        private final BatchSourceSplitDescriptor descriptor = new BatchSourceSplitDescriptor(
                1, "test-blocking", 1, "blocking-source",
                SchemaFingerprint.of(META), "empty-blocking-snapshot",
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);
        @Override public DataSchema getSchema() { return META; }
        @Override public BatchSourceSplitDescriptor batchSourceSplit() { return descriptor; }
        @Override public Iterator<DataRow> iterator() {
            blockingInputOpened.countDown();
            try {
                if (!releaseBlockingInput.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("blocking input release timed out");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("blocking input interrupted", interrupted);
            }
            return Collections.emptyIterator();
        }
    }

    @Test
    void realWorkerPersistsReceiptBeforeOpeningInput(@TempDir Path tmp) throws Exception {
        blockingInputOpened = new CountDownLatch(1);
        releaseBlockingInput = new CountDownLatch(1);
        InProcessRegistry registry = new InProcessRegistry();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        File workerDir = tmp.resolve("worker").toFile();
        workerDir.mkdirs();
        try (MasterServer master = new MasterServer(60_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "real-worker", 8, "127.0.0.1", master.port(), workerDir)) {
            Future<RpcMessage> submitted = executor.submit(() -> master.handler().apply(
                    RpcMessage.submitRun(blockingEnvelope())));
            assertTrue(blockingInputOpened.await(5, TimeUnit.SECONDS));

            io.rowweave.run.StageAssignmentSnapshot assignments =
                    registry.durableStageAssignmentAuthority().assignmentSnapshot(
                            new io.rowweave.run.RunAttemptFence(
                                    "run-real-worker", "attempt-real-worker", 1L));
            assertNotNull(assignments);
            assertTrue(assignments.acceptedCount() >= 1,
                    "user Source may open only after its durable receipt exists");

            releaseBlockingInput.countDown();
            RpcMessage result = submitted.get(10, TimeUnit.SECONDS);
            assertTrue(result.success(), result.errorMessage());
            assertNull(registry.durableStageAssignmentAuthority().assignmentSnapshot(
                    new io.rowweave.run.RunAttemptFence(
                            "run-real-worker", "attempt-real-worker", 1L)),
                    "terminal Run must atomically clean assignment receipts");
        } finally {
            releaseBlockingInput.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void successfulRunUsesSnapshotFenceAndReplayNeverDispatchesAgain() {
        InProcessRegistry registry = new InProcessRegistry();
        AtomicInteger assignments = new AtomicInteger();

        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
            InProcessRpcRegistry.Registration endpoint = InProcessRpcRegistry.register(HOST, PORT, request -> {
                if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                    assignments.incrementAndGet();
                    assertEquals("run-success", request.runAttemptFence().runId());
                    assertEquals("attempt-success", request.runAttemptFence().attemptId());
                    assertEquals(1L, request.runAttemptFence().fenceToken());
                    acceptAndReport(master, request, 7L, 7L);
                }
                return RpcMessage.ack();
            });
            try {
                register(master);
                byte[] envelope = envelope("run-success", "attempt-success", "publication-success");
                RpcMessage first = master.handler().apply(RpcMessage.submitRun(envelope));
                assertTrue(first.success(), first.errorMessage());
                assertEquals(7L, first.rowCount());
                assertEquals(io.rowweave.run.DurableRunDisposition.SUCCEEDED,
                        first.durableRunDisposition());

                RunSnapshot succeeded = registry.runStateStore().get("run-success");
                assertEquals(RunStatus.SUCCEEDED, succeeded.status());
                assertEquals(AttemptStatus.SUCCEEDED, succeeded.currentAttempt().status());
                assertEquals(6L, succeeded.revision());
                assertEquals(2, assignments.get(),
                        "durable single sink uses source + prepared sink stages");

                RpcMessage replay = master.handler().apply(RpcMessage.submitRun(envelope));
                assertTrue(replay.success(), replay.errorMessage());
                assertEquals(7L, replay.rowCount());
                assertEquals(io.rowweave.run.DurableRunDisposition.SUCCEEDED,
                        replay.durableRunDisposition());
                assertEquals(2, assignments.get(), "transport replay must never execute again");
                assertEquals(6L, registry.runStateStore().get("run-success").revision());
            } finally {
                endpoint.close();
            }
        }
    }

    @Test
    void concurrentSameSubmissionHasExactlyOneDispatchOwner() throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        AtomicInteger assignments = new AtomicInteger();
        CountDownLatch assigned = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
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
                    acceptAndReport(master, request, 0L, 0L);
                }
                return RpcMessage.ack();
            });
            try {
                register(master);
                byte[] envelope = envelope("run-race", "attempt-race", "publication-race");
                Future<RpcMessage> owner = executor.submit(
                        () -> master.handler().apply(RpcMessage.submitRun(envelope)));
                assertTrue(assigned.await(5, TimeUnit.SECONDS));

                RpcMessage contender = master.handler().apply(RpcMessage.submitRun(envelope));
                assertFalse(contender.success());
                assertEquals(1, assignments.get());

                release.countDown();
                assertTrue(owner.get(5, TimeUnit.SECONDS).success());
                assertEquals(2, assignments.get(),
                        "the sole dispatch owner executes both durable physical stages");
            } finally {
                endpoint.close();
            }
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void failureBeforeFirstAssignmentExhaustsTheOnlySupportedAttempt() {
        InProcessRegistry registry = new InProcessRegistry();
        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
            RpcMessage result = master.handler().apply(RpcMessage.submitRun(
                    envelope("run-no-worker", "attempt-1", "publication-1")));
            assertFalse(result.success());
            assertEquals(io.rowweave.run.DurableRunDisposition.TERMINAL_FAILED,
                    result.durableRunDisposition());
            assertTrue(result.errorMessage().contains("no registered worker"), result.errorMessage());

            RunSnapshot snapshot = registry.runStateStore().get("run-no-worker");
            assertEquals(RunStatus.FAILED, snapshot.status());
            assertEquals(AttemptStatus.FAILED, snapshot.currentAttempt().status());
            assertEquals(4L, snapshot.revision());
        }
    }

    @Test
    void retryableFailureBeforeFirstAssignmentReturnsNonTerminalWithoutSelfRetry() {
        InProcessRegistry registry = new InProcessRegistry();
        InMemoryJobMonitor monitor = new InMemoryJobMonitor(10);
        byte[] request = envelope(
                "run-no-worker-retry", "attempt-1", "publication-1", 2);
        try (MasterServer master = new MasterServer(
                60_000L, 0, registry, monitor)) {
            RpcMessage first = master.handler().apply(RpcMessage.submitRun(request));

            assertFalse(first.success());
            assertEquals(io.rowweave.run.DurableRunDisposition.ACCEPTED_NON_TERMINAL,
                    first.durableRunDisposition());
            RunSnapshot waiting = registry.runStateStore().get(
                    "run-no-worker-retry");
            assertEquals(RunStatus.READY, waiting.status());
            assertEquals(AttemptStatus.FAILED,
                    waiting.currentAttempt().status());
            assertEquals("RETRY_PRE_DISPATCH_ABSENT",
                    waiting.currentAttempt().reasonCode());
            assertEquals(1, waiting.attemptCount());
            long scheduledRevision = waiting.revision();

            RpcMessage replay = master.handler().apply(
                    RpcMessage.submitRun(request));
            assertFalse(replay.success());
            assertEquals(io.rowweave.run.DurableRunDisposition.ACCEPTED_NON_TERMINAL,
                    replay.durableRunDisposition());
            assertEquals(scheduledRevision, registry.runStateStore()
                    .get("run-no-worker-retry").revision());
            assertEquals(1, registry.runStateStore()
                    .get("run-no-worker-retry").attemptCount());
            assertNotNull(monitor.retryRecovery());
            assertEquals(1L, monitor.retryRecovery().scheduledRetries());
        }
    }

    @Test
    void activePreDispatchRetrySignalExecutesWhenWorkerArrives(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        try (MasterServer master = new MasterServer(60_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();
            long inventory = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(
                    inventory, 5, TimeUnit.SECONDS));

            byte[] request = envelope(
                    "run-signaled-retry", "attempt-1", "publication-1", 2);
            RpcMessage first = master.handler().apply(RpcMessage.submitRun(request));
            assertEquals(io.rowweave.run.DurableRunDisposition.ACCEPTED_NON_TERMINAL,
                    first.durableRunDisposition());
            assertEquals(RunStatus.READY, registry.runStateStore()
                    .get("run-signaled-retry").status());

            File spill = tmp.resolve("signaled-retry-worker").toFile();
            assertTrue(spill.mkdirs());
            try (WorkerServer worker = WorkerServer.newUnguarded(
                    "signaled-retry-worker", 8, "127.0.0.1",
                    master.port(), spill)) {
                long deadline = System.nanoTime()
                        + TimeUnit.SECONDS.toNanos(8);
                RunSnapshot completed = registry.runStateStore()
                        .get("run-signaled-retry");
                while (completed.status() != RunStatus.SUCCEEDED
                        && System.nanoTime() < deadline) {
                    Thread.sleep(10L);
                    completed = registry.runStateStore()
                            .get("run-signaled-retry");
                }
                assertEquals(RunStatus.SUCCEEDED, completed.status());
                assertEquals(2, completed.attemptCount());
            }
        }
    }

    @Test
    void objectSnapshotRestartsAfterAcceptedWorkerLossAndCredentialRotation(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        byte[] objectBytes = "7\n8\n".getBytes(StandardCharsets.UTF_8);
        DurableObjectProvider.reset(objectBytes);
        DurableObjectProvider.credentialGeneration.set(1);
        int lostPort = 62_021;
        String workerId = "lost-then-replaced-worker";
        String lostIncarnation = "lost-incarnation";
        // The replacement Worker beats every 1s by default. Keep the fake lost
        // endpoint finite, but never classify the healthy replacement as stale.
        try (MasterServer master = new MasterServer(2_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()));
             InProcessRpcRegistry.Registration lostEndpoint =
                     InProcessRpcRegistry.register(
                             "127.0.0.1", lostPort, request -> {
                                 if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                                     acceptWithoutTerminal(master, request);
                                     return RpcMessage.ack();
                                 }
                                 if (request.type()
                                         == RpcMessage.Type.RECONCILE_DURABLE_STAGE) {
                                     return RpcMessage.durableStageReconciledAbsent();
                                 }
                                 return RpcMessage.ack();
                             })) {
            master.bindLeadership(election);
            election.start();
            long inventory = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(
                    inventory, 5, TimeUnit.SECONDS));
            master.handler().apply(RpcMessage.registerWorker(
                    workerId, "127.0.0.1", lostPort, 8, -1,
                    lostIncarnation));

            RpcMessage first = master.handler().apply(RpcMessage.submitRun(
                    objectEnvelope("run-active-worker-lost", "attempt-1",
                            "publication-1", 2, objectBytes)));
            assertEquals(io.rowweave.run.DurableRunDisposition.ACCEPTED_NON_TERMINAL,
                    first.durableRunDisposition());
            long readyDeadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(5);
            RunSnapshot waiting = registry.runStateStore()
                    .get("run-active-worker-lost");
            while (waiting.status() != RunStatus.READY
                    && System.nanoTime() < readyDeadline) {
                Thread.sleep(10L);
                waiting = registry.runStateStore()
                        .get("run-active-worker-lost");
            }
            assertEquals(RunStatus.READY, waiting.status());
            assertEquals("RETRY_WORKER_LOST",
                    waiting.currentAttempt().reasonCode());

            DurableObjectProvider.credentialGeneration.set(2);
            File spill = tmp.resolve("replacement-worker").toFile();
            assertTrue(spill.mkdirs());
            try (WorkerServer replacement = WorkerServer.newUnguarded(
                    workerId, 8, "127.0.0.1", master.port(), spill)) {
                long successDeadline = System.nanoTime()
                        + TimeUnit.SECONDS.toNanos(8);
                RunSnapshot completed = waiting;
                while (completed.status() != RunStatus.SUCCEEDED
                        && System.nanoTime() < successDeadline) {
                    Thread.sleep(10L);
                    completed = registry.runStateStore()
                            .get("run-active-worker-lost");
                }
                assertEquals(RunStatus.SUCCEEDED, completed.status());
                assertEquals(2, completed.attemptCount());
                assertEquals(Collections.singletonList(2),
                        DurableObjectProvider.openedCredentialGenerations);
                assertEquals(Collections.singletonList("credential-prod"),
                        DurableObjectProvider.openedCredentialRefs);
                assertEquals(Collections.singletonList("generation-42"),
                        DurableObjectProvider.openedVersions);
                assertEquals(1, DurableObjectProvider.opens.get());
                assertEquals(1, DurableObjectProvider.closes.get());
            }
        }
    }

    @Test
    void objectQualityRestartsTheWholeThreeMemberAttemptAfterAcceptedWorkerLoss(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        byte[] objectBytes = "7\nbad\n8\n".getBytes(StandardCharsets.UTF_8);
        DurableObjectProvider.reset(objectBytes);
        DurableObjectProvider.credentialGeneration.set(1);
        Path domain = tmp.resolve("object-quality-retry-domain");
        InMemoryJobMonitor monitor = new InMemoryJobMonitor(20);
        int lostPort = 62_023;
        int lostDataPort = 62_024;
        String workerId = "object-quality-lost-then-replaced-worker";
        String lostIncarnation = "object-quality-lost-incarnation";
        try (MasterServer master = new MasterServer(2_000L, 0, registry, monitor);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()));
             InProcessRpcRegistry.Registration lostEndpoint =
                     InProcessRpcRegistry.register(
                             "127.0.0.1", lostPort, request -> {
                                 if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                                     acceptWithoutTerminal(master, request);
                                     return RpcMessage.ack();
                                 }
                                 if (request.type()
                                         == RpcMessage.Type.RECONCILE_DURABLE_STAGE) {
                                     return RpcMessage.durableStageReconciledAbsent();
                                 }
                                 return RpcMessage.ack();
                             })) {
            master.bindLeadership(election);
            election.start();
            long inventory = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(
                    inventory, 5, TimeUnit.SECONDS));
            master.handler().apply(RpcMessage.registerWorker(
                    workerId, "127.0.0.1", lostPort, 16, lostDataPort,
                    lostIncarnation));

            RpcMessage first = master.handler().apply(RpcMessage.submitRun(
                    objectQualityEnvelope(
                            "run-object-quality-worker-lost", "attempt-1",
                            "publication-1", 2, objectBytes, domain)));
            assertEquals(io.rowweave.run.DurableRunDisposition.ACCEPTED_NON_TERMINAL,
                    first.durableRunDisposition());
            RunSnapshot waiting = awaitRunState(
                    registry, "run-object-quality-worker-lost",
                    RunStatus.READY, 1, 8);
            assertEquals("RETRY_WORKER_LOST",
                    waiting.currentAttempt().reasonCode());

            DurableObjectProvider.credentialGeneration.set(2);
            File spill = tmp.resolve("object-quality-replacement-worker").toFile();
            assertTrue(spill.mkdirs());
            try (WorkerServer replacement = WorkerServer.newUnguarded(
                    workerId, 16, "127.0.0.1", master.port(), spill)) {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                RunSnapshot completed = registry.runStateStore().get(
                        "run-object-quality-worker-lost");
                while (completed.status() != RunStatus.SUCCEEDED
                        && System.nanoTime() < deadline) {
                    Thread.sleep(10L);
                    completed = registry.runStateStore().get(
                            "run-object-quality-worker-lost");
                }
                io.rowweave.monitor.JobView job = monitor.job(
                        "run-object-quality-worker-lost");
                assertEquals(RunStatus.SUCCEEDED, completed.status(),
                        job == null ? "missing monitor job" : job.error());
                assertEquals(2, completed.attemptCount());
                assertEquals(6L, completed.resultSummary().rowsOut());
                assertEquals(Collections.singletonList(2),
                        DurableObjectProvider.openedCredentialGenerations);
                assertEquals(1, DurableObjectProvider.opens.get());
                assertEquals(1, DurableObjectProvider.closes.get());
                assertEquals(0, replacement.preparedOutputActiveCount());
            }
        }

        CommitManifest manifest = new ManifestFileStore(domain).latest();
        assertEquals(3, manifest.members().size());
        assertEquals(2, countRows(new ManifestFileSource(
                META, domain.toString(), "valid", manifest.generation())));
        assertEquals(1, countRows(new ManifestFileSource(
                objectQualityRejectedMeta(), domain.toString(), "rejected",
                manifest.generation())));
        assertEquals(Arrays.asList(
                "ACCEPTED_TOTAL|||-1||2",
                "REJECTED_REASON|QUALITY_RAW_FORMAT_INVALID|OBJECT_ROW_DECODE_V1|-1|ROWWEAVE_ROW_TEXT_V1|1",
                "REJECTED_TOTAL|||-1||1"), objectQualitySummaryLines(
                domain, manifest.generation()));
    }

    @Test
    void jdbcSnapshotRestartsAfterAcceptedWorkerLossAndCredentialRotation(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        DurableJdbcTestProvider.reset(7, 8);
        DurableJdbcTestProvider.credentialGeneration.set(1);
        int lostPort = 62_022;
        String workerId = "jdbc-lost-then-replaced-worker";
        // Match the default 1s Worker heartbeat while the fake endpoint still
        // has no heartbeat and is deterministically marked lost.
        try (MasterServer master = new MasterServer(2_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()));
             InProcessRpcRegistry.Registration lostEndpoint =
                     InProcessRpcRegistry.register(
                             "127.0.0.1", lostPort, request -> {
                                 if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                                     acceptWithoutTerminal(master, request);
                                     return RpcMessage.ack();
                                 }
                                 if (request.type()
                                         == RpcMessage.Type.RECONCILE_DURABLE_STAGE) {
                                     return RpcMessage.durableStageReconciledAbsent();
                                 }
                                 return RpcMessage.ack();
                             })) {
            master.bindLeadership(election);
            election.start();
            long inventory = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(
                    inventory, 5, TimeUnit.SECONDS));
            master.handler().apply(RpcMessage.registerWorker(
                    workerId, "127.0.0.1", lostPort, 8, -1,
                    "jdbc-lost-incarnation"));

            RpcMessage first = master.handler().apply(RpcMessage.submitRun(
                    jdbcEnvelope("run-jdbc-worker-lost", "attempt-1",
                            "publication-1", 2)));
            assertEquals(io.rowweave.run.DurableRunDisposition.ACCEPTED_NON_TERMINAL,
                    first.durableRunDisposition());
            long readyDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            RunSnapshot waiting = registry.runStateStore().get("run-jdbc-worker-lost");
            while (waiting.status() != RunStatus.READY
                    && System.nanoTime() < readyDeadline) {
                Thread.sleep(10L);
                waiting = registry.runStateStore().get("run-jdbc-worker-lost");
            }
            assertEquals(RunStatus.READY, waiting.status());
            assertEquals("RETRY_WORKER_LOST", waiting.currentAttempt().reasonCode());

            DurableJdbcTestProvider.credentialGeneration.set(2);
            File spill = tmp.resolve("jdbc-replacement-worker").toFile();
            assertTrue(spill.mkdirs());
            try (WorkerServer replacement = WorkerServer.newUnguarded(
                    workerId, 8, "127.0.0.1", master.port(), spill)) {
                RunSnapshot completed = awaitRunState(
                        registry, "run-jdbc-worker-lost",
                        RunStatus.SUCCEEDED, 2, 15);
                assertEquals(2, completed.attemptCount());
                assertEquals(Collections.singletonList(2),
                        DurableJdbcTestProvider.openedCredentialGenerations);
                assertEquals(Collections.singletonList("credential-prod"),
                        DurableJdbcTestProvider.openedCredentialRefs);
                assertEquals(1, DurableJdbcTestProvider.opens.get());
                assertEquals(1, DurableJdbcTestProvider.closes.get());
            }
        }
    }

    @Test
    void repeatedAcceptedWorkerLossExhaustsExactlyThreeAttempts()
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        InMemoryJobMonitor monitor = new InMemoryJobMonitor(10);
        String runId = "run-worker-lost-exhaustion";
        try (MasterServer master = new MasterServer(
                300L, 0, registry, monitor);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();
            long inventory = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(
                    inventory, 5, TimeUnit.SECONDS));

            try (AcceptedThenLostWorker first = new AcceptedThenLostWorker(
                    master, "exhaustion-worker", 62_031, "incarnation-1")) {
                RpcMessage submitted = master.handler().apply(RpcMessage.submitRun(
                        envelope(runId, "attempt-1", "publication-1", 3)));
                assertEquals(
                        io.rowweave.run.DurableRunDisposition.ACCEPTED_NON_TERMINAL,
                        submitted.durableRunDisposition());
                awaitRunState(registry, runId, RunStatus.READY, 1, 5);
            }
            try (AcceptedThenLostWorker second = new AcceptedThenLostWorker(
                    master, "exhaustion-worker", 62_032, "incarnation-2")) {
                awaitRunState(registry, runId, RunStatus.READY, 2, 8);
            }
            try (AcceptedThenLostWorker third = new AcceptedThenLostWorker(
                    master, "exhaustion-worker", 62_033, "incarnation-3")) {
                RunSnapshot exhausted = awaitRunState(
                        registry, runId, RunStatus.FAILED, 3, 12);
                assertEquals("RETRY_ATTEMPTS_EXHAUSTED",
                        exhausted.currentAttempt().reasonCode());
                assertNotNull(monitor.retryRecovery());
                assertEquals(2L, monitor.retryRecovery().scheduledRetries());
                assertEquals(1L, monitor.retryRecovery().exhaustedRetries());
            }
        }
    }

    @Test
    void durableTerminalFailureConvergesRunBeforeReturning() {
        InProcessRegistry registry = new InProcessRegistry();
        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
            InProcessRpcRegistry.Registration endpoint = InProcessRpcRegistry.register(
                    HOST, PORT, request -> {
                        if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                            acceptAndFail(master, request);
                        }
                        return RpcMessage.ack();
                    });
            try {
                register(master);
                RpcMessage result = master.handler().apply(RpcMessage.submitRun(
                        envelope("run-terminal-failure", "attempt-1", "publication-1")));

                assertFalse(result.success());
                assertEquals(io.rowweave.run.DurableRunDisposition.TERMINAL_FAILED,
                        result.durableRunDisposition());
                RunSnapshot snapshot = registry.runStateStore().get("run-terminal-failure");
                assertEquals(RunStatus.FAILED, snapshot.status());
                assertEquals(AttemptStatus.FAILED, snapshot.currentAttempt().status());
            } finally {
                endpoint.close();
            }
        }
    }

    @Test
    void assignmentUncertaintyLeavesRunRunningAndNeverOpensRetry() {
        InProcessRegistry registry = new InProcessRegistry();
        AtomicInteger assignments = new AtomicInteger();
        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
            InProcessRpcRegistry.Registration endpoint = InProcessRpcRegistry.register(HOST, PORT, request -> {
                if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                    assignments.incrementAndGet();
                    return RpcMessage.jobResult(false, 0, "assignment outcome unknown");
                }
                return RpcMessage.ack();
            });
            try {
                register(master);
                byte[] envelope = envelope("run-uncertain", "attempt-1", "publication-1");
                RpcMessage result = master.handler().apply(RpcMessage.submitRun(envelope));
                assertFalse(result.success());

                RunSnapshot snapshot = registry.runStateStore().get("run-uncertain");
                assertEquals(RunStatus.RUNNING, snapshot.status());
                assertEquals(AttemptStatus.RUNNING, snapshot.currentAttempt().status());
                assertEquals(4L, snapshot.revision());

                RpcMessage replay = master.handler().apply(RpcMessage.submitRun(envelope));
                assertFalse(replay.success());
                assertEquals(1, assignments.get());
                assertEquals(RunStatus.RUNNING,
                        registry.runStateStore().get("run-uncertain").status());
            } finally {
                endpoint.close();
            }
        }
    }

    @Test
    void deterministicResidentExhaustionAfterRunningWritesAuthoritativeFailed() {
        InProcessRegistry registry = new InProcessRegistry();
        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
            InProcessRpcRegistry.Registration endpoint = InProcessRpcRegistry.register(
                    HOST, PORT, request -> {
                        if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                            ControlResidentBudget budget = ControlResidentBudget.forTesting(1L);
                            ControlResidentLease lease = budget.openLease();
                            lease.reserve(lease.ownerToken(), 2L);
                        }
                        return RpcMessage.ack();
                    });
            try {
                register(master);
                RpcMessage result = master.handler().apply(RpcMessage.submitRun(
                        envelope("run-resident-fail", "attempt-1", "publication-1")));

                assertFalse(result.success());
                assertTrue(result.errorMessage().startsWith(
                        io.rowweave.cluster.rpc.ControlResidentBudgetExceededException.CODE),
                        result.errorMessage());
                RunSnapshot snapshot = registry.runStateStore().get("run-resident-fail");
                assertEquals(RunStatus.FAILED, snapshot.status());
                assertEquals(AttemptStatus.FAILED, snapshot.currentAttempt().status());
            } finally {
                endpoint.close();
            }
        }
    }

    @Test
    void genericDagEveryAssignmentUsesTheDurableSnapshotFence() {
        InProcessRegistry registry = new InProcessRegistry();
        Set<io.rowweave.run.RunAttemptFence> fences = ConcurrentHashMap.newKeySet();
        AtomicInteger assignments = new AtomicInteger();
        try (MasterServer master = new MasterServer(60_000L, 0, registry)) {
            InProcessRpcRegistry.Registration endpoint = InProcessRpcRegistry.register(HOST, PORT, request -> {
                if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                    assignments.incrementAndGet();
                    fences.add(request.runAttemptFence());
                    acceptAndReport(master, request, 0L, 0L);
                }
                return RpcMessage.ack();
            });
            try {
                master.handler().apply(RpcMessage.registerWorker(
                        "w", HOST, PORT, 16, 62_167, INCARNATION));
                RunSubmission submission = new RunSubmission(
                        "run-dag", "attempt-dag", "publication-dag", 1, 0L, 10L);
                byte[] envelope = RunSubmissionCodec.encode(submission, PlanArtifactCodec.encode(
                        LegacyPlanArtifactAdapter.wrap("durable-dag", genericDag())));
                RpcMessage result = master.handler().apply(RpcMessage.submitRun(envelope));

                assertTrue(result.success(), result.errorMessage());
                assertTrue(assignments.get() >= 2);
                assertEquals(Collections.singleton(
                        new io.rowweave.run.RunAttemptFence("run-dag", "attempt-dag", 1L)), fences);
                assertEquals(RunStatus.SUCCEEDED,
                        registry.runStateStore().get("run-dag").status());
            } finally {
                endpoint.close();
            }
        }
    }

    private static void register(MasterServer master) {
        master.handler().apply(RpcMessage.registerWorker(
                "w", HOST, PORT, 8, -1, INCARNATION));
    }

    private static void acceptAndReport(MasterServer master, RpcMessage assignment,
                                        long inputRows, long outputRows) {
        try {
            io.rowweave.run.StageAssignmentToken token =
                    io.rowweave.run.StageAssignmentManifestCodec.decodeToken(
                            assignment.stageAssignmentTokenBytes());
            byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(assignment.stageCommandBlob());
            RpcMessage accepted = master.handler().apply(RpcMessage.assignmentAccepted(
                    assignment.workerId(), assignment.stageId(),
                    assignment.stageAssignmentTokenBytes(), digest,
                    INCARNATION, assignment.runAttemptFence()));
            assertEquals(RpcMessage.Type.ACK, accepted.type(), accepted.errorMessage());
            RpcMessage terminal = master.handler().apply(RpcMessage.durableStageReport(
                    assignment.workerId(), io.rowweave.run.StageTerminalReceipt.succeeded(
                            token, digest, inputRows, outputRows, System.currentTimeMillis())));
            assertEquals(RpcMessage.Type.ACK, terminal.type(), terminal.errorMessage());
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static void acceptAndFail(MasterServer master, RpcMessage assignment) {
        try {
            io.rowweave.run.StageAssignmentToken token =
                    io.rowweave.run.StageAssignmentManifestCodec.decodeToken(
                            assignment.stageAssignmentTokenBytes());
            byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(assignment.stageCommandBlob());
            RpcMessage accepted = master.handler().apply(RpcMessage.assignmentAccepted(
                    assignment.workerId(), assignment.stageId(),
                    assignment.stageAssignmentTokenBytes(), digest,
                    INCARNATION, assignment.runAttemptFence()));
            assertEquals(RpcMessage.Type.ACK, accepted.type(), accepted.errorMessage());
            RpcMessage terminal = master.handler().apply(RpcMessage.durableStageReport(
                    assignment.workerId(), io.rowweave.run.StageTerminalReceipt.failed(
                            token, digest, 0L, 0L, "STAGE_FAILED", new byte[32],
                            System.currentTimeMillis())));
            assertEquals(RpcMessage.Type.ACK, terminal.type(), terminal.errorMessage());
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static void acceptWithoutTerminal(
            MasterServer master, RpcMessage assignment) {
        try {
            byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(assignment.stageCommandBlob());
            RpcMessage accepted = master.handler().apply(
                    RpcMessage.assignmentAccepted(
                            assignment.workerId(), assignment.stageId(),
                            assignment.stageAssignmentTokenBytes(), digest,
                            assignment.incarnationId(),
                            assignment.runAttemptFence()));
            assertEquals(RpcMessage.Type.ACK, accepted.type(),
                    accepted.errorMessage());
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static RunSnapshot awaitRunState(
            InProcessRegistry registry, String runId, RunStatus status,
            int attemptCount, long timeoutSeconds) throws InterruptedException {
        long deadline = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        RunSnapshot observed = registry.runStateStore().get(runId);
        while ((observed.status() != status
                || observed.attemptCount() != attemptCount)
                && System.nanoTime() < deadline) {
            Thread.sleep(10L);
            observed = registry.runStateStore().get(runId);
        }
        assertEquals(status, observed.status(),
                observed.currentAttempt() == null ? "no attempt"
                        : observed.currentAttempt().status() + " "
                        + observed.currentAttempt().reasonCode() + " "
                        + observed.currentAttempt().message());
        assertEquals(attemptCount, observed.attemptCount());
        return observed;
    }

    private static final class AcceptedThenLostWorker implements AutoCloseable {
        private final ScheduledExecutorService heartbeats =
                Executors.newSingleThreadScheduledExecutor();
        private final InProcessRpcRegistry.Registration endpoint;
        private final java.util.concurrent.atomic.AtomicBoolean keepAlive =
                new java.util.concurrent.atomic.AtomicBoolean(true);

        AcceptedThenLostWorker(
                MasterServer master, String workerId, int port,
                String incarnationId) {
            endpoint = InProcessRpcRegistry.register(
                    "127.0.0.1", port, request -> {
                        if (request.type() == RpcMessage.Type.ASSIGN_STAGE) {
                            acceptWithoutTerminal(master, request);
                            keepAlive.set(false);
                            return RpcMessage.ack();
                        }
                        if (request.type()
                                == RpcMessage.Type.RECONCILE_DURABLE_STAGE) {
                            return RpcMessage.durableStageReconciledAbsent();
                        }
                        return RpcMessage.ack();
                    });
            master.handler().apply(RpcMessage.registerWorker(
                    workerId, "127.0.0.1", port, 8, -1,
                    incarnationId));
            heartbeats.scheduleAtFixedRate(() -> {
                if (keepAlive.get()) {
                    master.handler().apply(RpcMessage.heartbeat(
                            workerId, 8L, incarnationId));
                }
            }, 0L, 50L, TimeUnit.MILLISECONDS);
        }

        @Override
        public void close() {
            keepAlive.set(false);
            heartbeats.shutdownNow();
            endpoint.close();
        }
    }

    private static byte[] envelope(String runId, String attemptId, String publicationId) {
        return envelope(runId, attemptId, publicationId, 1);
    }

    private static byte[] envelope(
            String runId, String attemptId, String publicationId,
            int maxAttempts) {
        RunSubmission submission = new RunSubmission(
                runId, attemptId, publicationId, maxAttempts, 0L, 10L);
        return RunSubmissionCodec.encode(submission, PlanArtifactCodec.encode(
                LegacyPlanArtifactAdapter.wrap("durable-dispatch", wholeGraph())));
    }

    private static byte[] blockingEnvelope() {
        RunSubmission submission = new RunSubmission(
                "run-real-worker", "attempt-real-worker", "publication-real-worker",
                1, 0L, 10L);
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META,
                        Collections.singletonList(new BlockingInput()))
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
        return RunSubmissionCodec.encode(submission, PlanArtifactCodec.encode(
                LegacyPlanArtifactAdapter.wrap("durable-real-worker", graph)));
    }

    private static byte[] objectEnvelope(
            String runId, String attemptId, String publicationId,
            int maxAttempts, byte[] content) {
        RunSubmission submission = new RunSubmission(
                runId, attemptId, publicationId, maxAttempts, 0L, 10L);
        ObjectStorageSnapshotSource input = new ObjectStorageSnapshotSource(
                META, new DurableObjectProvider(), "endpoint-prod", "credential-prod",
                "domain", "ledger/retry.tsv", "generation-42", content.length,
                sha256Hex(content), ObjectStorageSnapshotSource.MIN_ROW_BYTES);
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META, Collections.singletonList(input))
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
        return RunSubmissionCodec.encode(submission, PlanArtifactCodec.encode(
                LegacyPlanArtifactAdapter.wrap("durable-object-retry", graph)));
    }

    private static byte[] objectQualityEnvelope(
            String runId, String attemptId, String publicationId,
            int maxAttempts, byte[] content, Path domain) {
        RunSubmission submission = new RunSubmission(
                runId, attemptId, publicationId, maxAttempts, 0L, 10L);
        ObjectStorageSnapshotSource input = new ObjectStorageSnapshotSource(
                META, new DurableObjectProvider(), "endpoint-prod", "credential-prod",
                "domain", "ledger/retry.tsv", "generation-42", content.length,
                sha256Hex(content), ObjectStorageSnapshotSource.MIN_ROW_BYTES);
        ExecutionEnvironment session = new ExecutionEnvironment(
                "durable-object-quality-retry", "durable object quality retry");
        session.setDefinitionId("durable-object-quality-retry");
        session.setParallelism(2);
        session.classifyObjectStorageRows("ledger.object.source", input)
                .writeToManifest(TypedQualityManifest.of(
                        domain.toString(), "valid", "rejected", "summary"));
        return RunSubmissionCodec.encode(
                submission, PlanArtifactCodec.encode(session.toPlanArtifact()));
    }

    private static DataSchema objectQualityRejectedMeta() {
        return new DataSchema(
                new Field("source_operator_uid", DataType.StringType),
                new Field("split_identity_sha256", DataType.StringType),
                new Field("split_local_ordinal", DataType.LongType),
                new Field("quality_error_code", DataType.StringType),
                new Field("quality_rule_id", DataType.StringType),
                new Field("quality_field_index", DataType.IntegerType),
                new Field("quality_detail", DataType.StringType));
    }

    private static int countRows(Source input) {
        int count = 0;
        for (DataRow ignored : input) count++;
        return count;
    }

    private static java.util.List<String> objectQualitySummaryLines(
            Path domain, long generation) {
        java.util.List<String> result = new java.util.ArrayList<>();
        for (DataRow row : new ManifestFileSource(
                TypedQualityManifest.summarySchema(), domain.toString(),
                "summary", generation)) {
            result.add(row.getString(0) + "|" + row.get(1) + "|" + row.get(2)
                    + "|" + row.getInteger(3) + "|" + row.get(4)
                    + "|" + row.getLong(5));
        }
        return result;
    }

    private static byte[] jdbcEnvelope(
            String runId, String attemptId, String publicationId,
            int maxAttempts) {
        RunSubmission submission = new RunSubmission(
                runId, attemptId, publicationId, maxAttempts, 0L, 10L);
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META, Collections.<Source>singletonList(
                        DurableJdbcTestProvider.input()))
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
        return RunSubmissionCodec.encode(submission, PlanArtifactCodec.encode(
                LegacyPlanArtifactAdapter.wrap("durable-jdbc-retry", graph)));
    }

    private static String sha256Hex(byte[] value) {
        try {
            byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(value);
            char[] output = new char[digest.length * 2];
            char[] digits = "0123456789abcdef".toCharArray();
            for (int i = 0; i < digest.length; i++) {
                output[i * 2] = digits[(digest[i] >>> 4) & 0xf];
                output[i * 2 + 1] = digits[digest[i] & 0xf];
            }
            return new String(output);
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    public static final class DurableObjectProvider
            implements ObjectStorageSourceProvider {
        private static final long serialVersionUID = 1L;
        private static volatile byte[] content;
        static final AtomicInteger credentialGeneration = new AtomicInteger();
        static final AtomicInteger opens = new AtomicInteger();
        static final AtomicInteger closes = new AtomicInteger();
        static final java.util.List<Integer> openedCredentialGenerations =
                new java.util.concurrent.CopyOnWriteArrayList<>();
        static final java.util.List<String> openedCredentialRefs =
                new java.util.concurrent.CopyOnWriteArrayList<>();
        static final java.util.List<String> openedVersions =
                new java.util.concurrent.CopyOnWriteArrayList<>();

        public DurableObjectProvider() { }

        static void reset(byte[] bytes) {
            content = bytes.clone();
            credentialGeneration.set(0);
            opens.set(0);
            closes.set(0);
            openedCredentialGenerations.clear();
            openedCredentialRefs.clear();
            openedVersions.clear();
        }

        @Override public String providerType() { return "durable-retry-fixture"; }
        @Override public int providerVersion() { return 1; }

        @Override
        public ObjectStorageReadHandle open(ObjectStorageReadRequest request) {
            opens.incrementAndGet();
            openedCredentialGenerations.add(credentialGeneration.get());
            openedCredentialRefs.add(request.credentialRef());
            openedVersions.add(request.versionId());
            return new DurableObjectHandle(request, content.clone());
        }
    }

    private static final class DurableObjectHandle
            implements ObjectStorageReadHandle {
        private final ObjectStorageReadRequest request;
        private final InputStream stream;
        private boolean closed;

        private DurableObjectHandle(ObjectStorageReadRequest request, byte[] content) {
            this.request = request;
            this.stream = new ByteArrayInputStream(content);
        }

        @Override public String providerType() { return "durable-retry-fixture"; }
        @Override public int providerVersion() { return 1; }
        @Override public String versionId() { return request.versionId(); }
        @Override public long contentLength() { return request.contentLength(); }
        @Override public InputStream stream() { return stream; }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            DurableObjectProvider.closes.incrementAndGet();
        }
    }

    private static PlanGraph wholeGraph() {
        return new PlanGraph(Arrays.asList(
                DurableTestSources.source(META, Collections.emptyList(), "dispatch-source")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
    }

    private static PlanGraph genericDag() {
        return new PlanGraph(Arrays.asList(
                DurableTestSources.source(META, Collections.emptyList(), "dispatch-dag-source")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.unary("distinct", META, 0)
                        .listParam("keys", Collections.singletonList("id")).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "distinct"),
                PlanNode.sink(META, 1, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")), 2);
    }
}
