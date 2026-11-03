package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.CollectionSource;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.ManifestFileSource;
import io.rowweave.ManifestFileSink;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.artifact.FileArtifactRepository;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.cluster.registry.zk.ZkServiceRegistry;
import io.rowweave.cluster.registry.zk.HookedZkServiceRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.DurableStageAssignmentAuthority;
import io.rowweave.run.ExecutionEvent;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCancellationAuthority;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunRecoveryOwnershipProvider;
import io.rowweave.run.RunSubmission;
import io.rowweave.run.RunSubmissionCodec;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.RunStateStoreProvider;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStatus;
import io.rowweave.run.StageAcceptanceDecision;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZkMasterRecoveryInventoryTest {
    private static final DataSchema META = new DataSchema(new Field("id", DataType.IntegerType));

    @TempDir
    Path temporary;

    @Test
    void failoverAfterIntentBeforeAcceptConvergesFailedAndFencesLateAcceptance()
            throws Exception {
        FailoverEvidence evidence = failoverAt(
                "zk-intent-cut", CrashCut.INTENT_BEFORE_ACCEPT, false);

        assertEquals(0, evidence.beforeClaim.acceptedCount());
        assertTerminalFailedAndCleaned(evidence);
        assertEquals(StageAcceptanceDecision.Code.STALE_RUN,
                evidence.lateFirstAccept.code());
    }

    @Test
    void failoverAfterAcceptBeforeAckConvergesFailedWithoutRedispatch() throws Exception {
        FailoverEvidence evidence = failoverAt(
                "zk-accept-ack-cut", CrashCut.ACCEPT_BEFORE_ACK, false);

        assertEquals(1, evidence.beforeClaim.acceptedCount());
        assertTerminalFailedAndCleaned(evidence);
        assertEquals(StageAcceptanceDecision.Code.STALE_RUN,
                evidence.existingReceiptReplay.code());
    }

    @Test
    void failoverWithPartialWaveConvergesFailedAndFencesWholeAssignmentSet()
            throws Exception {
        FailoverEvidence evidence = failoverAt(
                "zk-partial-wave", CrashCut.PARTIAL_WAVE_ACCEPTED, true);

        assertTrue(evidence.beforeClaim.manifest().assignmentCount() > 1);
        assertEquals(1, evidence.beforeClaim.acceptedCount());
        assertTerminalFailedAndCleaned(evidence);
        assertEquals(StageAcceptanceDecision.Code.STALE_RUN,
                evidence.lateFirstAccept.code());
        assertEquals(StageAcceptanceDecision.Code.STALE_RUN,
                evidence.existingReceiptReplay.code());
    }

    private static void assertTerminalFailedAndCleaned(FailoverEvidence evidence) {
        assertEquals(RunStatus.FAILED, evidence.terminal.status());
        assertEquals(AttemptStatus.FAILED,
                evidence.terminal.currentAttempt().status());
        assertNull(evidence.afterClaim,
                "terminal Run must atomically remove assignment authority");
    }

    @Test
    void independentZkMastersClaimOnlyAsLeaderAndReplaceEpochAfterFailover() throws Exception {
        try (TestingServer server = new TestingServer(true);
             ZkServiceRegistry firstRegistry = new ZkServiceRegistry(server.getConnectString());
             ZkServiceRegistry secondRegistry = new ZkServiceRegistry(server.getConnectString())) {
            FileArtifactRepository repository =
                    new FileArtifactRepository(temporary.resolve("artifacts"));
            byte[] artifact = artifact("definition-zk");
            String artifactId = repository.put(artifact);
            RunStateStore store = ((RunStateStoreProvider) firstRegistry).runStateStore();
            assertEquals(RunDecision.Code.ACCEPTED, store.apply(RunCommand.createRun(
                    "create-zk-inherited", "zk-inherited", "definition-zk",
                    artifactId, 1, 0L, 1L)).code());
            RunSnapshot attempted = accepted(store, RunCommand.createAttempt(
                    "attempt-zk-inherited", "zk-inherited", 1L,
                    "attempt-zk-inherited", "publication-zk-inherited", 2L));
            accepted(store, RunCommand.advanceAttempt(
                    "dispatch-zk-inherited", "zk-inherited", attempted.revision(),
                    attempted.currentAttempt().attemptId(),
                    attempted.currentAttempt().fenceToken(), AttemptStatus.DISPATCHING,
                    false, 3L, null, null));

            MasterServer first = new MasterServer(5_000L, 0, firstRegistry,
                    io.rowweave.monitor.JobMonitorRegistry.NOOP, repository);
            MasterServer second = new MasterServer(5_000L, 0, secondRegistry,
                    io.rowweave.monitor.JobMonitorRegistry.NOOP, repository);
            MasterElection firstElection = new MasterElection(firstRegistry,
                    new Endpoint("127.0.0.1", first.port()));
            MasterElection secondElection = new MasterElection(secondRegistry,
                    new Endpoint("127.0.0.1", second.port()));
            try {
                first.bindLeadership(firstElection);
                second.bindLeadership(secondElection);
                firstElection.start();
                secondElection.start();
                assertTrue(firstElection.isLeader());
                assertFalse(secondElection.isLeader());
                long firstGeneration = first.recoveryInventoryGeneration();
                assertTrue(first.awaitRecoveryInventory(
                        firstGeneration, 10, TimeUnit.SECONDS));
                RecoveryToken firstToken = ((RunRecoveryOwnershipProvider) firstRegistry)
                        .runRecoveryOwnership().ownership("zk-inherited");
                assertEquals(1L, firstToken.ownershipEpoch());
                assertEquals(firstElection.termToken(),
                        firstToken.owner().leaderTermToken());
                assertEquals(0L, second.recoveryInventoryGeneration());

                firstElection.close();
                awaitLeader(secondElection, 5_000L);
                long secondGeneration = awaitInventoryGeneration(second, 5_000L);
                assertTrue(second.awaitRecoveryInventory(
                        secondGeneration, 10, TimeUnit.SECONDS));
                RecoveryToken replacement = ((RunRecoveryOwnershipProvider) secondRegistry)
                        .runRecoveryOwnership().ownership("zk-inherited");
                assertEquals(2L, replacement.ownershipEpoch());
                assertEquals(secondElection.termToken(),
                        replacement.owner().leaderTermToken());
                assertFalse(((RunRecoveryOwnershipProvider) firstRegistry)
                        .runRecoveryOwnership().current(firstToken));
            } finally {
                secondElection.close();
                firstElection.close();
                second.close();
                first.close();
            }
        }
    }

    @Test
    void zkLeaderRecoversReadyRunThroughRealWorker() throws Exception {
        try (TestingServer server = new TestingServer(true);
             ZkServiceRegistry firstRegistry = new ZkServiceRegistry(server.getConnectString());
             ZkServiceRegistry secondRegistry = new ZkServiceRegistry(server.getConnectString());
             ZkServiceRegistry workerRegistry = new ZkServiceRegistry(server.getConnectString())) {
            FileArtifactRepository repository =
                    new FileArtifactRepository(temporary.resolve("success-artifacts"));
            Path domain = temporary.resolve("success-output");
            PlanArtifact artifact = executableArtifact(
                    "zk-ready-definition",
                    new ManifestFileSink(META, domain.toString(), "ledger"));
            String artifactId = repository.put(PlanArtifactCodec.encode(artifact));
            RunStateStore store = firstRegistry.runStateStore();
            accepted(store, RunCommand.createRun(
                    "create-zk-ready", "zk-ready", artifact.definitionId(),
                    artifactId, 1, 0L, 5L));

            MasterServer first = new MasterServer(5_000L, 0, firstRegistry,
                    io.rowweave.monitor.JobMonitorRegistry.NOOP, repository);
            MasterServer second = new MasterServer(5_000L, 0, secondRegistry,
                    io.rowweave.monitor.JobMonitorRegistry.NOOP, repository);
            MasterElection firstElection = new MasterElection(firstRegistry,
                    new Endpoint("127.0.0.1", first.port()));
            MasterElection secondElection = new MasterElection(secondRegistry,
                    new Endpoint("127.0.0.1", second.port()));
            java.io.File spill = temporary.resolve("success-worker-spill").toFile();
            assertTrue(spill.mkdirs());
            try {
                first.bindLeadership(firstElection);
                second.bindLeadership(secondElection);
                firstElection.start();
                secondElection.start();
                MasterServer leader = firstElection.isLeader() ? first : second;
                MasterServer standby = firstElection.isLeader() ? second : first;
                try (WorkerServer worker = WorkerServer.newUnguarded(
                        "zk-recovery-worker", 8,
                        new RegistryMasterLocator(workerRegistry), spill)) {
                    long generation = leader.recoveryInventoryGeneration();
                    assertTrue(leader.awaitRecoveryInventory(
                            generation, 15, TimeUnit.SECONDS));
                    RunSnapshot recovered = secondRegistry.runStateStore().get("zk-ready");
                    assertEquals(RunStatus.SUCCEEDED, recovered.status());
                    assertEquals(AttemptStatus.SUCCEEDED,
                            recovered.currentAttempt().status());
                    assertTrue(recovered.currentAttempt().attemptId()
                            .startsWith("recovery-attempt-v1-"));
                    assertManifestRows(domain);
                    assertEquals(0L, standby.recoveryInventoryGeneration());
                }
            } finally {
                secondElection.close();
                firstElection.close();
                second.close();
                first.close();
            }
        }
    }

    @Test
    void zkReplacementLeaderConvergesDurableUserCancellation() throws Exception {
        try (TestingServer server = new TestingServer(true);
             ZkServiceRegistry registry = new ZkServiceRegistry(
                     server.getConnectString())) {
            String runId = "zk-recovered-user-cancel";
            FileArtifactRepository repository = new FileArtifactRepository(
                    temporary.resolve("cancel-artifacts"));
            byte[] artifactBytes = artifact("zk-cancel-definition");
            String artifactId = repository.put(artifactBytes);
            RunStateStore store = registry.runStateStore();
            RunSnapshot snapshot = accepted(store, RunCommand.createRun(
                    "create-zk-cancel", runId, "zk-cancel-definition",
                    artifactId, 1, 0L, 1L));
            snapshot = accepted(store, RunCommand.createAttempt(
                    "attempt-zk-cancel", runId, snapshot.revision(),
                    "attempt-zk-cancel", "publication-zk-cancel", 2L));
            snapshot = accepted(store, RunCommand.advanceAttempt(
                    "dispatch-zk-cancel", runId, snapshot.revision(),
                    snapshot.currentAttempt().attemptId(),
                    snapshot.currentAttempt().fenceToken(),
                    AttemptStatus.DISPATCHING, false, 3L, null, null));
            RunAttemptFence fence = new RunAttemptFence(
                    runId, snapshot.currentAttempt().attemptId(),
                    snapshot.currentAttempt().fenceToken());
            StageAssignmentRecord record = StageAssignmentRecord.create(
                    fence, 1L, 0, 701L, 0, 0,
                    StageAssignmentRole.WHOLE_GRAPH, "departed-zk-worker",
                    "127.0.0.1", 61_701, -1, "departed-zk-incarnation");
            DurableStageAssignmentAuthority assignments =
                    registry.durableStageAssignmentAuthority();
            StageAssignmentSnapshot assignment = assignments.publish(
                    new StageAssignmentManifest(fence, artifactId, 1L,
                            Collections.singletonList(record))).snapshot();
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    "zk-cancel-command".getBytes(StandardCharsets.UTF_8));
            assignments.accept(new StageAssignmentAcceptance(
                    assignment.token(0), digest, 4L));
            snapshot = accepted(store, RunCommand.advanceAttempt(
                    "running-zk-cancel", runId, snapshot.revision(),
                    snapshot.currentAttempt().attemptId(),
                    snapshot.currentAttempt().fenceToken(),
                    AttemptStatus.RUNNING, false, 5L, null, null));
            ((RunCancellationAuthority) store).requestUserCancellation(
                    runId, "request-zk-recovery");

            try (MasterServer master = new MasterServer(
                    5_000L, 0, registry,
                    io.rowweave.monitor.JobMonitorRegistry.NOOP, repository);
                 MasterElection election = new MasterElection(
                         registry, new Endpoint("127.0.0.1", master.port()))) {
                master.bindLeadership(election);
                election.start();
                long generation = master.recoveryInventoryGeneration();
                assertTrue(master.awaitRecoveryInventory(
                        generation, 15, TimeUnit.SECONDS));

                RunSnapshot canceled = store.get(runId);
                assertEquals(RunStatus.CANCELED, canceled.status());
                assertEquals(AttemptStatus.CANCELED,
                        canceled.currentAttempt().status());
                assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                        master.recoveryInventorySnapshot().state());
            }
        }
    }

    @Test
    void zkLeaderUsesServerTimeToExpireInheritedReadyRun() throws Exception {
        try (TestingServer server = new TestingServer(true);
             ZkServiceRegistry registry = new ZkServiceRegistry(
                     server.getConnectString())) {
            String runId = "zk-recovered-ready-deadline";
            FileArtifactRepository repository = new FileArtifactRepository(
                    temporary.resolve("deadline-artifacts"));
            byte[] artifactBytes = artifact("zk-deadline-definition");
            String artifactId = repository.put(artifactBytes);
            RunStateStore store = registry.runStateStore();
            accepted(store, RunCommand.createRun(
                    "create-zk-deadline", runId, "zk-deadline-definition",
                    artifactId, 1, 2L, 1L));

            try (MasterServer master = new MasterServer(
                    5_000L, 0, registry,
                    io.rowweave.monitor.JobMonitorRegistry.NOOP, repository);
                 MasterElection election = new MasterElection(
                         registry, new Endpoint("127.0.0.1", master.port()))) {
                master.bindLeadership(election);
                election.start();
                long generation = awaitInventoryGeneration(master, 15_000L);
                assertTrue(master.awaitRecoveryInventory(
                        generation, 15, TimeUnit.SECONDS));

                RunSnapshot timedOut = store.get(runId);
                assertEquals(RunStatus.FAILED, timedOut.status());
                assertEquals(0, timedOut.attemptCount());
                assertNull(timedOut.currentAttempt());
                assertEquals(1L, store.events(runId, 0L, 10).stream()
                        .filter(event -> event.type()
                                == ExecutionEvent.Type.TIMEOUT_REQUESTED)
                        .count());
                assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                        master.recoveryInventorySnapshot().state());
            }
        }
    }

    @Test
    void failoverOwnerFencesOldControllerAfterCommittingBarrier() throws Exception {
        FirstInvocationBlockingHook commitHook = new FirstInvocationBlockingHook();
        try (TestingServer server = new TestingServer(true);
             ZkServiceRegistry firstRegistry = new ZkServiceRegistry(server.getConnectString());
             ZkServiceRegistry secondRegistry = new ZkServiceRegistry(server.getConnectString());
             ZkServiceRegistry workerRegistry = new ZkServiceRegistry(server.getConnectString())) {
            FileArtifactRepository repository =
                    new FileArtifactRepository(temporary.resolve("fence-artifacts"));
            Path domain = temporary.resolve("fence-output");
            PlanArtifact artifact = executableArtifact(
                    "zk-fence-definition",
                    new ManifestFileSink(META, domain.toString(), "ledger"));
            String artifactId = repository.put(PlanArtifactCodec.encode(artifact));
            RunStateStore store = firstRegistry.runStateStore();
            accepted(store, RunCommand.createRun(
                    "create-zk-fence", "zk-fence", artifact.definitionId(),
                    artifactId, 1, 0L, 7L));

            MasterServer first = new MasterServer(5_000L, 0, firstRegistry,
                    io.rowweave.monitor.JobMonitorRegistry.NOOP, repository);
            MasterServer second = new MasterServer(5_000L, 0, secondRegistry,
                    io.rowweave.monitor.JobMonitorRegistry.NOOP, repository);
            MasterElection firstElection = new MasterElection(firstRegistry,
                    new Endpoint("127.0.0.1", first.port()));
            MasterElection secondElection = new MasterElection(secondRegistry,
                    new Endpoint("127.0.0.1", second.port()));
            first.setManifestPublishHooks(commitHook, null);
            second.setManifestPublishHooks(commitHook, null);
            java.io.File spill = temporary.resolve("fence-worker-spill").toFile();
            assertTrue(spill.mkdirs());
            try {
                first.bindLeadership(firstElection);
                second.bindLeadership(secondElection);
                firstElection.start();
                secondElection.start();
                boolean firstLed = firstElection.isLeader();
                MasterServer newMaster = firstLed ? second : first;
                MasterElection oldElection = firstLed ? firstElection : secondElection;
                MasterElection newElection = firstLed ? secondElection : firstElection;
                ZkServiceRegistry ownerRegistry = firstLed ? firstRegistry : secondRegistry;
                try (WorkerServer worker = WorkerServer.newUnguarded(
                        "zk-fence-worker", 8,
                        new RegistryMasterLocator(workerRegistry), spill)) {
                    assertTrue(commitHook.awaitEntered(15, TimeUnit.SECONDS));
                    RunSnapshot committing = store.get("zk-fence");
                    assertEquals(AttemptStatus.COMMITTING,
                            committing.currentAttempt().status());
                    RecoveryToken oldToken = ownerRegistry.runRecoveryOwnership()
                            .ownership("zk-fence");

                    oldElection.close();
                    awaitLeader(newElection, 5_000L);
                    long generation = awaitInventoryGeneration(
                            newMaster, 15_000L);
                    assertTrue(newMaster.awaitRecoveryInventory(
                            generation, 15, TimeUnit.SECONDS));
                    RecoveryToken replacement = secondRegistry.runRecoveryOwnership()
                            .ownership("zk-fence");
                    assertEquals(oldToken.ownershipEpoch() + 1L,
                            replacement.ownershipEpoch());
                    assertFalse(firstRegistry.runRecoveryOwnership().current(oldToken));

                    RunSnapshot resolved = store.get("zk-fence");
                    assertEquals(RunStatus.SUCCEEDED, resolved.status());
                    assertEquals(AttemptStatus.SUCCEEDED,
                            resolved.currentAttempt().status());
                    assertTrue(resolved.revision() > committing.revision());
                    assertManifestRows(domain);

                    commitHook.release();
                    Thread.sleep(500L);
                    RunSnapshot fenced = store.get("zk-fence");
                    assertEquals(RunStatus.SUCCEEDED, fenced.status());
                    assertEquals(AttemptStatus.SUCCEEDED,
                            fenced.currentAttempt().status());
                    assertEquals(resolved.revision(), fenced.revision(),
                            "old controller must not mutate the replacement owner's terminal");
                }
            } finally {
                commitHook.release();
                secondElection.close();
                firstElection.close();
                second.close();
                first.close();
            }
        }
    }

    private FailoverEvidence failoverAt(String runId, CrashCut cut,
                                        boolean multiOutput) throws Exception {
        BlockingHook publishHook = new BlockingHook();
        BlockingHook acceptanceHook = new BlockingHook();
        Runnable afterPublish = cut == CrashCut.INTENT_BEFORE_ACCEPT
                ? publishHook : () -> { };
        Runnable afterAccept = cut == CrashCut.INTENT_BEFORE_ACCEPT
                ? () -> { } : acceptanceHook;
        ExecutorService submitter = Executors.newSingleThreadExecutor();
        try (TestingServer server = new TestingServer(true);
             HookedZkServiceRegistry firstRegistry = new HookedZkServiceRegistry(
                     server.getConnectString(), afterPublish, afterAccept);
             ZkServiceRegistry secondRegistry = new ZkServiceRegistry(server.getConnectString());
             ZkServiceRegistry workerRegistry = new ZkServiceRegistry(server.getConnectString())) {
            FileArtifactRepository repository = new FileArtifactRepository(
                    temporary.resolve(runId + "-artifacts"));
            PlanArtifact artifact = multiOutput
                    ? multiOutputArtifact(runId + "-definition")
                    : executableArtifact(runId + "-definition", new DiscardSink(META));
            byte[] artifactBytes = PlanArtifactCodec.encode(artifact);
            byte[] envelope = RunSubmissionCodec.encode(new RunSubmission(
                    runId, runId + "-attempt", runId + "-publication",
                    1, 0L, 10L), artifactBytes);

            MasterServer first = new MasterServer(5_000L, 0, firstRegistry,
                    io.rowweave.monitor.JobMonitorRegistry.NOOP, repository);
            MasterServer second = new MasterServer(5_000L, 0, secondRegistry,
                    io.rowweave.monitor.JobMonitorRegistry.NOOP, repository);
            MasterElection firstElection = new MasterElection(firstRegistry,
                    new Endpoint("127.0.0.1", first.port()));
            MasterElection secondElection = new MasterElection(secondRegistry,
                    new Endpoint("127.0.0.1", second.port()));
            java.io.File spill = temporary.resolve(runId + "-worker-spill").toFile();
            assertTrue(spill.mkdirs());
            Future<RpcMessage> submission = null;
            try {
                first.bindLeadership(firstElection);
                second.bindLeadership(secondElection);
                firstElection.start();
                assertTrue(firstElection.isLeader());
                secondElection.start();
                assertFalse(secondElection.isLeader());
                long initialGeneration = first.recoveryInventoryGeneration();
                assertTrue(first.awaitRecoveryInventory(
                        initialGeneration, 10, TimeUnit.SECONDS));
                try (WorkerServer worker = WorkerServer.newUnguarded(
                        runId + "-worker", 16,
                        new RegistryMasterLocator(workerRegistry), spill)) {
                    awaitWorker(firstRegistry, runId + "-worker", 5_000L);
                    submission = submitter.submit(
                            () -> first.handler().apply(RpcMessage.submitRun(envelope)));
                    BlockingHook cutHook = cut == CrashCut.INTENT_BEFORE_ACCEPT
                            ? publishHook : acceptanceHook;
                    assertTrue(cutHook.awaitEntered(15, TimeUnit.SECONDS),
                            "did not reach crash cut " + cut);

                    RunSnapshot beforeFailover = secondRegistry.runStateStore().get(runId);
                    RunAttemptFence fence = new RunAttemptFence(runId,
                            beforeFailover.currentAttempt().attemptId(),
                            beforeFailover.currentAttempt().fenceToken());
                    StageAssignmentSnapshot beforeClaim = secondRegistry
                            .durableStageAssignmentAuthority().assignmentSnapshot(fence);
                    assertTrue(beforeClaim != null);
                    assertEquals(cut == CrashCut.INTENT_BEFORE_ACCEPT ? 0 : 1,
                            beforeClaim.acceptedCount());

                    firstElection.close();
                    awaitLeader(secondElection, 5_000L);
                    long generation = awaitInventoryGeneration(second, 5_000L);
                    boolean settled = second.awaitRecoveryInventory(
                            generation, 10, TimeUnit.SECONDS);
                    LeaderRecoveryInventory.Snapshot observed =
                            second.recoveryInventorySnapshot();
                    StageAssignmentSnapshot observedAssignment = secondRegistry
                            .durableStageAssignmentAuthority().assignmentSnapshot(fence);
                    RecoveryToken observedOwner = ((RunRecoveryOwnershipProvider) secondRegistry)
                            .runRecoveryOwnership().ownership(runId);
                    assertTrue(settled, "recovery inventory did not settle: state="
                            + observed.state() + " activeRuns=" + observed.activeRuns()
                            + " samples=" + observed.samples()
                            + " failure=" + observed.failure()
                            + " owner=" + observedOwner
                            + " accepted=" + (observedAssignment == null ? -1
                            : observedAssignment.acceptedCount())
                            + " terminal=" + (observedAssignment == null ? -1
                            : observedAssignment.terminalCount()));
                    LeaderRecoveryInventory.Snapshot inventory =
                            second.recoveryInventorySnapshot();
                    assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                            inventory.state());
                    assertTrue(inventory.samples().isEmpty());

                    StageAssignmentSnapshot afterClaim = secondRegistry
                            .durableStageAssignmentAuthority().assignmentSnapshot(fence);
                    StageAcceptanceDecision replay = null;
                    StageAssignmentAcceptance committed = firstAcceptance(beforeClaim);
                    if (committed != null) {
                        replay = firstRegistry.durableStageAssignmentAuthority()
                                .accept(committed);
                    }
                    StageAcceptanceDecision late = null;
                    int unaccepted = firstUnaccepted(beforeClaim);
                    if (unaccepted >= 0) {
                        byte[] digest = new byte[32];
                        java.util.Arrays.fill(digest, (byte) 7);
                        late = firstRegistry.durableStageAssignmentAuthority().accept(
                                new StageAssignmentAcceptance(
                                        beforeClaim.token(unaccepted), digest, 20L));
                    }
                    RunSnapshot terminal = secondRegistry.runStateStore().get(runId);
                    return new FailoverEvidence(beforeClaim, afterClaim, terminal,
                            replay, late);
                } finally {
                    publishHook.release();
                    acceptanceHook.release();
                    if (submission != null) {
                        try {
                            submission.get(10, TimeUnit.SECONDS);
                        } catch (Exception ignored) {
                            submission.cancel(true);
                        }
                    }
                }
            } finally {
                publishHook.release();
                acceptanceHook.release();
                secondElection.close();
                firstElection.close();
                second.close();
                first.close();
            }
        } finally {
            submitter.shutdownNow();
            assertTrue(submitter.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static StageAssignmentAcceptance firstAcceptance(
            StageAssignmentSnapshot snapshot) {
        for (int i = 0; i < snapshot.manifest().assignmentCount(); i++) {
            if (snapshot.acceptance(i) != null) return snapshot.acceptance(i);
        }
        return null;
    }

    private static int firstUnaccepted(StageAssignmentSnapshot snapshot) {
        for (int i = 0; i < snapshot.manifest().assignmentCount(); i++) {
            if (snapshot.acceptance(i) == null) return i;
        }
        return -1;
    }

    private static void awaitWorker(HookedZkServiceRegistry registry, String workerId,
                                    long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (registry.worker(workerId) == null && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertTrue(registry.worker(workerId) != null);
    }

    private static long awaitInventoryGeneration(MasterServer master, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        long generation;
        while ((generation = master.recoveryInventoryGeneration()) == 0L
                && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertTrue(generation > 0L);
        return generation;
    }

    private static byte[] artifact(String definitionId) {
        PlanGraph graph = new PlanGraph(Arrays.asList(
                exactSource("source", Collections.emptyList())
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
        return PlanArtifactCodec.encode(LegacyPlanArtifactAdapter.wrap(definitionId, graph));
    }

    private static PlanArtifact executableArtifact(String definitionId, Sink output) {
        DataRow first = new DataRow(1);
        first.set(0, 7);
        DataRow second = new DataRow(1);
        second.set(0, 8);
        PlanGraph graph = new PlanGraph(Arrays.asList(
                exactSource("source", Arrays.asList(first, second))
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, output).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")), 1);
        return LegacyPlanArtifactAdapter.wrap(definitionId, graph);
    }

    private static void assertManifestRows(Path domain) throws Exception {
        try (io.rowweave.FileSource.Reader reader =
                     new ManifestFileSource(META, domain.toString(), "ledger").openReader()) {
            assertEquals(7, reader.next().get(0));
            assertEquals(8, reader.next().get(0));
            assertFalse(reader.hasNext());
        }
    }

    private static PlanArtifact multiOutputArtifact(String definitionId) {
        DataRow row = new DataRow(1);
        row.set(0, 9);
        PlanGraph graph = new PlanGraph(Arrays.asList(
                exactSource("source", Collections.singletonList(row))
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output-a"),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output-b")), 2);
        return LegacyPlanArtifactAdapter.wrap(definitionId, graph);
    }

    private static PlanNode exactSource(String splitId, java.util.List<DataRow> rows) {
        return PlanNode.sourceFromInputs(META, Collections.singletonList(
                new CollectionSource(META, splitId, rows)));
    }

    private static RunSnapshot accepted(RunStateStore store, RunCommand command) {
        RunDecision decision = store.apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }

    private static void awaitLeader(MasterElection election, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (!election.isLeader() && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertTrue(election.isLeader());
    }

    private enum CrashCut {
        INTENT_BEFORE_ACCEPT,
        ACCEPT_BEFORE_ACK,
        PARTIAL_WAVE_ACCEPTED
    }

    private static final class FailoverEvidence {
        final StageAssignmentSnapshot beforeClaim;
        final StageAssignmentSnapshot afterClaim;
        final RunSnapshot terminal;
        final StageAcceptanceDecision existingReceiptReplay;
        final StageAcceptanceDecision lateFirstAccept;

        FailoverEvidence(StageAssignmentSnapshot beforeClaim,
                         StageAssignmentSnapshot afterClaim, RunSnapshot terminal,
                         StageAcceptanceDecision existingReceiptReplay,
                         StageAcceptanceDecision lateFirstAccept) {
            this.beforeClaim = beforeClaim;
            this.afterClaim = afterClaim;
            this.terminal = terminal;
            this.existingReceiptReplay = existingReceiptReplay;
            this.lateFirstAccept = lateFirstAccept;
        }
    }

    private static final class BlockingHook implements Runnable {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        @Override
        public void run() {
            entered.countDown();
            boolean interrupted = false;
            while (true) {
                try {
                    release.await();
                    break;
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            if (interrupted) Thread.currentThread().interrupt();
        }

        boolean awaitEntered(long timeout, TimeUnit unit) throws InterruptedException {
            return entered.await(timeout, unit);
        }

        void release() { release.countDown(); }
    }

    private static final class FirstInvocationBlockingHook implements Runnable {
        private final java.util.concurrent.atomic.AtomicBoolean first =
                new java.util.concurrent.atomic.AtomicBoolean(true);
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        @Override
        public void run() {
            if (!first.compareAndSet(true, false)) return;
            entered.countDown();
            boolean interrupted = false;
            while (true) {
                try {
                    release.await();
                    break;
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            if (interrupted) Thread.currentThread().interrupt();
        }

        boolean awaitEntered(long timeout, TimeUnit unit) throws InterruptedException {
            return entered.await(timeout, unit);
        }

        void release() { release.countDown(); }
    }
}
