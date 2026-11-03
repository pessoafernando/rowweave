package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.ManifestFileSink;
import io.rowweave.DataSchema;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.CancellationCause;
import io.rowweave.run.ClockedRunServiceRegistry;
import io.rowweave.run.DurableStageAssignmentAuthority;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunCancellationAuthority;
import io.rowweave.run.RunDeadlineAuthority;
import io.rowweave.run.RunDeadlineResult;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStatus;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageTerminalReceipt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** inherited READY/CREATED/in-flight deadline convergence. */
class MasterRunDeadlineRecoveryTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));

    @Test
    void dueReadyRunFailsWithoutAttemptOrWorker() throws Exception {
        AtomicLong clock = new AtomicLong(100L);
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(clock::get);
        createReady(registry, "deadline-ready-recovery", 100L);

        RunSnapshot terminal = recoverUntilTerminal(
                registry, "deadline-ready-recovery");
        assertEquals(RunStatus.FAILED, terminal.status());
        assertEquals(0, terminal.attemptCount());
        assertNull(terminal.currentAttempt());
    }

    @Test
    void dueCreatedRunTimesOutWithoutDispatchOrWorker() throws Exception {
        AtomicLong clock = new AtomicLong(100L);
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(clock::get);
        RunSnapshot ready = createReady(
                registry, "deadline-created-recovery", 100L);
        accepted(registry, RunCommand.createAttempt(
                "attempt-created-recovery", ready.runId(), ready.revision(),
                "attempt", "publication", 2L));

        RunSnapshot terminal = recoverUntilTerminal(
                registry, "deadline-created-recovery");
        assertEquals(RunStatus.FAILED, terminal.status());
        assertEquals(AttemptStatus.TIMED_OUT,
                terminal.currentAttempt().status());
        assertNull(registry.durableStageAssignmentAuthority().assignmentSnapshot(
                new RunAttemptFence(
                        terminal.runId(), "attempt",
                terminal.currentAttempt().fenceToken())));
    }

    @Test
    void dueDispatchingRunWithoutPublishedManifestTimesOutAsUnassigned()
            throws Exception {
        AtomicLong clock = new AtomicLong(100L);
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(clock::get);
        String runId = "deadline-dispatching-recovery";
        RunSnapshot snapshot = createReady(registry, runId, 100L);
        snapshot = accepted(registry, RunCommand.createAttempt(
                "attempt-" + runId, runId, snapshot.revision(),
                "attempt", "publication-" + runId, 2L));
        accepted(registry, RunCommand.advanceAttempt(
                "dispatch-" + runId, runId, snapshot.revision(),
                "attempt", snapshot.currentAttempt().fenceToken(),
                AttemptStatus.DISPATCHING, false, 3L, null, null));

        RunSnapshot terminal = recoverUntilTerminal(registry, runId);

        assertEquals(AttemptStatus.TIMED_OUT,
                terminal.currentAttempt().status());
        assertNull(registry.durableStageAssignmentAuthority().assignmentSnapshot(
                new RunAttemptFence(
                        terminal.runId(), "attempt",
                        terminal.currentAttempt().fenceToken())));
    }

    @Test
    void dueRunningRunConvergesDepartedAssignmentToTimedOut() throws Exception {
        AtomicLong clock = new AtomicLong(99L);
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(clock::get);
        createRunningAssignment(
                registry, "deadline-running-recovery", 100L,
                artifact("deadline-running-recovery"),
                "departed-worker", 61_001, "departed-incarnation");
        clock.set(100L);

        RunSnapshot terminal = recoverUntilTerminal(
                registry, "deadline-running-recovery");
        assertEquals(RunStatus.FAILED, terminal.status());
        assertEquals(AttemptStatus.TIMED_OUT,
                terminal.currentAttempt().status());
    }

    @Test
    void replacementWorkerIncarnationProvesOldAssignmentLost() throws Exception {
        AtomicLong clock = new AtomicLong(100L);
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(clock::get);
        createRunningAssignment(
                registry, "deadline-replaced-worker", 100L,
                artifact("deadline-replaced-worker"),
                "worker", 61_101, "old-incarnation");
        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.handler().apply(io.rowweave.cluster.rpc.RpcMessage.registerWorker(
                    "worker", "127.0.0.1", 61_102, 1, -1,
                    "replacement-incarnation"));
            master.bindLeadership(election);
            election.start();

            RunSnapshot terminal = awaitTerminal(
                    registry, "deadline-replaced-worker");
            assertEquals(AttemptStatus.TIMED_OUT,
                    terminal.currentAttempt().status());
        }
    }

    @Test
    void exactLiveWorkerIsDrainedBeforeTimedOut(@TempDir Path temp)
            throws Exception {
        AtomicLong clock = new AtomicLong(99L);
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(clock::get);
        File spill = temp.resolve("deadline-recovery-live-worker").toFile();
        assertTrue(spill.mkdirs());
        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "worker", 1, "127.0.0.1", master.port(), spill);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            AssignmentFixture fixture = createRunningAssignment(
                    registry, "deadline-live-worker", 100L,
                    artifact("deadline-live-worker"),
                    "worker", worker.port(), worker.incarnationId());
            WorkerAssignmentLedger.Registration registration = assignments(worker)
                    .registerDurableSocket(fixture.token.runtimeStageId(), new byte[] {1},
                            fixture.token.fence(), fixture.token, fixture.commandDigest,
                            null, null);
            clock.set(100L);
            master.bindLeadership(election);
            election.start();

            RunSnapshot terminal = awaitTerminal(
                    registry, "deadline-live-worker");
            assertEquals(AttemptStatus.TIMED_OUT,
                    terminal.currentAttempt().status());
            assertEquals(0, worker.runningStageCount());
        }
    }

    @Test
    void unresponsiveExactWorkerKeepsTimeoutCanceling() throws Exception {
        AtomicLong clock = new AtomicLong(100L);
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(clock::get);
        createRunningAssignment(
                registry, "deadline-unresponsive-worker", 100L,
                artifact("deadline-unresponsive-worker"),
                "worker", 61_103, "exact-incarnation");
        try (io.rowweave.cluster.rpc.InProcessRpcRegistry.Registration ignored =
                     io.rowweave.cluster.rpc.InProcessRpcRegistry.register(
                             "127.0.0.1", 61_103, request ->
                                     io.rowweave.cluster.rpc.RpcMessage.jobResult(
                                             false, 0, "worker drain unavailable"));
             MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.handler().apply(io.rowweave.cluster.rpc.RpcMessage.registerWorker(
                    "worker", "127.0.0.1", 61_103, 1, -1,
                    "exact-incarnation"));
            master.bindLeadership(election);
            election.start();
            awaitInventoryState(master,
                    LeaderRecoveryInventory.State.BLOCKED_UNAVAILABLE);

            RunSnapshot pending = registry.runStateStore().get(
                    "deadline-unresponsive-worker");
            assertEquals(RunStatus.CANCELING, pending.status());
            assertEquals(AttemptStatus.CANCELING,
                    pending.currentAttempt().status());
            assertEquals(CancellationCause.TIMEOUT,
                    pending.currentAttempt().cancellationCause());
        }
    }

    @Test
    void manifestTimeoutPersistsAbortBeforeTerminal(@TempDir Path temp)
            throws Exception {
        AtomicLong clock = new AtomicLong(99L);
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(clock::get);
        String runId = "deadline-manifest-recovery";
        Path domain = temp.resolve("deadline-manifest-recovery-domain");
        AssignmentFixture fixture = createRunningAssignment(
                registry, runId, 100L,
                artifact(runId, new ManifestFileSink(
                        META, domain.toString(), "ledger")),
                "departed-worker", 61_104, "departed-incarnation");
        new io.rowweave.commit.FileManifestCommitAuthority().reserve(
                domain, fixture.snapshot.currentAttempt().publicationId());
        clock.set(100L);

        RunSnapshot terminal = recoverUntilTerminal(registry, runId);

        assertEquals(AttemptStatus.TIMED_OUT,
                terminal.currentAttempt().status());
        BatchSinkAbortRecord abort = registry.durableStageAssignmentAuthority()
                .batchSinkAbort(fixture.token.fence());
        assertEquals(BatchSinkAbortRecord.State.ABORT_DECIDED, abort.state());
        assertEquals("RUN_DEADLINE", abort.reasonCode());
        assertNull(new io.rowweave.commit.ManifestFileStore(domain).latest());
    }

    @Test
    void committingBeforeDeadlineRemainsPublishableAndSucceeds() throws Exception {
        AtomicLong clock = new AtomicLong(99L);
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(clock::get);
        String runId = "deadline-committing-recovery";
        AssignmentFixture fixture = createRunningAssignment(
                registry, runId, 100L, artifact(runId),
                "departed-worker", 61_105, "departed-incarnation");
        registry.durableStageAssignmentAuthority().recordTerminal(
                StageTerminalReceipt.succeeded(
                        fixture.token, fixture.commandDigest, 1L, 1L, 6L));
        RunSnapshot running = registry.runStateStore().get(runId);
        accepted(registry, RunCommand.advanceAttempt(
                "committing-" + runId, runId, running.revision(),
                running.currentAttempt().attemptId(),
                running.currentAttempt().fenceToken(), AttemptStatus.COMMITTING,
                false, 7L, null, null));
        clock.set(100L);

        RunSnapshot terminal = recoverUntilTerminal(registry, runId);

        assertEquals(RunStatus.SUCCEEDED, terminal.status());
        assertEquals(AttemptStatus.SUCCEEDED,
                terminal.currentAttempt().status());
    }

    @Test
    void userCancellationBeforeDeadlineRemainsCanceledDuringRecovery()
            throws Exception {
        AtomicLong clock = new AtomicLong(99L);
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(clock::get);
        String runId = "user-before-deadline-recovery";
        createRunningAssignment(
                registry, runId, 100L, artifact(runId),
                "departed-worker", 61_108, "departed-incarnation");
        ((RunCancellationAuthority) registry.runStateStore())
                .requestUserCancellation(runId, "user-wins-recovery");
        clock.set(100L);

        RunSnapshot terminal = recoverUntilTerminal(registry, runId);

        assertEquals(RunStatus.CANCELED, terminal.status());
        assertEquals(AttemptStatus.CANCELED,
                terminal.currentAttempt().status());
        assertEquals(CancellationCause.USER,
                terminal.currentAttempt().cancellationCause());
    }

    @Test
    void deadlineWinningDuringReconcileIsReclassifiedNotFailed()
            throws Exception {
        AtomicLong clock = new AtomicLong(99L);
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(clock::get);
        String runId = "deadline-races-recovery";
        createRunningAssignment(
                registry, runId, 100L, artifact(runId),
                "worker", 61_106, "exact-incarnation");
        CountDownLatch reconcileEntered = new CountDownLatch(1);
        CountDownLatch releaseReconcile = new CountDownLatch(1);
        try (io.rowweave.cluster.rpc.InProcessRpcRegistry.Registration ignored =
                     io.rowweave.cluster.rpc.InProcessRpcRegistry.register(
                             "127.0.0.1", 61_106, request -> {
                                 reconcileEntered.countDown();
                                 try {
                                     assertTrue(releaseReconcile.await(
                                             5, TimeUnit.SECONDS));
                                 } catch (InterruptedException interrupted) {
                                     Thread.currentThread().interrupt();
                                 }
                                 return io.rowweave.cluster.rpc.RpcMessage
                                         .durableStageReconciledAbsent();
                             });
             MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.handler().apply(io.rowweave.cluster.rpc.RpcMessage.registerWorker(
                    "worker", "127.0.0.1", 61_106, 1, -1,
                    "exact-incarnation"));
            master.bindLeadership(election);
            election.start();
            assertTrue(reconcileEntered.await(5, TimeUnit.SECONDS));
            clock.set(100L);
            assertEquals(RunDeadlineResult.Disposition.ACCEPTED,
                    ((RunDeadlineAuthority) registry.runStateStore())
                            .evaluateDeadline(runId).disposition());
            releaseReconcile.countDown();

            RunSnapshot terminal = awaitTerminal(registry, runId);
            assertEquals(AttemptStatus.TIMED_OUT,
                    terminal.currentAttempt().status());
        } finally {
            releaseReconcile.countDown();
        }
    }

    @Test
    void replacementLeaderCompletesTimeoutAfterOwnerAndWorkerCut()
            throws Exception {
        AtomicLong clock = new AtomicLong(99L);
        AtomicInteger reconciliations = new AtomicInteger();
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(clock::get);
        String runId = "deadline-replacement-leader";
        createRunningAssignment(
                registry, runId, 100L, artifact(runId),
                "worker", 61_107, "exact-incarnation");
        CountDownLatch firstReconcile = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        try (io.rowweave.cluster.rpc.InProcessRpcRegistry.Registration ignored =
                     io.rowweave.cluster.rpc.InProcessRpcRegistry.register(
                             "127.0.0.1", 61_107, request -> {
                                 if (reconciliations.incrementAndGet() == 1) {
                                     firstReconcile.countDown();
                                     try {
                                         releaseFirst.await(5, TimeUnit.SECONDS);
                                     } catch (InterruptedException interrupted) {
                                         Thread.currentThread().interrupt();
                                     }
                                 }
                                 return io.rowweave.cluster.rpc.RpcMessage
                                         .durableStageReconciledAbsent();
                             });
             MasterServer first = new MasterServer(5_000L, 0, registry);
             MasterServer second = new MasterServer(5_000L, 0, registry);
             MasterElection firstElection = new MasterElection(
                     registry, new Endpoint("127.0.0.1", first.port()));
             MasterElection secondElection = new MasterElection(
                     registry, new Endpoint("127.0.0.1", second.port()))) {
            first.handler().apply(io.rowweave.cluster.rpc.RpcMessage.registerWorker(
                    "worker", "127.0.0.1", 61_107, 1, -1,
                    "exact-incarnation"));
            first.bindLeadership(firstElection);
            second.bindLeadership(secondElection);
            firstElection.start();
            secondElection.start();
            assertTrue(firstReconcile.await(5, TimeUnit.SECONDS));
            clock.set(100L);
            assertEquals(RunDeadlineResult.Disposition.ACCEPTED,
                    ((RunDeadlineAuthority) registry.runStateStore())
                            .evaluateDeadline(runId).disposition());
            firstElection.close();
            releaseFirst.countDown();
            long leaderDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!secondElection.isLeader()
                    && System.nanoTime() < leaderDeadline) {
                Thread.sleep(10L);
            }
            assertTrue(secondElection.isLeader());

            RunSnapshot terminal = awaitTerminal(registry, runId);
            assertEquals(AttemptStatus.TIMED_OUT,
                    terminal.currentAttempt().status());
            assertTrue(reconciliations.get() >= 2,
                    "replacement leader must reconcile durable assignment evidence");
        } finally {
            releaseFirst.countDown();
        }
    }

    @Test
    void missingAssignmentManifestAfterRunningTimeoutFailsClosed() throws Exception {
        AtomicLong clock = new AtomicLong(99L);
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(clock::get);
        String runId = "deadline-missing-assignment-manifest";
        RunSnapshot snapshot = createReady(registry, runId, 100L);
        snapshot = accepted(registry, RunCommand.createAttempt(
                "attempt-" + runId, runId, snapshot.revision(),
                "attempt", "publication-" + runId, 2L));
        snapshot = accepted(registry, RunCommand.advanceAttempt(
                "dispatch-" + runId, runId, snapshot.revision(),
                "attempt", snapshot.currentAttempt().fenceToken(),
                AttemptStatus.DISPATCHING, false, 3L, null, null));
        accepted(registry, RunCommand.advanceAttempt(
                "running-" + runId, runId, snapshot.revision(),
                "attempt", snapshot.currentAttempt().fenceToken(),
                AttemptStatus.RUNNING, false, 4L, null, null));
        clock.set(100L);
        assertEquals(RunDeadlineResult.Disposition.ACCEPTED,
                ((RunDeadlineAuthority) registry.runStateStore())
                        .evaluateDeadline(runId).disposition());

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();
            awaitInventoryState(master,
                    LeaderRecoveryInventory.State.BLOCKED_CORRUPT);

            RunSnapshot pending = registry.runStateStore().get(runId);
            assertEquals(RunStatus.CANCELING, pending.status());
            assertEquals(AttemptStatus.CANCELING,
                    pending.currentAttempt().status());
        }
    }

    @Test
    void readyDeadlineWinningBeforeCreateAttemptIsResolvedNotCorrupt()
            throws Exception {
        AtomicLong clock = new AtomicLong(99L);
        BlockingWorkerRegistry workers = new BlockingWorkerRegistry();
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(
                clock::get, workers);
        String runId = "deadline-ready-create-race";
        createReady(registry, runId, 100L);
        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.handler().apply(io.rowweave.cluster.rpc.RpcMessage.registerWorker(
                    "worker", "127.0.0.1", 61_109, 1, -1, "incarnation"));
            workers.arm();
            master.bindLeadership(election);
            election.start();
            assertTrue(workers.entered.await(5, TimeUnit.SECONDS));
            clock.set(100L);
            assertEquals(RunDeadlineResult.Disposition.ACCEPTED,
                    ((RunDeadlineAuthority) registry.runStateStore())
                            .evaluateDeadline(runId).disposition());
            workers.release.countDown();

            awaitInventoryState(master, LeaderRecoveryInventory.State.EMPTY_READY);
            RunSnapshot terminal = registry.runStateStore().get(runId);
            assertEquals(RunStatus.FAILED, terminal.status());
            assertEquals(0, terminal.attemptCount());
        } finally {
            workers.release.countDown();
        }
    }

    @Test
    void createdDeadlineWinningBeforeDispatchIsReconciledNotCorrupt()
            throws Exception {
        AtomicLong clock = new AtomicLong(99L);
        BlockingWorkerRegistry workers = new BlockingWorkerRegistry();
        ClockedRunServiceRegistry registry = new ClockedRunServiceRegistry(
                clock::get, workers);
        String runId = "deadline-created-dispatch-race";
        RunSnapshot ready = createReady(registry, runId, 100L);
        accepted(registry, RunCommand.createAttempt(
                "attempt-" + runId, runId, ready.revision(),
                "attempt", "publication-" + runId, 2L));
        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.handler().apply(io.rowweave.cluster.rpc.RpcMessage.registerWorker(
                    "worker", "127.0.0.1", 61_110, 1, -1, "incarnation"));
            workers.arm();
            master.bindLeadership(election);
            election.start();
            assertTrue(workers.entered.await(5, TimeUnit.SECONDS));
            clock.set(100L);
            assertEquals(RunDeadlineResult.Disposition.ACCEPTED,
                    ((RunDeadlineAuthority) registry.runStateStore())
                            .evaluateDeadline(runId).disposition());
            workers.release.countDown();

            awaitInventoryState(master, LeaderRecoveryInventory.State.EMPTY_READY);
            RunSnapshot terminal = registry.runStateStore().get(runId);
            assertEquals(AttemptStatus.TIMED_OUT,
                    terminal.currentAttempt().status());
        } finally {
            workers.release.countDown();
        }
    }

    private static RunSnapshot recoverUntilTerminal(
            ClockedRunServiceRegistry registry, String runId) throws Exception {
        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();
            return awaitTerminal(registry, runId);
        }
    }

    private static RunSnapshot awaitTerminal(
            ClockedRunServiceRegistry registry, String runId) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        RunSnapshot snapshot;
        do {
            snapshot = registry.runStateStore().get(runId);
            if (snapshot != null && snapshot.status().isTerminal()) return snapshot;
            Thread.sleep(10L);
        } while (System.nanoTime() < deadline);
        assertTrue(snapshot != null && snapshot.status().isTerminal(),
                "recovered Run did not become terminal: "
                        + (snapshot == null ? "missing" : snapshot.status()));
        return snapshot;
    }

    private static void awaitInventoryState(
            MasterServer master, LeaderRecoveryInventory.State expected)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (master.recoveryInventorySnapshot().state() != expected
                && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertEquals(expected, master.recoveryInventorySnapshot().state());
    }

    private static RunSnapshot createReady(
            ClockedRunServiceRegistry registry, String runId, long deadline) {
        return createReady(registry, runId, deadline, artifact(runId));
    }

    private static RunSnapshot createReady(
            ClockedRunServiceRegistry registry, String runId, long deadline,
            PlanArtifact artifact) {
        String artifactId = registry.artifactRepository().put(
                PlanArtifactCodec.encode(artifact));
        return accepted(registry, RunCommand.createRun(
                "create-" + runId, runId, artifact.definitionId(), artifactId,
                1, deadline, 1L));
    }

    private static AssignmentFixture createRunningAssignment(
            ClockedRunServiceRegistry registry, String runId, long deadline,
            PlanArtifact artifact, String workerId, int workerPort,
            String workerIncarnation) throws Exception {
        RunSnapshot snapshot = createReady(registry, runId, deadline, artifact);
        snapshot = accepted(registry, RunCommand.createAttempt(
                "attempt-" + runId, runId, snapshot.revision(),
                "attempt", "publication-" + runId, 2L));
        snapshot = accepted(registry, RunCommand.advanceAttempt(
                "dispatch-" + runId, runId, snapshot.revision(),
                "attempt", snapshot.currentAttempt().fenceToken(),
                AttemptStatus.DISPATCHING, false, 3L, null, null));
        RunAttemptFence fence = new RunAttemptFence(
                runId, "attempt", snapshot.currentAttempt().fenceToken());
        StageAssignmentRecord record = StageAssignmentRecord.create(
                fence, 1L, 0, 101L, 0, 0, StageAssignmentRole.WHOLE_GRAPH,
                workerId, "127.0.0.1", workerPort, -1, workerIncarnation);
        DurableStageAssignmentAuthority authority =
                registry.durableStageAssignmentAuthority();
        StageAssignmentSnapshot assignment = authority.publish(
                new StageAssignmentManifest(
                        fence, snapshot.artifactId(), 1L,
                        Collections.singletonList(record))).snapshot();
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                "deadline-recovery-command".getBytes(StandardCharsets.UTF_8));
        authority.accept(new StageAssignmentAcceptance(
                assignment.token(0), digest, 4L));
        snapshot = accepted(registry, RunCommand.advanceAttempt(
                "running-" + runId, runId, snapshot.revision(),
                "attempt", snapshot.currentAttempt().fenceToken(),
                AttemptStatus.RUNNING, false, 5L, null, null));
        return new AssignmentFixture(snapshot, assignment.token(0), digest);
    }

    private static PlanArtifact artifact(String runId) {
        return artifact(runId, new DiscardSink(META));
    }

    private static PlanArtifact artifact(String runId, io.rowweave.Sink output) {
        return LegacyPlanArtifactAdapter.wrap(
                "definition-" + runId,
                new PlanGraph(Arrays.asList(
                        DurableTestSources.source(
                                        META, Collections.emptyList(),
                                        "source-" + runId)
                                .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                        PlanNode.sink(META, 0, output).build()
                                .withParam(PlanNode.OPERATOR_UID_PARAM, "output"))));
    }

    private static RunSnapshot accepted(
            ClockedRunServiceRegistry registry, RunCommand command) {
        RunDecision decision = registry.runStateStore().apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }

    private static WorkerAssignmentLedger assignments(WorkerServer worker)
            throws Exception {
        java.lang.reflect.Field field = WorkerServer.class.getDeclaredField("assignments");
        field.setAccessible(true);
        return (WorkerAssignmentLedger) field.get(worker);
    }

    private static final class AssignmentFixture {
        final RunSnapshot snapshot;
        final io.rowweave.run.StageAssignmentToken token;
        final byte[] commandDigest;

        AssignmentFixture(RunSnapshot snapshot,
                          io.rowweave.run.StageAssignmentToken token,
                          byte[] commandDigest) {
            this.snapshot = snapshot;
            this.token = token;
            this.commandDigest = commandDigest;
        }
    }

    private static final class BlockingWorkerRegistry implements ServiceRegistry {
        private final InProcessRegistry delegate = new InProcessRegistry();
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        private volatile boolean armed;

        void arm() { armed = true; }

        @Override public void register(RegistryWorker worker) {
            delegate.register(worker);
        }
        @Override public boolean tryClaimExclusive(RegistryWorker worker) {
            return delegate.tryClaimExclusive(worker);
        }
        @Override public void report(RegistryWorker worker) { delegate.report(worker); }
        @Override public void renew(String workerId) { delegate.renew(workerId); }
        @Override public void deregister(String workerId) { delegate.deregister(workerId); }
        @Override public RegistryWorker worker(String workerId) {
            return delegate.worker(workerId);
        }
        @Override public void watch(WatchListener listener) { delegate.watch(listener); }

        @Override
        public List<RegistryWorker> liveWorkers() {
            if (armed) {
                armed = false;
                entered.countDown();
                try {
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            return delegate.liveWorkers();
        }
    }
}
