package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.CollectionSource;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.ManifestFileSource;
import io.rowweave.ManifestFileSink;
import io.rowweave.QualityGateDecision;
import io.rowweave.QualityGatePolicy;
import io.rowweave.QualityPreparedEvidence;
import io.rowweave.QualityPreparedEvidenceReader;
import io.rowweave.QualityEvidenceReadGuard;
import io.rowweave.DataRowClassification;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.TypedQualityManifest;
import io.rowweave.artifact.ArtifactRepository;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.rpc.InProcessRpcRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.commit.BatchSinkOutcome;
import io.rowweave.commit.ManifestCommitWire;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.ops.RowValidation;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.AttemptOutputProfile;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.DurableStageAssignmentAuthority;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunCancellationAuthority;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStatus;
import io.rowweave.run.QualityGateAssignmentBinding;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.ManifestMemberReceipt;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;

/** inherited in-flight attempts converge only from durable evidence. */
class MasterInFlightRunRecoveryTest {
    private static final DataSchema META = new DataSchema(new Field("id", DataType.IntegerType));
    private static final DataSchema QUALITY_META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("period", DataType.IntegerType));

    @Test
    void qualityDecisionBeforeCrashIsRecomputedFromArtifactAndPublished(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "quality-recover-before-decision";
        Path domain = tmp.resolve(runId);
        QualityRecoveryFixture fixture = createQualityManifestEvidence(
                registry, runId, domain, QualityGatePolicy.allowAll(), false);

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();
            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(
                    generation, 10, TimeUnit.SECONDS));

            RunSnapshot terminal = registry.runStateStore().get(runId);
            assertEquals(RunStatus.SUCCEEDED, terminal.status());
            QualityGateDecision decision = registry
                    .durableStageAssignmentAuthority()
                    .qualityGateDecision(fixture.fence);
            assertEquals(QualityGateDecision.Outcome.PASS,
                    decision.outcome());
            assertEquals("publication-" + runId,
                    new ManifestFileStore(domain).latest().attemptId());
        }
    }

    @Test
    void failedQualityDecisionBeforeCrashCompletesAbortWithoutPublication(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "quality-recover-after-fail";
        Path domain = tmp.resolve(runId);
        QualityRecoveryFixture fixture = createQualityManifestEvidence(
                registry, runId, domain,
                QualityGatePolicy.builder().maxRejectedRows(0L).build(), true);

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();
            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(
                    generation, 10, TimeUnit.SECONDS));

            RunSnapshot terminal = registry.runStateStore().get(runId);
            assertEquals(RunStatus.FAILED, terminal.status());
            assertEquals("QUALITY_GATE_REJECTED",
                    terminal.currentAttempt().reasonCode());
            QualityGateDecision decision = registry
                    .durableStageAssignmentAuthority()
                    .qualityGateDecision(fixture.fence);
            assertEquals(QualityGateDecision.Outcome.FAIL,
                    decision.outcome());
            BatchSinkAbortRecord abort = registry
                    .durableStageAssignmentAuthority()
                    .batchSinkAbort(fixture.fence);
            assertEquals(BatchSinkAbortRecord.State.ABORTED, abort.state());
            assertEquals("QUALITY_GATE_REJECTED", abort.reasonCode());
            assertNull(new ManifestFileStore(domain).latest());
        }
    }

    @Test
    void qualityArtifactPolicyMismatchBeforeDecisionFailsClosed(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "quality-recover-policy-mismatch";
        Path domain = tmp.resolve(runId);
        QualityRecoveryFixture fixture = createQualityManifestEvidence(
                registry, runId, domain,
                QualityGatePolicy.builder().maxRejectedRows(0L).build(),
                QualityGatePolicy.allowAll(), false, false);

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();
            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(
                    generation, 10, TimeUnit.SECONDS));

            RunSnapshot unresolved = registry.runStateStore().get(runId);
            assertEquals(RunStatus.RUNNING, unresolved.status());
            assertEquals(AttemptStatus.COMMITTING,
                    unresolved.currentAttempt().status());
            assertNull(registry.durableStageAssignmentAuthority()
                    .qualityGateDecision(fixture.fence));
            assertNull(new ManifestFileStore(domain).latest());
            assertEquals(LeaderRecoveryInventory.State.BLOCKED_CORRUPT,
                    master.recoveryInventorySnapshot().state());
        }
    }

    @Test
    void existingQualityIntentIsPublishedWithoutPolicyReevaluation(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "quality-recover-existing-intent";
        Path domain = tmp.resolve(runId);
        createQualityManifestEvidence(
                registry, runId, domain,
                QualityGatePolicy.builder().maxRejectedRows(0L).build(),
                QualityGatePolicy.allowAll(), true, true);

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();
            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(
                    generation, 10, TimeUnit.SECONDS));

            RunSnapshot terminal = registry.runStateStore().get(runId);
            assertEquals(RunStatus.SUCCEEDED, terminal.status());
            assertEquals("publication-" + runId,
                    new ManifestFileStore(domain).latest().attemptId());
        }
    }

    @Test
    void completeDiscardEvidenceConvergesSuccessAndFailureWithoutALiveWorker()
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        createTerminalAttempt(registry, "recovered-success", AttemptStatus.COMMITTING, true);
        createTerminalAttempt(registry, "recovered-failure", AttemptStatus.RUNNING, false);
        createAttemptEvidence(registry, "recovered-lost", AttemptStatus.RUNNING, null);

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();

            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(generation, 10, TimeUnit.SECONDS));
            RunSnapshot succeeded = registry.runStateStore().get("recovered-success");
            RunSnapshot failed = registry.runStateStore().get("recovered-failure");
            RunSnapshot lost = registry.runStateStore().get("recovered-lost");
            assertEquals(RunStatus.SUCCEEDED, succeeded.status());
            assertEquals(AttemptStatus.SUCCEEDED, succeeded.currentAttempt().status());
            assertEquals(4L, succeeded.resultSummary().rowsOut());
            assertEquals(io.rowweave.run.RunResultSummary.PublicationKind.DISCARD,
                    succeeded.resultSummary().publicationKind());
            assertEquals(RunStatus.FAILED, failed.status());
            assertEquals(AttemptStatus.FAILED, failed.currentAttempt().status());
            assertEquals(RunStatus.FAILED, lost.status());
            assertEquals("STAGE_TERMINAL", lost.currentAttempt().reasonCode());
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    master.recoveryInventorySnapshot().state());
        }
    }

    @Test
    void providerTerminalWithoutLocatorAndWithoutExactWorkerAbortsAndFailsClosed(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "provider-terminal-without-locator";
        Path domain = tmp.resolve(runId);
        RunAttemptFence fence = createProviderTerminalWithoutLocator(
                registry, runId, domain);

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();
            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(
                    generation, 10, TimeUnit.SECONDS));

            RunSnapshot failed = registry.runStateStore().get(runId);
            assertEquals(RunStatus.FAILED, failed.status());
            assertEquals(AttemptStatus.FAILED,
                    failed.currentAttempt().status());
            assertEquals("PREPARED_LOCATOR_RECOVERY_FAILED",
                    failed.currentAttempt().reasonCode());
            BatchSinkAbortRecord abort = registry
                    .durableStageAssignmentAuthority().batchSinkAbort(fence);
            assertEquals(BatchSinkAbortRecord.State.ABORT_DECIDED, abort.state());
            assertEquals("PREPARED_LOCATOR_RECOVERY_FAILED",
                    abort.reasonCode());
            assertNull(new ManifestFileStore(domain).latest());
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    master.recoveryInventorySnapshot().state());
        }
    }

    @Test
    void departedDiscardWorkerSchedulesRetryInsteadOfGenericFailure()
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        createRetryableDiscardAttemptEvidence(
                registry, "recovered-retry-lost");

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();

            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(
                    generation, 10, TimeUnit.SECONDS));
            RunSnapshot waiting = registry.runStateStore().get(
                    "recovered-retry-lost");
            assertEquals(RunStatus.READY, waiting.status());
            assertEquals(AttemptStatus.FAILED,
                    waiting.currentAttempt().status());
            assertEquals("RETRY_WORKER_LOST",
                    waiting.currentAttempt().reasonCode());
            assertEquals(1, waiting.attemptCount());
            assertEquals(LeaderRecoveryInventory.State.BLOCKED_PENDING_RECOVERY,
                    master.recoveryInventorySnapshot().state());
        }
    }

    @Test
    void exactWorkerAbsentProofSchedulesDiscardRetry() throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "recovered-retry-exact-absent";
        createRetryableDiscardAttemptEvidence(registry, runId);

        try (InProcessRpcRegistry.Registration endpoint =
                     InProcessRpcRegistry.register(
                             "127.0.0.1", 61_001, request ->
                                     request.type()
                                             == RpcMessage.Type.RECONCILE_DURABLE_STAGE
                                             ? RpcMessage.durableStageReconciledAbsent()
                                             : RpcMessage.ack());
             MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.handler().apply(RpcMessage.registerWorker(
                    "departed-worker", "127.0.0.1", 61_001, 1, -1,
                    "departed-incarnation"));
            master.bindLeadership(election);
            election.start();

            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(
                    generation, 10, TimeUnit.SECONDS));
            RunSnapshot waiting = registry.runStateStore().get(runId);
            assertEquals(RunStatus.READY, waiting.status());
            assertEquals("RETRY_WORKER_LOST",
                    waiting.currentAttempt().reasonCode());
        }
    }

    @Test
    void replacementWorkerIncarnationSchedulesDiscardRetry() throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "recovered-retry-replacement";
        createRetryableDiscardAttemptEvidence(registry, runId);

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.handler().apply(RpcMessage.registerWorker(
                    "departed-worker", "127.0.0.1", 61_001, 1, -1,
                    "replacement-incarnation"));
            master.bindLeadership(election);
            election.start();

            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(
                    generation, 10, TimeUnit.SECONDS));
            RunSnapshot waiting = registry.runStateStore().get(runId);
            assertEquals(RunStatus.READY, waiting.status());
            assertEquals("RETRY_WORKER_LOST",
                    waiting.currentAttempt().reasonCode());
        }
    }

    @Test
    void unresponsiveExactWorkerCannotAuthorizeDiscardRetry() throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "recovered-retry-unresponsive";
        createRetryableDiscardAttemptEvidence(registry, runId);

        try (InProcessRpcRegistry.Registration endpoint =
                     InProcessRpcRegistry.register(
                             "127.0.0.1", 61_001, request ->
                                     request.type()
                                             == RpcMessage.Type.RECONCILE_DURABLE_STAGE
                                             ? RpcMessage.jobResult(
                                                     false, 0,
                                                     "worker drain unavailable")
                                             : RpcMessage.ack());
             MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.handler().apply(RpcMessage.registerWorker(
                    "departed-worker", "127.0.0.1", 61_001, 1, -1,
                    "departed-incarnation"));
            master.bindLeadership(election);
            election.start();

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (master.recoveryInventorySnapshot().state()
                    != LeaderRecoveryInventory.State.BLOCKED_UNAVAILABLE
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertEquals(LeaderRecoveryInventory.State.BLOCKED_UNAVAILABLE,
                    master.recoveryInventorySnapshot().state());
            RunSnapshot running = registry.runStateStore().get(runId);
            assertEquals(RunStatus.RUNNING, running.status());
            assertEquals(AttemptStatus.RUNNING,
                    running.currentAttempt().status());
            RunAttemptFence fence = new RunAttemptFence(
                    runId, running.currentAttempt().attemptId(),
                    running.currentAttempt().fenceToken());
            assertNull(registry.durableStageAssignmentAuthority()
                    .assignmentSnapshot(fence).terminal(0));
        }
    }

    @Test
    void replacementLeaderConvergesUserCancellationFromDepartedAssignment()
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "recovered-user-cancel";
        createAttemptEvidence(registry, runId, AttemptStatus.RUNNING, null);
        ((RunCancellationAuthority) registry.runStateStore())
                .requestUserCancellation(runId, "request-recovered-user-cancel");

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();

            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(generation, 10, TimeUnit.SECONDS));
            RunSnapshot canceled = registry.runStateStore().get(runId);
            assertEquals(RunStatus.CANCELED, canceled.status());
            assertEquals(AttemptStatus.CANCELED,
                    canceled.currentAttempt().status());
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    master.recoveryInventorySnapshot().state());
        }
    }

    @Test
    void replacementLeaderDrainsExactWorkerBeforeCompletingUserCancellation(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "live-worker", 1, "127.0.0.1", master.port(),
                     new File(tmp.toFile(), "cancel-recovery-worker"));
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            AcceptedFixture fixture = createLiveAcceptedAttempt(registry, worker);
            WorkerAssignmentLedger ledger = assignments(worker);
            WorkerAssignmentLedger.Registration registration = ledger.registerDurableSocket(
                    fixture.token.runtimeStageId(), new byte[] {1},
                    fixture.token.fence(), fixture.token, fixture.commandDigest,
                    null, null);
            ((RunCancellationAuthority) registry.runStateStore())
                    .requestUserCancellation("recovered-live", "request-live-recovery");

            master.bindLeadership(election);
            election.start();
            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(generation, 10, TimeUnit.SECONDS));

            RunSnapshot canceled = registry.runStateStore().get("recovered-live");
            assertEquals(RunStatus.CANCELED, canceled.status());
            assertEquals(AttemptStatus.CANCELED,
                    canceled.currentAttempt().status());
            assertEquals(0, worker.runningStageCount());
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    master.recoveryInventorySnapshot().state());
        }
    }

    @Test
    void replacementWorkerIncarnationProvesOldAssignmentLostAndCancellationCompletes()
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "recovered-replaced-worker-cancel";
        createAttemptEvidence(registry, runId, AttemptStatus.RUNNING, null);
        ((RunCancellationAuthority) registry.runStateStore())
                .requestUserCancellation(runId, "request-replaced-worker");

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.handler().apply(RpcMessage.registerWorker(
                    "departed-worker", "127.0.0.1", 61_001, 1, -1,
                    "replacement-incarnation"));
            master.bindLeadership(election);
            election.start();
            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(generation, 10, TimeUnit.SECONDS));

            assertEquals(RunStatus.CANCELED,
                    registry.runStateStore().get(runId).status());
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    master.recoveryInventorySnapshot().state());
        }
    }

    @Test
    void durableStageFailureStillConvergesCanceledWhenUserIntentWon()
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "recovered-terminal-user-cancel";
        createAttemptEvidence(registry, runId, AttemptStatus.RUNNING, false);
        ((RunCancellationAuthority) registry.runStateStore())
                .requestUserCancellation(runId, "request-terminal-user-cancel");

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();
            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(generation, 10, TimeUnit.SECONDS));

            RunSnapshot canceled = registry.runStateStore().get(runId);
            assertEquals(RunStatus.CANCELED, canceled.status());
            assertEquals(AttemptStatus.CANCELED,
                    canceled.currentAttempt().status());
        }
    }

    @Test
    void unresponsiveExactWorkerKeepsRecoveredCancellationNonTerminal()
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "recovered-unresponsive-user-cancel";
        createAttemptEvidence(registry, runId, AttemptStatus.RUNNING, null);
        ((RunCancellationAuthority) registry.runStateStore())
                .requestUserCancellation(runId, "request-unresponsive-recovery");

        try (InProcessRpcRegistry.Registration endpoint = InProcessRpcRegistry.register(
                "127.0.0.1", 61_001, request -> request.type()
                        == RpcMessage.Type.RECONCILE_DURABLE_STAGE
                        ? RpcMessage.jobResult(false, 0, "worker drain unavailable")
                        : RpcMessage.ack());
             MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.handler().apply(RpcMessage.registerWorker(
                    "departed-worker", "127.0.0.1", 61_001, 1, -1,
                    "departed-incarnation"));
            master.bindLeadership(election);
            election.start();

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (master.recoveryInventorySnapshot().state()
                    != LeaderRecoveryInventory.State.BLOCKED_UNAVAILABLE
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertEquals(LeaderRecoveryInventory.State.BLOCKED_UNAVAILABLE,
                    master.recoveryInventorySnapshot().state());
            RunSnapshot canceling = registry.runStateStore().get(runId);
            assertEquals(RunStatus.CANCELING, canceling.status());
            RunAttemptFence fence = new RunAttemptFence(
                    runId, canceling.currentAttempt().attemptId(),
                    canceling.currentAttempt().fenceToken());
            assertNull(registry.durableStageAssignmentAuthority()
                    .assignmentSnapshot(fence).terminal(0));
        }
    }

    @Test
    void userCancellationWinningDuringInFlightRecoveryIsReclassifiedNotCorrupt()
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "recovery-races-user-cancel";
        createAttemptEvidence(registry, runId, AttemptStatus.RUNNING, null);
        CountDownLatch reconcileEntered = new CountDownLatch(1);
        CountDownLatch releaseReconcile = new CountDownLatch(1);

        try (InProcessRpcRegistry.Registration endpoint = InProcessRpcRegistry.register(
                "127.0.0.1", 61_001, request -> {
                    if (request.type() == RpcMessage.Type.RECONCILE_DURABLE_STAGE) {
                        reconcileEntered.countDown();
                        try {
                            if (!releaseReconcile.await(5, TimeUnit.SECONDS)) {
                                return RpcMessage.jobResult(
                                        false, 0, "reconcile test barrier timed out");
                            }
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            return RpcMessage.jobResult(
                                    false, 0, "reconcile test barrier interrupted");
                        }
                        return RpcMessage.durableStageReconciledAbsent();
                    }
                    return RpcMessage.ack();
                });
             MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.handler().apply(RpcMessage.registerWorker(
                    "departed-worker", "127.0.0.1", 61_001, 1, -1,
                    "departed-incarnation"));
            master.bindLeadership(election);
            election.start();
            assertTrue(reconcileEntered.await(5, TimeUnit.SECONDS));

            RpcMessage cancellation = master.handler().apply(
                    RpcMessage.cancelRun(runId, "request-racing-recovery"));
            assertEquals(RunCancellationResult.Disposition.ACCEPTED.name(),
                    cancellation.runCancellationDisposition());
            releaseReconcile.countDown();
            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(generation, 10, TimeUnit.SECONDS));

            assertEquals(RunStatus.CANCELED,
                    registry.runStateStore().get(runId).status());
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    master.recoveryInventorySnapshot().state());
        } finally {
            releaseReconcile.countDown();
        }
    }

    @Test
    void replacementLeaderPersistsManifestAbortBeforeCanceledTerminal(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "recovered-manifest-user-cancel";
        String publicationId = "publication-" + runId;
        Path domain = tmp.resolve("recovered-manifest-user-cancel-domain");
        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrap(
                "definition-" + runId,
                new PlanGraph(Arrays.asList(
                        PlanNode.source(META, Collections.emptyList())
                                .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                        PlanNode.sink(META, 0, new ManifestFileSink(
                                        META, domain.toString(), "ledger"))
                                .build().withParam(
                                        PlanNode.OPERATOR_UID_PARAM, "output"))));
        createAttemptEvidence(
                registry, runId, AttemptStatus.RUNNING, null, artifact);
        long reservedGeneration = new io.rowweave.commit.FileManifestCommitAuthority()
                .reserve(domain, publicationId);
        ((RunCancellationAuthority) registry.runStateStore())
                .requestUserCancellation(runId, "request-manifest-user-cancel");

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();

            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(generation, 10, TimeUnit.SECONDS));
            RunSnapshot canceled = registry.runStateStore().get(runId);
            assertEquals(RunStatus.CANCELED, canceled.status());
            RunAttemptFence fence = new RunAttemptFence(
                    runId, canceled.currentAttempt().attemptId(),
                    canceled.currentAttempt().fenceToken());
            BatchSinkAbortRecord abort = registry.durableStageAssignmentAuthority()
                    .batchSinkAbort(fence);
            assertEquals(BatchSinkAbortRecord.State.ABORT_DECIDED, abort.state());
            assertEquals(reservedGeneration, abort.generation());
            assertNull(new ManifestFileStore(domain).latest());
        }
    }

    @Test
    void replacementLeaderAbortsBeforeDrainingLivePreparedManifest(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "recovered-live-manifest-user-cancel";
        String publicationId = "publication-" + runId;
        Path domain = tmp.resolve("recovered-live-manifest-user-cancel-domain");
        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "live-manifest-cancel-worker", 1,
                     "127.0.0.1", master.port(),
                     new File(tmp.toFile(), "live-manifest-cancel-worker"));
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            ManifestFileSink artifactOutput = new ManifestFileSink(
                    META, domain.toString(), "ledger");
            PlanArtifact artifact = LegacyPlanArtifactAdapter.wrap(
                    "definition-" + runId,
                    new PlanGraph(Arrays.asList(
                            PlanNode.source(META, Collections.emptyList())
                                    .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                            PlanNode.sink(META, 0, artifactOutput).build()
                                    .withParam(PlanNode.OPERATOR_UID_PARAM, "output"))));
            String artifactId = registry.artifactRepository().put(
                    PlanArtifactCodec.encode(artifact));
            RunSnapshot snapshot = accepted(registry, RunCommand.createRun(
                    "create-" + runId, runId, artifact.definitionId(), artifactId,
                    1, 0L, 1L));
            snapshot = accepted(registry, RunCommand.createAttempt(
                    "attempt-" + runId, runId, snapshot.revision(),
                    "attempt-" + runId, publicationId, 2L));
            snapshot = accepted(registry, RunCommand.advanceAttempt(
                    "dispatch-" + runId, runId, snapshot.revision(),
                    snapshot.currentAttempt().attemptId(),
                    snapshot.currentAttempt().fenceToken(),
                    AttemptStatus.DISPATCHING, false, 3L, null, null));
            RunAttemptFence fence = new RunAttemptFence(
                    runId, snapshot.currentAttempt().attemptId(),
                    snapshot.currentAttempt().fenceToken());
            long stageId = 801L;
            StageAssignmentRecord record = StageAssignmentRecord.create(
                    fence, 1L, 0, stageId, 0, 0,
                    StageAssignmentRole.WHOLE_GRAPH, "live-manifest-cancel-worker",
                    "127.0.0.1", worker.port(), -1, worker.incarnationId());
            DurableStageAssignmentAuthority authority =
                    registry.durableStageAssignmentAuthority();
            StageAssignmentSnapshot assignment = authority.publish(
                    new StageAssignmentManifest(fence, artifactId, 1L,
                            Collections.singletonList(record))).snapshot();
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    "live-manifest-cancel-command".getBytes(StandardCharsets.UTF_8));
            authority.accept(new StageAssignmentAcceptance(
                    assignment.token(0), digest, 4L));
            snapshot = accepted(registry, RunCommand.advanceAttempt(
                    "running-" + runId, runId, snapshot.revision(),
                    snapshot.currentAttempt().attemptId(),
                    snapshot.currentAttempt().fenceToken(),
                    AttemptStatus.RUNNING, false, 5L, null, null));

            long reservedGeneration = new io.rowweave.commit.FileManifestCommitAuthority()
                    .reserve(domain, publicationId);
            ManifestFileSink prepared = new ManifestFileSink(
                    META, domain.toString(), "ledger");
            prepared.bindManifestAttempt(publicationId, reservedGeneration);
            prepared.collect(row(91));
            prepared.prepareCommit();
            preparedOutputs(worker).register(
                    "manifest:" + publicationId, stageId, prepared, fence);
            WorkerAssignmentLedger.Registration active = assignments(worker)
                    .registerDurableSocket(stageId, new byte[] {1}, fence,
                            assignment.token(0), digest, null, null);
            assertEquals(1, worker.preparedOutputActiveCount());
            ((RunCancellationAuthority) registry.runStateStore())
                    .requestUserCancellation(runId, "request-live-manifest-recovery");

            master.bindLeadership(election);
            election.start();
            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(generation, 10, TimeUnit.SECONDS));

            RunSnapshot canceled = registry.runStateStore().get(runId);
            assertEquals(RunStatus.CANCELED, canceled.status());
            BatchSinkAbortRecord abort = authority.batchSinkAbort(fence);
            assertEquals(BatchSinkAbortRecord.State.ABORT_DECIDED, abort.state());
            assertEquals(reservedGeneration, abort.generation());
            assertEquals(0, worker.preparedOutputActiveCount());
            assertNull(new ManifestFileStore(domain).latest());
        }
    }

    @Test
    void failedManifestRecoveryPersistsAbortBeforeTerminalCleanup(@TempDir Path tmp)
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "recovered-manifest-failure";
        String publicationId = "publication-" + runId;
        Path domain = tmp.resolve("recovered-manifest-failure-domain");
        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrap(
                "definition-" + runId,
                new PlanGraph(Arrays.asList(
                        PlanNode.source(META, Collections.emptyList())
                                .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                        PlanNode.sink(META, 0,
                                        new ManifestFileSink(
                                                META, domain.toString(), "ledger"))
                                .build().withParam(
                                        PlanNode.OPERATOR_UID_PARAM, "output"))));
        createAttemptEvidence(
                registry, runId, AttemptStatus.RUNNING, false, artifact);
        long reservedGeneration = new io.rowweave.commit.FileManifestCommitAuthority()
                .reserve(domain, publicationId);

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();
            long inventory = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(inventory, 10, TimeUnit.SECONDS));

            RunSnapshot failed = registry.runStateStore().get(runId);
            assertEquals(RunStatus.FAILED, failed.status());
            RunAttemptFence fence = new RunAttemptFence(
                    runId, failed.currentAttempt().attemptId(),
                    failed.currentAttempt().fenceToken());
            BatchSinkAbortRecord abort = registry.durableStageAssignmentAuthority()
                    .batchSinkAbort(fence);
            assertEquals(BatchSinkAbortRecord.State.ABORT_DECIDED, abort.state());
            assertEquals(reservedGeneration, abort.generation());
            BatchSinkOutcome outcome = new RealClusterJobClient(
                    "127.0.0.1", master.port()).queryBatchSinkOutcome(runId);
            assertEquals(BatchSinkOutcome.State.ABORT_DECIDED, outcome.state());
            assertEquals("MANIFEST_ABORT_DECIDED", outcome.detailCode());
            assertNull(new ManifestFileStore(domain).latest());
        }
    }

    @Test
    void departedManifestWorkerCompletesAbortBeforeSchedulingRetry(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "recovered-manifest-retry";
        String publicationId = "publication-" + runId;
        Path domain = tmp.resolve("recovered-manifest-retry-domain");
        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrap(
                "definition-" + runId,
                new PlanGraph(Arrays.asList(
                        PlanNode.source(META, Collections.emptyList())
                                .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                        PlanNode.sink(META, 0, new ManifestFileSink(
                                        META, domain.toString(), "ledger"))
                                .build().withParam(
                                        PlanNode.OPERATOR_UID_PARAM, "output"))));
        createAttemptEvidence(registry, runId, AttemptStatus.RUNNING, null,
                artifact, 2, AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1);
        new io.rowweave.commit.FileManifestCommitAuthority()
                .reserve(domain, publicationId);

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();
            long inventory = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(
                    inventory, 10, TimeUnit.SECONDS));

            RunSnapshot waiting = registry.runStateStore().get(runId);
            assertEquals(RunStatus.READY, waiting.status());
            assertEquals(AttemptStatus.FAILED,
                    waiting.currentAttempt().status());
            assertEquals("RETRY_WORKER_LOST",
                    waiting.currentAttempt().reasonCode());
            assertNull(new ManifestFileStore(domain).latest());
            assertNull(registry.durableStageAssignmentAuthority()
                    .assignmentSnapshot(new RunAttemptFence(
                            runId, "attempt-" + runId, 1L)));
        }
    }

    @Test
    void recoveredManifestRetryRollsBackLivePreparedSinkBeforeAbortCompletes(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "recovered-manifest-prepared-retry";
        String publicationId = "publication-" + runId;
        Path domain = tmp.resolve("recovered-manifest-prepared-retry-domain");
        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "retry-manifest-worker", 1, "127.0.0.1", master.port(),
                     new File(tmp.toFile(), "retry-manifest-worker"));
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            PlanArtifact artifact = LegacyPlanArtifactAdapter.wrap(
                    "definition-" + runId,
                    new PlanGraph(Arrays.asList(
                            PlanNode.source(META, Collections.emptyList())
                                    .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                            PlanNode.sink(META, 0, new ManifestFileSink(
                                            META, domain.toString(), "ledger"))
                                    .build().withParam(
                                            PlanNode.OPERATOR_UID_PARAM, "output"))));
            String artifactId = registry.artifactRepository().put(
                    PlanArtifactCodec.encode(artifact));
            RunSnapshot snapshot = accepted(registry, RunCommand.createRun(
                    "create-" + runId, runId, artifact.definitionId(), artifactId,
                    2, 0L, 1L));
            snapshot = accepted(registry, RunCommand.createAttempt(
                    "attempt-" + runId, runId, snapshot.revision(),
                    "attempt-" + runId, publicationId, 2L));
            snapshot = accepted(registry, RunCommand.advanceAttempt(
                    "dispatch-" + runId, runId, snapshot.revision(),
                    snapshot.currentAttempt().attemptId(),
                    snapshot.currentAttempt().fenceToken(),
                    AttemptStatus.DISPATCHING, false, 3L, null, null));
            RunAttemptFence fence = new RunAttemptFence(
                    runId, snapshot.currentAttempt().attemptId(),
                    snapshot.currentAttempt().fenceToken());
            StageAssignmentRecord source = StageAssignmentRecord.create(
                    fence, 1L, 0, 901L, 0, 0,
                    StageAssignmentRole.SOURCE_STAGE, "departed-source-worker",
                    "127.0.0.1", 61_013, -1, "departed-source-incarnation");
            StageAssignmentRecord sink = StageAssignmentRecord.create(
                    fence, 1L, 1, 902L, 1, 0,
                    StageAssignmentRole.SINK, "retry-manifest-worker",
                    "127.0.0.1", worker.port(), -1, worker.incarnationId());
            DurableStageAssignmentAuthority authority =
                    registry.durableStageAssignmentAuthority();
            StageAssignmentSnapshot assignments = authority.publish(
                    new StageAssignmentManifest(
                            fence, artifactId, 1L,
                            AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                            Arrays.asList(source, sink))).snapshot();
            byte[] sourceDigest = MessageDigest.getInstance("SHA-256").digest(
                    "retry-manifest-source".getBytes(StandardCharsets.UTF_8));
            byte[] sinkDigest = MessageDigest.getInstance("SHA-256").digest(
                    "retry-manifest-sink".getBytes(StandardCharsets.UTF_8));
            authority.accept(new StageAssignmentAcceptance(
                    assignments.token(0), sourceDigest, 4L));
            authority.accept(new StageAssignmentAcceptance(
                    assignments.token(1), sinkDigest, 4L));
            snapshot = accepted(registry, RunCommand.advanceAttempt(
                    "running-" + runId, runId, snapshot.revision(),
                    snapshot.currentAttempt().attemptId(),
                    snapshot.currentAttempt().fenceToken(),
                    AttemptStatus.RUNNING, false, 5L, null, null));
            authority.recordTerminal(StageTerminalReceipt.succeeded(
                    assignments.token(1), sinkDigest, 1L, 1L, 6L));

            long generation = new io.rowweave.commit.FileManifestCommitAuthority()
                    .reserve(domain, publicationId);
            ManifestFileSink prepared = new ManifestFileSink(
                    META, domain.toString(), "ledger");
            prepared.bindManifestAttempt(publicationId, generation);
            prepared.collect(row(92));
            prepared.prepareCommit();
            preparedOutputs(worker).register(
                    "manifest:" + publicationId, 902L, prepared, fence);
            assertEquals(1, worker.preparedOutputActiveCount());

            master.bindLeadership(election);
            election.start();
            long inventory = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(
                    inventory, 10, TimeUnit.SECONDS));

            RunSnapshot waiting = registry.runStateStore().get(runId);
            assertEquals(RunStatus.READY, waiting.status());
            assertEquals("RETRY_WORKER_LOST",
                    waiting.currentAttempt().reasonCode());
            assertEquals(0, worker.preparedOutputActiveCount());
            assertNull(new ManifestFileStore(domain).latest());
        }
    }

    @Test
    void terminalInFlightEvidenceNeverReopensMissingExactSource(@TempDir Path tmp)
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        Path sourceDomain = tmp.resolve("completed-source");
        long sourceGeneration = publishSourceSnapshot(sourceDomain);
        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrap(
                "definition-completed-exact-source",
                new PlanGraph(Arrays.asList(
                        PlanNode.sourceFromInputs(META, Collections.singletonList(
                                        new ManifestFileSource(META, sourceDomain.toString(),
                                                "ledger", sourceGeneration)))
                                .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                        PlanNode.sink(META, 0, new DiscardSink(META)).build()
                                .withParam(PlanNode.OPERATOR_UID_PARAM, "output"))));
        createAttemptEvidence(registry, "completed-exact-source",
                AttemptStatus.COMMITTING, true, artifact);
        Files.delete(sourceDomain.resolve("manifests")
                .resolve(sourceGeneration + ".manifest"));

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();

            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(generation, 10, TimeUnit.SECONDS));
            RunSnapshot recovered = registry.runStateStore().get("completed-exact-source");
            assertEquals(RunStatus.SUCCEEDED, recovered.status());
            assertEquals(AttemptStatus.SUCCEEDED, recovered.currentAttempt().status());
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    master.recoveryInventorySnapshot().state());
        }
    }

    @Test
    void liveExactWorkerIsCanceledAndDrainedWithoutAssignmentReplay(@TempDir java.nio.file.Path tmp)
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "live-worker", 1, "127.0.0.1", master.port(),
                     new File(tmp.toFile(), "worker"));
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            AcceptedFixture fixture = createLiveAcceptedAttempt(registry, worker);
            WorkerAssignmentLedger ledger = assignments(worker);
            WorkerAssignmentLedger.Registration registration = ledger.registerDurableSocket(
                    fixture.token.runtimeStageId(), new byte[] {1}, fixture.token.fence(),
                    fixture.token, fixture.commandDigest, null, null);

            master.bindLeadership(election);
            election.start();
            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(generation, 10, TimeUnit.SECONDS));

            RunSnapshot recovered = registry.runStateStore().get("recovered-live");
            assertEquals(RunStatus.FAILED, recovered.status());
            assertEquals(AttemptStatus.FAILED, recovered.currentAttempt().status());
            assertEquals(0, worker.runningStageCount());
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    master.recoveryInventorySnapshot().state());
        }
    }

    @Test
    void sealedManifestJournalCommitsIntentBeforeRecoveryPublishes(@TempDir java.nio.file.Path tmp)
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        java.nio.file.Path firstDomain = tmp.resolve("manifest-without-intent");
        java.nio.file.Path secondDomain = tmp.resolve("manifest-with-intent");
        createManifestEvidence(registry, "manifest-build-intent", firstDomain, false);
        createManifestEvidence(registry, "manifest-replay-intent", secondDomain, true);

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();
            long generation = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(generation, 10, TimeUnit.SECONDS));

            for (String runId : new String[] {
                    "manifest-build-intent", "manifest-replay-intent"}) {
                RunSnapshot recovered = registry.runStateStore().get(runId);
                assertEquals(RunStatus.SUCCEEDED, recovered.status(), runId);
                assertEquals(AttemptStatus.SUCCEEDED,
                        recovered.currentAttempt().status(), runId);
                io.rowweave.commit.ManifestFileStore store =
                        new io.rowweave.commit.ManifestFileStore(
                                runId.equals("manifest-build-intent")
                                        ? firstDomain : secondDomain);
                io.rowweave.commit.ManifestCommitOutcome outcome = store.queryOutcome(
                        "publication-" + runId, 1L);
                assertEquals(1L, recovered.resultSummary().rowsOut());
                assertEquals(Long.valueOf(1L),
                        recovered.resultSummary().manifestGeneration());
                assertArrayEquals(outcome.manifestHash(),
                        recovered.resultSummary().manifestSha256());
            }
            assertEquals("publication-manifest-build-intent",
                    new io.rowweave.commit.ManifestFileStore(firstDomain)
                            .exact(1L).attemptId());
            assertEquals("publication-manifest-replay-intent",
                    new io.rowweave.commit.ManifestFileStore(secondDomain)
                            .exact(1L).attemptId());
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    master.recoveryInventorySnapshot().state());
        }
    }

    @Test
    void replacementLeaderPublishesFiveMemberIntentWithoutPartialVisibility(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "manifest-five-member-recovery";
        Path domain = tmp.resolve("manifest-five-member-recovery-domain");
        byte[] expectedIdentity = createMultiManifestEvidence(
                registry, runId, domain, 5, true);

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.setManifestPublishHooks(() -> assertNull(
                    new ManifestFileStore(domain).latest(),
                    "recovery must publish the whole group at one visibility point"), null);
            master.bindLeadership(election);
            election.start();
            long inventory = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(inventory, 10, TimeUnit.SECONDS));

            RunSnapshot recovered = registry.runStateStore().get(runId);
            assertEquals(RunStatus.SUCCEEDED, recovered.status());
            assertArrayEquals(expectedIdentity,
                    recovered.resultSummary().sinkIdentityDigest());
            assertEquals(5, new ManifestFileStore(domain).latest().members().size());
            for (int member = 0; member < 5; member++) {
                try (io.rowweave.FileSource.Reader reader = new ManifestFileSource(
                        META, domain.toString(), "member-" + member).openReader()) {
                    assertEquals(40 + member, reader.next().get(0));
                    assertFalse(reader.hasNext());
                }
            }
            BatchSinkOutcome outcome = new RealClusterJobClient(
                    "127.0.0.1", master.port()).queryBatchSinkOutcome(runId);
            assertEquals(BatchSinkOutcome.State.COMMITTED, outcome.state());
            assertArrayEquals(expectedIdentity, outcome.identityDigest());
        }
    }

    @Test
    void locatorBoundManifestIntentCannotPublishWithoutAnAdmittedWorker(
            @TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        String runId = "manifest-provider-worker-required";
        Path domain = tmp.resolve("manifest-provider-worker-required-domain");
        createMultiManifestEvidence(registry, runId, domain, 1, true, true);

        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            master.bindLeadership(election);
            election.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
            while (master.recoveryInventorySnapshot().state()
                    != LeaderRecoveryInventory.State.BLOCKED_UNAVAILABLE
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertEquals(LeaderRecoveryInventory.State.BLOCKED_UNAVAILABLE,
                    master.recoveryInventorySnapshot().state(),
                    master.recoveryInventorySnapshot().failure());
            RunSnapshot unresolved = registry.runStateStore().get(runId);
            assertEquals(RunStatus.RUNNING, unresolved.status());
            assertEquals(AttemptStatus.COMMITTING,
                    unresolved.currentAttempt().status());
            assertNull(new ManifestFileStore(domain).latest());
            assertEquals(BatchSinkOutcome.State.COMMIT_DECIDED,
                    new RealClusterJobClient("127.0.0.1", master.port())
                            .queryBatchSinkOutcome(runId).state());
        }
    }

    @Test
    void replacementLeaderFinishesAfterManifestPublishAckLoss(@TempDir java.nio.file.Path tmp)
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        java.nio.file.Path domain = tmp.resolve("manifest-leader-cut");
        createManifestEvidence(registry, "manifest-leader-cut", domain, true);
        try (MasterServer first = new MasterServer(5_000L, 0, registry);
             MasterServer second = new MasterServer(5_000L, 0, registry);
             MasterElection firstElection = new MasterElection(
                     registry, new Endpoint("127.0.0.1", first.port()));
             MasterElection secondElection = new MasterElection(
                     registry, new Endpoint("127.0.0.1", second.port()))) {
            first.bindLeadership(firstElection);
            second.bindLeadership(secondElection);
            java.util.concurrent.atomic.AtomicBoolean cut =
                    new java.util.concurrent.atomic.AtomicBoolean();
            first.setManifestPublishHooks(null, () -> {
                if (cut.compareAndSet(false, true)) {
                    firstElection.close();
                    throw new IllegalStateException("injected manifest publish ACK loss");
                }
            });
            firstElection.start();
            secondElection.start();

            long leaderDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!secondElection.isLeader() && System.nanoTime() < leaderDeadline) {
                Thread.sleep(10L);
            }
            assertTrue(cut.get(), "first leader must cross the publish-before-ACK cut");
            assertTrue(secondElection.isLeader());
            // Election publishes its role before invoking the recovery-start listener.
            long inventoryDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
            long generation;
            while ((generation = second.recoveryInventoryGeneration()) == 0L
                    && System.nanoTime() < inventoryDeadline) {
                Thread.sleep(10L);
            }
            assertTrue(generation > 0L, "replacement leader must start its recovery inventory");
            assertTrue(second.awaitRecoveryInventory(generation, 10, TimeUnit.SECONDS));

            RunSnapshot recovered = registry.runStateStore().get("manifest-leader-cut");
            assertEquals(RunStatus.SUCCEEDED, recovered.status());
            assertEquals("publication-manifest-leader-cut",
                    new io.rowweave.commit.ManifestFileStore(domain)
                            .exact(1L).attemptId());
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    second.recoveryInventorySnapshot().state());
        }
    }

    @Test
    void livePreparedManifestIsInstalledSealedJournaledThenPublished(@TempDir java.nio.file.Path tmp)
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        java.nio.file.Path domain = tmp.resolve("manifest-live-prepared");
        try (MasterServer master = new MasterServer(5_000L, 0, registry);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "manifest-worker", 1, "127.0.0.1", master.port(),
                     new File(tmp.toFile(), "manifest-worker"));
             MasterElection election = new MasterElection(
                     registry, new Endpoint("127.0.0.1", master.port()))) {
            createLivePreparedManifest(registry, worker, domain);
            master.bindLeadership(election);
            election.start();
            long inventory = master.recoveryInventoryGeneration();
            assertTrue(master.awaitRecoveryInventory(inventory, 10, TimeUnit.SECONDS));

            RunSnapshot recovered = registry.runStateStore().get("manifest-live-prepared");
            assertEquals(RunStatus.SUCCEEDED, recovered.status());
            assertEquals("publication-manifest-live-prepared",
                    new io.rowweave.commit.ManifestFileStore(domain)
                            .exact(1L).attemptId());
            assertEquals(LeaderRecoveryInventory.State.EMPTY_READY,
                    master.recoveryInventorySnapshot().state());
        }
    }

    private static void createTerminalAttempt(InProcessRegistry registry, String runId,
                                              AttemptStatus target, boolean success)
            throws Exception {
        createAttemptEvidence(registry, runId, target, success);
    }

    private static void createAttemptEvidence(InProcessRegistry registry, String runId,
                                              AttemptStatus target, Boolean success)
            throws Exception {
        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrap(
                "definition-" + runId,
                new PlanGraph(Arrays.asList(
                        PlanNode.source(META, Collections.emptyList())
                                .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                        PlanNode.sink(META, 0, new DiscardSink(META)).build()
                                .withParam(PlanNode.OPERATOR_UID_PARAM, "output"))));
        createAttemptEvidence(registry, runId, target, success, artifact);
    }

    private static void createAttemptEvidence(InProcessRegistry registry, String runId,
                                              AttemptStatus target, Boolean success,
                                              PlanArtifact artifact) throws Exception {
        createAttemptEvidence(registry, runId, target, success, artifact,
                1, AttemptOutputProfile.UNKNOWN_LEGACY);
    }

    private static void createRetryableDiscardAttemptEvidence(
            InProcessRegistry registry, String runId) throws Exception {
        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrap(
                "definition-" + runId,
                new PlanGraph(Arrays.asList(
                        PlanNode.source(META, Collections.emptyList())
                                .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                        PlanNode.sink(META, 0, new DiscardSink(META)).build()
                                .withParam(PlanNode.OPERATOR_UID_PARAM, "output"))));
        createAttemptEvidence(registry, runId, AttemptStatus.RUNNING, null,
                artifact, 2, AttemptOutputProfile.DISCARD);
    }

    private static void createAttemptEvidence(
            InProcessRegistry registry, String runId,
            AttemptStatus target, Boolean success, PlanArtifact artifact,
            int maxAttempts, AttemptOutputProfile outputProfile) throws Exception {
        ArtifactRepository repository = registry.artifactRepository();
        String artifactId = repository.put(PlanArtifactCodec.encode(artifact));
        RunSnapshot snapshot = accepted(registry, RunCommand.createRun(
                "create-" + runId, runId, artifact.definitionId(), artifactId,
                maxAttempts, 0L, 1L));
        snapshot = accepted(registry, RunCommand.createAttempt(
                "attempt-" + runId, runId, snapshot.revision(),
                "attempt-" + runId, "publication-" + runId, 2L));
        snapshot = accepted(registry, RunCommand.advanceAttempt(
                "advance-" + runId + "-dispatch", runId, snapshot.revision(),
                snapshot.currentAttempt().attemptId(), snapshot.currentAttempt().fenceToken(),
                AttemptStatus.DISPATCHING, false, 3L, null, null));

        RunAttemptFence fence = new RunAttemptFence(
                runId, snapshot.currentAttempt().attemptId(),
                snapshot.currentAttempt().fenceToken());
        StageAssignmentRecord record = StageAssignmentRecord.create(
                fence, 1L, 0, 101L, 0, 0, StageAssignmentRole.WHOLE_GRAPH,
                "departed-worker", "127.0.0.1", 61_001, -1, "departed-incarnation");
        DurableStageAssignmentAuthority authority =
                registry.durableStageAssignmentAuthority();
        StageAssignmentSnapshot assignments = authority.publish(
                new StageAssignmentManifest(fence, artifactId, 1L,
                        outputProfile,
                        Collections.singletonList(record))).snapshot();
        byte[] commandDigest = MessageDigest.getInstance("SHA-256")
                .digest(("command-" + runId).getBytes(StandardCharsets.UTF_8));
        authority.accept(new StageAssignmentAcceptance(
                assignments.token(0), commandDigest, 10L));
        if (target == AttemptStatus.RUNNING || target == AttemptStatus.COMMITTING) {
            snapshot = accepted(registry, RunCommand.advanceAttempt(
                    "advance-" + runId + "-running", runId, snapshot.revision(),
                    snapshot.currentAttempt().attemptId(), snapshot.currentAttempt().fenceToken(),
                    AttemptStatus.RUNNING, false, 4L, null, null));
        }
        if (target == AttemptStatus.COMMITTING) {
            snapshot = accepted(registry, RunCommand.advanceAttempt(
                    "advance-" + runId + "-committing", runId, snapshot.revision(),
                    snapshot.currentAttempt().attemptId(), snapshot.currentAttempt().fenceToken(),
                    AttemptStatus.COMMITTING, false, 5L, null, null));
        }
        if (success == null) return;
        StageTerminalReceipt terminal = success.booleanValue()
                ? StageTerminalReceipt.succeeded(
                        assignments.token(0), commandDigest, 5L, 4L, 11L)
                : StageTerminalReceipt.failed(
                        assignments.token(0), commandDigest, 5L, 0L,
                        "STAGE_FAILED", MessageDigest.getInstance("SHA-256")
                                .digest("failure".getBytes(StandardCharsets.UTF_8)), 11L);
        authority.recordTerminal(terminal);
    }

    private static long publishSourceSnapshot(Path domain) {
        DataRow first = new DataRow(1);
        first.set(0, 7);
        DataRow second = new DataRow(1);
        second.set(0, 8);
        ExecutionEnvironment session = io.rowweave.RowWeave.createEnvironment(
                "in-flight-source-snapshot");
        session.fromSource(new CollectionSource(META, Arrays.asList(first, second)))
                .sinkTo(new ManifestFileSink(META, domain.toString(), "ledger"));
        session.execute();
        return new ManifestFileStore(domain).latest().generation();
    }

    private static RunSnapshot accepted(InProcessRegistry registry, RunCommand command) {
        RunDecision decision = registry.runStateStore().apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }

    private static AcceptedFixture createLiveAcceptedAttempt(
            InProcessRegistry registry, WorkerServer worker) throws Exception {
        String runId = "recovered-live";
        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrap(
                "definition-live",
                new PlanGraph(Arrays.asList(
                        PlanNode.source(META, Collections.emptyList())
                                .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                        PlanNode.sink(META, 0, new DiscardSink(META)).build()
                                .withParam(PlanNode.OPERATOR_UID_PARAM, "output"))));
        String artifactId = registry.artifactRepository().put(
                PlanArtifactCodec.encode(artifact));
        RunSnapshot snapshot = accepted(registry, RunCommand.createRun(
                "create-live", runId, artifact.definitionId(), artifactId,
                1, 0L, 1L));
        snapshot = accepted(registry, RunCommand.createAttempt(
                "attempt-live", runId, snapshot.revision(),
                "attempt-live", "publication-live", 2L));
        snapshot = accepted(registry, RunCommand.advanceAttempt(
                "dispatch-live", runId, snapshot.revision(), "attempt-live",
                snapshot.currentAttempt().fenceToken(), AttemptStatus.DISPATCHING,
                false, 3L, null, null));
        RunAttemptFence fence = new RunAttemptFence(
                runId, "attempt-live", snapshot.currentAttempt().fenceToken());
        StageAssignmentRecord record = StageAssignmentRecord.create(
                fence, 1L, 0, 202L, 0, 0, StageAssignmentRole.WHOLE_GRAPH,
                "live-worker", "127.0.0.1", worker.port(), -1, worker.incarnationId());
        DurableStageAssignmentAuthority authority =
                registry.durableStageAssignmentAuthority();
        StageAssignmentSnapshot assignments = authority.publish(
                new StageAssignmentManifest(fence, artifactId, 1L,
                        Collections.singletonList(record))).snapshot();
        byte[] digest = MessageDigest.getInstance("SHA-256")
                .digest("live-command".getBytes(StandardCharsets.UTF_8));
        authority.accept(new StageAssignmentAcceptance(assignments.token(0), digest, 4L));
        snapshot = accepted(registry, RunCommand.advanceAttempt(
                "running-live", runId, snapshot.revision(), "attempt-live",
                snapshot.currentAttempt().fenceToken(), AttemptStatus.RUNNING,
                false, 5L, null, null));
        return new AcceptedFixture(assignments.token(0), digest);
    }

    private static WorkerAssignmentLedger assignments(WorkerServer worker) throws Exception {
        java.lang.reflect.Field field = WorkerServer.class.getDeclaredField("assignments");
        field.setAccessible(true);
        return (WorkerAssignmentLedger) field.get(worker);
    }

    private static void createManifestEvidence(
            InProcessRegistry registry, String runId, java.nio.file.Path domain,
            boolean commitIntent) throws Exception {
        String publicationId = "publication-" + runId;
        ManifestFileSink artifactOutput = new ManifestFileSink(
                META, domain.toString(), "ledger");
        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrap(
                "definition-" + runId,
                new PlanGraph(Arrays.asList(
                        PlanNode.source(META, Collections.emptyList())
                                .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                        PlanNode.sink(META, 0, artifactOutput).build()
                                .withParam(PlanNode.OPERATOR_UID_PARAM, "output"))));
        String artifactId = registry.artifactRepository().put(
                PlanArtifactCodec.encode(artifact));
        RunSnapshot snapshot = accepted(registry, RunCommand.createRun(
                "create-" + runId, runId, artifact.definitionId(), artifactId,
                1, 0L, 1L));
        snapshot = accepted(registry, RunCommand.createAttempt(
                "attempt-" + runId, runId, snapshot.revision(),
                "attempt-" + runId, publicationId, 2L));
        snapshot = accepted(registry, RunCommand.advanceAttempt(
                "dispatch-" + runId, runId, snapshot.revision(),
                snapshot.currentAttempt().attemptId(), snapshot.currentAttempt().fenceToken(),
                AttemptStatus.DISPATCHING, false, 3L, null, null));
        RunAttemptFence fence = new RunAttemptFence(
                runId, snapshot.currentAttempt().attemptId(),
                snapshot.currentAttempt().fenceToken());
        StageAssignmentRecord record = StageAssignmentRecord.create(
                fence, 1L, 0, 301L, 0, 0, StageAssignmentRole.WHOLE_GRAPH,
                "departed-worker", "127.0.0.1", 61_003, -1, "departed-incarnation");
        DurableStageAssignmentAuthority authority =
                registry.durableStageAssignmentAuthority();
        StageAssignmentSnapshot assignment = authority.publish(
                new StageAssignmentManifest(fence, artifactId, 1L,
                        Collections.singletonList(record))).snapshot();
        byte[] digest = MessageDigest.getInstance("SHA-256")
                .digest(("command-" + runId).getBytes(StandardCharsets.UTF_8));
        authority.accept(new StageAssignmentAcceptance(assignment.token(0), digest, 4L));
        snapshot = accepted(registry, RunCommand.advanceAttempt(
                "running-" + runId, runId, snapshot.revision(),
                snapshot.currentAttempt().attemptId(), snapshot.currentAttempt().fenceToken(),
                AttemptStatus.RUNNING, false, 5L, null, null));
        authority.recordTerminal(StageTerminalReceipt.succeeded(
                assignment.token(0), digest, 1L, 1L, 6L));
        snapshot = accepted(registry, RunCommand.advanceAttempt(
                "committing-" + runId, runId, snapshot.revision(),
                snapshot.currentAttempt().attemptId(), snapshot.currentAttempt().fenceToken(),
                AttemptStatus.COMMITTING, false, 7L, null, null));

        ManifestFileSink installed = new ManifestFileSink(
                META, domain.toString(), "ledger");
        long reservedGeneration = new io.rowweave.commit.FileManifestCommitAuthority()
                .reserve(domain, publicationId);
        installed.bindManifestAttempt(publicationId, reservedGeneration);
        DataRow row = new DataRow(1);
        row.set(0, 42);
        installed.collect(row);
        installed.prepareCommit();
        installed.commitPrepared();
        installed.sealInstalledMember();
        io.rowweave.run.ManifestMemberReceipt member =
                io.rowweave.run.ManifestMemberReceipt.sealed(
                        assignment.token(0), digest, installed.domainId(), reservedGeneration,
                        publicationId, installed.installedMember(), 8L);
        authority.recordManifestMember(member);
        if (commitIntent) {
            io.rowweave.run.ManifestCommitJournalSnapshot journal =
                    authority.manifestCommitJournal(fence);
            authority.commitManifestIntent(journal.buildIntent());
        }
    }

    private static RunAttemptFence createProviderTerminalWithoutLocator(
            InProcessRegistry registry, String runId, Path domain)
            throws Exception {
        String publicationId = "publication-" + runId;
        PlanGraph graph = new PlanGraph(Arrays.asList(
                DurableTestSources.source(META, Collections.emptyList(),
                                "provider-terminal-source")
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new ManifestFileSink(
                                META, domain.toString(), "ledger"))
                        .build().withParam(
                                PlanNode.OPERATOR_UID_PARAM, "output")), 1);
        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrapDurable(
                "definition-" + runId, graph,
                io.rowweave.plan.ObjectTrustContext.loopbackCluster(),
                ignored -> { });
        String artifactId = registry.artifactRepository().put(
                PlanArtifactCodec.encode(artifact));
        RunSnapshot snapshot = accepted(registry, RunCommand.createRun(
                "create-" + runId, runId, artifact.definitionId(), artifactId,
                1, 0L, 1L));
        snapshot = accepted(registry, RunCommand.createAttempt(
                "attempt-" + runId, runId, snapshot.revision(),
                "attempt-" + runId, publicationId, 2L));
        snapshot = accepted(registry, RunCommand.advanceAttempt(
                "dispatch-" + runId, runId, snapshot.revision(),
                snapshot.currentAttempt().attemptId(),
                snapshot.currentAttempt().fenceToken(),
                AttemptStatus.DISPATCHING, false, 3L, null, null));
        RunAttemptFence fence = new RunAttemptFence(
                runId, snapshot.currentAttempt().attemptId(),
                snapshot.currentAttempt().fenceToken());
        StageAssignmentRecord record = StageAssignmentRecord.create(
                fence, 1L, 0, 901L, 0, 0,
                StageAssignmentRole.WHOLE_GRAPH,
                "departed-provider-worker", "127.0.0.1", 64_001,
                -1, "departed-provider-incarnation");
        DurableStageAssignmentAuthority authority =
                registry.durableStageAssignmentAuthority();
        StageAssignmentSnapshot assignments = authority.publish(
                new StageAssignmentManifest(fence, artifactId, 1L,
                        Collections.singletonList(record))).snapshot();
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                ("command-" + runId).getBytes(StandardCharsets.UTF_8));
        authority.accept(new StageAssignmentAcceptance(
                assignments.token(0), digest, 4L));
        snapshot = accepted(registry, RunCommand.advanceAttempt(
                "running-" + runId, runId, snapshot.revision(),
                snapshot.currentAttempt().attemptId(),
                snapshot.currentAttempt().fenceToken(),
                AttemptStatus.RUNNING, false, 5L, null, null));
        authority.recordTerminal(StageTerminalReceipt.succeeded(
                assignments.token(0), digest, 0L, 0L, 6L));
        accepted(registry, RunCommand.advanceAttempt(
                "committing-" + runId, runId, snapshot.revision(),
                snapshot.currentAttempt().attemptId(),
                snapshot.currentAttempt().fenceToken(),
                AttemptStatus.COMMITTING, false, 7L, null, null));
        new io.rowweave.commit.FileManifestCommitAuthority()
                .reserve(domain, publicationId);
        return fence;
    }

    private static byte[] createMultiManifestEvidence(
            InProcessRegistry registry, String runId, Path domain,
            int sinkCount, boolean commitIntent) throws Exception {
        return createMultiManifestEvidence(
                registry, runId, domain, sinkCount, commitIntent, false);
    }

    private static byte[] createMultiManifestEvidence(
            InProcessRegistry registry, String runId, Path domain,
            int sinkCount, boolean commitIntent,
            boolean providerBound) throws Exception {
        String publicationId = "publication-" + runId;
        List<PlanNode> nodes = new ArrayList<>();
        nodes.add(PlanNode.source(META, Collections.emptyList())
                .withParam(PlanNode.OPERATOR_UID_PARAM, "source"));
        for (int member = 0; member < sinkCount; member++) {
            PlanNode sink = PlanNode.sink(META, 0, new ManifestFileSink(
                            META, domain.toString(), "member-" + member))
                    .build().withParam(
                            PlanNode.OPERATOR_UID_PARAM, "output-" + member);
            if (providerBound) {
                sink = sink.withParam(
                        PlanGraph.DURABLE_PROVIDER_BINDING_PARAM, "true");
            }
            nodes.add(sink);
        }
        PlanGraph graph = new PlanGraph(nodes, 1);
        PlanArtifact artifact = providerBound
                ? LegacyPlanArtifactAdapter.wrapDurable(
                "definition-" + runId, graph,
                io.rowweave.plan.ObjectTrustContext.loopbackCluster(),
                ignored -> { })
                : LegacyPlanArtifactAdapter.wrap(
                "definition-" + runId, graph);
        String artifactId = registry.artifactRepository().put(
                PlanArtifactCodec.encode(artifact));
        RunSnapshot snapshot = accepted(registry, RunCommand.createRun(
                "create-" + runId, runId, artifact.definitionId(), artifactId,
                1, 0L, 1L));
        snapshot = accepted(registry, RunCommand.createAttempt(
                "attempt-" + runId, runId, snapshot.revision(),
                "attempt-" + runId, publicationId, 2L));
        snapshot = accepted(registry, RunCommand.advanceAttempt(
                "dispatch-" + runId, runId, snapshot.revision(),
                snapshot.currentAttempt().attemptId(), snapshot.currentAttempt().fenceToken(),
                AttemptStatus.DISPATCHING, false, 3L, null, null));
        RunAttemptFence fence = new RunAttemptFence(
                runId, snapshot.currentAttempt().attemptId(),
                snapshot.currentAttempt().fenceToken());

        ExecutionGraph execution = ExecutionGraphBuilder.buildWithSinkBoundary(graph);
        List<StageAssignmentRecord> records = new ArrayList<>();
        List<ManifestFileSink> outputs = new ArrayList<>();
        for (Stage stage : execution.stages()) {
            if (!stage.isSink()) continue;
            for (int nodeIndex : stage.members()) {
                PlanNode node = graph.node(nodeIndex);
                if (node.kind() != PlanNode.Kind.SINK) continue;
                int ordinal = records.size();
                records.add(StageAssignmentRecord.create(
                        fence, 1L, ordinal, 500L + ordinal,
                        stage.index(), 0, StageAssignmentRole.SINK,
                        "departed-worker-" + ordinal, "127.0.0.1",
                        62_000 + ordinal, -1, "departed-incarnation-" + ordinal));
                outputs.add((ManifestFileSink) node.output());
            }
        }
        assertEquals(sinkCount, records.size());
        DurableStageAssignmentAuthority authority =
                registry.durableStageAssignmentAuthority();
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, artifactId, 1L, records);
        StageAssignmentSnapshot assignments = authority.publish(manifest).snapshot();
        List<byte[]> commandDigests = new ArrayList<>();
        for (int ordinal = 0; ordinal < sinkCount; ordinal++) {
            byte[] commandDigest = MessageDigest.getInstance("SHA-256").digest(
                    ("command-" + runId + "-" + ordinal)
                            .getBytes(StandardCharsets.UTF_8));
            commandDigests.add(commandDigest);
            authority.accept(new StageAssignmentAcceptance(
                    assignments.token(ordinal), commandDigest, 4L));
        }
        snapshot = accepted(registry, RunCommand.advanceAttempt(
                "running-" + runId, runId, snapshot.revision(),
                snapshot.currentAttempt().attemptId(), snapshot.currentAttempt().fenceToken(),
                AttemptStatus.RUNNING, false, 5L, null, null));
        for (int ordinal = 0; ordinal < sinkCount; ordinal++) {
            authority.recordTerminal(StageTerminalReceipt.succeeded(
                    assignments.token(ordinal), commandDigests.get(ordinal),
                    1L, 1L, 6L));
        }
        accepted(registry, RunCommand.advanceAttempt(
                "committing-" + runId, runId, snapshot.revision(),
                snapshot.currentAttempt().attemptId(), snapshot.currentAttempt().fenceToken(),
                AttemptStatus.COMMITTING, false, 7L, null, null));

        long generation = new io.rowweave.commit.FileManifestCommitAuthority()
                .reserve(domain, publicationId);
        MessageDigest groupIdentity = MessageDigest.getInstance("SHA-256");
        for (int ordinal = 0; ordinal < sinkCount; ordinal++) {
            ManifestFileSink installed = new ManifestFileSink(
                    META, domain.toString(), outputs.get(ordinal).memberName());
            installed.bindManifestAttempt(publicationId, generation);
            DataRow row = new DataRow(1);
            row.set(0, 40 + ordinal);
            installed.collect(row);
            installed.prepareCommit();
            installed.commitPrepared();
            installed.sealInstalledMember();
            io.rowweave.run.SinkAttemptIdentity identity =
                    DurableStageAssignmentPlan.sinkAttemptIdentity(
                            manifest, records.get(ordinal), graph);
            groupIdentity.update(identity.identityDigest());
            io.rowweave.commit.CommitManifest.Member member =
                    installed.installedMember();
            long sealedAt = 8L + ordinal;
            io.rowweave.run.PreparedSinkLocator locator =
                    new io.rowweave.run.PreparedSinkLocator(
                            io.rowweave.run.PreparedSinkLocator.CURRENT_VERSION,
                            identity,
                            io.rowweave.BatchSinkDescriptor.CapabilityProfile
                                    .MANIFEST_ATOMIC_GROUP_V1,
                            installed.domainId(),
                            io.rowweave.commit.ManifestCommitWire.encodeMember(member),
                            member.length(), member.sha256(), sealedAt);
            if (providerBound) {
                authority.recordPreparedSink(
                        new io.rowweave.run.PreparedSinkReceipt(
                                assignments.token(ordinal),
                                commandDigests.get(ordinal), locator));
            }
            authority.recordManifestMember(
                    io.rowweave.run.ManifestMemberReceipt.sealed(
                            assignments.token(ordinal), commandDigests.get(ordinal),
                            installed.domainId(), generation, publicationId,
                            member, sealedAt, locator));
        }
        if (commitIntent) {
            io.rowweave.run.ManifestCommitJournalSnapshot journal =
                    authority.manifestCommitJournal(fence);
            authority.commitManifestIntent(journal.buildIntent());
        }
        return groupIdentity.digest();
    }

    private static void createLivePreparedManifest(
            InProcessRegistry registry, WorkerServer worker,
            java.nio.file.Path domain) throws Exception {
        String runId = "manifest-live-prepared";
        String publicationId = "publication-" + runId;
        ManifestFileSink artifactOutput = new ManifestFileSink(
                META, domain.toString(), "ledger");
        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrap(
                "definition-" + runId,
                new PlanGraph(Arrays.asList(
                        PlanNode.source(META, Collections.emptyList())
                                .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                        PlanNode.sink(META, 0, artifactOutput).build()
                                .withParam(PlanNode.OPERATOR_UID_PARAM, "output"))));
        String artifactId = registry.artifactRepository().put(
                PlanArtifactCodec.encode(artifact));
        RunSnapshot snapshot = accepted(registry, RunCommand.createRun(
                "create-" + runId, runId, artifact.definitionId(), artifactId,
                1, 0L, 1L));
        snapshot = accepted(registry, RunCommand.createAttempt(
                "attempt-" + runId, runId, snapshot.revision(),
                "attempt-" + runId, publicationId, 2L));
        snapshot = accepted(registry, RunCommand.advanceAttempt(
                "dispatch-" + runId, runId, snapshot.revision(),
                snapshot.currentAttempt().attemptId(), snapshot.currentAttempt().fenceToken(),
                AttemptStatus.DISPATCHING, false, 3L, null, null));
        RunAttemptFence fence = new RunAttemptFence(
                runId, snapshot.currentAttempt().attemptId(),
                snapshot.currentAttempt().fenceToken());
        StageAssignmentRecord record = StageAssignmentRecord.create(
                fence, 1L, 0, 401L, 0, 0, StageAssignmentRole.WHOLE_GRAPH,
                "manifest-worker", "127.0.0.1", worker.port(), -1,
                worker.incarnationId());
        DurableStageAssignmentAuthority authority =
                registry.durableStageAssignmentAuthority();
        StageAssignmentSnapshot assignment = authority.publish(
                new StageAssignmentManifest(fence, artifactId, 1L,
                        Collections.singletonList(record))).snapshot();
        byte[] digest = MessageDigest.getInstance("SHA-256")
                .digest("manifest-live-command".getBytes(StandardCharsets.UTF_8));
        authority.accept(new StageAssignmentAcceptance(assignment.token(0), digest, 4L));
        snapshot = accepted(registry, RunCommand.advanceAttempt(
                "running-" + runId, runId, snapshot.revision(),
                snapshot.currentAttempt().attemptId(), snapshot.currentAttempt().fenceToken(),
                AttemptStatus.RUNNING, false, 5L, null, null));
        authority.recordTerminal(StageTerminalReceipt.succeeded(
                assignment.token(0), digest, 1L, 1L, 6L));
        accepted(registry, RunCommand.advanceAttempt(
                "committing-" + runId, runId, snapshot.revision(),
                snapshot.currentAttempt().attemptId(), snapshot.currentAttempt().fenceToken(),
                AttemptStatus.COMMITTING, false, 7L, null, null));

        long generation = new io.rowweave.commit.FileManifestCommitAuthority()
                .reserve(domain, publicationId);
        ManifestFileSink prepared = new ManifestFileSink(
                META, domain.toString(), "ledger");
        prepared.bindManifestAttempt(publicationId, generation);
        DataRow row = new DataRow(1);
        row.set(0, 88);
        prepared.collect(row);
        prepared.prepareCommit();
        preparedOutputs(worker).register(
                "manifest:" + publicationId, 401L, prepared, fence);
    }

    private static QualityRecoveryFixture createQualityManifestEvidence(
            InProcessRegistry registry, String runId, Path domain,
            QualityGatePolicy policy, boolean recordDecision) throws Exception {
        return createQualityManifestEvidence(
                registry, runId, domain, policy, policy,
                recordDecision, false);
    }

    private static QualityRecoveryFixture createQualityManifestEvidence(
            InProcessRegistry registry, String runId, Path domain,
            QualityGatePolicy artifactPolicy, QualityGatePolicy rootPolicy,
            boolean recordDecision, boolean commitIntent) throws Exception {
        String definitionId = "definition-" + runId;
        String publicationId = "publication-" + runId;
        Path seedDomain = domain.resolveSibling(domain.getFileName() + "-seed");
        new io.rowweave.local.LocalJobClient().submit(
                qualityArtifact(definitionId + "-seed", seedDomain,
                        QualityGatePolicy.allowAll()));

        PlanArtifact artifact = qualityArtifact(
                definitionId, domain, artifactPolicy);
        PlanGraph graph = LegacyPlanArtifactAdapter.unwrapValidated(artifact);
        String artifactId = registry.artifactRepository().put(
                PlanArtifactCodec.encode(artifact));
        RunSnapshot snapshot = accepted(registry, RunCommand.createRun(
                "create-" + runId, runId, definitionId, artifactId,
                1, 0L, 1L));
        snapshot = accepted(registry, RunCommand.createAttempt(
                "attempt-" + runId, runId, snapshot.revision(),
                "attempt-" + runId, publicationId, 2L));
        snapshot = accepted(registry, RunCommand.advanceAttempt(
                "dispatch-" + runId, runId, snapshot.revision(),
                snapshot.currentAttempt().attemptId(),
                snapshot.currentAttempt().fenceToken(),
                AttemptStatus.DISPATCHING, false, 3L, null, null));
        RunAttemptFence fence = new RunAttemptFence(
                runId, snapshot.currentAttempt().attemptId(),
                snapshot.currentAttempt().fenceToken());

        ExecutionGraph execution = ExecutionGraphBuilder.buildWithSinkBoundary(graph);
        List<StageAssignmentRecord> records = new ArrayList<>();
        List<QualityGateAssignmentBinding.MemberBinding> bindings =
                new ArrayList<>();
        List<ManifestFileSink> outputs = new ArrayList<>();
        for (String role : QualityGateAssignmentBinding.canonicalRoles()) {
            int sinkNode = qualitySinkNode(graph, role);
            Stage stage = stageContaining(execution, sinkNode);
            int ordinal = records.size();
            StageAssignmentRecord record = StageAssignmentRecord.create(
                    fence, 1L, ordinal, 800L + ordinal,
                    stage.index(), 0, StageAssignmentRole.SINK,
                    "departed-quality-worker-" + ordinal, "127.0.0.1",
                    63_000 + ordinal, -1,
                    "departed-quality-incarnation-" + ordinal);
            records.add(record);
            ManifestFileSink output = (ManifestFileSink)
                    graph.node(sinkNode).output();
            outputs.add(output);
            bindings.add(new QualityGateAssignmentBinding.MemberBinding(
                    ordinal, role,
                    io.rowweave.BatchSinkDescriptorCodec.digest(
                            graph.node(sinkNode).batchSinkDescriptor())));
        }
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, artifactId, 1L,
                AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1, records,
                new QualityGateAssignmentBinding(
                        io.rowweave.QualityGatePolicyCodec.digest(rootPolicy),
                        bindings));
        DurableStageAssignmentAuthority authority =
                registry.durableStageAssignmentAuthority();
        StageAssignmentSnapshot assignments = authority.publish(manifest).snapshot();
        List<byte[]> commandDigests = new ArrayList<>();
        for (int ordinal = 0; ordinal < records.size(); ordinal++) {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    ("quality-command-" + runId + '-' + ordinal)
                            .getBytes(StandardCharsets.UTF_8));
            commandDigests.add(digest);
            authority.accept(new StageAssignmentAcceptance(
                    assignments.token(ordinal), digest, 4L));
        }
        snapshot = accepted(registry, RunCommand.advanceAttempt(
                "running-" + runId, runId, snapshot.revision(),
                snapshot.currentAttempt().attemptId(),
                snapshot.currentAttempt().fenceToken(),
                AttemptStatus.RUNNING, false, 5L, null, null));
        for (int ordinal = 0; ordinal < records.size(); ordinal++) {
            authority.recordTerminal(StageTerminalReceipt.succeeded(
                    assignments.token(ordinal), commandDigests.get(ordinal),
                    1L, 1L, 6L));
        }
        accepted(registry, RunCommand.advanceAttempt(
                "committing-" + runId, runId, snapshot.revision(),
                snapshot.currentAttempt().attemptId(),
                snapshot.currentAttempt().fenceToken(),
                AttemptStatus.COMMITTING, false, 7L, null, null));

        long generation = new io.rowweave.commit.FileManifestCommitAuthority()
                .reserve(domain, publicationId);
        for (int ordinal = 0; ordinal < outputs.size(); ordinal++) {
            ManifestFileSink output = outputs.get(ordinal);
            output.bindManifestAttempt(publicationId, generation);
            try (io.rowweave.FileSource.Reader reader = new ManifestFileSource(
                    output.getSchema(), seedDomain.toString(),
                    output.memberName()).openReader()) {
                while (reader.hasNext()) output.collect(reader.next());
            }
            output.prepareCommit();
            output.commitPrepared();
            output.sealInstalledMember();
            io.rowweave.commit.CommitManifest.Member member =
                    output.installedMember();
            io.rowweave.run.SinkAttemptIdentity identity =
                    DurableStageAssignmentPlan.sinkAttemptIdentity(
                            manifest, records.get(ordinal), graph);
            long sealedAt = 8L + ordinal;
            PreparedSinkLocator locator = new PreparedSinkLocator(
                    PreparedSinkLocator.CURRENT_VERSION, identity,
                    io.rowweave.BatchSinkDescriptor.CapabilityProfile
                            .MANIFEST_ATOMIC_GROUP_V1,
                    output.domainId(), ManifestCommitWire.encodeMember(member),
                    member.length(), member.sha256(), sealedAt);
            authority.recordManifestMember(ManifestMemberReceipt.sealed(
                    assignments.token(ordinal), commandDigests.get(ordinal),
                    output.domainId(), generation, publicationId,
                    member, sealedAt, locator));
        }
        if (recordDecision) {
            io.rowweave.run.ManifestCommitJournalSnapshot journal =
                    authority.manifestCommitJournal(fence);
            QualityPreparedEvidence evidence = QualityPreparedEvidenceReader.read(
                    graph, journal, QualityEvidenceReadGuard.until(
                            System.nanoTime() + TimeUnit.SECONDS.toNanos(5L),
                            () -> false));
            QualityGateDecision decision = QualityGateDecision.evaluate(
                    artifactId, fence, publicationId, rootPolicy, evidence);
            authority.recordQualityGateDecision(decision);
        }
        if (commitIntent) {
            authority.commitManifestIntent(
                    authority.manifestCommitJournal(fence).buildIntent());
        }
        return new QualityRecoveryFixture(fence);
    }

    private static PlanArtifact qualityArtifact(
            String definitionId, Path domain, QualityGatePolicy policy) {
        ExecutionEnvironment session = new ExecutionEnvironment("quality-recovery", "quality recovery");
        session.setDefinitionId(definitionId);
        session.setParallelism(2);
        DataRowClassification classified = session.fromSource(new CollectionSource(
                        QUALITY_META, "quality-recovery-split", Arrays.asList(
                        qualityRow("A", 1), qualityRow(null, 2),
                        qualityRow("B", 13))))
                .uid("ledger.source")
                .classifyRowsWithSourcePosition(RowValidation.builder(QUALITY_META)
                        .notNull("adaptor.required", 0)
                        .integerRange("period.range", 1, 1, 12)
                        .build());
        classified.writeToManifest(TypedQualityManifest.of(
                domain.toString(), "valid", "rejected", "summary")
                .withQualityGate(policy));
        return session.toPlanArtifact();
    }

    private static DataRow qualityRow(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }

    private static int qualitySinkNode(PlanGraph graph, String role) {
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() == PlanNode.Kind.SINK
                    && node.output() instanceof ManifestFileSink
                    && role.equals(((ManifestFileSink) node.output())
                    .qualityGateRole())) return i;
        }
        throw new AssertionError("missing quality Sink role " + role);
    }

    private static Stage stageContaining(ExecutionGraph graph, int nodeIndex) {
        for (Stage stage : graph.stages()) {
            if (stage.members().contains(nodeIndex)) return stage;
        }
        throw new AssertionError("missing stage for node " + nodeIndex);
    }

    private static PreparedOutputRegistry preparedOutputs(WorkerServer worker) throws Exception {
        java.lang.reflect.Field field = WorkerServer.class.getDeclaredField("preparedOutputs");
        field.setAccessible(true);
        return (PreparedOutputRegistry) field.get(worker);
    }

    private static DataRow row(int value) {
        DataRow row = new DataRow(1);
        row.set(0, value);
        return row;
    }

    private static final class AcceptedFixture {
        final io.rowweave.run.StageAssignmentToken token;
        final byte[] commandDigest;

        AcceptedFixture(io.rowweave.run.StageAssignmentToken token, byte[] commandDigest) {
            this.token = token;
            this.commandDigest = commandDigest;
        }
    }

    private static final class QualityRecoveryFixture {
        final RunAttemptFence fence;

        QualityRecoveryFixture(RunAttemptFence fence) {
            this.fence = fence;
        }
    }
}
