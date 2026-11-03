package io.rowweave.cluster.registry.zk;

import io.rowweave.run.AttemptStatus;
import io.rowweave.run.CancellationCause;
import io.rowweave.run.DurableExecutionEvidenceView;
import io.rowweave.run.ExecutionEvent;
import io.rowweave.run.ManifestCommitJournalSnapshot;
import io.rowweave.run.RetentionOwner;
import io.rowweave.run.RetentionToken;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunLifecycleRef;
import io.rowweave.run.RunPurgeProgress;
import io.rowweave.run.RunRetentionClaim;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStateStoreUnavailableException;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageManifestPublication;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** multi-read views must never combine Run incarnations. */
class ZkRunIncarnationReadCoherenceTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void replacementPrepublicationIsNotAnOrphanOfThePriorRoot()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            for (StableOrphanReadCase readCase : StableOrphanReadCase.values()) {
                String runId = "read-prepublication-"
                        + readCase.name().toLowerCase(Locale.ROOT);
                ActiveRun original = active(
                        writer, runId, "definition-a", "worker-a");
                AtomicBoolean replaced = new AtomicBoolean();
                CountDownLatch layoutReady = new CountDownLatch(1);
                CountDownLatch allowCreate = new CountDownLatch(1);
                AtomicReference<Throwable> createFailure = new AtomicReference<>();
                AtomicReference<Thread> creator = new AtomicReference<>();
                reader.evidenceReadObserverForTest(phase -> {
                    if (phase != readCase.transitionPhase
                            || !replaced.compareAndSet(false, true)) return;
                    RunSnapshot terminal = accepted(writer,
                            RunCommand.advanceAttempt(
                                    "terminal-a", runId,
                                    original.snapshot.revision(),
                                    original.fence.attemptId(),
                                    original.fence.fenceToken(),
                                    AttemptStatus.FAILED, false, 3L,
                                    "FAILED", "replace with prepublication"));
                    purgeAll(writer, claim(writer, terminal));
                    AtomicBoolean paused = new AtomicBoolean();
                    writer.evidenceReadObserverForTest(createPhase -> {
                        if (createPhase == ZkRunStateStore.EvidenceReadPhase
                                .APPLY_AFTER_INITIAL_SNAPSHOT
                                && paused.compareAndSet(false, true)) {
                            layoutReady.countDown();
                            if (!allowCreate.await(10, TimeUnit.SECONDS)) {
                                throw new AssertionError(
                                        "replacement CREATE release timed out");
                            }
                        }
                    });
                    Thread thread = new Thread(() -> {
                        try {
                            accepted(writer, RunCommand.createRun(
                                    "create-b", runId, "definition-b",
                                    "shared-artifact", 2, 0L, 4L));
                        } catch (Throwable failure) {
                            createFailure.set(failure);
                        }
                    }, "zk-read-prepublication-" + readCase);
                    creator.set(thread);
                    thread.start();
                    assertTrue(layoutReady.await(10, TimeUnit.SECONDS));
                });

                Object result;
                try {
                    result = readCase.read(reader, original);
                } finally {
                    allowCreate.countDown();
                    Thread thread = creator.get();
                    if (thread != null) thread.join(10_000L);
                    reader.evidenceReadObserverForTest(phase -> { });
                    writer.evidenceReadObserverForTest(phase -> { });
                }
                assertTrue(replaced.get());
                assertNull(createFailure.get());
                if (result instanceof DurableExecutionEvidenceView) {
                    assertEquals(DurableExecutionEvidenceView.State.MISSING,
                            ((DurableExecutionEvidenceView) result).state());
                } else if (result instanceof List) {
                    assertEquals(Collections.emptyList(), result);
                } else {
                    assertNull(result);
                }
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void rootAbsenceCannotHideAStableLifecycleOrphan() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            for (StableOrphanReadCase readCase : StableOrphanReadCase.values()) {
                String runId = "root-absent-lifecycle-"
                        + readCase.name().toLowerCase(Locale.ROOT);
                ActiveRun active = active(writer, runId, "definition", "worker");
                second.delete().deletingChildrenIfNeeded().forPath(runPath(runId));
                Throwable failure = assertThrows(IllegalStateException.class,
                        () -> readCase.read(reader, active));
                assertTrue(!(failure instanceof RunStateStoreUnavailableException));
                assertTrue(failure.getMessage().contains(runId));
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void snapshotOnlyFallbackConfirmsTheOriginalIncarnation() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            ActiveRun original = active(writer,
                    "snapshot-only-fence", "definition-a", "worker-a");
            AtomicBoolean replaced = new AtomicBoolean();
            reader.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase.BEFORE_ASSIGNMENT
                        && replaced.compareAndSet(false, true)) {
                    replace(writer, original, "definition-b", "worker-b");
                    throw new KeeperException.ConnectionLossException();
                }
            });

            assertThrows(RunStateStoreUnavailableException.class,
                    () -> reader.executionEvidence(original.snapshot.runId()));
            assertTrue(replaced.get());
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void stableSingleSidedLifecycleShardIsCorruption() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            String runId = "single-sided-lifecycle-shard";
            active(writer, runId, "definition", "worker");
            String entries = lifecycleGuardPath(runId) + "/entries";
            second.delete().deletingChildrenIfNeeded().forPath(entries);

            Throwable failure = assertThrows(IllegalStateException.class,
                    () -> reader.get(runId));
            assertTrue(!(failure instanceof RunStateStoreUnavailableException));
            assertTrue(failure.getMessage().contains(runId));
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void assignmentReadersRetrySameRootMutationBetweenAuthorityShards()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            for (boolean journalRead : new boolean[] {false, true}) {
                ZkRunStateStore reader = new ZkRunStateStore(first);
                ZkRunStateStore writer = new ZkRunStateStore(second);
                String runId = "same-root-authority-race-"
                        + (journalRead ? "journal" : "assignment");
                ActiveRun active = active(
                        writer, runId, "definition", "worker");
                AtomicBoolean mutated = new AtomicBoolean();
                reader.evidenceReadObserverForTest(phase -> {
                    if (phase == ZkRunStateStore.EvidenceReadPhase
                            .AUTHORITY_AFTER_SNAPSHOT
                            && mutated.compareAndSet(false, true)) {
                        accepted(writer, RunCommand.advanceAttempt(
                                "dispatch", runId,
                                active.snapshot.revision(),
                                active.fence.attemptId(),
                                active.fence.fenceToken(),
                                AttemptStatus.DISPATCHING, false,
                                3L, null, null));
                    }
                });

                Object result = journalRead
                        ? reader.manifestCommitJournal(active.fence)
                        : reader.assignmentSnapshot(active.fence);

                assertTrue(mutated.get());
                assertNotNull(result,
                        "same-root authority races must be retried");
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void firstCreateRetriesWhenLifecycleGuardAppearsMidObservation()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            String runId = "guard-appears-target";
            String collateral = sameLifecycleShardRunId(
                    runId, "guard-appears-collateral-");
            AtomicInteger observations = new AtomicInteger();
            AtomicBoolean changed = new AtomicBoolean();
            reader.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase
                        .LAYOUT_AFTER_ROOT_STAT) {
                    observations.incrementAndGet();
                    if (changed.compareAndSet(false, true)) {
                        accepted(writer, RunCommand.createRun(
                                "create-collateral", collateral,
                                "definition-c", "artifact-c", 1, 0L, 1L));
                    }
                }
            });

            RunDecision created = reader.apply(RunCommand.createRun(
                    "create-target", runId,
                    "definition-a", "artifact-a", 1, 0L, 1L));

            assertEquals(RunDecision.Code.ACCEPTED, created.code());
            assertTrue(changed.get());
            assertTrue(observations.get() >= 2,
                    "guard appearance must discard and reread the prepublication view");
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void unrelatedSameShardGuardChangeRetriesStableTargetRoot()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            String runId = "guard-change-target";
            active(writer, runId, "definition-a", "worker-a");
            String collateral = sameLifecycleShardRunId(
                    runId, "guard-change-collateral-");
            AtomicInteger observations = new AtomicInteger();
            AtomicBoolean changed = new AtomicBoolean();
            reader.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase
                        .LAYOUT_AFTER_ROOT_STAT) {
                    observations.incrementAndGet();
                    if (changed.compareAndSet(false, true)) {
                        accepted(writer, RunCommand.createRun(
                                "create-collateral", collateral,
                                "definition-c", "artifact-c", 1, 0L, 1L));
                    }
                }
            });

            RunSnapshot snapshot = reader.get(runId);

            assertEquals("definition-a", snapshot.definitionId());
            assertTrue(changed.get());
            assertTrue(observations.get() >= 2,
                    "a stable target root does not make a changed guard reusable");
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void lifecycleGuardDisappearanceDiscardsStableTargetRootObservation()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            String runId = "guard-disappears-target";
            active(writer, runId, "definition-a", "worker-a");
            String guard = lifecycleGuardPath(runId);
            AtomicInteger observations = new AtomicInteger();
            AtomicBoolean removed = new AtomicBoolean();
            reader.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase
                        .LAYOUT_AFTER_ROOT_STAT) {
                    observations.incrementAndGet();
                    if (removed.compareAndSet(false, true)) {
                        second.delete().forPath(lifecyclePath(runId));
                        second.delete().forPath(guard + "/entries");
                        second.delete().forPath(guard);
                    }
                }
            });

            assertThrows(IllegalStateException.class,
                    () -> reader.executionEvidence(runId));

            assertTrue(removed.get());
            assertTrue(observations.get() >= 2,
                    "guard disappearance must discard and reread the root view");
            assertNotNull(second.checkExists().forPath(
                            runPath(runId) + "/snapshot"),
                    "the corruption probe must not mutate the Run snapshot");
        }
    }

    @RepeatedTest(16)
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void getCannotReturnOriginalSnapshotAfterReplacementLifecycle() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            ActiveRun original = active(
                    writer, "get-incarnation", "definition-a", "worker-a");
            AtomicBoolean replaced = new AtomicBoolean();
            reader.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase
                        .EVIDENCE_AFTER_INITIAL_SNAPSHOT
                        && replaced.compareAndSet(false, true)) {
                    replace(writer, original, "definition-b", "worker-b");
                }
            });

            RunSnapshot snapshot = reader.get(original.snapshot.runId());

            assertTrue(replaced.get());
            assertEquals("definition-b", snapshot.definitionId(),
                    "get must linearize to one replacement incarnation");
        }
    }

    @RepeatedTest(16)
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void eventsCannotCombineOriginalChildrenWithReplacementSnapshot()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            ActiveRun original = active(
                    writer, "events-incarnation", "definition-a", "worker-a");
            AtomicBoolean replaced = new AtomicBoolean();
            reader.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase
                        .EVENTS_AFTER_CHILDREN
                        && replaced.compareAndSet(false, true)) {
                    ActiveRun replacement = replace(
                            writer, original, "definition-b", "worker-b");
                    accepted(writer, RunCommand.requestCancellation(
                            "cancel-b", replacement.snapshot.runId(),
                            replacement.snapshot.revision(),
                            replacement.fence.attemptId(),
                            replacement.fence.fenceToken(),
                            CancellationCause.USER, 3L,
                            "TEST", "replacement event"));
                }
            });

            List<ExecutionEvent> events = reader.events(
                    original.snapshot.runId(), 0L, 10);

            assertTrue(replaced.get());
            assertEquals(3, events.size(),
                    "events must retry the replacement incarnation as one page");
            assertEquals("cancel-b", events.get(2).commandId());
        }
    }

    @RepeatedTest(16)
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void assignmentSnapshotLinearizesToReplacementManifest() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            ActiveRun original = active(writer,
                    "assignment-read-incarnation", "definition-a", "worker-a");
            AtomicBoolean replaced = new AtomicBoolean();
            reader.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase
                        .EVIDENCE_AFTER_INITIAL_SNAPSHOT
                        && replaced.compareAndSet(false, true)) {
                    replace(writer, original, "definition-b", "worker-b");
                }
            });

            StageAssignmentSnapshot snapshot =
                    reader.assignmentSnapshot(original.fence);

            assertTrue(replaced.get());
            assertNotNull(snapshot);
            assertEquals("worker-b",
                    snapshot.manifest().assignment(0).workerId());
        }
    }

    @RepeatedTest(16)
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void manifestJournalLinearizesToReplacementManifest() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            ActiveRun original = active(writer,
                    "journal-read-incarnation", "definition-a", "worker-a");
            AtomicBoolean replaced = new AtomicBoolean();
            reader.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase
                        .EVIDENCE_AFTER_INITIAL_SNAPSHOT
                        && replaced.compareAndSet(false, true)) {
                    replace(writer, original, "definition-b", "worker-b");
                }
            });

            ManifestCommitJournalSnapshot journal =
                    reader.manifestCommitJournal(original.fence);

            assertTrue(replaced.get());
            assertNotNull(journal);
            assertEquals("worker-b", journal.assignments().manifest()
                    .assignment(0).workerId());
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void firstCreateAllowsPrepublicationWithUninitializedLifecycleShard()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore creator = new ZkRunStateStore(first);
            new ZkRunStateStore(second);
            String runId = "first-prepublication";
            String guard = lifecycleGuardPath(runId);
            AtomicBoolean observed = new AtomicBoolean();
            creator.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase
                        .APPLY_AFTER_INITIAL_SNAPSHOT
                        && observed.compareAndSet(false, true)) {
                    assertNotNull(second.checkExists().forPath(runPath(runId)));
                    assertNull(second.checkExists().forPath(
                            runPath(runId) + "/snapshot"));
                    assertNull(second.checkExists().forPath(guard));
                    assertNull(second.checkExists().forPath(guard + "/entries"));
                }
            });

            RunDecision created = creator.apply(RunCommand.createRun(
                    "create", runId, "definition", "artifact", 2, 0L, 1L));

            assertTrue(observed.get());
            assertEquals(RunDecision.Code.ACCEPTED, created.code());
            assertNotNull(second.checkExists().forPath(guard));
            assertNotNull(second.checkExists().forPath(guard + "/entries"));
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void committedCoreWithoutLifecycleGuardIsStableCorruption()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            RunSnapshot committed = accepted(writer, RunCommand.createRun(
                    "create", "missing-lifecycle-guard",
                    "definition", "artifact", 2, 0L, 1L));
            String guard = lifecycleGuardPath(committed.runId());
            second.delete().forPath(lifecyclePath(committed.runId()));
            second.delete().forPath(guard + "/entries");
            second.delete().forPath(guard);

            assertThrows(IllegalStateException.class,
                    () -> reader.executionEvidence(committed.runId()));
            assertNotNull(second.checkExists().forPath(
                            runPath(committed.runId()) + "/snapshot"),
                    "the corruption probe must not mutate the committed snapshot");
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void coreMismatchConfirmationTransportIsUnavailable() throws Exception {
        for (boolean connectionLoss : new boolean[] { false, true }) {
            try (TestingServer server = new TestingServer(true);
                 CuratorFramework first = connectedClient(server);
                 CuratorFramework second = connectedClient(server)) {
                ZkRunStateStore reader = new ZkRunStateStore(first);
                ZkRunStateStore writer = new ZkRunStateStore(second);
                String runId = "mismatch-transport-" + connectionLoss;
                ActiveRun run = active(
                        writer, runId, "definition", "worker");
                second.delete().forPath(lifecyclePath(run.snapshot.runId()));
                AtomicBoolean reached = new AtomicBoolean();
                reader.evidenceReadObserverForTest(phase -> {
                    if (phase == ZkRunStateStore.EvidenceReadPhase
                            .CORE_MISMATCH_BEFORE_CONFIRMATION) {
                        reached.set(true);
                        if (connectionLoss) {
                            throw new KeeperException.ConnectionLossException();
                        }
                        throw new KeeperException.OperationTimeoutException();
                    }
                });

                assertThrows(RunStateStoreUnavailableException.class,
                        () -> reader.executionEvidence(runId));
                assertTrue(reached.get(),
                        "the mismatch confirmation hook must be reachable");
                reader.evidenceReadObserverForTest(phase -> { });
                assertThrows(IllegalStateException.class,
                        () -> reader.executionEvidence(runId),
                        "a failed confirmation must not cache absence or "
                                + "consume the stable-corruption budget");
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void finalAbsenceConfirmationTransportCannotReturnMissing() throws Exception {
        for (boolean connectionLoss : new boolean[] { false, true }) {
            try (TestingServer server = new TestingServer(true);
                 CuratorFramework first = connectedClient(server);
                 CuratorFramework second = connectedClient(server)) {
                ZkRunStateStore reader = new ZkRunStateStore(first);
                ZkRunStateStore writer = new ZkRunStateStore(second);
                String runId = "absence-transport-" + connectionLoss;
                ActiveRun active = active(
                        writer, runId, "definition", "worker");
                RunSnapshot terminal = accepted(writer, RunCommand.advanceAttempt(
                        "terminal", runId, active.snapshot.revision(),
                        active.fence.attemptId(), active.fence.fenceToken(),
                        AttemptStatus.FAILED, false, 3L,
                        "FAILED", "final absence"));
                RetentionToken token = claim(writer, terminal);
                assertEquals(RunPurgeProgress.Code.IN_PROGRESS,
                        writer.purge(token, 1000).code());
                AtomicBoolean purged = new AtomicBoolean();
                AtomicBoolean reached = new AtomicBoolean();
                reader.evidenceReadObserverForTest(phase -> {
                    if (phase == ZkRunStateStore.EvidenceReadPhase
                            .BEFORE_INITIAL_SNAPSHOT
                            && purged.compareAndSet(false, true)) {
                        assertEquals(RunPurgeProgress.Code.PURGED,
                                writer.purge(token, 1000).code());
                    } else if (phase == ZkRunStateStore.EvidenceReadPhase
                            .FINAL_ABSENCE_BEFORE_CONFIRMATION) {
                        reached.set(true);
                        if (connectionLoss) {
                            throw new KeeperException.ConnectionLossException();
                        }
                        throw new KeeperException.OperationTimeoutException();
                    }
                });

                assertThrows(RunStateStoreUnavailableException.class,
                        () -> reader.executionEvidence(runId));
                assertTrue(purged.get());
                assertTrue(reached.get(),
                        "the final-absence confirmation hook must be reachable");
                reader.evidenceReadObserverForTest(phase -> { });
                assertEquals(DurableExecutionEvidenceView.State.MISSING,
                        reader.executionEvidence(runId).state(),
                        "fresh reread may converge only after transport clears");
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void executionEvidenceCannotCombineOldSnapshotWithReplacementAssignment()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            AtomicInteger mixedViews = new AtomicInteger();
            for (int round = 0; round < 16; round++) {
                ActiveRun original = active(writer,
                        "evidence-incarnation-" + round,
                        "definition-a", "worker-a");
                AtomicBoolean replaced = new AtomicBoolean();
                reader.evidenceReadObserverForTest(phase -> {
                    if (phase == ZkRunStateStore.EvidenceReadPhase.BEFORE_ASSIGNMENT
                            && replaced.compareAndSet(false, true)) {
                        replace(writer, original, "definition-b", "worker-b");
                    }
                });

                DurableExecutionEvidenceView view =
                        reader.executionEvidence(original.snapshot.runId());

                assertTrue(replaced.get());
                assertEquals(DurableExecutionEvidenceView.State.PRESENT, view.state());
                if (!"definition-b".equals(view.snapshot().definitionId())) {
                    mixedViews.incrementAndGet();
                }
                assertEquals("worker-b",
                        view.assignments().manifest().assignment(0).workerId(),
                        "all returned assignment evidence must be from replacement B");
            }
            assertEquals(0, mixedViews.get(),
                    "all 16 rounds must return a single replacement incarnation");
        }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void everyReadApiFailsClosedForEveryStableCoreOrphanWithoutMutation()
            throws Exception {
        List<String> violations = new ArrayList<>();
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            for (StableOrphanReadCase readCase
                    : StableOrphanReadCase.values()) {
                for (StableOrphanShape shape : StableOrphanShape.values()) {
                    String runId = "stable-orphan-"
                            + readCase.name().toLowerCase(Locale.ROOT) + "-"
                            + shape.name().toLowerCase(Locale.ROOT);
                    ActiveRun active = active(writer, runId,
                            "definition", "worker");
                    Stat rootBefore = second.checkExists()
                            .forPath(runPath(runId));
                    assertNotNull(rootBefore);
                    AtomicBoolean transitioned = new AtomicBoolean();
                    AtomicReference<List<String>> before =
                            new AtomicReference<>();
                    if (shape
                            == StableOrphanShape.LAYOUT_ONLY_AFTER_BOTH) {
                        reader.evidenceReadObserverForTest(phase -> {
                            if (phase == readCase.transitionPhase
                                    && transitioned.compareAndSet(
                                            false, true)) {
                                assertNotNull(second.checkExists().forPath(
                                        runPath(runId) + "/snapshot"));
                                assertNotNull(second.checkExists().forPath(
                                        lifecyclePath(runId)));
                                shape.corrupt(second, runId);
                                assertStableOrphanRoot(
                                        second, runId, rootBefore);
                                before.set(authorityFingerprint(second));
                            }
                        });
                    } else {
                        shape.corrupt(second, runId);
                        assertStableOrphanRoot(second, runId, rootBefore);
                        before.set(authorityFingerprint(second));
                    }
                    Throwable failure = null;
                    Object returned = null;
                    try {
                        returned = readCase.read(reader, active);
                    } catch (Throwable caught) {
                        failure = caught;
                    } finally {
                        reader.evidenceReadObserverForTest(phase -> { });
                    }
                    String label = readCase + " / " + shape;
                    if (shape == StableOrphanShape.LAYOUT_ONLY_AFTER_BOTH
                            && !transitioned.get()) {
                        violations.add(label
                                + " did not transition after the selected "
                                + "core observation phase");
                    }
                    if (!(failure instanceof IllegalStateException)
                            || failure instanceof RunStateStoreUnavailableException
                            || failure.getMessage() == null
                            || !failure.getMessage().contains(runId)) {
                        violations.add(label + " returned " + returned
                                + " instead of a run-specific stable-corruption "
                                + "IllegalStateException"
                                + (failure == null ? "" : " (" + failure + ")"));
                    }
                    List<String> after = authorityFingerprint(second);
                    if (before.get() == null || !before.get().equals(after)) {
                        violations.add(label
                                + " mutated the authoritative ZooKeeper subtree");
                    }
                }
            }
        }
        assertEquals(Collections.emptyList(), violations,
                "every read API must reject all stable core-orphan shapes "
                        + "without repair or progress");
    }

    private static ActiveRun replace(
            ZkRunStateStore store, ActiveRun original,
            String definitionId, String workerId) {
        RunSnapshot terminal = accepted(store, RunCommand.advanceAttempt(
                "terminal-a", original.snapshot.runId(),
                original.snapshot.revision(), original.fence.attemptId(),
                original.fence.fenceToken(), AttemptStatus.FAILED, false,
                3L, "FAILED", "replace incarnation"));
        RetentionToken token = claim(store, terminal);
        purgeAll(store, token);
        return active(store, original.snapshot.runId(), definitionId, workerId);
    }

    private static ActiveRun active(
            ZkRunStateStore store, String runId,
            String definitionId, String workerId) {
        RunSnapshot created = accepted(store, RunCommand.createRun(
                "create-" + definitionId, runId, definitionId,
                "shared-artifact", 2, 0L, 1L));
        RunSnapshot active = accepted(store, RunCommand.createAttempt(
                "attempt-" + definitionId, runId, created.revision(),
                "attempt", "publication", 2L));
        RunAttemptFence fence = new RunAttemptFence(
                runId, active.currentAttempt().attemptId(),
                active.currentAttempt().fenceToken());
        StageAssignmentRecord assignment = StageAssignmentRecord.create(
                fence, 1L, 0, 1L, 0, 0, StageAssignmentRole.SINK,
                workerId, "127.0.0.1", 7001, -1,
                "incarnation-" + workerId);
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, active.artifactId(), 1L,
                Collections.singletonList(assignment));
        assertEquals(StageManifestPublication.Code.PUBLISHED,
                store.publish(manifest).code());
        return new ActiveRun(active, fence);
    }

    private static RetentionToken claim(
            ZkRunStateStore store, RunSnapshot terminal) {
        RunLifecycleRef lifecycle = store.lifecycle(terminal.runId());
        RunRetentionClaim claim = store.claim(
                terminal.runId(), terminal.revision(),
                lifecycle.terminalAtMillis(),
                new RetentionOwner("retention-owner", 1L), 0L);
        assertEquals(RunRetentionClaim.Code.ACQUIRED, claim.code());
        return claim.token();
    }

    private static void purgeAll(
            ZkRunStateStore store, RetentionToken token) {
        RunPurgeProgress progress;
        do {
            progress = store.purge(token, 1000);
        } while (progress.code() == RunPurgeProgress.Code.IN_PROGRESS);
        assertEquals(RunPurgeProgress.Code.PURGED, progress.code());
    }

    private static RunSnapshot accepted(
            ZkRunStateStore store, RunCommand command) {
        RunDecision decision = store.apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }

    private static CuratorFramework connectedClient(TestingServer server)
            throws InterruptedException {
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), new ExponentialBackoffRetry(10, 3));
        client.start();
        if (!client.blockUntilConnected(10, TimeUnit.SECONDS)) {
            client.close();
            throw new IllegalStateException("Curator did not connect");
        }
        return client;
    }

    private static String lifecyclePath(String runId) {
        String digest = sha256Hex(runId);
        int shard = Integer.parseInt(digest.substring(0, 2), 16);
        return "/rowweave/run-lifecycle-v1/shards/"
                + String.format(Locale.ROOT, "%02x", shard)
                + "/entries/" + digest;
    }

    private static String lifecycleGuardPath(String runId) {
        String digest = sha256Hex(runId);
        int shard = Integer.parseInt(digest.substring(0, 2), 16);
        return "/rowweave/run-lifecycle-v1/shards/"
                + String.format(Locale.ROOT, "%02x", shard);
    }

    private static String runPath(String runId) {
        return "/rowweave/run-state-v1/runs/" + sha256Hex(runId);
    }

    private static String sameLifecycleShardRunId(
            String targetRunId, String prefix) {
        int target = Integer.parseInt(
                sha256Hex(targetRunId).substring(0, 2), 16);
        for (int value = 0; value < 10_000; value++) {
            String candidate = prefix + value;
            int shard = Integer.parseInt(
                    sha256Hex(candidate).substring(0, 2), 16);
            if (shard == target && !candidate.equals(targetRunId)) {
                return candidate;
            }
        }
        throw new AssertionError("unable to find same-shard runId");
    }

    private static String sha256Hex(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            char[] alphabet = "0123456789abcdef".toCharArray();
            char[] encoded = new char[digest.length * 2];
            for (int i = 0; i < digest.length; i++) {
                encoded[i * 2] = alphabet[(digest[i] >>> 4) & 0xf];
                encoded[i * 2 + 1] = alphabet[digest[i] & 0xf];
            }
            return new String(encoded);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static List<String> authorityFingerprint(CuratorFramework client)
            throws Exception {
        List<String> fingerprint = new ArrayList<>();
        appendSubtreeFingerprint(client, "/rowweave", fingerprint);
        Collections.sort(fingerprint);
        return fingerprint;
    }

    private static void assertStableOrphanRoot(
            CuratorFramework client, String runId, Stat originalRoot)
            throws Exception {
        Stat retainedRoot = client.checkExists().forPath(runPath(runId));
        assertNotNull(retainedRoot,
                "stable orphan corruption must retain the Run root");
        assertEquals(originalRoot.getCzxid(), retainedRoot.getCzxid(),
                "stable orphan must retain the same Run root incarnation");
        assertNotNull(client.checkExists().forPath(lifecycleGuardPath(runId)),
                "stable orphan must retain its initialized shard guard");
    }

    private static void appendSubtreeFingerprint(
            CuratorFramework client, String path, List<String> fingerprint)
            throws Exception {
        Stat exists = client.checkExists().forPath(path);
        if (exists == null) {
            fingerprint.add(path + "|ABSENT");
            return;
        }
        Stat stat = new Stat();
        byte[] data = client.getData().storingStatIn(stat).forPath(path);
        fingerprint.add(path + "|" + Base64.getEncoder().encodeToString(data)
                + "|v=" + stat.getVersion()
                + "|cv=" + stat.getCversion()
                + "|av=" + stat.getAversion()
                + "|cz=" + stat.getCzxid()
                + "|mz=" + stat.getMzxid()
                + "|pz=" + stat.getPzxid());
        List<String> children = client.getChildren().forPath(path);
        Collections.sort(children);
        for (String child : children) {
            appendSubtreeFingerprint(client, path + "/" + child, fingerprint);
        }
    }

    private enum StableOrphanShape {
        SNAPSHOT_ONLY {
            @Override
            void corrupt(CuratorFramework client, String runId) throws Exception {
                client.delete().forPath(lifecyclePath(runId));
            }
        },
        LIFECYCLE_ONLY {
            @Override
            void corrupt(CuratorFramework client, String runId) throws Exception {
                client.delete().forPath(runPath(runId) + "/snapshot");
            }
        },
        LAYOUT_ONLY_AFTER_BOTH {
            @Override
            void corrupt(CuratorFramework client, String runId) throws Exception {
                client.delete().forPath(lifecyclePath(runId));
                client.delete().forPath(runPath(runId) + "/snapshot");
            }
        };

        abstract void corrupt(CuratorFramework client, String runId)
                throws Exception;
    }

    private enum StableOrphanReadCase {
        GET(ZkRunStateStore.EvidenceReadPhase
                .CORE_AFTER_VALIDATED_PAIR) {
            @Override
            Object read(ZkRunStateStore store, ActiveRun active) {
                return store.get(active.snapshot.runId());
            }
        },
        EVENTS(ZkRunStateStore.EvidenceReadPhase
                .EVENTS_AFTER_INITIAL_LIFECYCLE) {
            @Override
            Object read(ZkRunStateStore store, ActiveRun active) {
                return store.events(active.snapshot.runId(), 0L, 10);
            }
        },
        EXECUTION_EVIDENCE(ZkRunStateStore.EvidenceReadPhase
                .BEFORE_ASSIGNMENT) {
            @Override
            Object read(ZkRunStateStore store, ActiveRun active) {
                return store.executionEvidence(active.snapshot.runId());
            }
        },
        ASSIGNMENT_SNAPSHOT(ZkRunStateStore.EvidenceReadPhase
                .CORE_AFTER_VALIDATED_PAIR) {
            @Override
            Object read(ZkRunStateStore store, ActiveRun active) {
                return store.assignmentSnapshot(active.fence);
            }
        },
        MANIFEST_COMMIT_JOURNAL(ZkRunStateStore.EvidenceReadPhase
                .CORE_AFTER_VALIDATED_PAIR) {
            @Override
            Object read(ZkRunStateStore store, ActiveRun active) {
                return store.manifestCommitJournal(active.fence);
            }
        };

        private final ZkRunStateStore.EvidenceReadPhase transitionPhase;

        StableOrphanReadCase(
                ZkRunStateStore.EvidenceReadPhase transitionPhase) {
            this.transitionPhase = transitionPhase;
        }

        abstract Object read(ZkRunStateStore store, ActiveRun active);
    }

    private static final class ActiveRun {
        private final RunSnapshot snapshot;
        private final RunAttemptFence fence;

        private ActiveRun(RunSnapshot snapshot, RunAttemptFence fence) {
            this.snapshot = snapshot;
            this.fence = fence;
        }
    }
}
