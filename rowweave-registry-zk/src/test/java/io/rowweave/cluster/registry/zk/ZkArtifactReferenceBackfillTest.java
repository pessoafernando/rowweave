package io.rowweave.cluster.registry.zk;

import io.rowweave.artifact.ArtifactBackfillStatus;
import io.rowweave.artifact.ArtifactLifecycleCursor;
import io.rowweave.artifact.ArtifactReferenceAuthority;
import io.rowweave.run.RetentionOwner;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunLifecycleRef;
import io.rowweave.run.RunPurgeProgress;
import io.rowweave.run.RunRetentionAuthority;
import io.rowweave.run.RunRetentionClaim;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStateStoreUnavailableException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A legacy lifecycle is backfilled within a bounded budget and passes the durable READY barrier. */
class ZkArtifactReferenceBackfillTest {
    private static final String REFERENCE_ROOT =
            "/rowweave/artifact-references-v1";
    private static final String ARTIFACT_A = repeat('a', 64);
    private static final String ARTIFACT_B = repeat('b', 64);
    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach
    void closeResources() throws Exception {
        for (int i = resources.size() - 1; i >= 0; i--) {
            resources.get(i).close();
        }
        resources.clear();
    }

    @Test
    void legacyAllStateLifecycleBackfillsAcrossRestartBeforeZeroProof()
            throws Exception {
        CuratorFramework client = newAuthority();
        ZkRunStateStore legacy = new ZkRunStateStore(client);
        create(legacy, "active-run", ARTIFACT_A);
        RunDecision terminal = create(legacy, "terminal-run", ARTIFACT_A);
        terminal = legacy.apply(RunCommand.cancelReadyRun(
                "terminal-cancel", "terminal-run",
                terminal.snapshot().revision(), 2L, "CANCELED", "done"));
        assertEquals(RunDecision.Code.ACCEPTED, terminal.code());
        RunDecision deleting = create(legacy, "deleting-run", ARTIFACT_B);
        deleting = legacy.apply(RunCommand.cancelReadyRun(
                "deleting-cancel", "deleting-run",
                deleting.snapshot().revision(), 2L, "CANCELED", "done"));
        RunLifecycleRef deletingLifecycle = legacy.lifecycle("deleting-run");
        assertEquals(RunRetentionClaim.Code.ACQUIRED,
                legacy.claim("deleting-run", deleting.snapshot().revision(),
                        deletingLifecycle.terminalAtMillis(),
                        new RetentionOwner("legacy-gc", 1L), 0L).code());

        removeReferenceIndex(client);
        ZkRunStateStore migrating = new ZkRunStateStore(client);
        assertEquals(ArtifactBackfillStatus.Code.BACKFILLING,
                migrating.backfillStatus().code());
        assertThrows(IllegalStateException.class,
                () -> migrating.inspect(ARTIFACT_A));

        ArtifactBackfillStatus first = migrating.advanceBackfill(1);
        assertEquals(ArtifactBackfillStatus.Code.BACKFILLING, first.code());
        ZkRunStateStore replacement = new ZkRunStateStore(client);
        ArtifactBackfillStatus resumed = replacement.backfillStatus();
        assertEquals(first.cursor(), resumed.cursor());
        assertEquals(first.processedEntries(), resumed.processedEntries());

        ArtifactBackfillStatus ready = finishBackfill(replacement, 1);
        assertEquals(ArtifactBackfillStatus.Code.READY, ready.code());
        assertEquals(2L, replacement.inspect(ARTIFACT_A).referenceCount());
        assertEquals(1L, replacement.inspect(ARTIFACT_B).referenceCount());
        assertEquals(ArtifactBackfillStatus.Code.READY,
                new ZkRunStateStore(client).backfillStatus().code());
    }

    @Test
    void historicalRunMutationLazyBackfillsWhileNewCreateDualWrites()
            throws Exception {
        CuratorFramework client = newAuthority();
        ZkRunStateStore legacy = new ZkRunStateStore(client);
        RunDecision historical = create(legacy, "historical-run", ARTIFACT_A);
        removeReferenceIndex(client);
        ZkRunStateStore migrating = new ZkRunStateStore(client);

        RunDecision terminal = migrating.apply(RunCommand.cancelReadyRun(
                "historical-cancel", "historical-run",
                historical.snapshot().revision(), 2L, "CANCELED", "done"));
        assertEquals(RunDecision.Code.ACCEPTED, terminal.code());
        assertEquals(1L, migrating.inspect(ARTIFACT_A).referenceCount());

        assertEquals(RunDecision.Code.ACCEPTED,
                create(migrating, "new-during-backfill", ARTIFACT_B).code());
        assertEquals(1L, migrating.inspect(ARTIFACT_B).referenceCount());
        finishBackfill(migrating, 2);
        assertEquals(1L, migrating.inspect(ARTIFACT_A).referenceCount());
        assertEquals(1L, migrating.inspect(ARTIFACT_B).referenceCount());
    }

    @Test
    void missingHistoricalReferenceBlocksFinalPurgeUntilBackfilled()
            throws Exception {
        CuratorFramework client = newAuthority();
        ZkRunStateStore legacy = new ZkRunStateStore(client);
        RunDecision run = create(legacy, "purge-race", ARTIFACT_A);
        RunDecision terminal = legacy.apply(RunCommand.cancelReadyRun(
                "purge-cancel", "purge-race", run.snapshot().revision(),
                2L, "CANCELED", "done"));
        RunLifecycleRef lifecycle = legacy.lifecycle("purge-race");
        removeReferenceIndex(client);
        ZkRunStateStore migrating = new ZkRunStateStore(client);
        RunRetentionClaim claim = migrating.claim(
                "purge-race", terminal.snapshot().revision(),
                lifecycle.terminalAtMillis(),
                new RetentionOwner("purge-owner", 1L), 0L);
        assertEquals(RunRetentionClaim.Code.ACQUIRED, claim.code());

        RunPurgeProgress before = purgeUntilSettled(migrating, claim, 10);
        assertEquals(RunPurgeProgress.Code.CORRUPT, before.code());
        assertNotEquals(null, migrating.get("purge-race"));

        finishBackfill(migrating, 10);
        RunPurgeProgress after = purgeUntilSettled(migrating, claim, 10);
        assertEquals(RunPurgeProgress.Code.PURGED, after.code());
        assertEquals(null, migrating.get("purge-race"));
    }

    @Test
    void corruptBarrierNeverBecomesZeroReferenceProof() throws Exception {
        CuratorFramework client = newAuthority();
        ZkRunStateStore legacy = new ZkRunStateStore(client);
        create(legacy, "legacy-corrupt-barrier", ARTIFACT_A);
        removeReferenceIndex(client);
        ZkRunStateStore migrating = new ZkRunStateStore(client);
        client.setData().forPath(REFERENCE_ROOT, new byte[] { 9, 8, 7 });

        assertEquals(ArtifactBackfillStatus.Code.CORRUPT,
                migrating.backfillStatus().code());
        assertEquals(ArtifactBackfillStatus.Code.CORRUPT,
                migrating.advanceBackfill(1).code());
        assertThrows(IllegalStateException.class,
                () -> migrating.inspect(ARTIFACT_B));
    }

    @Test
    void oneCallProcessesMoreThanCasRetryBudgetWithinOneShard()
            throws Exception {
        CuratorFramework client = newAuthority();
        ZkRunStateStore legacy = new ZkRunStateStore(client);
        List<String> runIds = sameLifecycleShardRunIds(70);
        for (String runId : runIds) create(legacy, runId, ARTIFACT_A);
        int targetShard = lifecycleShard(runIds.get(0));
        removeReferenceIndex(client);
        ZkRunStateStore migrating = new ZkRunStateStore(client);
        ArtifactBackfillStatus status = migrating.backfillStatus();
        while (cursor(status).shard() != targetShard) {
            status = migrating.advanceBackfill(100);
        }

        long before = status.processedEntries();
        status = migrating.advanceBackfill(100);
        assertEquals(before + 70L, status.processedEntries());
        assertEquals(70L, migrating.inspect(ARTIFACT_A).referenceCount());
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void backfillEntryCannotCommitOldCursorAcrossRunIncarnation()
            throws Exception {
        for (int round = 0; round < 16; round++) {
        // This test exercises run-incarnation fencing, not session expiry.
        // Keep the session alive through a 16-round full-reactor run.
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server, 10_000);
             CuratorFramework second = connectedClient(server, 10_000)) {
        String runId = "backfill-incarnation-" + round;
        ZkRunStateStore legacy = new ZkRunStateStore(first);
        RunDecision original = create(legacy, runId, ARTIFACT_A);
        removeReferenceIndex(first);
        ZkRunStateStore migrating = new ZkRunStateStore(first);
        ZkRunStateStore replacer = new ZkRunStateStore(second);
        RunDecision terminal = migrating.apply(RunCommand.cancelReadyRun(
                "terminal-a", runId, original.snapshot().revision(),
                2L, "CANCELED", "terminal A"));
        assertEquals(RunDecision.Code.ACCEPTED, terminal.code());
        RunLifecycleRef lifecycle = migrating.lifecycle(runId);
        RunRetentionClaim claim = migrating.claim(
                runId, terminal.snapshot().revision(),
                lifecycle.terminalAtMillis(),
                new RetentionOwner("retention-a", 1L), 0L);
        assertEquals(RunRetentionClaim.Code.ACQUIRED, claim.code());
        seekBackfillShard(migrating, lifecycleShard(runId));
        AtomicInteger commits = new AtomicInteger();
        migrating.guardedMutationObserverForTest((kind, ordinal, moment) -> {
            if (kind != ZkRunStateStore.GuardedMutationKind
                    .REFERENCE_BACKFILL_ENTRY
                    || moment != ZkRunStateStore.GuardedMutationMoment
                    .BEFORE_COMMIT) {
                return;
            }
            int call = commits.incrementAndGet();
            if (call == 1) {
                assertEquals(RunPurgeProgress.Code.PURGED,
                        purgeUntilSettled(replacer, claim, 1000).code());
                RunDecision replacement = create(replacer, runId, ARTIFACT_B);
                RunDecision replacementTerminal = replacer.apply(
                        RunCommand.cancelReadyRun(
                                "terminal-b", runId,
                                replacement.snapshot().revision(), 2L,
                                "CANCELED", "terminal B"));
                assertEquals(RunDecision.Code.ACCEPTED,
                        replacementTerminal.code());
                RunLifecycleRef replacementLifecycle =
                        replacer.lifecycle(runId);
                RunRetentionClaim replacementClaim = replacer.claim(
                        runId, replacementTerminal.snapshot().revision(),
                        replacementLifecycle.terminalAtMillis(),
                        new RetentionOwner("retention-b", 1L), 0L);
                assertEquals(RunRetentionClaim.Code.ACQUIRED,
                        replacementClaim.code());
            } else if (call == 2) {
                throw new KeeperException.OperationTimeoutException();
            }
        });

        assertThrows(RunStateStoreUnavailableException.class,
                () -> migrating.advanceBackfill(1));
        assertEquals(0L, migrating.backfillStatus().processedEntries(),
                "the old A cursor must not commit after replacement B");
        assertTrue(!referenceEntryExists(first, ARTIFACT_A, runId),
                "a BACKFILLING barrier cannot use inspect() as zero proof; "
                        + "the exact old entry must remain absent");
        assertEquals(1L, migrating.inspect(ARTIFACT_B).referenceCount());

        ArtifactBackfillStatus resumed = migrating.advanceBackfill(1);
        assertEquals(1L, resumed.processedEntries(),
                "a fresh call may process the current B incarnation");
        }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void backfillSelectionTreatsReplacementPrepublicationAsRace()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            String runId = "backfill-selection-prepublication";
            ZkRunStateStore legacy = new ZkRunStateStore(first);
            RunDecision original = create(legacy, runId, ARTIFACT_A);
            removeReferenceIndex(first);
            ZkRunStateStore migrating = new ZkRunStateStore(first);
            ZkRunStateStore replacer = new ZkRunStateStore(second);
            RunDecision terminal = migrating.apply(RunCommand.cancelReadyRun(
                    "terminal-selection-a", runId,
                    original.snapshot().revision(), 2L,
                    "CANCELED", "terminal A"));
            RunLifecycleRef lifecycle = migrating.lifecycle(runId);
            RunRetentionClaim claim = migrating.claim(
                    runId, terminal.snapshot().revision(),
                    lifecycle.terminalAtMillis(),
                    new RetentionOwner("selection-owner-a", 1L), 0L);
            assertEquals(RunRetentionClaim.Code.ACQUIRED, claim.code());
            seekBackfillShard(migrating, lifecycleShard(runId));
            AtomicBoolean replaced = new AtomicBoolean();
            AtomicBoolean paused = new AtomicBoolean();
            CountDownLatch layoutReady = new CountDownLatch(1);
            CountDownLatch allowCreate = new CountDownLatch(1);
            AtomicReference<Throwable> createFailure = new AtomicReference<>();
            AtomicReference<Thread> creator = new AtomicReference<>();
            migrating.evidenceReadObserverForTest(phase -> {
                if (phase != ZkRunStateStore.EvidenceReadPhase
                        .BACKFILL_AFTER_LIFECYCLE_SELECTION
                        || !replaced.compareAndSet(false, true)) {
                    return;
                }
                assertEquals(RunPurgeProgress.Code.PURGED,
                        purgeUntilSettled(replacer, claim, 1000).code());
                replacer.evidenceReadObserverForTest(createPhase -> {
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
                        create(replacer, runId, ARTIFACT_B);
                    } catch (Throwable failure) {
                        createFailure.set(failure);
                    }
                }, "zk-backfill-selection-prepublication");
                creator.set(thread);
                thread.start();
                if (!layoutReady.await(10, TimeUnit.SECONDS)) {
                    throw new AssertionError(
                            "replacement layout was not published");
                }
            });

            ArtifactBackfillStatus result = null;
            Throwable failure = null;
            try {
                result = migrating.advanceBackfill(1);
            } catch (Throwable caught) {
                failure = caught;
            } finally {
                allowCreate.countDown();
                Thread thread = creator.get();
                if (thread != null) {
                    thread.join(TimeUnit.SECONDS.toMillis(10));
                    assertTrue(!thread.isAlive(),
                            "replacement CREATE must terminate");
                }
            }

            assertTrue(replaced.get());
            assertTrue(paused.get());
            assertTrue(failure == null,
                    "legal replacement prepublication must be a race: "
                            + failure);
            assertNotNull(result);
            assertTrue(result.code() != ArtifactBackfillStatus.Code.CORRUPT);
            assertEquals(0L, result.processedEntries());
            assertTrue(!referenceEntryExists(first, ARTIFACT_A, runId));
            assertTrue(createFailure.get() == null,
                    "replacement CREATE failed: " + createFailure.get());
            assertEquals(1L, migrating.inspect(ARTIFACT_B).referenceCount());
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void backfillAckLossIsUnavailableButDoesNotDuplicateCommittedEntry()
            throws Exception {
        CuratorFramework client = newAuthority();
        ZkRunStateStore legacy = new ZkRunStateStore(client);
        create(legacy, "backfill-ack", ARTIFACT_A);
        removeReferenceIndex(client);
        ZkRunStateStore migrating = new ZkRunStateStore(client);
        seekBackfillShard(migrating, lifecycleShard("backfill-ack"));
        AtomicInteger callbacks = new AtomicInteger();
        migrating.guardedMutationObserverForTest((kind, ordinal, moment) -> {
            if (kind == ZkRunStateStore.GuardedMutationKind
                    .REFERENCE_BACKFILL_ENTRY
                    && moment == ZkRunStateStore.GuardedMutationMoment
                    .AFTER_COMMIT_BEFORE_ACK) {
                callbacks.incrementAndGet();
                throw new KeeperException.ConnectionLossException();
            }
        });

        assertThrows(RunStateStoreUnavailableException.class,
                () -> migrating.advanceBackfill(1));
        assertEquals(1, callbacks.get());
        assertEquals(1L, migrating.backfillStatus().processedEntries(),
                "the committed cursor must remain durable despite lost ACK");
        assertEquals(1L, migrating.inspect(ARTIFACT_A).referenceCount());

        ZkRunStateStore resumed = new ZkRunStateStore(client);
        resumed.advanceBackfill(1);
        assertEquals(1L, resumed.backfillStatus().processedEntries(),
                "retry must not count the committed entry twice");
        assertEquals(1L, resumed.inspect(ARTIFACT_A).referenceCount());
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void backfillAckLossFencesEveryReplacementState() throws Exception {
        List<String> violations = new ArrayList<>();
        for (BackfillReplacementState state
                : BackfillReplacementState.values()) {
            try (TestingServer server = new TestingServer(true);
                 CuratorFramework first = connectedClient(server);
                 CuratorFramework second = connectedClient(server)) {
                String runId = "backfill-ack-state-"
                        + state.name().toLowerCase(java.util.Locale.ROOT);
                ZkRunStateStore legacy = new ZkRunStateStore(first);
                RunDecision original = create(legacy, runId, ARTIFACT_A);
                RunDecision terminal = legacy.apply(
                        RunCommand.cancelReadyRun(
                                "terminal-" + runId, runId,
                                original.snapshot().revision(), 2L,
                                "CANCELED", "terminal A"));
                assertEquals(RunDecision.Code.ACCEPTED, terminal.code());
                RunLifecycleRef lifecycle = legacy.lifecycle(runId);
                RunRetentionClaim retention = legacy.claim(
                        runId, terminal.snapshot().revision(),
                        lifecycle.terminalAtMillis(),
                        new RetentionOwner("backfill-state-owner", 1L), 0L);
                assertEquals(RunRetentionClaim.Code.ACQUIRED,
                        retention.code());
                removeReferenceIndex(first);
                ZkRunStateStore migrating = new ZkRunStateStore(first);
                ZkRunStateStore replacer = new ZkRunStateStore(second);
                seekBackfillShard(migrating, lifecycleShard(runId));
                AtomicBoolean ackLost = new AtomicBoolean();
                CountDownLatch layoutReady = new CountDownLatch(1);
                CountDownLatch allowCreate = new CountDownLatch(1);
                AtomicReference<Thread> creator = new AtomicReference<>();
                AtomicReference<Throwable> createFailure =
                        new AtomicReference<>();
                migrating.guardedMutationObserverForTest(
                        (kind, ordinal, moment) -> {
                    if (kind == ZkRunStateStore.GuardedMutationKind
                            .REFERENCE_BACKFILL_ENTRY
                            && moment == ZkRunStateStore
                            .GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK
                            && ackLost.compareAndSet(false, true)) {
                        assertEquals(RunPurgeProgress.Code.PURGED,
                                purgeUntilSettled(
                                        replacer, retention, 1000).code());
                        if (state == BackfillReplacementState.PREPUBLICATION) {
                            AtomicBoolean paused = new AtomicBoolean();
                            replacer.evidenceReadObserverForTest(phase -> {
                                if (phase == ZkRunStateStore.EvidenceReadPhase
                                        .APPLY_AFTER_INITIAL_SNAPSHOT
                                        && paused.compareAndSet(false, true)) {
                                    layoutReady.countDown();
                                    if (!allowCreate.await(
                                            10, TimeUnit.SECONDS)) {
                                        throw new AssertionError(
                                                "replacement CREATE release "
                                                        + "timed out");
                                    }
                                }
                            });
                            Thread thread = new Thread(() -> {
                                try {
                                    create(replacer, runId, ARTIFACT_B);
                                } catch (Throwable failure) {
                                    createFailure.set(failure);
                                }
                            }, "zk-backfill-prepublication");
                            creator.set(thread);
                            thread.start();
                            if (!layoutReady.await(10, TimeUnit.SECONDS)) {
                                throw new AssertionError(
                                        "replacement layout not published");
                            }
                        } else if (state
                                != BackfillReplacementState.PURGED) {
                            RunDecision replacement =
                                    create(replacer, runId, ARTIFACT_B);
                            if (state
                                    == BackfillReplacementState.TERMINAL) {
                                RunDecision replacementTerminal = replacer.apply(
                                        RunCommand.cancelReadyRun(
                                                "terminal-b-" + runId, runId,
                                                replacement.snapshot()
                                                        .revision(),
                                                2L, "CANCELED", "terminal B"));
                                assertEquals(RunDecision.Code.ACCEPTED,
                                        replacementTerminal.code());
                            }
                        }
                        throw new KeeperException.ConnectionLossException();
                    }
                });

                RuntimeException failure = null;
                try {
                    migrating.advanceBackfill(1);
                } catch (RuntimeException thrown) {
                    failure = thrown;
                } finally {
                    allowCreate.countDown();
                    Thread thread = creator.get();
                    if (thread != null) {
                        thread.join(TimeUnit.SECONDS.toMillis(10));
                        assertTrue(!thread.isAlive(),
                                "replacement CREATE must terminate");
                    }
                }

                assertTrue(ackLost.get(), state
                        + " must reach AFTER_COMMIT_BEFORE_ACK");
                if (!(failure instanceof RunStateStoreUnavailableException)) {
                    violations.add(state + " returned "
                            + (failure == null ? "normally"
                                    : failure.getClass().getSimpleName()));
                }
                if (createFailure.get() != null) {
                    violations.add(state + " replacement CREATE threw "
                            + createFailure.get().getClass().getSimpleName());
                }
                ArtifactBackfillStatus status = migrating.backfillStatus();
                if (status.processedEntries() != 1L) {
                    violations.add(state + " cursor count="
                            + status.processedEntries());
                }
                if (referenceEntryExists(first, ARTIFACT_A, runId)) {
                    violations.add(state
                            + " retained old artifact A before resume");
                }
                boolean replacementReference =
                        referenceEntryExists(first, ARTIFACT_B, runId);
                if (state == BackfillReplacementState.PURGED) {
                    if (replacementReference) {
                        violations.add(state
                                + " created replacement reference");
                    }
                } else if (!replacementReference) {
                    violations.add(state
                            + " lost replacement reference before resume");
                } else if (referenceShardEntryCount(
                        first, ARTIFACT_B, runId) != 1) {
                    violations.add(state
                            + " duplicated replacement reference before "
                            + "resume");
                }
                finishBackfill(migrating, 100);
                if (migrating.inspect(ARTIFACT_A).referenceCount() != 0L) {
                    violations.add(state + " retained old artifact A");
                }
                RunSnapshot replacement = replacer.get(runId);
                if (state == BackfillReplacementState.PURGED) {
                    if (replacement != null) {
                        violations.add(state + " unexpectedly recreated Run");
                    }
                } else {
                    if (replacement == null) {
                        violations.add(state + " replacement missing");
                    } else {
                        long expectedRevision = state
                                == BackfillReplacementState.TERMINAL ? 2L : 1L;
                        if (replacement.revision() != expectedRevision) {
                            violations.add(state + " replacement revision="
                                    + replacement.revision());
                        }
                    }
                    if (migrating.inspect(ARTIFACT_B).referenceCount() != 1L) {
                        violations.add(state
                                + " duplicated replacement artifact B");
                    }
                }
            }
        }
        assertTrue(violations.isEmpty(),
                "backfill ACK reconciliation must fence every replacement "
                        + "state: " + violations);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void backfillAckConfirmationTransportFailsImmediately()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            String runId = "backfill-ack-confirmation-transport";
            ZkRunStateStore legacy = new ZkRunStateStore(first);
            create(legacy, runId, ARTIFACT_A);
            removeReferenceIndex(first);
            ZkRunStateStore migrating = new ZkRunStateStore(first);
            ZkRunStateStore witness = new ZkRunStateStore(second);
            seekBackfillShard(migrating, lifecycleShard(runId));
            AtomicBoolean ackLost = new AtomicBoolean();
            AtomicInteger confirmationFaults = new AtomicInteger();
            migrating.evidenceReadObserverForTest(phase -> {
                if (ackLost.get()
                        && phase == ZkRunStateStore.EvidenceReadPhase
                        .CORE_MISMATCH_BEFORE_CONFIRMATION) {
                    confirmationFaults.incrementAndGet();
                    throw new KeeperException.OperationTimeoutException();
                }
            });
            migrating.guardedMutationObserverForTest(
                    (kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind
                        .REFERENCE_BACKFILL_ENTRY
                        && moment == ZkRunStateStore.GuardedMutationMoment
                        .AFTER_COMMIT_BEFORE_ACK
                        && ackLost.compareAndSet(false, true)) {
                    throw new KeeperException.ConnectionLossException();
                }
            });

            assertThrows(RunStateStoreUnavailableException.class,
                    () -> migrating.advanceBackfill(1));
            assertTrue(ackLost.get());
            assertEquals(1, confirmationFaults.get(),
                    "committed backfill ACK loss must confirm the original "
                            + "Run incarnation exactly once");
            assertEquals(1L, witness.backfillStatus().processedEntries(),
                    "confirmation failure must not roll back or duplicate "
                            + "the committed cursor");
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void backfillFailsClosedForEveryStableCoreOrphanAtCommitBoundary()
            throws Exception {
        List<String> violations = new ArrayList<>();
        for (StableOrphanShape shape : StableOrphanShape.values()) {
            try (TestingServer server = new TestingServer(true);
                 CuratorFramework first = connectedClient(server);
                 CuratorFramework second = connectedClient(server)) {
                String runId = "backfill-stable-orphan-"
                        + shape.name().toLowerCase(java.util.Locale.ROOT);
                ZkRunStateStore legacy = new ZkRunStateStore(first);
                create(legacy, runId, ARTIFACT_A);
                removeReferenceIndex(first);
                ZkRunStateStore migrating = new ZkRunStateStore(first);
                seekBackfillShard(migrating, lifecycleShard(runId));
                ArtifactBackfillStatus beforeCall = migrating.backfillStatus();
                Stat rootBefore = second.checkExists().forPath(
                        runRootPath(runId));
                assertNotNull(rootBefore);
                assertNotNull(second.checkExists().forPath(
                        runRootPath(runId) + "/snapshot"));
                assertNotNull(second.checkExists().forPath(
                        lifecycleEntryPath(runId)));
                AtomicBoolean corrupted = new AtomicBoolean();
                AtomicInteger commitBoundaries = new AtomicInteger();
                AtomicReference<List<String>> boundaryFingerprint =
                        new AtomicReference<>();
                migrating.evidenceReadObserverForTest(phase -> {
                    if (shape == StableOrphanShape.LAYOUT_ONLY_AFTER_BOTH
                            && phase == ZkRunStateStore.EvidenceReadPhase
                                    .CORE_AFTER_VALIDATED_PAIR
                            && corrupted.compareAndSet(false, true)) {
                        shape.corrupt(second, runId);
                        Stat retainedRoot = second.checkExists().forPath(
                                runRootPath(runId));
                        assertNotNull(retainedRoot);
                        assertEquals(rootBefore.getCzxid(),
                                retainedRoot.getCzxid());
                        assertNotNull(second.checkExists().forPath(
                                lifecycleGuardPath(runId)));
                        boundaryFingerprint.set(authorityFingerprint(second));
                    }
                });
                migrating.guardedMutationObserverForTest(
                        (kind, ordinal, moment) -> {
                    if (kind == ZkRunStateStore.GuardedMutationKind
                            .REFERENCE_BACKFILL_ENTRY
                            && moment == ZkRunStateStore.GuardedMutationMoment
                            .BEFORE_COMMIT) {
                        commitBoundaries.incrementAndGet();
                        if (shape != StableOrphanShape.LAYOUT_ONLY_AFTER_BOTH
                                && corrupted.compareAndSet(false, true)) {
                            shape.corrupt(second, runId);
                            Stat retainedRoot = second.checkExists().forPath(
                                    runRootPath(runId));
                            assertNotNull(retainedRoot);
                            assertEquals(rootBefore.getCzxid(),
                                    retainedRoot.getCzxid());
                            assertNotNull(second.checkExists().forPath(
                                    lifecycleGuardPath(runId)));
                            boundaryFingerprint.set(
                                    authorityFingerprint(second));
                        }
                    }
                });

                Throwable failure = null;
                ArtifactBackfillStatus returned = null;
                try {
                    returned = migrating.advanceBackfill(1);
                } catch (Throwable caught) {
                    failure = caught;
                }
                String label = shape.name();
                if (!corrupted.get() || commitBoundaries.get() < 1) {
                    violations.add(label
                            + " did not reach the selected-entry commit boundary");
                }
                if (failure != null || returned == null
                        || returned.code() != ArtifactBackfillStatus.Code.CORRUPT
                        || returned.cursor() != null
                        || returned.processedEntries() != 0L) {
                    violations.add(label
                            + " did not return CORRUPT/null-cursor/0 "
                            + "(failure=" + failure + ")");
                }
                List<String> boundary = boundaryFingerprint.get();
                if (boundary == null
                        || !boundary.equals(authorityFingerprint(second))) {
                    violations.add(label
                            + " changed authority after the corrupted "
                            + "BEFORE_COMMIT boundary");
                }
                ArtifactBackfillStatus durable = migrating.backfillStatus();
                if (durable.code() != beforeCall.code()
                        || !java.util.Objects.equals(
                                durable.cursor(), beforeCall.cursor())
                        || durable.processedEntries()
                                != beforeCall.processedEntries()) {
                    violations.add(label
                            + " advanced the durable backfill cursor");
                }
                if (referenceEntryExists(first, ARTIFACT_A, runId)) {
                    violations.add(label
                            + " created an artifact reference for the orphan");
                }
            }
        }
        assertEquals(Collections.emptyList(), violations,
                "backfill must reject stable core orphans without reference "
                        + "or cursor progress");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void backfillEntryUsesExactD8OperationsAndRealRequestBytes()
            throws Exception {
        CuratorFramework client = newAuthority();
        ZkRunStateStore legacy = new ZkRunStateStore(client);
        String runId = "backfill-d8-resource";
        create(legacy, runId, ARTIFACT_A);
        removeReferenceIndex(client);
        ZkRunStateStore migrating = new ZkRunStateStore(client);
        seekBackfillShard(migrating, lifecycleShard(runId));
        AtomicReference<ZkRunStateStore.GuardedMutationKind> kind =
                new AtomicReference<>();
        AtomicInteger operations = new AtomicInteger();
        AtomicInteger requestBytes = new AtomicInteger();
        migrating.transactionWriteObserverForTest(
                (observedKind, ordinal, operationCount, bytes) -> {
                    assertEquals(-1, ordinal);
                    assertTrue(kind.compareAndSet(null, observedKind),
                            "one selected entry must use one transaction");
                    operations.set(operationCount);
                    requestBytes.set(bytes);
                });

        ArtifactBackfillStatus status = migrating.advanceBackfill(1);

        assertEquals(ArtifactBackfillStatus.Code.BACKFILLING, status.code());
        assertEquals(ZkRunStateStore.GuardedMutationKind
                .REFERENCE_BACKFILL_ENTRY, kind.get());
        assertEquals(8, operations.get(),
                "selected-entry reference+cursor multi must add one guard check");
        assertTrue(requestBytes.get() > 0,
                "request bytes must come from a real MultiOperationRecord");
        assertTrue(requestBytes.get() < 1_000_000);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void backfillTransportBeforeCommitLeavesReferenceAndCursorUntouched()
            throws Exception {
        CuratorFramework client = newAuthority();
        ZkRunStateStore legacy = new ZkRunStateStore(client);
        create(legacy, "backfill-transport", ARTIFACT_A);
        removeReferenceIndex(client);
        ZkRunStateStore migrating = new ZkRunStateStore(client);
        seekBackfillShard(migrating, lifecycleShard("backfill-transport"));
        AtomicInteger callbacks = new AtomicInteger();
        migrating.guardedMutationObserverForTest((kind, ordinal, moment) -> {
            if (kind == ZkRunStateStore.GuardedMutationKind
                    .REFERENCE_BACKFILL_ENTRY
                    && moment == ZkRunStateStore.GuardedMutationMoment
                    .BEFORE_COMMIT) {
                callbacks.incrementAndGet();
                throw new KeeperException.OperationTimeoutException();
            }
        });

        assertThrows(RunStateStoreUnavailableException.class,
                () -> migrating.advanceBackfill(1));
        assertEquals(1, callbacks.get());
        assertEquals(0L, migrating.backfillStatus().processedEntries());
        assertThrows(IllegalStateException.class,
                () -> migrating.inspect(ARTIFACT_A),
                "an unadvanced BACKFILLING barrier cannot prove a reference");
    }

    @Test
    @Tag("profile")
    void tenThousandAndOneLegacyRefsRemainPagedAndExact() throws Exception {
        CuratorFramework client = newAuthority();
        ZkRunStateStore legacy = new ZkRunStateStore(client);
        for (int value = 0; value < 10_001; value++) {
            create(legacy, "large-backfill-" + value, ARTIFACT_A);
        }
        removeReferenceIndex(client);
        ZkRunStateStore migrating = new ZkRunStateStore(client);
        ArtifactBackfillStatus ready = finishBackfill(migrating, 1000);
        assertEquals(ArtifactBackfillStatus.Code.READY, ready.code());
        assertEquals(10_001L,
                migrating.inspect(ARTIFACT_A).referenceCount());
    }

    private CuratorFramework newAuthority() throws Exception {
        TestingServer server = new TestingServer(true);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .sessionTimeoutMs(1_000)
                .connectionTimeoutMs(250)
                .retryPolicy(new ExponentialBackoffRetry(10, 1))
                .build();
        client.start();
        client.blockUntilConnected();
        resources.add(client);
        resources.add(server);
        return client;
    }

    private static CuratorFramework connectedClient(TestingServer server)
            throws InterruptedException {
        return connectedClient(server, 1_000);
    }

    private static CuratorFramework connectedClient(TestingServer server,
            int sessionTimeoutMillis) throws InterruptedException {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .sessionTimeoutMs(sessionTimeoutMillis)
                .connectionTimeoutMs(250)
                .retryPolicy(new ExponentialBackoffRetry(10, 1))
                .build();
        client.start();
        if (!client.blockUntilConnected(10, TimeUnit.SECONDS)) {
            client.close();
            throw new IllegalStateException("Curator did not connect");
        }
        return client;
    }

    private static RunDecision create(
            ZkRunStateStore store, String runId, String artifactId) {
        RunDecision decision = store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", artifactId,
                1, 0L, 1L));
        assertTrue(decision.code() == RunDecision.Code.ACCEPTED
                || decision.code() == RunDecision.Code.ALREADY_APPLIED);
        return decision;
    }

    private static ArtifactBackfillStatus finishBackfill(
            ArtifactReferenceAuthority authority, int pageSize) {
        ArtifactBackfillStatus status = authority.backfillStatus();
        for (int call = 0;
             call < 600 && status.code() == ArtifactBackfillStatus.Code.BACKFILLING;
             call++) {
            status = authority.advanceBackfill(pageSize);
        }
        assertEquals(ArtifactBackfillStatus.Code.READY, status.code());
        return status;
    }

    private static void seekBackfillShard(
            ArtifactReferenceAuthority authority, int targetShard) {
        ArtifactBackfillStatus status = authority.backfillStatus();
        while (cursor(status).shard() != targetShard) {
            status = authority.advanceBackfill(100);
        }
    }

    private static ArtifactLifecycleCursor cursor(
            ArtifactBackfillStatus status) {
        return ArtifactLifecycleCursor.decode(
                ArtifactLifecycleCursor.Kind.REFERENCE_BACKFILL,
                status.cursor());
    }

    private static RunPurgeProgress purgeUntilSettled(
            RunRetentionAuthority authority, RunRetentionClaim claim,
            int maxDeletes) {
        RunPurgeProgress progress = null;
        for (int call = 0; call < 20; call++) {
            progress = authority.purge(claim.token(), maxDeletes);
            if (progress.code() != RunPurgeProgress.Code.IN_PROGRESS) return progress;
        }
        throw new AssertionError("purge did not settle");
    }

    private static void removeReferenceIndex(CuratorFramework client)
            throws Exception {
        client.delete().deletingChildrenIfNeeded().forPath(REFERENCE_ROOT);
    }

    private static boolean referenceEntryExists(
            CuratorFramework client, String artifactId, String runId)
            throws Exception {
        return client.checkExists().forPath(
                referenceEntryPath(artifactId, runId)) != null;
    }

    private static int referenceShardEntryCount(
            CuratorFramework client, String artifactId, String runId)
            throws Exception {
        String entry = referenceEntryPath(artifactId, runId);
        String entries = entry.substring(0, entry.lastIndexOf('/'));
        return client.getChildren().forPath(entries).size();
    }

    private static String referenceEntryPath(
            String artifactId, String runId) {
        String digest =
                io.rowweave.run.RunLifecycleCatalogKernel.digestHex(runId);
        int shard = io.rowweave.run.RunLifecycleCatalogKernel.shard(digest);
        return REFERENCE_ROOT + "/artifacts/" + artifactId
                + "/shards/"
                + String.format(java.util.Locale.ROOT, "%02x", shard)
                + "/entries/" + digest;
    }

    private static List<String> authorityFingerprint(
            CuratorFramework client) throws Exception {
        List<String> fingerprint = new ArrayList<>();
        appendSubtreeFingerprint(client, "/rowweave", "/rowweave", fingerprint);
        return fingerprint;
    }

    private static void appendSubtreeFingerprint(
            CuratorFramework client, String root, String path,
            List<String> fingerprint) throws Exception {
        Stat stat = new Stat();
        byte[] data = client.getData().storingStatIn(stat).forPath(path);
        String relative = path.substring(root.length());
        fingerprint.add((relative.isEmpty() ? "/" : relative)
                + "|v=" + stat.getVersion()
                + "|cv=" + stat.getCversion()
                + "|av=" + stat.getAversion()
                + "|cz=" + stat.getCzxid()
                + "|mz=" + stat.getMzxid()
                + "|pz=" + stat.getPzxid()
                + "|data=" + Base64.getEncoder().encodeToString(data));
        List<String> children = client.getChildren().forPath(path);
        Collections.sort(children);
        for (String child : children) {
            appendSubtreeFingerprint(
                    client, root, path + "/" + child, fingerprint);
        }
    }

    private static String runRootPath(String runId) {
        return "/rowweave/run-state-v1/runs/"
                + io.rowweave.run.RunLifecycleCatalogKernel.digestHex(runId);
    }

    private static String lifecycleEntryPath(String runId) {
        String digest = io.rowweave.run.RunLifecycleCatalogKernel
                .digestHex(runId);
        return "/rowweave/run-lifecycle-v1/shards/" + digest.substring(0, 2)
                + "/entries/" + digest;
    }

    private static String lifecycleGuardPath(String runId) {
        String digest = io.rowweave.run.RunLifecycleCatalogKernel
                .digestHex(runId);
        return "/rowweave/run-lifecycle-v1/shards/" + digest.substring(0, 2);
    }

    private static List<String> sameLifecycleShardRunIds(int count) {
        List<String> ids = new ArrayList<>();
        int target = -1;
        for (int value = 0; ids.size() < count; value++) {
            String runId = "backfill-same-shard-" + value;
            int shard = lifecycleShard(runId);
            if (target < 0) target = shard;
            if (shard == target) ids.add(runId);
        }
        return ids;
    }

    private static int lifecycleShard(String runId) {
        String digest = io.rowweave.run.RunLifecycleCatalogKernel
                .digestHex(runId);
        return io.rowweave.run.RunLifecycleCatalogKernel.shard(digest);
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private enum BackfillReplacementState {
        PURGED,
        PREPUBLICATION,
        ACTIVE,
        TERMINAL
    }

    private enum StableOrphanShape {
        SNAPSHOT_ONLY {
            @Override
            void corrupt(CuratorFramework client, String runId) throws Exception {
                client.delete().forPath(lifecycleEntryPath(runId));
            }
        },
        LIFECYCLE_ONLY {
            @Override
            void corrupt(CuratorFramework client, String runId) throws Exception {
                client.delete().forPath(runRootPath(runId) + "/snapshot");
            }
        },
        LAYOUT_ONLY_AFTER_BOTH {
            @Override
            void corrupt(CuratorFramework client, String runId) throws Exception {
                client.delete().forPath(lifecycleEntryPath(runId));
                client.delete().forPath(runRootPath(runId) + "/snapshot");
            }
        };

        abstract void corrupt(CuratorFramework client, String runId)
                throws Exception;
    }
}
