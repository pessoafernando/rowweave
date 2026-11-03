package io.rowweave.cluster.registry.zk;

import io.rowweave.run.AttemptStatus;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RetentionOwner;
import io.rowweave.run.RetentionToken;
import io.rowweave.run.RetryAttemptClaim;
import io.rowweave.run.RetryReadyEvaluation;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDeadlineResult;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunLifecycleRef;
import io.rowweave.run.RunLifecycleState;
import io.rowweave.run.RunPurgeProgress;
import io.rowweave.run.RunRetentionClaim;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStateStoreUnavailableException;
import io.rowweave.run.WholeAttemptRetryDecision;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Deterministic tests for Run-incarnation fencing. */
class ZkRunIncarnationFencingTest {

    @Test
    @Timeout(value = 45, unit = TimeUnit.SECONDS)
    void twoFailureTransportFamilyUsesExactRetryBudget() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);

            RunSnapshot apply = created(writer, "transport-apply");
            assertTransportBudget(reader,
                    ZkRunStateStore.EvidenceReadPhase.APPLY_AFTER_INITIAL_SNAPSHOT,
                    2, () -> reader.apply(RunCommand.cancelReadyRun(
                            "cancel", apply.runId(), apply.revision(),
                            2L, "CANCELED", "transport")));

            RunSnapshot applyRecovered = created(
                    writer, "transport-apply-recovered");
            RecoveryClaim applyRecovery = writer.claim(
                    applyRecovered.runId(), applyRecovered.revision(), 0L,
                    new RecoveryOwner("transport-apply-owner", 1L));
            assertEquals(RecoveryClaim.Code.ACQUIRED, applyRecovery.code());
            assertTransportBudget(reader,
                    ZkRunStateStore.EvidenceReadPhase.APPLY_AFTER_INITIAL_SNAPSHOT,
                    2, () -> reader.applyRecovered(
                            applyRecovery.token(), RunCommand.cancelReadyRun(
                                    "cancel-recovered", applyRecovered.runId(),
                                    applyRecovered.revision(), 2L,
                                    "CANCELED", "transport")));

            ActiveAttempt cancel = runningAttempt(
                    writer, "transport-cancel", false);
            assertTransportBudget(reader,
                    ZkRunStateStore.EvidenceReadPhase.CANCEL_AFTER_INITIAL_SNAPSHOT,
                    2, () -> reader.requestUserCancellation(
                            cancel.snapshot.runId(), "request"));

            ActiveAttempt deadline = activeDueAttempt(
                    writer, "transport-deadline", false);
            assertTransportBudget(reader,
                    ZkRunStateStore.EvidenceReadPhase.DEADLINE_AFTER_INITIAL_SNAPSHOT,
                    2, () -> reader.evaluateDeadline(deadline.snapshot.runId()));

            ActiveAttempt schedule = activeAttempt(
                    writer, "transport-retry-schedule", 0L, false);
            assertTransportBudget(reader,
                    ZkRunStateStore.EvidenceReadPhase
                            .RETRY_SCHEDULE_AFTER_INITIAL_SNAPSHOT,
                    2, () -> reader.scheduleRetry(schedule.fence));

            ActiveAttempt scheduleRecovered = activeAttempt(
                    writer, "transport-retry-schedule-recovered", 0L, true);
            assertTransportBudget(reader,
                    ZkRunStateStore.EvidenceReadPhase
                            .RETRY_SCHEDULE_AFTER_INITIAL_SNAPSHOT,
                    2, () -> scheduleRetry(
                            reader, scheduleRecovered, true));

            ActiveAttempt claim = retryWaitingClaimableAttempt(
                    writer, "transport-retry-claim", false);
            assertTransportBudget(reader,
                    ZkRunStateStore.EvidenceReadPhase
                            .RETRY_CLAIM_AFTER_INITIAL_SNAPSHOT,
                    2, () -> reader.claimDueRetry(claim.snapshot.runId()));

            ActiveAttempt claimRecovered = retryWaitingClaimableAttempt(
                    writer, "transport-retry-claim-recovered", true);
            assertTransportBudget(reader,
                    ZkRunStateStore.EvidenceReadPhase
                            .RETRY_CLAIM_AFTER_INITIAL_SNAPSHOT,
                    2, () -> claimRetry(reader, claimRecovered, true));
        }
    }

    @Test
    @Timeout(value = 45, unit = TimeUnit.SECONDS)
    void oneFailureTransportFamilyFailsImmediately() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);

            ActiveAttempt ready = retryWaitingClaimableAttempt(
                    writer, "transport-retry-ready", false);
            assertTransportBudget(reader,
                    ZkRunStateStore.EvidenceReadPhase
                            .RETRY_READY_AFTER_INITIAL_SNAPSHOT,
                    1, () -> reader.evaluateRetryReady(ready.snapshot.runId()));

            RunSnapshot purgeTerminal = terminal(writer, "transport-purge");
            RetentionToken purgeToken = claim(
                    writer, purgeTerminal, "transport-purge-owner");
            assertTransportBudget(reader,
                    ZkRunStateStore.EvidenceReadPhase.PURGE_AFTER_LAYOUT_LOOKUP,
                    1, () -> reader.purge(purgeToken, 1));

            RunSnapshot recovery = created(writer, "transport-recovery-claim");
            assertTransportBudget(reader,
                    ZkRunStateStore.EvidenceReadPhase.LAYOUT_AFTER_ROOT_STAT,
                    1, () -> reader.claim(
                            recovery.runId(), recovery.revision(), 0L,
                            new RecoveryOwner("transport-owner", 1L)));
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void coreMutationAckConfirmationTransportFailsImmediately()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();

            for (boolean recovered : new boolean[] { false, true }) {
                String suffix = recovered ? "recovered" : "ordinary";
                RunSnapshot original = created(
                        writer, "ack-confirm-apply-" + suffix);
                RecoveryToken token = recovered
                        ? recoveryClaim(
                                writer, original,
                                new RecoveryOwner(
                                        "ack-confirm-apply-owner", 1L),
                                0L).token()
                        : null;
                RunCommand command = RunCommand.cancelReadyRun(
                        "ack-confirm-apply-command-" + suffix,
                        original.runId(), original.revision(),
                        2L, "CANCELED", "confirmation transport");
                assertAckConfirmationUnavailable(
                        reader,
                        ZkRunStateStore.GuardedMutationKind.APPLY_EXISTING,
                        "APPLY_EXISTING/" + suffix,
                        () -> {
                            if (recovered) {
                                reader.applyRecovered(token, command);
                            } else {
                                reader.apply(command);
                            }
                        },
                        violations);
                assertEquals(original.revision() + 1L,
                        writer.get(original.runId()).revision(),
                        "apply transaction must be durably committed");
            }

            for (UnaryAckKind kind : UnaryAckKind.values()) {
                String label = kind.name();
                ActiveAttempt attempt = kind.active(
                        writer, "ack-confirm-" + label.toLowerCase(
                                java.util.Locale.ROOT));
                assertAckConfirmationUnavailable(
                        reader, kind.mutationKind, label,
                        () -> kind.invoke(reader, attempt), violations);
                assertEquals(attempt.snapshot.revision() + 1L,
                        writer.get(attempt.snapshot.runId()).revision(),
                        label + " transaction must be durably committed");
            }

            for (boolean recovered : new boolean[] { false, true }) {
                for (RetryAckKind kind : RetryAckKind.values()) {
                    String suffix = recovered ? "recovered" : "ordinary";
                    String label = kind.name() + "/" + suffix;
                    ActiveAttempt attempt = kind.original(
                            writer, "ack-confirm-"
                                    + kind.name().toLowerCase(
                                            java.util.Locale.ROOT)
                                    + "-" + suffix,
                            recovered);
                    assertAckConfirmationUnavailable(
                            reader, kind.mutationKind, label,
                            () -> kind.invoke(reader, attempt, recovered),
                            violations);
                    assertEquals(attempt.snapshot.revision() + 1L,
                            writer.get(attempt.snapshot.runId()).revision(),
                            label + " transaction must be durably committed");
                }
            }

            assertTrue(violations.isEmpty(),
                    "ACK confirmation transport must not project cached "
                            + "business results: " + violations);
        }
    }

    @Test
    @Timeout(value = 45, unit = TimeUnit.SECONDS)
    void ownershipAndPurgeAckConfirmationTransportFailsImmediately()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();

            RunSnapshot recovery = created(
                    writer, "ack-confirm-recovery-claim");
            assertAckConfirmationUnavailable(
                    reader,
                    ZkRunStateStore.GuardedMutationKind.RECOVERY_CLAIM,
                    "RECOVERY_CLAIM",
                    () -> reader.claim(
                            recovery.runId(), recovery.revision(), 0L,
                            new RecoveryOwner(
                                    "ack-confirm-recovery-owner", 1L)),
                    violations);
            assertTrue(writer.ownership(recovery.runId()) != null,
                    "recovery ownership transaction must be committed");

            RunSnapshot retention = terminal(
                    writer, "ack-confirm-retention-claim");
            RunLifecycleRef retentionLifecycle =
                    writer.lifecycle(retention.runId());
            assertAckConfirmationUnavailable(
                    reader,
                    ZkRunStateStore.GuardedMutationKind.RETENTION_CLAIM,
                    "RETENTION_CLAIM",
                    () -> reader.claim(
                            retention.runId(), retention.revision(),
                            retentionLifecycle.terminalAtMillis(),
                            new RetentionOwner(
                                    "ack-confirm-retention-owner", 1L),
                            0L),
                    violations);
            assertEquals(RunLifecycleState.DELETING,
                    writer.lifecycle(retention.runId()).state(),
                    "retention claim transaction must be committed");

            RunSnapshot batch = terminal(writer, "ack-confirm-purge-batch");
            RetentionToken batchToken = claim(
                    writer, batch, "ack-confirm-purge-batch-owner");
            assertAckConfirmationUnavailable(
                    reader,
                    ZkRunStateStore.GuardedMutationKind.PURGE_BATCH,
                    "PURGE_BATCH",
                    () -> reader.purge(batchToken, 1),
                    violations);

            RunSnapshot finish = terminal(writer, "ack-confirm-purge-final");
            RetentionToken finalToken = claim(
                    writer, finish, "ack-confirm-purge-final-owner");
            advanceToFinalReady(writer, finalToken);
            assertAckConfirmationUnavailable(
                    reader,
                    ZkRunStateStore.GuardedMutationKind.PURGE_FINAL,
                    ZkRunStateStore.EvidenceReadPhase
                            .FINAL_ABSENCE_BEFORE_CONFIRMATION,
                    "PURGE_FINAL",
                    () -> reader.purge(finalToken, 1000),
                    violations);
            assertNull(writer.get(finish.runId()),
                    "final purge transaction must be committed");

            assertTrue(violations.isEmpty(),
                    "ownership/purge ACK confirmation transport must not "
                            + "project cached business results: "
                            + violations);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void retentionUnknownOutcomeUsesSixteenConfirmedMisses()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            RunSnapshot terminal = terminal(
                    writer, "retention-sixteen-confirmed-misses");
            RunLifecycleRef lifecycle = writer.lifecycle(terminal.runId());
            AtomicInteger attempts = new AtomicInteger();
            AtomicInteger confirmations = new AtomicInteger();
            reader.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase
                        .CORE_MISMATCH_BEFORE_CONFIRMATION) {
                    confirmations.incrementAndGet();
                }
            });
            reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind
                        .RETENTION_CLAIM
                        && moment == ZkRunStateStore.GuardedMutationMoment
                        .BEFORE_COMMIT) {
                    attempts.incrementAndGet();
                    throw new KeeperException.ConnectionLossException();
                }
            });

            assertThrows(RunStateStoreUnavailableException.class,
                    () -> reader.claim(
                            terminal.runId(), terminal.revision(),
                            lifecycle.terminalAtMillis(),
                            new RetentionOwner(
                                    "retention-sixteen-owner", 1L),
                            0L));
            assertEquals(16, attempts.get(),
                    "retention unknown outcomes must consume the exact "
                            + "bounded retry budget");
            assertEquals(16, confirmations.get(),
                    "each unknown retention write must receive a fresh, "
                            + "successful incarnation confirmation");
            assertEquals(RunLifecycleState.TERMINAL,
                    writer.lifecycle(terminal.runId()).state(),
                    "confirmed misses must leave retention ownership absent");
        }
    }

    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void everyMutationApiFailsClosedForEveryStableCoreOrphanWithoutMutation()
            throws Exception {
        List<String> violations = new ArrayList<>();
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            for (StableOrphanMutationCase mutation
                    : StableOrphanMutationCase.values()) {
                for (StableOrphanShape shape : StableOrphanShape.values()) {
                    String runId = "stable-mutation-orphan-"
                            + mutation.name().toLowerCase(
                                    java.util.Locale.ROOT)
                            + "-" + shape.name().toLowerCase(
                                    java.util.Locale.ROOT);
                    StableMutationProbe probe = mutation.setup(
                            reader, writer, runId);
                    Stat rootBefore = second.checkExists().forPath(
                            runRootPath(runId));
                    assertNotNull(rootBefore);
                    assertNotNull(second.checkExists().forPath(
                            runRootPath(runId) + "/snapshot"));
                    assertNotNull(second.checkExists().forPath(
                            lifecycleEntryPath(runId)));
                    AtomicBoolean transitioned = new AtomicBoolean();
                    AtomicReference<List<String>> before =
                            new AtomicReference<>();
                    if (shape
                            == StableOrphanShape.LAYOUT_ONLY_AFTER_BOTH) {
                        mutation.installLayoutTransition(
                                reader, second, runId, rootBefore,
                                transitioned, before);
                    } else {
                        shape.corrupt(second, runId);
                        assertStableOrphanRoot(second, runId, rootBefore);
                        before.set(authorityFingerprint(second));
                    }
                    Throwable failure = null;
                    Object returned = null;
                    try {
                        returned = probe.invoke.get();
                    } catch (Throwable caught) {
                        failure = caught;
                    } finally {
                        reader.evidenceReadObserverForTest(phase -> { });
                        reader.guardedMutationObserverForTest(
                                (kind, ordinal, moment) -> { });
                    }
                    String label = mutation + " / " + shape;
                    if (shape == StableOrphanShape.LAYOUT_ONLY_AFTER_BOTH
                            && !transitioned.get()) {
                        violations.add(label
                                + " did not transition inside the selected "
                                + "mutation call");
                    }
                    if (mutation == StableOrphanMutationCase.PURGE) {
                        if (failure != null
                                || !(returned instanceof RunPurgeProgress)
                                || ((RunPurgeProgress) returned).code()
                                        != RunPurgeProgress.Code.CORRUPT
                                || ((RunPurgeProgress) returned)
                                        .deletedRecords() != 0) {
                            violations.add(label
                                    + " did not return CORRUPT/0 (returned="
                                    + returned + ", failure=" + failure + ")");
                        }
                    } else if (!(failure instanceof IllegalStateException)
                            || failure instanceof RunStateStoreUnavailableException
                            || failure.getMessage() == null
                            || !failure.getMessage().contains(runId)) {
                        violations.add(label
                                + " did not throw a run-specific stable-corruption "
                                + "IllegalStateException (returned=" + returned
                                + ", failure=" + failure + ")");
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
                "all mutation APIs must reject stable core orphans without "
                        + "business projection, repair, or progress");
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void everyNonAssignmentMutationUsesExactD8OperationsAndRealRequestBytes()
            throws Exception {
        List<String> violations = new ArrayList<>();
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            for (StableOrphanMutationCase mutation
                    : StableOrphanMutationCase.values()) {
                if (mutation == StableOrphanMutationCase.RETRY_READY) continue;
                String runId = "d8-core-" + mutation.name().toLowerCase(
                        java.util.Locale.ROOT);
                StableMutationProbe probe = mutation.setup(
                        store, store, runId);
                TransactionWrites writes = new TransactionWrites();
                store.transactionWriteObserverForTest(writes);
                Throwable failure = null;
                Object returned = null;
                try {
                    returned = probe.invoke.get();
                } catch (Throwable caught) {
                    failure = caught;
                }
                if (failure != null) {
                    violations.add(mutation + " failed before accounting: "
                            + failure);
                    continue;
                }
                if (writes.records.size() != 1) {
                    violations.add(mutation + " expected one write, got "
                            + writes.records);
                    continue;
                }
                if (mutation == StableOrphanMutationCase.PURGE) {
                    RunPurgeProgress progress = (RunPurgeProgress) returned;
                    if (progress.code() != RunPurgeProgress.Code.IN_PROGRESS
                            || progress.deletedRecords() != 1) {
                        violations.add("PURGE formula fixture expected "
                                + "IN_PROGRESS/deletedRecords=1, got "
                                + progress.code() + "/"
                                + progress.deletedRecords());
                    }
                }
                TransactionWrite actual = writes.records.get(0);
                int expected = expectedD8Operations(mutation);
                if (actual.kind != mutation.guardedKind()
                        || actual.ordinal != -1
                        || actual.operationCount != expected
                        || actual.requestBytes <= 0
                        || actual.requestBytes >= 1_000_000) {
                    violations.add(mutation + " expected "
                            + mutation.guardedKind() + "[-1] ops=" + expected
                            + " and 0<realBytes<1000000, got " + actual);
                }
            }
        }
        assertEquals(Collections.emptyList(), violations,
                "every non-assignment mutation must match the frozen publication rule using "
                        + "a real MultiOperationRecord byte count");
    }

    private static int expectedD8Operations(
            StableOrphanMutationCase mutation) {
        switch (mutation) {
            case APPLY:
                return 8;
            case APPLY_RECOVERED:
                return 9;
            case CANCEL:
            case DEADLINE:
                return 7;
            case RETRY_SCHEDULE:
                return 9;
            case RETRY_SCHEDULE_RECOVERED:
                return 10;
            case RETRY_CLAIM:
                return 8;
            case RETRY_CLAIM_RECOVERED:
                return 9;
            case RECOVERY_CLAIM:
                return 5;
            case RETENTION_CLAIM:
                return 3;
            case PURGE:
                return 5;
            default:
                throw new AssertionError(
                        "no D8 operation formula for " + mutation);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void cancellationAckLossReplaysWhileOriginalAuthorityRemains()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            ActiveAttempt original = runningAttempt(
                    writer, "cancel-ack-original", false);
            AtomicBoolean ackLost = new AtomicBoolean();
            reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.CANCEL
                        && moment == ZkRunStateStore.GuardedMutationMoment
                        .AFTER_COMMIT_BEFORE_ACK
                        && ackLost.compareAndSet(false, true)) {
                    throw new KeeperException.ConnectionLossException();
                }
            });

            RunCancellationResult replay = reader.requestUserCancellation(
                    original.snapshot.runId(), "shared-request");

            assertTrue(ackLost.get());
            assertEquals(RunCancellationResult.Disposition.ALREADY_REQUESTED,
                    replay.disposition());
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void cancellationAckLossCannotReplayEquivalentReplacementReceipt()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            ActiveAttempt original = runningAttempt(
                    writer, "cancel-ack-replacement", false);
            AtomicBoolean replaced = new AtomicBoolean();
            reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.CANCEL
                        && moment == ZkRunStateStore.GuardedMutationMoment
                        .AFTER_COMMIT_BEFORE_ACK
                        && replaced.compareAndSet(false, true)) {
                    purgeCancelingRun(writer, original);
                    ActiveAttempt replacement = runningAttempt(
                            writer, original.snapshot.runId(), false);
                    assertEquals(RunCancellationResult.Disposition.ACCEPTED,
                            writer.requestUserCancellation(
                                    replacement.snapshot.runId(),
                                    "shared-request").disposition());
                    throw new KeeperException.ConnectionLossException();
                }
            });

            RunCancellationResult stale = reader.requestUserCancellation(
                    original.snapshot.runId(), "shared-request");

            assertTrue(replaced.get());
            assertEquals(RunCancellationResult.Disposition.NOT_FOUND,
                    stale.disposition());
            assertNull(stale.status());
            assertEquals(0L, stale.revision());
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void deadlineAckLossReplaysWhileOriginalAuthorityRemains()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            ActiveAttempt attempt = activeDueAttempt(
                    writer, "deadline-ack-original", false);
            AtomicBoolean beforeCommit = new AtomicBoolean();
            AtomicBoolean ackLost = new AtomicBoolean();
            reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.DEADLINE) {
                    if (moment == ZkRunStateStore.GuardedMutationMoment
                            .BEFORE_COMMIT) {
                        beforeCommit.set(true);
                    } else if (ackLost.compareAndSet(false, true)) {
                        throw new KeeperException.ConnectionLossException();
                    }
                }
            });

            RunDeadlineResult replay =
                    reader.evaluateDeadline(attempt.snapshot.runId());

            assertTrue(beforeCommit.get(), "DEADLINE must reach BEFORE_COMMIT");
            assertTrue(ackLost.get(),
                    "DEADLINE must reach AFTER_COMMIT_BEFORE_ACK");
            assertEquals(RunDeadlineResult.Disposition.ALREADY_REQUESTED,
                    replay.disposition());
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void deadlineAckLossCannotReplayEquivalentReplacementReceipt()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            ActiveAttempt original = activeDueAttempt(
                    writer, "deadline-ack-replacement", false);
            AtomicBoolean beforeCommit = new AtomicBoolean();
            AtomicBoolean replaced = new AtomicBoolean();
            reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.DEADLINE) {
                    if (moment == ZkRunStateStore.GuardedMutationMoment
                            .BEFORE_COMMIT) {
                        beforeCommit.set(true);
                    } else if (replaced.compareAndSet(false, true)) {
                        purgeCancelingRun(writer, original);
                        ActiveAttempt replacement = activeDueAttempt(
                                writer, original.snapshot.runId(), false);
                        assertEquals(RunDeadlineResult.Disposition.ACCEPTED,
                                writer.evaluateDeadline(replacement.snapshot
                                        .runId()).disposition());
                        throw new KeeperException.ConnectionLossException();
                    }
                }
            });

            RunDeadlineResult stale =
                    reader.evaluateDeadline(original.snapshot.runId());

            assertTrue(beforeCommit.get(), "DEADLINE must reach BEFORE_COMMIT");
            assertTrue(replaced.get(),
                    "DEADLINE must reach AFTER_COMMIT_BEFORE_ACK");
            assertDeadlineMissing(stale);
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void cancellationAndDeadlineAckLossFenceEveryReplacementState()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (UnaryAckKind kind : UnaryAckKind.values()) {
                for (ApplyReplacementState state
                        : ApplyReplacementState.values()) {
                    String runId = "unary-ack-state-"
                            + kind.name().toLowerCase(java.util.Locale.ROOT)
                            + "-" + state.name().toLowerCase(
                                    java.util.Locale.ROOT);
                    ActiveAttempt original = kind.active(writer, runId);
                    AtomicBoolean ackLost = new AtomicBoolean();
                    CountDownLatch layoutReady = new CountDownLatch(1);
                    CountDownLatch allowCreate = new CountDownLatch(1);
                    AtomicReference<Thread> creator = new AtomicReference<>();
                    AtomicReference<Throwable> createFailure =
                            new AtomicReference<>();
                    reader.guardedMutationObserverForTest(
                            (observed, ordinal, moment) -> {
                        if (observed == kind.mutationKind
                                && moment == ZkRunStateStore
                                .GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK
                                && ackLost.compareAndSet(false, true)) {
                            purgeCancelingRun(writer, original);
                            if (state == ApplyReplacementState.PREPUBLICATION) {
                                AtomicBoolean paused = new AtomicBoolean();
                                writer.evidenceReadObserverForTest(phase -> {
                                    if (phase == ZkRunStateStore.EvidenceReadPhase
                                            .APPLY_AFTER_INITIAL_SNAPSHOT
                                            && paused.compareAndSet(
                                                    false, true)) {
                                        layoutReady.countDown();
                                        if (!allowCreate.await(
                                                10, TimeUnit.SECONDS)) {
                                            throw new AssertionError(
                                                    "replacement CREATE "
                                                            + "release timed out");
                                        }
                                    }
                                });
                                Thread thread = new Thread(() -> {
                                    try {
                                        created(writer, runId);
                                    } catch (Throwable failure) {
                                        createFailure.set(failure);
                                    }
                                }, "zk-unary-prepublication-" + kind);
                                creator.set(thread);
                                thread.start();
                                if (!layoutReady.await(
                                        10, TimeUnit.SECONDS)) {
                                    throw new AssertionError(
                                            "replacement layout not published");
                                }
                            } else if (state
                                    != ApplyReplacementState.PURGED) {
                                ActiveAttempt replacement =
                                        kind.active(writer, runId);
                                kind.invoke(writer, replacement);
                                if (state
                                        == ApplyReplacementState.TERMINAL) {
                                    finishCancelingRun(writer, replacement);
                                }
                            }
                            throw new KeeperException
                                    .ConnectionLossException();
                        }
                    });

                    Object result = null;
                    RuntimeException failure = null;
                    try {
                        result = kind.invoke(reader, original);
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

                    assertTrue(ackLost.get(), kind + "/" + state
                            + " must reach AFTER_COMMIT_BEFORE_ACK");
                    if (failure != null) {
                        violations.add(kind + "/" + state + " threw "
                                + failure.getClass().getSimpleName());
                    } else if (!kind.isMissing(result)) {
                        violations.add(kind + "/" + state + " returned "
                                + kind.resultCode(result));
                    }
                    if (createFailure.get() != null) {
                        violations.add(kind + "/" + state
                                + " replacement CREATE threw "
                                + createFailure.get().getClass().getSimpleName());
                    }
                    try {
                        RunSnapshot replacement = writer.get(runId);
                        if (state == ApplyReplacementState.PURGED) {
                            if (replacement != null) {
                                violations.add(kind + "/" + state
                                        + " recreated revision "
                                        + replacement.revision());
                            }
                        } else if (replacement == null) {
                            violations.add(kind + "/" + state
                                    + " replacement is missing");
                        } else {
                            long expectedRevision = state
                                    == ApplyReplacementState.PREPUBLICATION
                                    ? 1L : state
                                            == ApplyReplacementState.ACTIVE
                                            ? 5L : 6L;
                            if (replacement.revision() != expectedRevision) {
                                violations.add(kind + "/" + state
                                        + " replacement revision="
                                        + replacement.revision());
                            }
                        }
                    } catch (RuntimeException inspectionFailure) {
                        violations.add(kind + "/" + state
                                + " replacement inspection threw "
                                + inspectionFailure.getClass().getSimpleName());
                    }
                }
            }
            assertTrue(violations.isEmpty(),
                    "cancellation/deadline ACK reconciliation must fence every "
                            + "replacement state: " + violations);
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void retryAckLossReplaysWhileOriginalAuthorityRemains()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
            CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (boolean recovered : new boolean[] { false, true }) {
                ActiveAttempt schedule = activeAttempt(
                        writer, "schedule-ack-original-" + recovered,
                        0L, recovered);
                AtomicBoolean scheduleBeforeCommit = new AtomicBoolean();
                AtomicBoolean scheduleAckLost = new AtomicBoolean();
                reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                    if (kind == ZkRunStateStore.GuardedMutationKind
                            .RETRY_SCHEDULE) {
                        if (moment == ZkRunStateStore.GuardedMutationMoment
                                .BEFORE_COMMIT) {
                            scheduleBeforeCommit.set(true);
                        } else if (scheduleAckLost.compareAndSet(false, true)) {
                            throw new KeeperException.ConnectionLossException();
                        }
                    }
                });
                WholeAttemptRetryDecision scheduleReplay = scheduleRetry(
                        reader, schedule, recovered);
                assertTrue(scheduleBeforeCommit.get(),
                        "RETRY_SCHEDULE must reach BEFORE_COMMIT");
                assertTrue(scheduleAckLost.get(),
                        "RETRY_SCHEDULE must reach AFTER_COMMIT_BEFORE_ACK");
                assertEquals(WholeAttemptRetryDecision.Code.ALREADY_SCHEDULED,
                        scheduleReplay.code());

                ActiveAttempt claim = retryWaitingClaimableAttempt(
                        writer, "claim-ack-original-" + recovered, recovered);
                AtomicBoolean claimBeforeCommit = new AtomicBoolean();
                AtomicBoolean claimAckLost = new AtomicBoolean();
                reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                    if (kind == ZkRunStateStore.GuardedMutationKind.RETRY_CLAIM) {
                        if (moment == ZkRunStateStore.GuardedMutationMoment
                                .BEFORE_COMMIT) {
                            claimBeforeCommit.set(true);
                        } else if (claimAckLost.compareAndSet(false, true)) {
                            throw new KeeperException.ConnectionLossException();
                        }
                    }
                });
                RetryAttemptClaim claimReplay = claimRetry(
                        reader, claim, recovered);
                assertTrue(claimBeforeCommit.get(),
                        "RETRY_CLAIM must reach BEFORE_COMMIT");
                assertTrue(claimAckLost.get(),
                        "RETRY_CLAIM must reach AFTER_COMMIT_BEFORE_ACK");
                assertEquals(RetryAttemptClaim.Code.ALREADY_CLAIMED,
                        claimReplay.code());
            }
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void retryAckLossCannotReplayEquivalentReplacementAuthority()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (boolean recovered : new boolean[] { false, true }) {
                String scheduleRun = "schedule-ack-replacement-" + recovered;
                ActiveAttempt schedule = activeAttempt(
                        writer, scheduleRun, 0L, recovered);
                AtomicBoolean scheduleBeforeCommit = new AtomicBoolean();
                AtomicBoolean scheduleReplaced = new AtomicBoolean();
                reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                    if (kind == ZkRunStateStore.GuardedMutationKind
                            .RETRY_SCHEDULE) {
                        if (moment == ZkRunStateStore.GuardedMutationMoment
                                .BEFORE_COMMIT) {
                            scheduleBeforeCommit.set(true);
                        } else if (scheduleReplaced.compareAndSet(false, true)) {
                            purgeRetryWaitingRun(writer, schedule);
                            ActiveAttempt replacement = activeAttempt(
                                    writer, scheduleRun, 0L, recovered);
                            assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                                    scheduleRetry(writer, replacement,
                                            recovered).code());
                            throw new KeeperException.ConnectionLossException();
                        }
                    }
                });
                WholeAttemptRetryDecision scheduleResult = scheduleRetry(
                        reader, schedule, recovered);
                assertTrue(scheduleBeforeCommit.get(),
                        "RETRY_SCHEDULE must reach BEFORE_COMMIT");
                assertTrue(scheduleReplaced.get(),
                        "RETRY_SCHEDULE must reach AFTER_COMMIT_BEFORE_ACK");
                if (scheduleResult.code()
                        != WholeAttemptRetryDecision.Code.NOT_FOUND) {
                    violations.add("schedule/" + recovered + "="
                            + scheduleResult.code());
                } else {
                    assertRetryScheduleMissing(scheduleResult);
                }

                String claimRun = "claim-ack-replacement-" + recovered;
                ActiveAttempt claim = retryWaitingClaimableAttempt(
                        writer, claimRun, recovered);
                AtomicBoolean claimBeforeCommit = new AtomicBoolean();
                AtomicBoolean claimReplaced = new AtomicBoolean();
                reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                    if (kind == ZkRunStateStore.GuardedMutationKind.RETRY_CLAIM) {
                        if (moment == ZkRunStateStore.GuardedMutationMoment
                                .BEFORE_COMMIT) {
                            claimBeforeCommit.set(true);
                        } else if (claimReplaced.compareAndSet(false, true)) {
                            purgeClaimedRetryRun(writer, claim);
                            ActiveAttempt replacement =
                                    retryWaitingClaimableAttempt(
                                            writer, claimRun, recovered);
                            assertEquals(RetryAttemptClaim.Code.CLAIMED,
                                    claimRetry(writer, replacement,
                                            recovered).code());
                            throw new KeeperException.ConnectionLossException();
                        }
                    }
                });
                RetryAttemptClaim claimResult = claimRetry(
                        reader, claim, recovered);
                assertTrue(claimBeforeCommit.get(),
                        "RETRY_CLAIM must reach BEFORE_COMMIT");
                assertTrue(claimReplaced.get(),
                        "RETRY_CLAIM must reach AFTER_COMMIT_BEFORE_ACK");
                if (claimResult.code() != RetryAttemptClaim.Code.NOT_FOUND) {
                    violations.add("claim/" + recovered + "="
                            + claimResult.code());
                } else {
                    assertRetryClaimMissing(claimResult);
                }
            }
            assertTrue(violations.isEmpty(),
                    "retry reconciliation must reject replacement B: " + violations);
        }
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void retryAckLossFencesEveryReplacementState() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (boolean recovered : new boolean[] { false, true }) {
                for (RetryAckKind kind : RetryAckKind.values()) {
                    for (ApplyReplacementState state
                            : ApplyReplacementState.values()) {
                        String runId = "retry-ack-state-"
                                + kind.name().toLowerCase(java.util.Locale.ROOT)
                                + "-" + state.name().toLowerCase(
                                        java.util.Locale.ROOT)
                                + (recovered
                                        ? "-recovered" : "-ordinary");
                        ActiveAttempt original =
                                kind.original(writer, runId, recovered);
                        AtomicBoolean ackLost = new AtomicBoolean();
                        CountDownLatch layoutReady = new CountDownLatch(1);
                        CountDownLatch allowCreate = new CountDownLatch(1);
                        AtomicReference<Thread> creator =
                                new AtomicReference<>();
                        AtomicReference<Throwable> createFailure =
                                new AtomicReference<>();
                        reader.guardedMutationObserverForTest(
                                (observed, ordinal, moment) -> {
                            if (observed == kind.mutationKind
                                    && moment == ZkRunStateStore
                                    .GuardedMutationMoment
                                    .AFTER_COMMIT_BEFORE_ACK
                                    && ackLost.compareAndSet(false, true)) {
                                kind.purgeOriginal(writer, original);
                                if (state == ApplyReplacementState
                                        .PREPUBLICATION) {
                                    AtomicBoolean paused = new AtomicBoolean();
                                    writer.evidenceReadObserverForTest(phase -> {
                                        if (phase == ZkRunStateStore
                                                .EvidenceReadPhase
                                                .APPLY_AFTER_INITIAL_SNAPSHOT
                                                && paused.compareAndSet(
                                                        false, true)) {
                                            layoutReady.countDown();
                                            if (!allowCreate.await(
                                                    10, TimeUnit.SECONDS)) {
                                                throw new AssertionError(
                                                        "replacement CREATE "
                                                                + "release "
                                                                + "timed out");
                                            }
                                        }
                                    });
                                    Thread thread = new Thread(() -> {
                                        try {
                                            created(writer, runId);
                                        } catch (Throwable failure) {
                                            createFailure.set(failure);
                                        }
                                    }, "zk-retry-prepublication-" + kind
                                            + "-" + recovered);
                                    creator.set(thread);
                                    thread.start();
                                    if (!layoutReady.await(
                                            10, TimeUnit.SECONDS)) {
                                        throw new AssertionError(
                                                "replacement layout not "
                                                        + "published");
                                    }
                                } else if (state
                                        != ApplyReplacementState.PURGED) {
                                    ActiveAttempt replacement = kind.original(
                                            writer, runId, recovered);
                                    kind.invoke(writer, replacement, recovered);
                                    if (state
                                            == ApplyReplacementState.TERMINAL) {
                                        kind.finish(writer, replacement);
                                    }
                                }
                                throw new KeeperException
                                        .ConnectionLossException();
                            }
                        });

                        Object result = null;
                        RuntimeException failure = null;
                        try {
                            result = kind.invoke(
                                    reader, original, recovered);
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

                        assertTrue(ackLost.get(), kind + "/" + state + "/"
                                + recovered
                                + " must reach AFTER_COMMIT_BEFORE_ACK");
                        if (failure != null) {
                            violations.add(kind + "/" + state + "/"
                                    + recovered + " threw "
                                    + failure.getClass().getSimpleName());
                        } else if (!kind.isMissing(result)) {
                            violations.add(kind + "/" + state + "/"
                                    + recovered + " returned "
                                    + kind.resultCode(result));
                        }
                        if (createFailure.get() != null) {
                            violations.add(kind + "/" + state + "/"
                                    + recovered + " replacement CREATE threw "
                                    + createFailure.get().getClass()
                                            .getSimpleName());
                        }
                        try {
                            RunSnapshot replacement = writer.get(runId);
                            if (state == ApplyReplacementState.PURGED) {
                                if (replacement != null) {
                                    violations.add(kind + "/" + state + "/"
                                            + recovered + " recreated revision "
                                            + replacement.revision());
                                }
                            } else if (replacement == null) {
                                violations.add(kind + "/" + state + "/"
                                        + recovered + " replacement missing");
                            } else {
                                long expectedRevision = state
                                        == ApplyReplacementState
                                        .PREPUBLICATION
                                        ? 1L : state
                                                == ApplyReplacementState.ACTIVE
                                                ? kind.activeRevision
                                                : kind.activeRevision + 1L;
                                if (replacement.revision()
                                        != expectedRevision) {
                                    violations.add(kind + "/" + state + "/"
                                            + recovered
                                            + " replacement revision="
                                            + replacement.revision());
                                }
                            }
                        } catch (RuntimeException inspectionFailure) {
                            violations.add(kind + "/" + state + "/"
                                    + recovered + " inspection threw "
                                    + inspectionFailure.getClass()
                                            .getSimpleName());
                        }
                    }
                }
            }
            assertTrue(violations.isEmpty(),
                    "retry ACK reconciliation must fence every replacement "
                            + "state: " + violations);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void recoveryClaimAckLossReplaysWhileOriginalAuthorityRemains()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            RunSnapshot original = created(writer, "recovery-ack-original");
            RecoveryOwner owner = new RecoveryOwner("stable-owner", 7L);
            AtomicBoolean ackLost = new AtomicBoolean();
            reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.RECOVERY_CLAIM
                        && moment == ZkRunStateStore.GuardedMutationMoment
                        .AFTER_COMMIT_BEFORE_ACK
                        && ackLost.compareAndSet(false, true)) {
                    throw new KeeperException.ConnectionLossException();
                }
            });

            RecoveryClaim replay = reader.claim(
                    original.runId(), original.revision(), 0L, owner);

            assertTrue(ackLost.get());
            assertEquals(RecoveryClaim.Code.REPLAYED, replay.code());
        }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void recoveryClaimAckLossCannotReplayEquivalentReplacementToken()
            throws Exception {
        for (int round = 0; round < 16; round++) {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            RunSnapshot original = created(
                    writer, "recovery-ack-replacement-" + round);
            RecoveryOwner owner = new RecoveryOwner("stable-owner", 7L);
            AtomicBoolean replaced = new AtomicBoolean();
            AtomicReference<List<String>> replacementFingerprint =
                    new AtomicReference<>();
            reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.RECOVERY_CLAIM
                        && moment == ZkRunStateStore.GuardedMutationMoment
                        .AFTER_COMMIT_BEFORE_ACK
                        && replaced.compareAndSet(false, true)) {
                    RecoveryClaim replayed = writer.claim(
                            original.runId(), original.revision(), 0L, owner);
                    assertEquals(RecoveryClaim.Code.REPLAYED, replayed.code());
                    RecoveryToken originalToken = replayed.token();
                    RunSnapshot terminal = acceptedRecovered(
                            writer, originalToken, RunCommand.cancelReadyRun(
                                    "retire-recovery-ack", original.runId(),
                                    original.revision(), 2L,
                                    "CANCELED", "replacement setup"));
                    RetentionToken retention = claim(
                            writer, terminal, "recovery-ack-retention");
                    purgeAll(writer, retention);
                    RunSnapshot replacement = created(writer, original.runId());
                    RecoveryToken replacementToken = recoveryClaim(
                            writer, replacement, owner, 0L).token();
                    assertEquals(originalToken, replacementToken,
                            "fixture must recreate the exact public recovery token");
                    replacementFingerprint.set(
                            runSubtreeFingerprint(second, original.runId()));
                    throw new KeeperException.ConnectionLossException();
                }
            });

            RecoveryClaim stale = reader.claim(
                    original.runId(), original.revision(), 0L, owner);

            assertTrue(replaced.get());
            assertEquals(replacementFingerprint.get(),
                    runSubtreeFingerprint(second, original.runId()),
                    "old recovery claim must not mutate replacement B");
            assertEquals(RecoveryClaim.Code.STALE_RUN, stale.code());
            assertNull(stale.token());
        }
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void recoveryClaimAckLossFencesEveryReplacementState()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (ApplyReplacementState state
                    : ApplyReplacementState.values()) {
                String runId = "recovery-ack-state-"
                        + state.name().toLowerCase(java.util.Locale.ROOT);
                RunSnapshot original = created(writer, runId);
                RecoveryOwner owner = new RecoveryOwner(
                        "recovery-state-owner", 1L);
                AtomicBoolean ackLost = new AtomicBoolean();
                CountDownLatch layoutReady = new CountDownLatch(1);
                CountDownLatch allowCreate = new CountDownLatch(1);
                AtomicReference<Thread> creator = new AtomicReference<>();
                AtomicReference<Throwable> createFailure =
                        new AtomicReference<>();
                reader.guardedMutationObserverForTest(
                        (kind, ordinal, moment) -> {
                    if (kind == ZkRunStateStore.GuardedMutationKind
                            .RECOVERY_CLAIM
                            && moment == ZkRunStateStore
                            .GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK
                            && ackLost.compareAndSet(false, true)) {
                        RecoveryClaim replay = writer.claim(
                                runId, original.revision(), 0L, owner);
                        assertEquals(RecoveryClaim.Code.REPLAYED,
                                replay.code());
                        RunSnapshot oldTerminal = acceptedRecovered(
                                writer, replay.token(),
                                RunCommand.cancelReadyRun(
                                        "recovery-state-cancel", runId,
                                        original.revision(), 2L,
                                        "CANCELED", "replacement setup"));
                        RetentionToken retention = claim(
                                writer, oldTerminal,
                                "recovery-state-retention");
                        purgeAll(writer, retention);
                        if (state == ApplyReplacementState.PREPUBLICATION) {
                            AtomicBoolean paused = new AtomicBoolean();
                            writer.evidenceReadObserverForTest(phase -> {
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
                                    created(writer, runId);
                                } catch (Throwable failure) {
                                    createFailure.set(failure);
                                }
                            }, "zk-recovery-prepublication");
                            creator.set(thread);
                            thread.start();
                            if (!layoutReady.await(10, TimeUnit.SECONDS)) {
                                throw new AssertionError(
                                        "replacement layout not published");
                            }
                        } else if (state == ApplyReplacementState.ACTIVE) {
                            created(writer, runId);
                        } else if (state
                                == ApplyReplacementState.TERMINAL) {
                            terminal(writer, runId);
                        }
                        throw new KeeperException.ConnectionLossException();
                    }
                });

                RecoveryClaim result = null;
                RuntimeException failure = null;
                try {
                    result = reader.claim(
                            runId, original.revision(), 0L, owner);
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
                if (failure != null) {
                    violations.add(state + " threw "
                            + failure.getClass().getSimpleName());
                } else if (result.code() != RecoveryClaim.Code.STALE_RUN
                        || result.token() != null) {
                    violations.add(state + " returned " + result.code());
                }
                if (createFailure.get() != null) {
                    violations.add(state + " replacement CREATE threw "
                            + createFailure.get().getClass().getSimpleName());
                }
                RunSnapshot replacement = writer.get(runId);
                if (state == ApplyReplacementState.PURGED) {
                    if (replacement != null) {
                        violations.add(state + " recreated revision "
                                + replacement.revision());
                    }
                } else if (replacement == null) {
                    violations.add(state + " replacement missing");
                } else {
                    long expectedRevision = state
                            == ApplyReplacementState.TERMINAL ? 2L : 1L;
                    if (replacement.revision() != expectedRevision) {
                        violations.add(state + " replacement revision="
                                + replacement.revision());
                    }
                    if (writer.ownership(runId) != null) {
                        violations.add(state
                                + " replacement gained recovery ownership");
                    }
                }
            }
            assertTrue(violations.isEmpty(),
                    "recovery ACK reconciliation must fence every replacement "
                            + "state: " + violations);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void retentionClaimAckLossReplaysWhileOriginalAuthorityRemains()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            RunSnapshot original = terminal(writer, "retention-ack-original");
            RunLifecycleRef lifecycle = writer.lifecycle(original.runId());
            RetentionOwner owner = new RetentionOwner("stable-owner", 7L);
            AtomicBoolean ackLost = new AtomicBoolean();
            reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.RETENTION_CLAIM
                        && moment == ZkRunStateStore.GuardedMutationMoment
                        .AFTER_COMMIT_BEFORE_ACK
                        && ackLost.compareAndSet(false, true)) {
                    throw new KeeperException.ConnectionLossException();
                }
            });

            RunRetentionClaim replay = reader.claim(
                    original.runId(), original.revision(),
                    lifecycle.terminalAtMillis(), owner, 0L);

            assertTrue(ackLost.get());
            assertEquals(RunRetentionClaim.Code.REPLAYED, replay.code());
        }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void retentionClaimAckLossCannotReplayReplacementAuthority()
            throws Exception {
        for (int round = 0; round < 16; round++) {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            RunSnapshot original = terminal(
                    writer, "retention-ack-replacement-" + round);
            RunLifecycleRef originalLifecycle = writer.lifecycle(original.runId());
            RetentionOwner owner = new RetentionOwner("stable-owner", 7L);
            AtomicBoolean replaced = new AtomicBoolean();
            AtomicReference<List<String>> replacementFingerprint =
                    new AtomicReference<>();
            AtomicReference<List<String>> replacementAuthorityFingerprint =
                    new AtomicReference<>();
            reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.RETENTION_CLAIM
                        && moment == ZkRunStateStore.GuardedMutationMoment
                        .AFTER_COMMIT_BEFORE_ACK
                        && replaced.compareAndSet(false, true)) {
                    RunRetentionClaim replayed = writer.claim(
                            original.runId(), original.revision(),
                            originalLifecycle.terminalAtMillis(), owner, 0L);
                    assertEquals(RunRetentionClaim.Code.REPLAYED, replayed.code());
                    purgeAll(writer, replayed.token());
                    while (System.currentTimeMillis()
                            <= originalLifecycle.terminalAtMillis()) {
                        Thread.yield();
                    }
                    RunSnapshot replacement = terminal(writer, original.runId());
                    RunLifecycleRef replacementLifecycle =
                            writer.lifecycle(original.runId());
                    RunRetentionClaim replacementClaim = writer.claim(
                            replacement.runId(), replacement.revision(),
                            replacementLifecycle.terminalAtMillis(), owner, 0L);
                    assertEquals(RunRetentionClaim.Code.ACQUIRED,
                            replacementClaim.code());
                    assertTrue(replacementLifecycle.terminalAtMillis()
                            > originalLifecycle.terminalAtMillis());
                    assertNotEquals(replayed.token(), replacementClaim.token(),
                            "fixture must use distinct A/B retention tokens");
                    replacementFingerprint.set(
                            runSubtreeFingerprint(second, original.runId()));
                    replacementAuthorityFingerprint.set(
                            authorityFingerprint(second));
                    throw new KeeperException.ConnectionLossException();
                }
            });

            RunRetentionClaim stale = reader.claim(
                    original.runId(), original.revision(),
                    originalLifecycle.terminalAtMillis(), owner, 0L);

            assertTrue(replaced.get());
            assertEquals(replacementFingerprint.get(),
                    runSubtreeFingerprint(second, original.runId()),
                    "old retention claim must not mutate replacement B");
            assertEquals(replacementAuthorityFingerprint.get(),
                    authorityFingerprint(second),
                    "old retention claim must not mutate B lifecycle authority");
            assertEquals(RunRetentionClaim.Code.STALE_RUN, stale.code());
            assertNull(stale.token());
        }
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void retentionClaimAckLossFencesEveryReplacementState()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (ApplyReplacementState state
                    : ApplyReplacementState.values()) {
                String runId = "retention-ack-state-"
                        + state.name().toLowerCase(java.util.Locale.ROOT);
                RunSnapshot original = terminal(writer, runId);
                RunLifecycleRef lifecycle = writer.lifecycle(runId);
                RetentionOwner owner = new RetentionOwner(
                        "retention-state-owner", 1L);
                AtomicBoolean ackLost = new AtomicBoolean();
                CountDownLatch layoutReady = new CountDownLatch(1);
                CountDownLatch allowCreate = new CountDownLatch(1);
                AtomicReference<Thread> creator = new AtomicReference<>();
                AtomicReference<Throwable> createFailure =
                        new AtomicReference<>();
                reader.guardedMutationObserverForTest(
                        (kind, ordinal, moment) -> {
                    if (kind == ZkRunStateStore.GuardedMutationKind
                            .RETENTION_CLAIM
                            && moment == ZkRunStateStore
                            .GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK
                            && ackLost.compareAndSet(false, true)) {
                        RunRetentionClaim replay = writer.claim(
                                runId, original.revision(),
                                lifecycle.terminalAtMillis(), owner, 0L);
                        assertEquals(RunRetentionClaim.Code.REPLAYED,
                                replay.code());
                        purgeAll(writer, replay.token());
                        if (state == ApplyReplacementState.PREPUBLICATION) {
                            AtomicBoolean paused = new AtomicBoolean();
                            writer.evidenceReadObserverForTest(phase -> {
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
                                    created(writer, runId);
                                } catch (Throwable failure) {
                                    createFailure.set(failure);
                                }
                            }, "zk-retention-prepublication");
                            creator.set(thread);
                            thread.start();
                            if (!layoutReady.await(10, TimeUnit.SECONDS)) {
                                throw new AssertionError(
                                        "replacement layout not published");
                            }
                        } else if (state == ApplyReplacementState.ACTIVE) {
                            created(writer, runId);
                        } else if (state
                                == ApplyReplacementState.TERMINAL) {
                            terminal(writer, runId);
                        }
                        throw new KeeperException.ConnectionLossException();
                    }
                });

                RunRetentionClaim result = null;
                RuntimeException failure = null;
                try {
                    result = reader.claim(
                            runId, original.revision(),
                            lifecycle.terminalAtMillis(), owner, 0L);
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
                if (failure != null) {
                    violations.add(state + " threw "
                            + failure.getClass().getSimpleName());
                } else if (result.code()
                        != RunRetentionClaim.Code.STALE_RUN
                        || result.token() != null) {
                    violations.add(state + " returned " + result.code());
                }
                if (createFailure.get() != null) {
                    violations.add(state + " replacement CREATE threw "
                            + createFailure.get().getClass().getSimpleName());
                }
                RunSnapshot replacement = writer.get(runId);
                if (state == ApplyReplacementState.PURGED) {
                    if (replacement != null) {
                        violations.add(state + " recreated revision "
                                + replacement.revision());
                    }
                } else if (replacement == null) {
                    violations.add(state + " replacement missing");
                } else {
                    long expectedRevision = state
                            == ApplyReplacementState.TERMINAL ? 2L : 1L;
                    if (replacement.revision() != expectedRevision) {
                        violations.add(state + " replacement revision="
                                + replacement.revision());
                    }
                    RunLifecycleState expectedLifecycle = state
                            == ApplyReplacementState.TERMINAL
                            ? RunLifecycleState.TERMINAL
                            : RunLifecycleState.ACTIVE;
                    if (writer.lifecycle(runId).state()
                            != expectedLifecycle) {
                        violations.add(state + " replacement lifecycle="
                                + writer.lifecycle(runId).state());
                    }
                }
            }
            assertTrue(violations.isEmpty(),
                    "retention ACK reconciliation must fence every replacement "
                            + "state: " + violations);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void deadlineInitialSnapshotCannotEscapeFinalPurge() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            RunSnapshot terminal = terminal(writer, "deadline-initial");
            installFinalPurge(reader, writer, terminal,
                    ZkRunStateStore.EvidenceReadPhase.DEADLINE_AFTER_INITIAL_SNAPSHOT);

            RunDeadlineResult missing = reader.evaluateDeadline(terminal.runId());

            assertDeadlineMissing(missing);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void retryScheduleInitialSnapshotCannotEscapeFinalPurge() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            TerminalAttempt terminal = terminalAttempt(writer, "schedule-initial");
            installFinalPurge(reader, writer, terminal.snapshot,
                    ZkRunStateStore.EvidenceReadPhase
                            .RETRY_SCHEDULE_AFTER_INITIAL_SNAPSHOT);

            WholeAttemptRetryDecision missing = reader.scheduleRetry(terminal.fence);

            assertRetryScheduleMissing(missing);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void retryReadyInitialSnapshotCannotEscapeFinalPurge() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            TerminalAttempt terminal = terminalAttempt(writer, "ready-initial");
            installFinalPurge(reader, writer, terminal.snapshot,
                    ZkRunStateStore.EvidenceReadPhase
                            .RETRY_READY_AFTER_INITIAL_SNAPSHOT);

            RetryReadyEvaluation missing =
                    reader.evaluateRetryReady(terminal.snapshot.runId());

            assertRetryReadyMissing(missing);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void retryClaimInitialSnapshotCannotEscapeFinalPurge() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            TerminalAttempt terminal = terminalAttempt(writer, "claim-initial");
            installFinalPurge(reader, writer, terminal.snapshot,
                    ZkRunStateStore.EvidenceReadPhase
                            .RETRY_CLAIM_AFTER_INITIAL_SNAPSHOT);

            RetryAttemptClaim missing =
                    reader.claimDueRetry(terminal.snapshot.runId());

            assertRetryClaimMissing(missing);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void recoveredRetryScheduleInitialSnapshotCannotEscapeFinalPurge()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            TerminalAttempt terminal = terminalAttemptRecovered(
                    writer, "schedule-recovered-initial");
            installFinalPurge(reader, writer, terminal.snapshot,
                    ZkRunStateStore.EvidenceReadPhase
                            .RETRY_SCHEDULE_AFTER_INITIAL_SNAPSHOT);

            WholeAttemptRetryDecision missing = reader.scheduleRetryRecovered(
                    terminal.recoveryToken, terminal.fence);

            assertRetryScheduleMissing(missing);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void recoveredRetryClaimInitialSnapshotCannotEscapeFinalPurge()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            TerminalAttempt terminal = terminalAttemptRecovered(
                    writer, "claim-recovered-initial");
            installFinalPurge(reader, writer, terminal.snapshot,
                    ZkRunStateStore.EvidenceReadPhase
                            .RETRY_CLAIM_AFTER_INITIAL_SNAPSHOT);

            RetryAttemptClaim missing = reader.claimDueRetryRecovered(
                    terminal.recoveryToken, terminal.snapshot.runId());

            assertRetryClaimMissing(missing);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void retrySchedulePostDeadlineCannotRequirePurgedSnapshot() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            ActiveAttempt active = activeDueAttempt(writer, "schedule-post", false);
            installPostDeadlinePurge(reader, writer, active,
                    ZkRunStateStore.EvidenceReadPhase
                            .RETRY_SCHEDULE_AFTER_DEADLINE_EVALUATION);

            WholeAttemptRetryDecision missing = reader.scheduleRetry(active.fence);

            assertRetryScheduleMissing(missing);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void recoveredRetrySchedulePostDeadlineCannotRequirePurgedSnapshot()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            ActiveAttempt active = activeDueAttempt(
                    writer, "schedule-recovered-post", true);
            installPostDeadlinePurge(reader, writer, active,
                    ZkRunStateStore.EvidenceReadPhase
                            .RETRY_SCHEDULE_AFTER_DEADLINE_EVALUATION);

            WholeAttemptRetryDecision missing = reader.scheduleRetryRecovered(
                    active.recoveryToken, active.fence);

            assertRetryScheduleMissing(missing);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void retryClaimPostDeadlineCannotRequirePurgedSnapshot() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            ActiveAttempt active = retryWaitingDueAttempt(
                    writer, "claim-post", false);
            installPostDeadlinePurge(reader, writer, active,
                    ZkRunStateStore.EvidenceReadPhase
                            .RETRY_CLAIM_AFTER_DEADLINE_EVALUATION);

            RetryAttemptClaim missing =
                    reader.claimDueRetry(active.snapshot.runId());

            assertRetryClaimMissing(missing);
        }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void recoveredRetryClaimPostDeadlineCannotRequirePurgedSnapshot()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            for (int round = 0; round < 16; round++) {
                ActiveAttempt active = retryWaitingDueAttempt(
                        writer, "claim-recovered-post-" + round, true);
                AtomicBoolean replaced = new AtomicBoolean();
                AtomicReference<List<String>> replacementFingerprint =
                        new AtomicReference<>();
                installPostDeadlineReplacement(reader, writer, second, active,
                        ZkRunStateStore.EvidenceReadPhase
                                .RETRY_CLAIM_AFTER_DEADLINE_EVALUATION,
                        replaced, replacementFingerprint);

                RetryAttemptClaim missing = reader.claimDueRetryRecovered(
                        active.recoveryToken, active.snapshot.runId());

                assertTrue(replaced.get(), "round " + round + " must replace A");
                assertEquals(replacementFingerprint.get(),
                        runSubtreeFingerprint(second, active.snapshot.runId()),
                        "old retry claim must not mutate replacement B");
                assertRetryClaimMissing(missing);
            }
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void retryNestedDeadlineCannotCrossIntoReplacementRun() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            for (boolean recoveredClaim : new boolean[] {false, true}) {
                ZkRunStateStore reader = new ZkRunStateStore(first);
                ZkRunStateStore writer = new ZkRunStateStore(second);
                String runId = "nested-deadline-replacement-"
                        + (recoveredClaim ? "claim" : "schedule");
                ActiveAttempt original = recoveredClaim
                        ? retryWaitingDueAttempt(writer, runId, true)
                        : activeDueAttempt(writer, runId, false);
                AtomicBoolean replaced = new AtomicBoolean();
                AtomicReference<List<String>> replacementFingerprint =
                        new AtomicReference<>();
                reader.evidenceReadObserverForTest(phase -> {
                    ZkRunStateStore.EvidenceReadPhase expected = recoveredClaim
                            ? ZkRunStateStore.EvidenceReadPhase
                                    .RETRY_CLAIM_BEFORE_DEADLINE_EVALUATION
                            : ZkRunStateStore.EvidenceReadPhase
                                    .RETRY_SCHEDULE_BEFORE_DEADLINE_EVALUATION;
                    if (phase != expected
                            || !replaced.compareAndSet(false, true)) return;
                    if (recoveredClaim) {
                        purgeRetryWaitingRun(writer, original);
                        ActiveAttempt replacement = retryWaitingDueAttempt(
                                writer, runId, true);
                        assertEquals(original.recoveryToken,
                                replacement.recoveryToken,
                                "fixture must recreate the same public token");
                    } else {
                        RunSnapshot terminal = terminalize(writer, original);
                        purgeAll(writer, claim(
                                writer, terminal, "nested-deadline-owner"));
                        activeDueAttempt(writer, runId, false);
                    }
                    replacementFingerprint.set(authorityFingerprint(second));
                });

                if (recoveredClaim) {
                    RetryAttemptClaim result = reader.claimDueRetryRecovered(
                            original.recoveryToken, runId);
                    assertRetryClaimMissing(result);
                } else {
                    WholeAttemptRetryDecision result =
                            reader.scheduleRetry(original.fence);
                    assertRetryScheduleMissing(result);
                }

                assertTrue(replaced.get());
                assertEquals(replacementFingerprint.get(),
                        authorityFingerprint(second),
                        "old retry must not evaluate deadline on replacement B");
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void finalPurgeAfterLayoutRootStatIsClassifiedAsMissing() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            RunSnapshot terminal = terminal(writer, "layout-window");
            RetentionToken token = claim(writer, terminal, "layout-owner");
            advanceToFinalReady(writer, token);
            AtomicBoolean purged = new AtomicBoolean();
            reader.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase.LAYOUT_AFTER_ROOT_STAT
                        && purged.compareAndSet(false, true)) {
                    assertEquals(RunPurgeProgress.Code.PURGED,
                            writer.purge(token, 1000).code());
                }
            });

            assertNull(reader.get(terminal.runId()));
            assertTrue(purged.get());
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void finalPurgeCannotRefreshGuardFromReplacement() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            String runId = "final-guard-refresh-replacement";
            RunSnapshot original = terminal(writer, runId);
            RetentionToken token = claim(
                    writer, original, "final-guard-refresh-owner-a");
            advanceToFinalReady(writer, token);
            AtomicBoolean replaced = new AtomicBoolean();
            AtomicReference<List<String>> replacementFingerprint =
                    new AtomicReference<>();
            reader.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase
                        .PURGE_FINAL_BEFORE_GUARD_MUTATION
                        && replaced.compareAndSet(false, true)) {
                    purgeAll(writer, token);
                    RunSnapshot replacement = terminal(writer, runId);
                    RetentionToken replacementToken = claim(
                            writer, replacement,
                            "final-guard-refresh-owner-b");
                    advanceToFinalReady(writer, replacementToken);
                    replacementFingerprint.set(
                            authorityFingerprint(second));
                }
            });

            RunPurgeProgress result = reader.purge(token, 1000);

            assertTrue(replaced.get(),
                    "test must replace A before final guard operation");
            assertEquals(RunPurgeProgress.Code.STALE_OWNER, result.code());
            assertEquals(0, result.deletedRecords());
            assertEquals(replacementFingerprint.get(),
                    authorityFingerprint(second),
                    "old purge token must not mutate final-ready B");
            assertNotNull(writer.get(runId));
            assertEquals(2L, writer.get(runId).revision());
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void finalPurgeCandidateClassifiesGapAndActiveReplacement()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (boolean replacement : new boolean[] { false, true }) {
                String runId = "final-candidate-"
                        + (replacement ? "active" : "purged");
                RunSnapshot original = terminal(writer, runId);
                RetentionToken token = claim(
                        writer, original, "final-candidate-owner-a");
                advanceToFinalReady(writer, token);
                AtomicBoolean transitioned = new AtomicBoolean();
                AtomicReference<List<String>> replacementFingerprint =
                        new AtomicReference<>();
                reader.evidenceReadObserverForTest(phase -> {
                    if (phase == ZkRunStateStore.EvidenceReadPhase
                            .PURGE_FINAL_BEFORE_GUARD_MUTATION
                            && transitioned.compareAndSet(false, true)) {
                        purgeAll(writer, token);
                        if (replacement) {
                            created(writer, runId);
                            replacementFingerprint.set(
                                    authorityFingerprint(second));
                        }
                    }
                });

                RunPurgeProgress result = reader.purge(token, 1000);

                assertTrue(transitioned.get());
                RunPurgeProgress.Code expected = replacement
                        ? RunPurgeProgress.Code.STALE_OWNER
                        : RunPurgeProgress.Code.PURGED;
                if (result.code() != expected
                        || result.deletedRecords() != 0) {
                    violations.add((replacement ? "active" : "purged")
                            + " returned " + result.code() + "/"
                            + result.deletedRecords());
                }
                if (replacement) {
                    if (!replacementFingerprint.get().equals(
                            authorityFingerprint(second))) {
                        violations.add("old purge token mutated active B");
                    }
                    if (writer.get(runId) == null) {
                        violations.add("active B is missing");
                    }
                }
            }
            assertEquals(Collections.emptyList(), violations,
                    "final purge candidates must be classified against the "
                            + "original incarnation");
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void finalPurgeCandidateConfirmationTransportFailsClosed()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            RunSnapshot original = terminal(
                    writer, "final-candidate-confirmation-transport");
            RetentionToken token = claim(
                    writer, original, "final-candidate-confirmation-owner");
            advanceToFinalReady(writer, token);
            AtomicBoolean purged = new AtomicBoolean();
            AtomicInteger confirmationFaults = new AtomicInteger();
            reader.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase
                        .PURGE_FINAL_BEFORE_GUARD_MUTATION
                        && purged.compareAndSet(false, true)) {
                    purgeAll(writer, token);
                } else if (purged.get()
                        && phase == ZkRunStateStore.EvidenceReadPhase
                                .FINAL_ABSENCE_BEFORE_CONFIRMATION
                        && confirmationFaults.incrementAndGet() == 1) {
                    throw new KeeperException.OperationTimeoutException();
                }
            });

            assertThrows(RunStateStoreUnavailableException.class,
                    () -> reader.purge(token, 1000));
            assertTrue(purged.get());
            assertEquals(1, confirmationFaults.get());

            reader.evidenceReadObserverForTest(phase -> { });
            assertEquals(RunPurgeProgress.Code.PURGED,
                    reader.purge(token, 1000).code(),
                    "only a fresh confirmation may project PURGED");
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void applyResultConfirmationFencesOrdinaryAndRecoveredReplacement()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
            CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();

            String ordinaryRun = "result-fence-apply-ordinary";
            RunSnapshot ordinary = created(writer, ordinaryRun);
            RunCommand ordinaryStale = RunCommand.cancelReadyRun(
                    "result-fence-apply-ordinary-stale", ordinaryRun,
                    ordinary.revision() + 1L, 10L,
                    "CANCELED", "stale result");
            RunDecision ordinaryResult = (RunDecision)
                    invokeAcrossResultFenceReplacement(
                            reader, second, ordinaryRun,
                            () -> replaceReadyRun(
                                    writer, ordinary,
                                    "result-fence-apply-ordinary-replace"),
                            () -> reader.apply(ordinaryStale));
            if (ordinaryResult.code() != RunDecision.Code.RUN_NOT_FOUND
                    || ordinaryResult.snapshot() != null
                    || ordinaryResult.event() != null) {
                violations.add("ordinary apply returned "
                        + ordinaryResult.code());
            }

            String recoveredRun = "result-fence-apply-recovered";
            RunSnapshot recovered = created(writer, recoveredRun);
            RecoveryToken token = recoveryClaim(
                    writer, recovered,
                    new RecoveryOwner("result-fence-owner", 1L),
                    0L).token();
            RunCommand recoveredStale = RunCommand.cancelReadyRun(
                    "result-fence-apply-recovered-stale", recoveredRun,
                    recovered.revision() + 1L, 10L,
                    "CANCELED", "stale recovered result");
            RunDecision recoveredResult = (RunDecision)
                    invokeAcrossResultFenceReplacement(
                            reader, second, recoveredRun,
                            () -> {
                                RunSnapshot terminal = acceptedRecovered(
                                        writer, token,
                                        RunCommand.cancelReadyRun(
                                                "result-fence-recovered-end",
                                                recoveredRun,
                                                recovered.revision(), 2L,
                                                "CANCELED",
                                                "replacement setup"));
                                RetentionToken retention = claim(
                                        writer, terminal,
                                        "result-fence-recovered-retention");
                                purgeAll(writer, retention);
                                created(writer, recoveredRun);
                            },
                            () -> reader.applyRecovered(
                                    token, recoveredStale));
            if (recoveredResult.code()
                    != RunDecision.Code.STALE_RECOVERY_OWNER
                    || recoveredResult.snapshot() != null
                    || recoveredResult.event() != null) {
                violations.add("recovered apply returned "
                        + recoveredResult.code());
            }
            assertEquals(Collections.emptyList(), violations,
                    "apply result projection must fence replacement");
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void cancellationAndDeadlineResultConfirmationFenceReplacement()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
            CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();

            String cancellationRun = "result-fence-cancellation";
            RunSnapshot terminal = terminal(writer, cancellationRun);
            RetentionToken retention = claim(
                    writer, terminal, "result-fence-cancel-retention");
            RunCancellationResult cancellation = (RunCancellationResult)
                    invokeAcrossResultFenceReplacement(
                            reader, second, cancellationRun,
                            () -> {
                                purgeAll(writer, retention);
                                created(writer, cancellationRun);
                            },
                            () -> reader.requestUserCancellation(
                                    cancellationRun,
                                    "result-fence-cancel-request"));
            if (cancellation.disposition()
                    != RunCancellationResult.Disposition.NOT_FOUND
                    || cancellation.status() != null
                    || cancellation.revision() != 0L) {
                violations.add("cancellation returned "
                        + cancellation.disposition());
            }

            String deadlineRun = "result-fence-deadline";
            RunSnapshot deadline = created(writer, deadlineRun);
            RunDeadlineResult deadlineResult = (RunDeadlineResult)
                    invokeAcrossResultFenceReplacement(
                            reader, second, deadlineRun,
                            () -> replaceReadyRun(
                                    writer, deadline,
                                    "result-fence-deadline-replace"),
                            () -> reader.evaluateDeadline(deadlineRun));
            if (deadlineResult.disposition()
                    != RunDeadlineResult.Disposition.NOT_FOUND
                    || deadlineResult.status() != null
                    || deadlineResult.revision() != 0L) {
                violations.add("deadline returned "
                        + deadlineResult.disposition());
            }
            assertEquals(Collections.emptyList(), violations,
                    "cancellation/deadline result projection must fence "
                            + "replacement");
        }
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void retryResultConfirmationFencesOrdinaryAndRecoveredReplacement()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (boolean recovered : new boolean[] { false, true }) {
                String suffix = recovered ? "recovered" : "ordinary";
                String scheduleRun = "result-fence-schedule-" + suffix;
                ActiveAttempt schedule = activeAttempt(
                        writer, scheduleRun, 0L, recovered);
                RunAttemptFence staleFence = new RunAttemptFence(
                        scheduleRun, schedule.fence.attemptId(),
                        schedule.fence.fenceToken() + 1L);
                WholeAttemptRetryDecision scheduled =
                        (WholeAttemptRetryDecision)
                                invokeAcrossResultFenceReplacement(
                                        reader, second, scheduleRun,
                                        () -> replaceActiveAttempt(
                                                writer, schedule,
                                                "result-fence-schedule"),
                                        () -> recovered
                                                ? reader.scheduleRetryRecovered(
                                                        schedule.recoveryToken,
                                                        staleFence)
                                                : reader.scheduleRetry(
                                                        staleFence));
                if (scheduled.code()
                        != WholeAttemptRetryDecision.Code.NOT_FOUND
                        || scheduled.snapshot() != null) {
                    violations.add("schedule/" + suffix + " returned "
                            + scheduled.code());
                }

                String claimRun = "result-fence-claim-" + suffix;
                ActiveAttempt claim = activeAttempt(
                        writer, claimRun, 0L, recovered);
                RetryAttemptClaim claimed = (RetryAttemptClaim)
                        invokeAcrossResultFenceReplacement(
                                reader, second, claimRun,
                                () -> replaceActiveAttempt(
                                        writer, claim,
                                        "result-fence-claim"),
                                () -> recovered
                                        ? reader.claimDueRetryRecovered(
                                                claim.recoveryToken, claimRun)
                                        : reader.claimDueRetry(claimRun));
                if (claimed.code() != RetryAttemptClaim.Code.NOT_FOUND
                        || claimed.snapshot() != null
                        || claimed.identity() != null) {
                    violations.add("claim/" + suffix + " returned "
                            + claimed.code());
                }
            }

            String readyRun = "result-fence-retry-ready";
            RunSnapshot ready = created(writer, readyRun);
            RetryReadyEvaluation evaluation = (RetryReadyEvaluation)
                    invokeAcrossResultFenceReplacement(
                            reader, second, readyRun,
                            () -> replaceReadyRun(
                                    writer, ready,
                                    "result-fence-ready-replace"),
                            () -> reader.evaluateRetryReady(readyRun));
            if (evaluation.code() != RetryReadyEvaluation.Code.NOT_FOUND
                    || evaluation.snapshot() != null
                    || evaluation.identity() != null) {
                violations.add("ready returned " + evaluation.code());
            }
            assertEquals(Collections.emptyList(), violations,
                    "retry result projection must fence replacement");
        }
    }

    @Test
    @Timeout(value = 45, unit = TimeUnit.SECONDS)
    void resultConfirmationTransportFailsImmediately() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            for (boolean connectionLoss : new boolean[] { true, false }) {
                String suffix = String.valueOf(connectionLoss);

                RunSnapshot apply = created(
                        writer, "result-transport-apply-" + suffix);
                RunCommand stale = RunCommand.cancelReadyRun(
                        "result-transport-apply-stale-" + suffix,
                        apply.runId(), apply.revision() + 1L, 10L,
                        "CANCELED", "stale result");
                assertResultConfirmationTransport(reader, second,
                        connectionLoss, "apply", () -> reader.apply(stale));

                RunSnapshot terminal = terminal(
                        writer, "result-transport-cancel-" + suffix);
                claim(writer, terminal,
                        "result-transport-cancel-owner-" + suffix);
                assertResultConfirmationTransport(reader, second,
                        connectionLoss, "cancellation",
                        () -> reader.requestUserCancellation(
                                terminal.runId(), "cancel"));

                String deadlineRun = "result-transport-deadline-" + suffix;
                created(writer, deadlineRun);
                assertResultConfirmationTransport(reader, second,
                        connectionLoss, "deadline",
                        () -> reader.evaluateDeadline(deadlineRun));

                ActiveAttempt schedule = activeAttempt(
                        writer, "result-transport-schedule-" + suffix,
                        0L, false);
                RunAttemptFence staleFence = new RunAttemptFence(
                        schedule.fence.runId(), schedule.fence.attemptId(),
                        schedule.fence.fenceToken() + 1L);
                assertResultConfirmationTransport(reader, second,
                        connectionLoss, "retry schedule",
                        () -> reader.scheduleRetry(staleFence));

                String readyRun = "result-transport-ready-" + suffix;
                created(writer, readyRun);
                assertResultConfirmationTransport(reader, second,
                        connectionLoss, "retry readiness",
                        () -> reader.evaluateRetryReady(readyRun));

                ActiveAttempt claim = activeAttempt(
                        writer, "result-transport-claim-" + suffix,
                        0L, false);
                assertResultConfirmationTransport(reader, second,
                        connectionLoss, "retry claim",
                        () -> reader.claimDueRetry(claim.snapshot.runId()));

                assertEquals(RunDeadlineResult.Disposition.NO_DEADLINE,
                        reader.evaluateDeadline(deadlineRun).disposition(),
                        "only a fresh call may project the stable result");
            }
        }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void concurrentFinalPurgeWithSameTokenIsIdempotentlyPurged()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            for (int round = 0; round < 16; round++) {
                String runId = "purge-layout-" + round;
                RunSnapshot terminal = terminal(writer, runId);
                RetentionToken token = claim(
                        writer, terminal, "purge-layout-owner-" + round);
                advanceToFinalReady(writer, token);
                AtomicBoolean purged = new AtomicBoolean();
                reader.evidenceReadObserverForTest(phase -> {
                    if (phase == ZkRunStateStore.EvidenceReadPhase
                            .PURGE_AFTER_LAYOUT_LOOKUP
                            && purged.compareAndSet(false, true)) {
                        assertEquals(RunPurgeProgress.Code.PURGED,
                                writer.purge(token, 1000).code());
                    }
                });

                RunPurgeProgress result = reader.purge(token, 1000);

                assertEquals(RunPurgeProgress.Code.PURGED, result.code(),
                        "round " + round);
                assertEquals(0, result.deletedRecords(), "round " + round);
                assertTrue(purged.get(), "round " + round);
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void purgeAckLossReconcilesBatchAndFinalOnOriginalIncarnation()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);

            RunSnapshot batchTerminal = terminal(writer, "batch-ack-original");
            RetentionToken batchToken = claim(
                    writer, batchTerminal, "batch-ack-original-owner");
            AtomicBoolean batchBeforeCommit = new AtomicBoolean();
            AtomicBoolean batchAckLost = new AtomicBoolean();
            reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.PURGE_BATCH) {
                    if (moment == ZkRunStateStore.GuardedMutationMoment
                            .BEFORE_COMMIT) {
                        batchBeforeCommit.set(true);
                    } else if (batchAckLost.compareAndSet(false, true)) {
                        throw new KeeperException.ConnectionLossException();
                    }
                }
            });
            RunPurgeProgress batch = reader.purge(batchToken, 1);
            assertTrue(batchBeforeCommit.get(),
                    "PURGE_BATCH must reach BEFORE_COMMIT");
            assertTrue(batchAckLost.get(),
                    "PURGE_BATCH must reach AFTER_COMMIT_BEFORE_ACK");
            assertEquals(RunPurgeProgress.Code.IN_PROGRESS, batch.code());
            assertEquals(1, batch.deletedRecords());

            RunSnapshot finalTerminal = terminal(writer, "final-ack-original");
            RetentionToken finalToken = claim(
                    writer, finalTerminal, "final-ack-original-owner");
            advanceToFinalReady(writer, finalToken);
            AtomicBoolean finalBeforeCommit = new AtomicBoolean();
            AtomicBoolean finalAckLost = new AtomicBoolean();
            reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.PURGE_FINAL) {
                    if (moment == ZkRunStateStore.GuardedMutationMoment
                            .BEFORE_COMMIT) {
                        finalBeforeCommit.set(true);
                    } else if (finalAckLost.compareAndSet(false, true)) {
                        throw new KeeperException.ConnectionLossException();
                    }
                }
            });
            RunPurgeProgress finished = reader.purge(finalToken, 1000);
            assertTrue(finalBeforeCommit.get(),
                    "PURGE_FINAL must reach BEFORE_COMMIT");
            assertTrue(finalAckLost.get(),
                    "PURGE_FINAL must reach AFTER_COMMIT_BEFORE_ACK");
            assertEquals(RunPurgeProgress.Code.PURGED, finished.code());
            assertEquals(0, finished.deletedRecords());
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void finalPurgeConfirmationTransportCannotReturnPurged()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            RunSnapshot terminal = terminal(writer, "final-confirmation-transport");
            RetentionToken token = claim(
                    writer, terminal, "final-confirmation-owner");
            advanceToFinalReady(writer, token);
            AtomicBoolean ackLost = new AtomicBoolean();
            AtomicBoolean confirmationFailed = new AtomicBoolean();
            reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.PURGE_FINAL
                        && moment == ZkRunStateStore.GuardedMutationMoment
                        .AFTER_COMMIT_BEFORE_ACK
                        && ackLost.compareAndSet(false, true)) {
                    throw new KeeperException.ConnectionLossException();
                }
            });
            reader.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase
                        .FINAL_ABSENCE_BEFORE_CONFIRMATION
                        && confirmationFailed.compareAndSet(false, true)) {
                    throw new KeeperException.OperationTimeoutException();
                }
            });

            assertThrows(io.rowweave.run.RunStateStoreUnavailableException.class,
                    () -> reader.purge(token, 1000));
            assertTrue(ackLost.get());
            assertTrue(confirmationFailed.get());
            reader.evidenceReadObserverForTest(phase -> { });
            assertEquals(RunPurgeProgress.Code.PURGED,
                    reader.purge(token, 1000).code(),
                    "only a fresh confirmation may project PURGED");
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void finalPurgeBeforeCommitTransportCannotProjectStaleOwner()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            RunSnapshot terminal = terminal(
                    writer, "final-before-commit-transport");
            RetentionToken token = claim(
                    writer, terminal, "final-before-commit-owner");
            advanceToFinalReady(writer, token);
            AtomicBoolean failed = new AtomicBoolean();
            reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.PURGE_FINAL
                        && moment == ZkRunStateStore.GuardedMutationMoment
                        .BEFORE_COMMIT
                        && failed.compareAndSet(false, true)) {
                    throw new KeeperException.ConnectionLossException();
                }
            });

            assertThrows(RunStateStoreUnavailableException.class,
                    () -> reader.purge(token, 1000));
            assertTrue(failed.get());
            assertNotNull(writer.get(terminal.runId()));
            assertEquals(RunLifecycleState.DELETING,
                    writer.lifecycle(terminal.runId()).state());
        }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void finalPurgeAckLossWithReplacementIsStaleOwner() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            for (int round = 0; round < 16; round++) {
                RunSnapshot terminal = terminal(
                        writer, "final-ack-replacement-" + round);
                RetentionToken token = claim(
                        writer, terminal, "final-ack-owner-" + round);
                advanceToFinalReady(writer, token);
                AtomicBoolean replaced = new AtomicBoolean();
                AtomicReference<List<String>> replacementFingerprint =
                        new AtomicReference<>();
                reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                    if (kind == ZkRunStateStore.GuardedMutationKind.PURGE_FINAL
                            && moment == ZkRunStateStore.GuardedMutationMoment
                            .AFTER_COMMIT_BEFORE_ACK
                            && replaced.compareAndSet(false, true)) {
                        created(writer, terminal.runId());
                        replacementFingerprint.set(
                                runSubtreeFingerprint(second, terminal.runId()));
                        throw new KeeperException.ConnectionLossException();
                    }
                });

                RunPurgeProgress result = reader.purge(token, 1000);

                assertTrue(replaced.get(), "round " + round + " must replace A");
                assertEquals(replacementFingerprint.get(),
                        runSubtreeFingerprint(second, terminal.runId()),
                        "old final purge must not mutate replacement B");
                assertEquals(RunPurgeProgress.Code.STALE_OWNER, result.code());
                assertEquals(0, result.deletedRecords());
                assertEquals(1L, writer.get(terminal.runId()).revision());
            }
        }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void batchPurgeAckLossCannotConfirmAgainstReplacementCursor()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            for (int round = 0; round < 16; round++) {
                RunSnapshot terminal = terminal(
                        writer, "batch-ack-replacement-" + round);
                RetentionToken token = claim(
                        writer, terminal, "batch-ack-owner-" + round);
                AtomicBoolean replaced = new AtomicBoolean();
                AtomicReference<List<String>> replacementFingerprint =
                        new AtomicReference<>();
                reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                    if (kind == ZkRunStateStore.GuardedMutationKind.PURGE_BATCH
                            && moment == ZkRunStateStore.GuardedMutationMoment
                            .AFTER_COMMIT_BEFORE_ACK
                            && replaced.compareAndSet(false, true)) {
                        purgeAll(writer, token);
                        created(writer, terminal.runId());
                        replacementFingerprint.set(
                                runSubtreeFingerprint(second, terminal.runId()));
                        throw new KeeperException.ConnectionLossException();
                    }
                });

                RunPurgeProgress result = reader.purge(token, 1);

                assertTrue(replaced.get(), "round " + round + " must replace A");
                assertEquals(replacementFingerprint.get(),
                        runSubtreeFingerprint(second, terminal.runId()),
                        "old batch purge must not mutate replacement B");
                assertEquals(RunPurgeProgress.Code.STALE_OWNER, result.code());
                assertEquals(0, result.deletedRecords());
                assertEquals(1L, writer.get(terminal.runId()).revision());
            }
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void purgeAckLossFencesEveryReplacementState() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (boolean finalPurge : new boolean[] { false, true }) {
                ZkRunStateStore.GuardedMutationKind mutationKind = finalPurge
                        ? ZkRunStateStore.GuardedMutationKind.PURGE_FINAL
                        : ZkRunStateStore.GuardedMutationKind.PURGE_BATCH;
                for (ApplyReplacementState state
                        : ApplyReplacementState.values()) {
                    String runId = "purge-ack-state-"
                            + (finalPurge ? "final-" : "batch-")
                            + state.name().toLowerCase(java.util.Locale.ROOT);
                    RunSnapshot original = terminal(writer, runId);
                    RetentionToken token = claim(
                            writer, original, "purge-state-owner");
                    if (finalPurge) advanceToFinalReady(writer, token);
                    AtomicBoolean ackLost = new AtomicBoolean();
                    CountDownLatch layoutReady = new CountDownLatch(1);
                    CountDownLatch allowCreate = new CountDownLatch(1);
                    AtomicReference<Thread> creator = new AtomicReference<>();
                    AtomicReference<Throwable> createFailure =
                            new AtomicReference<>();
                    AtomicReference<List<String>> replacementFingerprint =
                            new AtomicReference<>();
                    reader.guardedMutationObserverForTest(
                            (observed, ordinal, moment) -> {
                        if (observed == mutationKind
                                && moment == ZkRunStateStore
                                .GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK
                                && ackLost.compareAndSet(false, true)) {
                            if (!finalPurge) purgeAll(writer, token);
                            if (state == ApplyReplacementState.PREPUBLICATION) {
                                AtomicBoolean paused = new AtomicBoolean();
                                writer.evidenceReadObserverForTest(phase -> {
                                    if (phase == ZkRunStateStore.EvidenceReadPhase
                                            .APPLY_AFTER_INITIAL_SNAPSHOT
                                            && paused.compareAndSet(
                                                    false, true)) {
                                        layoutReady.countDown();
                                        if (!allowCreate.await(
                                                10, TimeUnit.SECONDS)) {
                                            throw new AssertionError(
                                                    "replacement CREATE "
                                                            + "release timed out");
                                        }
                                    }
                                });
                                Thread thread = new Thread(() -> {
                                    try {
                                        created(writer, runId);
                                    } catch (Throwable failure) {
                                        createFailure.set(failure);
                                    }
                                }, "zk-purge-prepublication-" + finalPurge);
                                creator.set(thread);
                                thread.start();
                                if (!layoutReady.await(
                                        10, TimeUnit.SECONDS)) {
                                    throw new AssertionError(
                                            "replacement layout not published");
                                }
                                replacementFingerprint.set(
                                        runSubtreeFingerprint(second, runId));
                            } else if (state == ApplyReplacementState.ACTIVE) {
                                created(writer, runId);
                                replacementFingerprint.set(
                                        runSubtreeFingerprint(second, runId));
                            } else if (state
                                    == ApplyReplacementState.TERMINAL) {
                                terminal(writer, runId);
                                replacementFingerprint.set(
                                        runSubtreeFingerprint(second, runId));
                            }
                            throw new KeeperException
                                    .ConnectionLossException();
                        }
                    });

                    RunPurgeProgress result = null;
                    RuntimeException failure = null;
                    try {
                        result = reader.purge(token, finalPurge ? 1000 : 1);
                    } catch (RuntimeException thrown) {
                        failure = thrown;
                    } finally {
                        if (state == ApplyReplacementState.PREPUBLICATION
                                && replacementFingerprint.get() != null) {
                            try {
                                if (!replacementFingerprint.get().equals(
                                        runSubtreeFingerprint(
                                                second, runId))) {
                                    violations.add(mutationKind + "/" + state
                                            + " mutated paused "
                                            + "prepublication subtree");
                                }
                            } catch (Exception inspectionFailure) {
                                violations.add(mutationKind + "/" + state
                                        + " prepublication inspection threw "
                                        + inspectionFailure.getClass()
                                                .getSimpleName());
                            }
                        }
                        allowCreate.countDown();
                        Thread thread = creator.get();
                        if (thread != null) {
                            thread.join(TimeUnit.SECONDS.toMillis(10));
                            assertTrue(!thread.isAlive(),
                                    "replacement CREATE must terminate");
                        }
                    }

                    String label = mutationKind + "/" + state;
                    assertTrue(ackLost.get(), label
                            + " must reach AFTER_COMMIT_BEFORE_ACK");
                    if (failure != null) {
                        violations.add(label + " threw "
                                + failure.getClass().getSimpleName());
                    } else {
                        RunPurgeProgress.Code expected = state
                                == ApplyReplacementState.PURGED
                                ? RunPurgeProgress.Code.PURGED
                                : RunPurgeProgress.Code.STALE_OWNER;
                        if (result.code() != expected
                                || result.deletedRecords() != 0) {
                            violations.add(label + " returned "
                                    + result.code() + "/"
                                    + result.deletedRecords());
                        }
                    }
                    if (createFailure.get() != null) {
                        violations.add(label + " replacement CREATE threw "
                                + createFailure.get().getClass()
                                        .getSimpleName());
                    }
                    RunSnapshot replacement = writer.get(runId);
                    if (state == ApplyReplacementState.PURGED) {
                        if (replacement != null) {
                            violations.add(label + " recreated revision "
                                    + replacement.revision());
                        }
                    } else if (replacement == null) {
                        violations.add(label + " replacement missing");
                    } else {
                        long expectedRevision = state
                                == ApplyReplacementState.TERMINAL ? 2L : 1L;
                        if (replacement.revision() != expectedRevision) {
                            violations.add(label + " replacement revision="
                                    + replacement.revision());
                        }
                        if (state != ApplyReplacementState.PREPUBLICATION) {
                            try {
                                if (!replacementFingerprint.get().equals(
                                        runSubtreeFingerprint(
                                                second, runId))) {
                                    violations.add(label
                                            + " mutated replacement subtree");
                                }
                            } catch (Exception inspectionFailure) {
                                violations.add(label
                                        + " subtree inspection threw "
                                        + inspectionFailure.getClass()
                                                .getSimpleName());
                            }
                        }
                    }
                }
            }
            assertTrue(violations.isEmpty(),
                    "purge ACK reconciliation must fence every replacement "
                            + "state before cursor confirmation: " + violations);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void oldApplyCannotCrossFinalPurgeAndCommitIntoRecreatedRun() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            RunSnapshot original = created(writer, "before-commit");
            AtomicBoolean replaced = new AtomicBoolean();
            reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.APPLY_EXISTING
                        && moment == ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT
                        && replaced.compareAndSet(false, true)) {
                    replaceReadyRun(writer, original, "writer-terminal-before");
                }
            });

            RunDecision stale = reader.apply(RunCommand.cancelReadyRun(
                    "reader-cancel-before", original.runId(), original.revision(),
                    10L, "CANCELED", "old invocation"));

            assertEquals(RunDecision.Code.RUN_NOT_FOUND, stale.code());
            assertNull(stale.snapshot());
            assertNull(stale.event());
            assertEquals(1L, writer.get(original.runId()).revision(),
                    "the old invocation must not mutate the recreated Run");
        }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void oldRecoveredApplyCannotCrossIntoEquivalentlyOwnedReplacement()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            RecoveryOwner owner = new RecoveryOwner("stable-owner", 7L);
            for (int round = 0; round < 16; round++) {
                RunSnapshot original = created(
                        writer, "recovered-apply-before-" + round);
                RecoveryToken token = recoveryClaim(
                        writer, original, owner, 0L).token();
                AtomicBoolean replaced = new AtomicBoolean();
                AtomicReference<List<String>> replacementFingerprint =
                        new AtomicReference<>();
                reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                    if (kind == ZkRunStateStore.GuardedMutationKind.APPLY_EXISTING
                            && moment == ZkRunStateStore.GuardedMutationMoment
                            .BEFORE_COMMIT
                            && replaced.compareAndSet(false, true)) {
                        RunSnapshot terminal = acceptedRecovered(
                                writer, token, RunCommand.cancelReadyRun(
                                        "writer-cancel-recovered-"
                                                + original.runId(),
                                        original.runId(), original.revision(),
                                        2L, "CANCELED", "replacement setup"));
                        RetentionToken retention = claim(
                                writer, terminal,
                                "recovered-apply-retention-"
                                        + original.runId());
                        purgeAll(writer, retention);
                        RunSnapshot replacement = created(
                                writer, original.runId());
                        RecoveryToken replacementToken = recoveryClaim(
                                writer, replacement, owner, 0L).token();
                        assertEquals(token, replacementToken,
                                "fixture must recreate the same public recovery token");
                        replacementFingerprint.set(
                                runSubtreeFingerprint(second, original.runId()));
                    }
                });

                RunDecision stale = reader.applyRecovered(
                        token, RunCommand.cancelReadyRun(
                                "reader-cancel-recovered-"
                                        + original.runId(),
                                original.runId(), original.revision(), 10L,
                                "CANCELED", "old recovered invocation"));

                assertTrue(replaced.get(), "round " + round + " must replace A");
                assertEquals(replacementFingerprint.get(),
                        runSubtreeFingerprint(second, original.runId()),
                        "old apply must not mutate replacement B");
                assertEquals(RunDecision.Code.STALE_RECOVERY_OWNER, stale.code());
                assertNull(stale.snapshot());
                assertNull(stale.event());
                assertEquals(1L, writer.get(original.runId()).revision());
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void oldPublicCancellationCannotMutateRecreatedRunningRun()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            ActiveAttempt original = runningAttempt(
                    writer, "cancel-before", false);
            AtomicBoolean replaced = new AtomicBoolean();
            reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.CANCEL
                        && moment == ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT
                        && replaced.compareAndSet(false, true)) {
                    RunSnapshot terminal = terminalize(writer, original);
                    RetentionToken token = claim(
                            writer, terminal, "cancel-before-owner");
                    purgeAll(writer, token);
                    runningAttempt(writer, original.snapshot.runId(), false);
                }
            });

            RunCancellationResult stale = reader.requestUserCancellation(
                    original.snapshot.runId(), "reader-request");

            assertEquals(RunCancellationResult.Disposition.NOT_FOUND,
                    stale.disposition());
            assertNull(stale.status());
            assertEquals(0L, stale.revision());
            assertEquals(4L, writer.get(original.snapshot.runId()).revision());
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void oldRetentionClaimCannotMarkRecreatedTerminalRunDeleting()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            RunSnapshot original = terminal(writer, "retention-before");
            RunLifecycleRef lifecycle = writer.lifecycle(original.runId());
            AtomicBoolean replaced = new AtomicBoolean();
            reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.RETENTION_CLAIM
                        && moment == ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT
                        && replaced.compareAndSet(false, true)) {
                    RetentionToken competing = claim(
                            writer, original, "competing-retention");
                    purgeAll(writer, competing);
                    terminal(writer, original.runId());
                }
            });

            RunRetentionClaim stale = reader.claim(
                    original.runId(), original.revision(),
                    lifecycle.terminalAtMillis(),
                    new RetentionOwner("reader-retention", 1L), 0L);

            assertEquals(RunRetentionClaim.Code.STALE_RUN, stale.code());
            assertNull(stale.token());
            assertEquals(RunLifecycleState.TERMINAL,
                    writer.lifecycle(original.runId()).state());
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void oldRecoveryClaimCannotOwnRecreatedReadyRun() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            RunSnapshot original = created(writer, "recovery-before");
            AtomicBoolean replaced = new AtomicBoolean();
            reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.RECOVERY_CLAIM
                        && moment == ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT
                        && replaced.compareAndSet(false, true)) {
                    RecoveryToken owner = recoveryClaim(
                            writer, original,
                            new RecoveryOwner("competing-recovery", 2L), 0L).token();
                    RunSnapshot terminal = acceptedRecovered(
                            writer, owner, RunCommand.cancelReadyRun(
                                    "competing-cancel", original.runId(),
                                    original.revision(), 2L,
                                    "CANCELED", "replacement setup"));
                    RetentionToken retention = claim(
                            writer, terminal, "recovery-before-retention");
                    purgeAll(writer, retention);
                    created(writer, original.runId());
                }
            });

            RecoveryClaim stale = reader.claim(
                    original.runId(), original.revision(), 0L,
                    new RecoveryOwner("reader-recovery", 1L));

            assertEquals(RecoveryClaim.Code.STALE_RUN, stale.code());
            assertNull(stale.token());
            assertNull(writer.ownership(original.runId()));
            assertEquals(1L, writer.get(original.runId()).revision());
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void recoveryClaimEarlyOwnerClassificationCannotConsumeReplacement()
            throws Exception {
        for (boolean replacementOwned : new boolean[] { false, true }) {
            try (TestingServer server = new TestingServer(true);
                 CuratorFramework first = connectedClient(server);
                 CuratorFramework second = connectedClient(server)) {
                ZkRunStateStore reader = new ZkRunStateStore(first);
                ZkRunStateStore writer = new ZkRunStateStore(second);
                String suffix = replacementOwned ? "owned" : "unowned";
                RunSnapshot original = created(
                        writer, "recovery-early-" + suffix);
                AtomicBoolean replaced = new AtomicBoolean();
                reader.evidenceReadObserverForTest(phase -> {
                    if (phase == ZkRunStateStore.EvidenceReadPhase
                            .RECOVERY_CLAIM_BEFORE_OWNER_CLASSIFICATION
                            && replaced.compareAndSet(false, true)) {
                        RecoveryToken retiring = recoveryClaim(
                                writer, original,
                                new RecoveryOwner(
                                        "retiring-" + suffix, 1L),
                                0L).token();
                        RunSnapshot terminal = acceptedRecovered(
                                writer, retiring, RunCommand.cancelReadyRun(
                                        "retire-" + suffix, original.runId(),
                                        original.revision(), 2L,
                                        "CANCELED", "replacement setup"));
                        RetentionToken retention = claim(
                                writer, terminal,
                                "recovery-early-retention-" + suffix);
                        purgeAll(writer, retention);
                        RunSnapshot replacement = created(
                                writer, original.runId());
                        if (replacementOwned) {
                            recoveryClaim(writer, replacement,
                                    new RecoveryOwner(
                                            "replacement-owner", 2L),
                                    0L);
                        }
                    }
                });

                RecoveryClaim stale = reader.claim(
                        original.runId(), original.revision(),
                        replacementOwned ? 0L : 1L,
                        new RecoveryOwner("reader-" + suffix, 3L));

                assertTrue(replaced.get());
                assertEquals(RecoveryClaim.Code.STALE_RUN, stale.code());
                assertNull(stale.token());
                assertEquals(replacementOwned,
                        writer.ownership(original.runId()) != null);
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void recoveryClaimOwnerClassificationConfirmationFailsImmediately()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            RunSnapshot original = created(
                    writer, "recovery-owner-confirmation");
            AtomicBoolean classificationReached = new AtomicBoolean();
            AtomicInteger confirmationFaults = new AtomicInteger();
            reader.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase
                        .RECOVERY_CLAIM_BEFORE_OWNER_CLASSIFICATION) {
                    classificationReached.set(true);
                } else if (classificationReached.get()
                        && phase == ZkRunStateStore.EvidenceReadPhase
                        .RECOVERY_CLAIM_BEFORE_OWNER_CONFIRMATION
                        && confirmationFaults.incrementAndGet() == 1) {
                    throw new KeeperException.OperationTimeoutException();
                }
            });

            assertThrows(RunStateStoreUnavailableException.class,
                    () -> reader.claim(
                            original.runId(), original.revision(), 0L,
                            new RecoveryOwner(
                                    "confirmation-reader", 1L)));
            assertTrue(classificationReached.get());
            assertEquals(1, confirmationFaults.get());
            assertNull(writer.ownership(original.runId()));
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void recoveryClaimAckReconciliationCannotConsumeLateReplacement()
            throws Exception {
        for (boolean prepublication : new boolean[] { false, true }) {
            try (TestingServer server = new TestingServer(true);
                 CuratorFramework first = connectedClient(server);
                 CuratorFramework second = connectedClient(server)) {
                ZkRunStateStore reader = new ZkRunStateStore(first);
                ZkRunStateStore writer = new ZkRunStateStore(second);
                String suffix = prepublication ? "prepublication" : "active";
                RunSnapshot original = created(
                        writer, "recovery-late-ack-" + suffix);
                RecoveryOwner owner = new RecoveryOwner(
                        "recovery-late-owner", 1L);
                AtomicBoolean ackLost = new AtomicBoolean();
                AtomicBoolean replaced = new AtomicBoolean();
                CountDownLatch layoutReady = new CountDownLatch(1);
                CountDownLatch allowCreate = new CountDownLatch(1);
                AtomicReference<Thread> creator = new AtomicReference<>();
                AtomicReference<Throwable> createFailure =
                        new AtomicReference<>();
                reader.guardedMutationObserverForTest(
                        (kind, ordinal, moment) -> {
                    if (kind == ZkRunStateStore.GuardedMutationKind
                            .RECOVERY_CLAIM
                            && moment == ZkRunStateStore
                            .GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK
                            && ackLost.compareAndSet(false, true)) {
                        throw new KeeperException.ConnectionLossException();
                    }
                });
                reader.evidenceReadObserverForTest(phase -> {
                    if (phase == ZkRunStateStore.EvidenceReadPhase
                            .RECOVERY_CLAIM_RECONCILE_AFTER_AUTHORITY
                            && replaced.compareAndSet(false, true)) {
                        RecoveryClaim replay = writer.claim(
                                original.runId(), original.revision(),
                                0L, owner);
                        assertEquals(RecoveryClaim.Code.REPLAYED,
                                replay.code());
                        RunSnapshot terminal = acceptedRecovered(
                                writer, replay.token(),
                                RunCommand.cancelReadyRun(
                                        "retire-late-" + suffix,
                                        original.runId(),
                                        original.revision(), 2L,
                                        "CANCELED", "replacement setup"));
                        RetentionToken retention = claim(
                                writer, terminal,
                                "recovery-late-retention-" + suffix);
                        purgeAll(writer, retention);
                        if (prepublication) {
                            AtomicBoolean paused = new AtomicBoolean();
                            writer.evidenceReadObserverForTest(createPhase -> {
                                if (createPhase == ZkRunStateStore
                                        .EvidenceReadPhase
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
                                    created(writer, original.runId());
                                } catch (Throwable failure) {
                                    createFailure.set(failure);
                                }
                            }, "zk-recovery-late-prepublication");
                            creator.set(thread);
                            thread.start();
                            if (!layoutReady.await(10, TimeUnit.SECONDS)) {
                                throw new AssertionError(
                                        "replacement layout not published");
                            }
                        } else {
                            created(writer, original.runId());
                        }
                    }
                });

                RecoveryClaim result = null;
                RuntimeException failure = null;
                try {
                    result = reader.claim(
                            original.runId(), original.revision(),
                            0L, owner);
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

                assertTrue(ackLost.get());
                assertTrue(replaced.get());
                assertNull(failure, suffix + " threw " + failure);
                assertNotNull(result);
                assertEquals(RecoveryClaim.Code.STALE_RUN, result.code());
                assertNull(result.token());
                assertNull(createFailure.get());
                RunSnapshot replacement = writer.get(original.runId());
                assertNotNull(replacement);
                assertEquals(1L, replacement.revision());
                assertNull(writer.ownership(original.runId()));
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void recoveryClaimLateAckConfirmationFailsImmediately()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            RunSnapshot original = created(
                    writer, "recovery-late-confirmation");
            AtomicBoolean ackLost = new AtomicBoolean();
            AtomicInteger confirmationFaults = new AtomicInteger();
            reader.guardedMutationObserverForTest(
                    (kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.RECOVERY_CLAIM
                        && moment == ZkRunStateStore.GuardedMutationMoment
                        .AFTER_COMMIT_BEFORE_ACK
                        && ackLost.compareAndSet(false, true)) {
                    throw new KeeperException.ConnectionLossException();
                }
            });
            reader.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase
                        .RECOVERY_CLAIM_RECONCILE_BEFORE_CONFIRMATION) {
                    confirmationFaults.incrementAndGet();
                    throw new KeeperException.OperationTimeoutException();
                }
            });

            assertThrows(RunStateStoreUnavailableException.class,
                    () -> reader.claim(
                            original.runId(), original.revision(), 0L,
                            new RecoveryOwner(
                                    "late-confirmation-owner", 1L)));
            assertTrue(ackLost.get());
            assertEquals(1, confirmationFaults.get());
            assertNotNull(writer.ownership(original.runId()));
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void committedApplyAckLossCannotReplayAgainstRecreatedRun() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            RunSnapshot original = created(writer, "after-commit");
            AtomicBoolean replaced = new AtomicBoolean();
            reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.APPLY_EXISTING
                        && moment == ZkRunStateStore.GuardedMutationMoment
                        .AFTER_COMMIT_BEFORE_ACK
                        && replaced.compareAndSet(false, true)) {
                    replaceTerminalRun(writer, original.runId(), "ack-loss-owner");
                    throw new KeeperException.ConnectionLossException();
                }
            });

            RunDecision stale = reader.apply(RunCommand.cancelReadyRun(
                    "reader-cancel-after", original.runId(), original.revision(),
                    10L, "CANCELED", "lost response"));

            assertEquals(RunDecision.Code.RUN_NOT_FOUND, stale.code());
            assertNull(stale.snapshot());
            assertNull(stale.event());
            assertEquals(1L, writer.get(original.runId()).revision(),
                    "unknown-outcome reconciliation must not mutate the replacement");
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void committedApplyAckLossReplaysForOriginalOrdinaryAndRecoveredAuthority()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            for (boolean recovered : new boolean[] { false, true }) {
                String runId = "apply-ack-original-"
                        + (recovered ? "recovered" : "ordinary");
                RunSnapshot original = created(writer, runId);
                RecoveryToken token = null;
                if (recovered) {
                    RecoveryClaim claim = writer.claim(
                            runId, original.revision(), 0L,
                            new RecoveryOwner("apply-owner", 1L));
                    assertEquals(RecoveryClaim.Code.ACQUIRED, claim.code());
                    token = claim.token();
                }
                RunCommand command = RunCommand.cancelReadyRun(
                        "apply-ack", runId, original.revision(),
                        10L, "CANCELED", "lost response");
                AtomicBoolean ackLost = new AtomicBoolean();
                reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                    if (kind == ZkRunStateStore.GuardedMutationKind.APPLY_EXISTING
                            && moment == ZkRunStateStore.GuardedMutationMoment
                            .AFTER_COMMIT_BEFORE_ACK
                            && ackLost.compareAndSet(false, true)) {
                        throw new KeeperException.ConnectionLossException();
                    }
                });

                RunDecision replay = recovered
                        ? reader.applyRecovered(token, command)
                        : reader.apply(command);

                assertTrue(ackLost.get());
                assertEquals(RunDecision.Code.ALREADY_APPLIED, replay.code());
                assertNotEquals(null, replay.snapshot());
            }
        }
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void recoveredApplyAckLossCannotReplayEquivalentReplacementReceipt()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            String runId = "apply-ack-recovered-replacement";
            RunSnapshot original = created(writer, runId);
            RecoveryOwner owner = new RecoveryOwner("equivalent-owner", 1L);
            RecoveryClaim originalClaim = writer.claim(
                    runId, original.revision(), 0L, owner);
            assertEquals(RecoveryClaim.Code.ACQUIRED, originalClaim.code());
            RecoveryToken oldToken = originalClaim.token();
            RunCommand command = RunCommand.cancelReadyRun(
                    "equivalent-command", runId, original.revision(),
                    10L, "CANCELED", "equivalent response");
            AtomicBoolean replaced = new AtomicBoolean();
            reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.APPLY_EXISTING
                        && moment == ZkRunStateStore.GuardedMutationMoment
                        .AFTER_COMMIT_BEFORE_ACK
                        && replaced.compareAndSet(false, true)) {
                    RunSnapshot terminal = writer.get(runId);
                    RetentionToken retention = claim(
                            writer, terminal, "apply-ack-retention");
                    purgeAll(writer, retention);
                    RunSnapshot replacement = created(writer, runId);
                    RecoveryClaim replacementClaim = writer.claim(
                            runId, replacement.revision(), 0L, owner);
                    assertEquals(RecoveryClaim.Code.ACQUIRED,
                            replacementClaim.code());
                    assertEquals(RunDecision.Code.ACCEPTED,
                            writer.applyRecovered(
                                    replacementClaim.token(), command).code());
                    throw new KeeperException.ConnectionLossException();
                }
            });

            RunDecision stale = reader.applyRecovered(oldToken, command);

            assertTrue(replaced.get());
            assertEquals(RunDecision.Code.STALE_RECOVERY_OWNER, stale.code());
            assertNull(stale.snapshot());
            assertNull(stale.event());
        }
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void applyAckLossFencesEveryReplacementLifecycleState() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (boolean recovered : new boolean[] { false, true }) {
                for (ApplyReplacementState state
                        : ApplyReplacementState.values()) {
                    String runId = "apply-ack-state-"
                            + state.name().toLowerCase(java.util.Locale.ROOT)
                            + (recovered ? "-recovered" : "-ordinary");
                    RunSnapshot original = created(writer, runId);
                    RecoveryOwner owner = new RecoveryOwner(
                            "apply-state-owner", 1L);
                    RecoveryToken originalToken = recovered
                            ? recoveryClaim(writer, original, owner, 0L).token()
                            : null;
                    RunCommand command = RunCommand.cancelReadyRun(
                            "apply-state-command", runId, original.revision(),
                            10L, "CANCELED", "lost response");
                    AtomicBoolean ackLost = new AtomicBoolean();
                    CountDownLatch layoutReady = new CountDownLatch(1);
                    CountDownLatch allowCreate = new CountDownLatch(1);
                    AtomicReference<Thread> creator = new AtomicReference<>();
                    AtomicReference<Throwable> createFailure =
                            new AtomicReference<>();
                    reader.guardedMutationObserverForTest(
                            (kind, ordinal, moment) -> {
                        if (kind == ZkRunStateStore.GuardedMutationKind
                                .APPLY_EXISTING
                                && moment == ZkRunStateStore
                                .GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK
                                && ackLost.compareAndSet(false, true)) {
                            RunSnapshot committed = writer.get(runId);
                            RetentionToken retention = claim(
                                    writer, committed,
                                    "apply-state-retention-" + state);
                            purgeAll(writer, retention);
                            if (state == ApplyReplacementState.PREPUBLICATION) {
                                AtomicBoolean paused = new AtomicBoolean();
                                writer.evidenceReadObserverForTest(phase -> {
                                    if (phase == ZkRunStateStore.EvidenceReadPhase
                                            .APPLY_AFTER_INITIAL_SNAPSHOT
                                            && paused.compareAndSet(
                                                    false, true)) {
                                        layoutReady.countDown();
                                        if (!allowCreate.await(
                                                10, TimeUnit.SECONDS)) {
                                            throw new AssertionError(
                                                    "replacement CREATE "
                                                            + "release timed out");
                                        }
                                    }
                                });
                                Thread thread = new Thread(() -> {
                                    try {
                                        created(writer, runId);
                                    } catch (Throwable failure) {
                                        createFailure.set(failure);
                                    }
                                }, "zk-apply-prepublication-" + recovered);
                                creator.set(thread);
                                thread.start();
                                if (!layoutReady.await(
                                        10, TimeUnit.SECONDS)) {
                                    throw new AssertionError(
                                            "replacement layout not published");
                                }
                            } else if (state
                                    != ApplyReplacementState.PURGED) {
                                RunSnapshot replacement = created(writer, runId);
                                RecoveryToken replacementToken = recovered
                                        ? recoveryClaim(
                                                writer, replacement,
                                                owner, 0L).token()
                                        : null;
                                if (state
                                        == ApplyReplacementState.TERMINAL) {
                                    RunDecision replacementDecision = recovered
                                            ? writer.applyRecovered(
                                                    replacementToken, command)
                                            : writer.apply(command);
                                    assertEquals(RunDecision.Code.ACCEPTED,
                                            replacementDecision.code());
                                }
                            }
                            throw new KeeperException
                                    .ConnectionLossException();
                        }
                    });

                    RunDecision decision = null;
                    RuntimeException failure = null;
                    try {
                        decision = recovered
                                ? reader.applyRecovered(
                                        originalToken, command)
                                : reader.apply(command);
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

                    assertTrue(ackLost.get(), state + "/" + recovered
                            + " must reach AFTER_COMMIT_BEFORE_ACK");
                    if (failure != null) {
                        violations.add(state + "/" + recovered + " threw "
                                + failure.getClass().getSimpleName());
                    } else if (recovered
                            ? decision.code()
                                    != RunDecision.Code.STALE_RECOVERY_OWNER
                            : decision.code() != RunDecision.Code.RUN_NOT_FOUND) {
                        violations.add(state + "/" + recovered + " returned "
                                + decision.code());
                    } else {
                        assertNull(decision.snapshot());
                        assertNull(decision.event());
                    }
                    if (createFailure.get() != null) {
                        violations.add(state + "/" + recovered
                                + " replacement CREATE threw "
                                + createFailure.get().getClass().getSimpleName());
                    }
                    try {
                        RunSnapshot replacement = writer.get(runId);
                        if (state == ApplyReplacementState.PURGED) {
                            if (replacement != null) {
                                violations.add(state + "/" + recovered
                                        + " recreated revision "
                                        + replacement.revision());
                            }
                        } else if (replacement == null) {
                            violations.add(state + "/" + recovered
                                    + " replacement is missing");
                        } else {
                            long expectedRevision = state
                                    == ApplyReplacementState.TERMINAL ? 2L : 1L;
                            if (replacement.revision() != expectedRevision) {
                                violations.add(state + "/" + recovered
                                        + " replacement revision="
                                        + replacement.revision());
                            }
                        }
                    } catch (RuntimeException inspectionFailure) {
                        violations.add(state + "/" + recovered
                                + " replacement inspection threw "
                                + inspectionFailure.getClass().getSimpleName());
                    }
                }
            }
            assertTrue(violations.isEmpty(),
                    "apply ACK reconciliation must fence every replacement "
                            + "state: " + violations);
        }
    }

    private static void assertTransportBudget(
            ZkRunStateStore store,
            ZkRunStateStore.EvidenceReadPhase target,
            int expectedFailures, Runnable invocation) {
        AtomicInteger failures = new AtomicInteger();
        store.evidenceReadObserverForTest(phase -> {
            if (phase == target) {
                failures.incrementAndGet();
                throw new KeeperException.OperationTimeoutException();
            }
        });
        assertThrows(io.rowweave.run.RunStateStoreUnavailableException.class,
                invocation::run);
        assertEquals(expectedFailures, failures.get(),
                target + " transport retry budget");
    }

    private static List<String> runSubtreeFingerprint(
            CuratorFramework client, String runId) throws Exception {
        String root = "/rowweave/run-state-v1/runs/"
                + io.rowweave.run.RunLifecycleCatalogKernel.digestHex(runId);
        List<String> fingerprint = new ArrayList<>();
        appendSubtreeFingerprint(client, root, root, fingerprint);
        return fingerprint;
    }

    private static List<String> authorityFingerprint(
            CuratorFramework client) throws Exception {
        List<String> fingerprint = new ArrayList<>();
        appendSubtreeFingerprint(client, "/rowweave", "/rowweave", fingerprint);
        return fingerprint;
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

    private static void assertStableOrphanRoot(
            CuratorFramework client, String runId, Stat originalRoot)
            throws Exception {
        Stat retainedRoot = client.checkExists().forPath(runRootPath(runId));
        assertNotNull(retainedRoot, "stable orphan must retain the Run root");
        assertEquals(originalRoot.getCzxid(), retainedRoot.getCzxid(),
                "stable orphan must retain the same root incarnation");
        assertNotNull(client.checkExists().forPath(lifecycleGuardPath(runId)),
                "stable orphan must retain its shard guard");
    }

    private static void transitionToStableLayoutOnly(
            CuratorFramework client, String runId, Stat originalRoot,
            AtomicBoolean transitioned,
            AtomicReference<List<String>> fingerprint) throws Exception {
        if (!transitioned.compareAndSet(false, true)) return;
        assertNotNull(client.checkExists().forPath(
                runRootPath(runId) + "/snapshot"));
        assertNotNull(client.checkExists().forPath(lifecycleEntryPath(runId)));
        StableOrphanShape.LAYOUT_ONLY_AFTER_BOTH.corrupt(client, runId);
        assertStableOrphanRoot(client, runId, originalRoot);
        fingerprint.set(authorityFingerprint(client));
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

    private static void assertAckConfirmationUnavailable(
            ZkRunStateStore store,
            ZkRunStateStore.GuardedMutationKind target,
            String label, Runnable invocation, List<String> violations) {
        assertAckConfirmationUnavailable(
                store, target,
                ZkRunStateStore.EvidenceReadPhase
                        .CORE_MISMATCH_BEFORE_CONFIRMATION,
                label, invocation, violations);
    }

    private static void assertAckConfirmationUnavailable(
            ZkRunStateStore store,
            ZkRunStateStore.GuardedMutationKind target,
            ZkRunStateStore.EvidenceReadPhase confirmationPhase,
            String label, Runnable invocation, List<String> violations) {
        AtomicBoolean ackLost = new AtomicBoolean();
        AtomicInteger confirmationFaults = new AtomicInteger();
        store.evidenceReadObserverForTest(phase -> {
            if (ackLost.get()
                    && phase == confirmationPhase) {
                confirmationFaults.incrementAndGet();
                throw new KeeperException.OperationTimeoutException();
            }
        });
        store.guardedMutationObserverForTest((kind, ordinal, moment) -> {
            if (kind == target
                    && moment == ZkRunStateStore.GuardedMutationMoment
                    .AFTER_COMMIT_BEFORE_ACK
                    && ackLost.compareAndSet(false, true)) {
                throw new KeeperException.ConnectionLossException();
            }
        });
        RuntimeException failure = null;
        try {
            invocation.run();
        } catch (RuntimeException thrown) {
            failure = thrown;
        } finally {
            store.evidenceReadObserverForTest(phase -> { });
            store.guardedMutationObserverForTest(
                    (kind, ordinal, moment) -> { });
        }
        if (!ackLost.get()) {
            violations.add(label + " did not lose its committed ACK");
        }
        if (!(failure instanceof RunStateStoreUnavailableException)) {
            violations.add(label + " returned "
                    + (failure == null ? "normally"
                            : failure.getClass().getSimpleName()));
        }
        if (confirmationFaults.get() != 1) {
            violations.add(label + " confirmation faults="
                    + confirmationFaults.get());
        }
    }

    private static Object invokeAcrossResultFenceReplacement(
            ZkRunStateStore reader, CuratorFramework inspector, String runId,
            Runnable replacement, ResultInvocation invocation)
            throws Exception {
        AtomicBoolean replaced = new AtomicBoolean();
        AtomicReference<List<String>> replacementFingerprint =
                new AtomicReference<>();
        reader.evidenceReadObserverForTest(phase -> {
            if (phase == ZkRunStateStore.EvidenceReadPhase
                    .RESULT_BEFORE_AUTHORITY_CONFIRMATION
                    && replaced.compareAndSet(false, true)) {
                replacement.run();
                replacementFingerprint.set(
                        authorityFingerprint(inspector));
            }
        });
        Object result;
        try {
            result = invocation.invoke();
        } finally {
            reader.evidenceReadObserverForTest(phase -> { });
        }
        assertTrue(replaced.get(),
                "result confirmation phase was not reached for " + runId);
        assertEquals(replacementFingerprint.get(),
                authorityFingerprint(inspector),
                "old result projection mutated replacement " + runId);
        return result;
    }

    private static void assertResultConfirmationTransport(
            ZkRunStateStore store, CuratorFramework inspector,
            boolean connectionLoss, String label, Runnable invocation)
            throws Exception {
        List<String> before = authorityFingerprint(inspector);
        AtomicInteger faults = new AtomicInteger();
        store.evidenceReadObserverForTest(phase -> {
            if (phase == ZkRunStateStore.EvidenceReadPhase
                    .RESULT_BEFORE_AUTHORITY_CONFIRMATION) {
                faults.incrementAndGet();
                if (connectionLoss) {
                    throw new KeeperException.ConnectionLossException();
                }
                throw new KeeperException.OperationTimeoutException();
            }
        });
        try {
            assertThrows(RunStateStoreUnavailableException.class, invocation::run,
                    label + " confirmation transport must fail closed");
        } finally {
            store.evidenceReadObserverForTest(phase -> { });
        }
        assertEquals(1, faults.get(),
                label + " confirmation transport must fail once");
        assertEquals(before, authorityFingerprint(inspector),
                label + " confirmation transport must not mutate authority");
    }

    private static void replaceActiveAttempt(
            ZkRunStateStore store, ActiveAttempt original,
            String ownerPrefix) {
        RunSnapshot terminal = terminalize(store, original);
        RetentionToken retention = claim(
                store, terminal, ownerPrefix + "-retention");
        purgeAll(store, retention);
        created(store, original.snapshot.runId());
    }

    private static void replaceReadyRun(
            ZkRunStateStore store, RunSnapshot original, String commandId) {
        RunSnapshot terminal = accepted(store, RunCommand.cancelReadyRun(
                commandId, original.runId(), original.revision(),
                2L, "CANCELED", "replacement setup"));
        RetentionToken token = claim(store, terminal, commandId + "-owner");
        purgeAll(store, token);
        created(store, original.runId());
    }

    private static void purgeCancelingRun(
            ZkRunStateStore store, ActiveAttempt original) {
        RunSnapshot terminal = finishCancelingRun(store, original);
        RetentionToken token = claim(
                store, terminal, "cancel-ack-retention");
        purgeAll(store, token);
    }

    private static RunSnapshot finishCancelingRun(
            ZkRunStateStore store, ActiveAttempt original) {
        RunSnapshot canceling = store.get(original.snapshot.runId());
        AttemptStatus terminalStatus = canceling.deadlineEpochMillis() > 0L
                ? AttemptStatus.TIMED_OUT : AttemptStatus.CANCELED;
        return accepted(store, RunCommand.advanceAttempt(
                "finish-canceling-" + original.snapshot.runId(),
                original.snapshot.runId(), canceling.revision(),
                original.fence.attemptId(), original.fence.fenceToken(),
                terminalStatus, false,
                canceling.updatedAtMillis() + 1L,
                "DRAINED", "replacement setup"));
    }

    private static WholeAttemptRetryDecision scheduleRetry(
            ZkRunStateStore store, ActiveAttempt attempt, boolean recovered) {
        return recovered
                ? store.scheduleRetryRecovered(
                        attempt.recoveryToken, attempt.fence)
                : store.scheduleRetry(attempt.fence);
    }

    private static RetryAttemptClaim claimRetry(
            ZkRunStateStore store, ActiveAttempt attempt, boolean recovered) {
        return recovered
                ? store.claimDueRetryRecovered(
                        attempt.recoveryToken, attempt.snapshot.runId())
                : store.claimDueRetry(attempt.snapshot.runId());
    }

    private static void purgeRetryWaitingRun(
            ZkRunStateStore store, ActiveAttempt original) {
        RunSnapshot terminal = finishRetryWaitingRun(store, original);
        RetentionToken token = claim(
                store, terminal, "retry-wait-retention");
        purgeAll(store, token);
    }

    private static RunSnapshot finishRetryWaitingRun(
            ZkRunStateStore store, ActiveAttempt original) {
        RunSnapshot waiting = store.get(original.snapshot.runId());
        RunCommand cancel = RunCommand.cancelReadyRun(
                "cancel-retry-wait-" + original.snapshot.runId(),
                original.snapshot.runId(), waiting.revision(),
                waiting.updatedAtMillis() + 1L,
                "CANCELED", "replacement setup");
        return original.recoveryToken == null
                ? accepted(store, cancel)
                : acceptedRecovered(store, original.recoveryToken, cancel);
    }

    private static void purgeClaimedRetryRun(
            ZkRunStateStore store, ActiveAttempt original) {
        RunSnapshot terminalSnapshot = finishClaimedRetryRun(store, original);
        RetentionToken token = claim(
                store, terminalSnapshot, "retry-claim-retention");
        purgeAll(store, token);
    }

    private static RunSnapshot finishClaimedRetryRun(
            ZkRunStateStore store, ActiveAttempt original) {
        RunSnapshot claimed = store.get(original.snapshot.runId());
        RunCommand terminal = RunCommand.advanceAttempt(
                "terminal-retry-claim-" + original.snapshot.runId(),
                original.snapshot.runId(), claimed.revision(),
                claimed.currentAttempt().attemptId(),
                claimed.currentAttempt().fenceToken(),
                AttemptStatus.FAILED, false,
                claimed.updatedAtMillis() + 1L,
                "FAILED", "replacement setup");
        return original.recoveryToken == null
                ? accepted(store, terminal)
                : acceptedRecovered(store, original.recoveryToken, terminal);
    }

    private static void replaceTerminalRun(
            ZkRunStateStore store, String runId, String owner) {
        RunSnapshot terminal = store.get(runId);
        RetentionToken token = claim(store, terminal, owner);
        purgeAll(store, token);
        created(store, runId);
    }

    private static RunSnapshot created(ZkRunStateStore store, String runId) {
        return accepted(store, RunCommand.createRun(
                "create-" + runId + "-" + System.nanoTime(), runId,
                "definition", "artifact", 1, 0L, 1L));
    }

    private static RunSnapshot terminal(ZkRunStateStore store, String runId) {
        RunSnapshot created = created(store, runId);
        return accepted(store, RunCommand.cancelReadyRun(
                "cancel-" + runId, runId, created.revision(),
                2L, "CANCELED", "terminal"));
    }

    private static TerminalAttempt terminalAttempt(
            ZkRunStateStore store, String runId) {
        ActiveAttempt active = activeAttempt(store, runId, 0L, false);
        RunSnapshot terminal = terminalize(store, active);
        return new TerminalAttempt(terminal, active.fence, null);
    }

    private static TerminalAttempt terminalAttemptRecovered(
            ZkRunStateStore store, String runId) {
        ActiveAttempt active = activeAttempt(store, runId, 0L, true);
        RunSnapshot terminal = terminalize(store, active);
        return new TerminalAttempt(
                terminal, active.fence, active.recoveryToken);
    }

    private static ActiveAttempt activeDueAttempt(
            ZkRunStateStore store, String runId, boolean recovered) {
        ActiveAttempt active = activeAttempt(store, runId, 1L, recovered);
        RunSnapshot dispatching = advance(
                store, active, AttemptStatus.DISPATCHING, 3L);
        ActiveAttempt dispatched = new ActiveAttempt(
                dispatching, active.fence, active.recoveryToken);
        RunSnapshot running = advance(
                store, dispatched, AttemptStatus.RUNNING, 4L);
        return new ActiveAttempt(running, active.fence, active.recoveryToken);
    }

    private static ActiveAttempt runningAttempt(
            ZkRunStateStore store, String runId, boolean recovered) {
        ActiveAttempt active = activeAttempt(store, runId, 0L, recovered);
        RunSnapshot dispatching = advance(
                store, active, AttemptStatus.DISPATCHING, 3L);
        ActiveAttempt dispatched = new ActiveAttempt(
                dispatching, active.fence, active.recoveryToken);
        RunSnapshot running = advance(
                store, dispatched, AttemptStatus.RUNNING, 4L);
        return new ActiveAttempt(running, active.fence, active.recoveryToken);
    }

    private static ActiveAttempt retryWaitingDueAttempt(
            ZkRunStateStore store, String runId, boolean recovered) {
        long deadline = System.currentTimeMillis() + 1_000L;
        ActiveAttempt active = activeAttempt(store, runId, deadline, recovered);
        WholeAttemptRetryDecision scheduled = recovered
                ? store.scheduleRetryRecovered(active.recoveryToken, active.fence)
                : store.scheduleRetry(active.fence);
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED, scheduled.code());
        while (System.currentTimeMillis() < deadline) {
            Thread.yield();
        }
        return new ActiveAttempt(
                scheduled.snapshot(), active.fence, active.recoveryToken);
    }

    private static ActiveAttempt retryWaitingClaimableAttempt(
            ZkRunStateStore store, String runId, boolean recovered) {
        ActiveAttempt active = activeAttempt(store, runId, 0L, recovered);
        WholeAttemptRetryDecision scheduled = scheduleRetry(
                store, active, recovered);
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED, scheduled.code());
        long timeout = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        RetryReadyEvaluation ready;
        do {
            ready = store.evaluateRetryReady(runId);
            if (ready.code() == RetryReadyEvaluation.Code.DUE) break;
            Thread.yield();
        } while (System.nanoTime() < timeout);
        assertEquals(RetryReadyEvaluation.Code.DUE, ready.code());
        return new ActiveAttempt(
                scheduled.snapshot(), active.fence, active.recoveryToken);
    }

    private static ActiveAttempt activeAttempt(
            ZkRunStateStore store, String runId, long deadline,
            boolean recovered) {
        RunSnapshot ready = accepted(store, RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact",
                2, deadline, 1L));
        RunSnapshot active = accepted(store, RunCommand.createAttempt(
                "attempt-" + runId, runId, ready.revision(),
                "attempt", "publication", 2L));
        RunAttemptFence fence = new RunAttemptFence(
                runId, active.currentAttempt().attemptId(),
                active.currentAttempt().fenceToken());
        RecoveryToken token = null;
        if (recovered) {
            RecoveryClaim claim = store.claim(
                    runId, active.revision(), 0L,
                    new RecoveryOwner("recovery-" + runId, 1L));
            assertEquals(RecoveryClaim.Code.ACQUIRED, claim.code());
            token = claim.token();
        }
        return new ActiveAttempt(active, fence, token);
    }

    private static RunSnapshot terminalize(
            ZkRunStateStore store, ActiveAttempt active) {
        RunCommand terminal = RunCommand.advanceAttempt(
                "terminal-" + active.snapshot.runId(), active.snapshot.runId(),
                active.snapshot.revision(),
                active.fence.attemptId(), active.fence.fenceToken(),
                AttemptStatus.FAILED, false,
                active.snapshot.updatedAtMillis() + 1L,
                "FAILED", "terminal");
        return active.recoveryToken == null
                ? accepted(store, terminal)
                : acceptedRecovered(store, active.recoveryToken, terminal);
    }

    private static RunSnapshot advance(
            ZkRunStateStore store, ActiveAttempt active,
            AttemptStatus target, long eventTime) {
        RunCommand command = RunCommand.advanceAttempt(
                target.name().toLowerCase(java.util.Locale.ROOT)
                        + "-" + active.snapshot.runId(),
                active.snapshot.runId(), active.snapshot.revision(),
                active.fence.attemptId(), active.fence.fenceToken(),
                target, false, eventTime, null, null);
        return active.recoveryToken == null
                ? accepted(store, command)
                : acceptedRecovered(store, active.recoveryToken, command);
    }

    private static void installPostDeadlinePurge(
            ZkRunStateStore reader, ZkRunStateStore writer,
            ActiveAttempt active, ZkRunStateStore.EvidenceReadPhase phase) {
        AtomicBoolean purged = new AtomicBoolean();
        reader.evidenceReadObserverForTest(observed -> {
            if (observed == phase && purged.compareAndSet(false, true)) {
                RunSnapshot afterDeadline = writer.get(active.snapshot.runId());
                RunSnapshot terminalSnapshot = afterDeadline;
                if (!afterDeadline.status().isTerminal()) {
                    AttemptStatus terminalStatus = afterDeadline.currentAttempt().status()
                            == AttemptStatus.CANCELING
                            ? AttemptStatus.TIMED_OUT : AttemptStatus.FAILED;
                    RunCommand terminal = RunCommand.advanceAttempt(
                            "finish-" + active.snapshot.runId(),
                            active.snapshot.runId(), afterDeadline.revision(),
                            active.fence.attemptId(), active.fence.fenceToken(),
                            terminalStatus, false,
                            afterDeadline.updatedAtMillis() + 1L,
                            "FAILED", "finish timeout cancellation");
                    terminalSnapshot = active.recoveryToken == null
                            ? accepted(writer, terminal)
                            : acceptedRecovered(
                                    writer, active.recoveryToken, terminal);
                }
                RetentionToken retention = claim(
                        writer, terminalSnapshot, phase.name() + "-owner");
                purgeAll(writer, retention);
            }
        });
    }

    private static void installPostDeadlineReplacement(
            ZkRunStateStore reader, ZkRunStateStore writer,
            CuratorFramework observer, ActiveAttempt active,
            ZkRunStateStore.EvidenceReadPhase phase,
            AtomicBoolean replaced,
            AtomicReference<List<String>> replacementFingerprint) {
        reader.evidenceReadObserverForTest(observed -> {
            if (observed == phase && replaced.compareAndSet(false, true)) {
                RunSnapshot afterDeadline = writer.get(active.snapshot.runId());
                RunSnapshot terminalSnapshot = afterDeadline;
                if (!afterDeadline.status().isTerminal()) {
                    terminalSnapshot = terminalize(
                            writer, new ActiveAttempt(
                                    afterDeadline, active.fence,
                                    active.recoveryToken));
                }
                RetentionToken token = claim(
                        writer, terminalSnapshot,
                        "post-deadline-aba-" + active.snapshot.runId());
                purgeAll(writer, token);
                ActiveAttempt replacement = activeAttempt(
                        writer, active.snapshot.runId(), 0L, true);
                assertEquals(active.recoveryToken, replacement.recoveryToken,
                        "fixture must recreate the same public recovery token");
                replacementFingerprint.set(runSubtreeFingerprint(
                        observer, active.snapshot.runId()));
            }
        });
    }

    private static RunSnapshot acceptedRecovered(
            ZkRunStateStore store, RecoveryToken token, RunCommand command) {
        RunDecision decision = store.applyRecovered(token, command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }

    private static RecoveryClaim recoveryClaim(
            ZkRunStateStore store, RunSnapshot snapshot,
            RecoveryOwner owner, long observedEpoch) {
        RecoveryClaim claim = store.claim(
                snapshot.runId(), snapshot.revision(), observedEpoch, owner);
        assertEquals(RecoveryClaim.Code.ACQUIRED, claim.code());
        return claim;
    }

    private static void installFinalPurge(
            ZkRunStateStore reader, ZkRunStateStore writer,
            RunSnapshot terminal, ZkRunStateStore.EvidenceReadPhase phase) {
        RetentionToken token = claim(writer, terminal, phase.name() + "-owner");
        advanceToFinalReady(writer, token);
        AtomicBoolean purged = new AtomicBoolean();
        reader.evidenceReadObserverForTest(observed -> {
            if (observed == phase && purged.compareAndSet(false, true)) {
                assertEquals(RunPurgeProgress.Code.PURGED,
                        writer.purge(token, 1000).code());
            }
        });
    }

    private static void assertDeadlineMissing(RunDeadlineResult result) {
        assertEquals(RunDeadlineResult.Disposition.NOT_FOUND,
                result.disposition());
        assertNull(result.status());
        assertEquals(0L, result.revision());
        assertEquals(0L, result.deadlineEpochMillis());
        assertEquals(0L, result.observedAuthorityTimeMillis());
    }

    private static void assertRetryScheduleMissing(
            WholeAttemptRetryDecision result) {
        assertEquals(WholeAttemptRetryDecision.Code.NOT_FOUND, result.code());
        assertNull(result.snapshot());
        assertNull(result.retryClass());
        assertEquals(0L, result.observedAtMillis());
    }

    private static void assertRetryReadyMissing(RetryReadyEvaluation result) {
        assertEquals(RetryReadyEvaluation.Code.NOT_FOUND, result.code());
        assertNull(result.snapshot());
        assertNull(result.identity());
        assertEquals(0L, result.observedAtMillis());
        assertEquals(0L, result.notBeforeMillis());
        assertEquals(0, result.remainingAttempts());
    }

    private static void assertRetryClaimMissing(RetryAttemptClaim result) {
        assertEquals(RetryAttemptClaim.Code.NOT_FOUND, result.code());
        assertNull(result.snapshot());
        assertNull(result.identity());
        assertEquals(0L, result.observedAtMillis());
        assertEquals(0L, result.notBeforeMillis());
    }

    private static RetentionToken claim(
            ZkRunStateStore store, RunSnapshot terminal, String owner) {
        RunLifecycleRef lifecycle = store.lifecycle(terminal.runId());
        RunRetentionClaim claim = store.claim(
                terminal.runId(), terminal.revision(),
                lifecycle.terminalAtMillis(), new RetentionOwner(owner, 1L), 0L);
        assertEquals(RunRetentionClaim.Code.ACQUIRED, claim.code());
        return claim.token();
    }

    private static void advanceToFinalReady(
            ZkRunStateStore store, RetentionToken token) {
        RunPurgeProgress progress = store.purge(token, 1000);
        assertEquals(RunPurgeProgress.Code.IN_PROGRESS, progress.code());
    }

    private static void purgeAll(ZkRunStateStore store, RetentionToken token) {
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
        if (!client.blockUntilConnected(30, TimeUnit.SECONDS)) {
            client.close();
            throw new IllegalStateException("Curator did not connect");
        }
        return client;
    }

    private static final class TerminalAttempt {
        private final RunSnapshot snapshot;
        private final RunAttemptFence fence;
        private final RecoveryToken recoveryToken;

        private TerminalAttempt(
                RunSnapshot snapshot, RunAttemptFence fence,
                RecoveryToken recoveryToken) {
            this.snapshot = snapshot;
            this.fence = fence;
            this.recoveryToken = recoveryToken;
        }
    }

    private enum ApplyReplacementState {
        PURGED,
        PREPUBLICATION,
        ACTIVE,
        TERMINAL
    }

    private interface ResultInvocation {
        Object invoke();
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

    private enum StableOrphanMutationCase {
        APPLY {
            @Override
            StableMutationProbe setup(
                    ZkRunStateStore reader, ZkRunStateStore writer,
                    String runId) {
                RunSnapshot snapshot = created(writer, runId);
                RunCommand command = RunCommand.cancelReadyRun(
                        "stable-apply", runId, snapshot.revision(),
                        2L, "CANCELED", "stable orphan");
                return new StableMutationProbe(() -> reader.apply(command));
            }
        },
        APPLY_RECOVERED {
            @Override
            StableMutationProbe setup(
                    ZkRunStateStore reader, ZkRunStateStore writer,
                    String runId) {
                RunSnapshot snapshot = created(writer, runId);
                RecoveryToken token = recoveryClaim(
                        writer, snapshot,
                        new RecoveryOwner("stable-apply-owner", 1L),
                        0L).token();
                RunCommand command = RunCommand.cancelReadyRun(
                        "stable-apply-recovered", runId, snapshot.revision(),
                        2L, "CANCELED", "stable orphan");
                return new StableMutationProbe(
                        () -> reader.applyRecovered(token, command));
            }
        },
        CANCEL {
            @Override
            StableMutationProbe setup(
                    ZkRunStateStore reader, ZkRunStateStore writer,
                    String runId) {
                ActiveAttempt attempt = runningAttempt(writer, runId, false);
                return new StableMutationProbe(
                        () -> reader.requestUserCancellation(
                                attempt.snapshot.runId(), "stable-cancel"));
            }
        },
        DEADLINE {
            @Override
            StableMutationProbe setup(
                    ZkRunStateStore reader, ZkRunStateStore writer,
                    String runId) {
                ActiveAttempt attempt = activeDueAttempt(
                        writer, runId, false);
                return new StableMutationProbe(
                        () -> reader.evaluateDeadline(
                                attempt.snapshot.runId()));
            }
        },
        RETRY_SCHEDULE {
            @Override
            StableMutationProbe setup(
                    ZkRunStateStore reader, ZkRunStateStore writer,
                    String runId) {
                ActiveAttempt attempt = activeAttempt(
                        writer, runId, 0L, false);
                return new StableMutationProbe(
                        () -> reader.scheduleRetry(attempt.fence));
            }
        },
        RETRY_SCHEDULE_RECOVERED {
            @Override
            StableMutationProbe setup(
                    ZkRunStateStore reader, ZkRunStateStore writer,
                    String runId) {
                ActiveAttempt attempt = activeAttempt(
                        writer, runId, 0L, true);
                return new StableMutationProbe(
                        () -> reader.scheduleRetryRecovered(
                                attempt.recoveryToken, attempt.fence));
            }
        },
        RETRY_READY {
            @Override
            StableMutationProbe setup(
                    ZkRunStateStore reader, ZkRunStateStore writer,
                    String runId) {
                retryWaitingClaimableAttempt(writer, runId, false);
                return new StableMutationProbe(
                        () -> reader.evaluateRetryReady(runId));
            }
        },
        RETRY_CLAIM {
            @Override
            StableMutationProbe setup(
                    ZkRunStateStore reader, ZkRunStateStore writer,
                    String runId) {
                retryWaitingClaimableAttempt(writer, runId, false);
                return new StableMutationProbe(
                        () -> reader.claimDueRetry(runId));
            }
        },
        RETRY_CLAIM_RECOVERED {
            @Override
            StableMutationProbe setup(
                    ZkRunStateStore reader, ZkRunStateStore writer,
                    String runId) {
                ActiveAttempt attempt = retryWaitingClaimableAttempt(
                        writer, runId, true);
                return new StableMutationProbe(
                        () -> reader.claimDueRetryRecovered(
                                attempt.recoveryToken, runId));
            }
        },
        RECOVERY_CLAIM {
            @Override
            StableMutationProbe setup(
                    ZkRunStateStore reader, ZkRunStateStore writer,
                    String runId) {
                RunSnapshot snapshot = created(writer, runId);
                return new StableMutationProbe(() -> reader.claim(
                        runId, snapshot.revision(), 0L,
                        new RecoveryOwner("stable-recovery-owner", 1L)));
            }
        },
        RETENTION_CLAIM {
            @Override
            StableMutationProbe setup(
                    ZkRunStateStore reader, ZkRunStateStore writer,
                    String runId) {
                RunSnapshot snapshot = terminal(writer, runId);
                long terminalAt = writer.lifecycle(runId).terminalAtMillis();
                return new StableMutationProbe(() -> reader.claim(
                        runId, snapshot.revision(), terminalAt,
                        new RetentionOwner("stable-retention-owner", 1L),
                        0L));
            }
        },
        PURGE {
            @Override
            StableMutationProbe setup(
                    ZkRunStateStore reader, ZkRunStateStore writer,
                    String runId) {
                RunSnapshot snapshot = terminal(writer, runId);
                RetentionToken token = claim(
                        writer, snapshot, "stable-purge-owner");
                return new StableMutationProbe(() -> reader.purge(token, 1));
            }
        };

        private void installLayoutTransition(
                ZkRunStateStore store, CuratorFramework client,
                String runId, Stat originalRoot,
                AtomicBoolean transitioned,
                AtomicReference<List<String>> fingerprint) {
            if (this == RETRY_READY) {
                store.evidenceReadObserverForTest(phase -> {
                    if (phase == ZkRunStateStore.EvidenceReadPhase
                            .CORE_AFTER_VALIDATED_PAIR) {
                        transitionToStableLayoutOnly(
                                client, runId, originalRoot,
                                transitioned, fingerprint);
                    }
                });
                return;
            }
            ZkRunStateStore.GuardedMutationKind target = guardedKind();
            store.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == target
                        && moment == ZkRunStateStore.GuardedMutationMoment
                        .BEFORE_COMMIT) {
                    transitionToStableLayoutOnly(
                            client, runId, originalRoot,
                            transitioned, fingerprint);
                }
            });
        }

        private ZkRunStateStore.GuardedMutationKind guardedKind() {
            switch (this) {
                case APPLY:
                case APPLY_RECOVERED:
                    return ZkRunStateStore.GuardedMutationKind.APPLY_EXISTING;
                case CANCEL:
                    return ZkRunStateStore.GuardedMutationKind.CANCEL;
                case DEADLINE:
                    return ZkRunStateStore.GuardedMutationKind.DEADLINE;
                case RETRY_SCHEDULE:
                case RETRY_SCHEDULE_RECOVERED:
                    return ZkRunStateStore.GuardedMutationKind.RETRY_SCHEDULE;
                case RETRY_CLAIM:
                case RETRY_CLAIM_RECOVERED:
                    return ZkRunStateStore.GuardedMutationKind.RETRY_CLAIM;
                case RECOVERY_CLAIM:
                    return ZkRunStateStore.GuardedMutationKind.RECOVERY_CLAIM;
                case RETENTION_CLAIM:
                    return ZkRunStateStore.GuardedMutationKind.RETENTION_CLAIM;
                case PURGE:
                    return ZkRunStateStore.GuardedMutationKind.PURGE_BATCH;
                case RETRY_READY:
                default:
                    throw new AssertionError(
                            "retry-ready has no mutation transaction");
            }
        }

        abstract StableMutationProbe setup(
                ZkRunStateStore reader, ZkRunStateStore writer, String runId);
    }

    @FunctionalInterface
    private interface MutationInvocation {
        Object get();
    }

    private static final class TransactionWrites
            implements ZkRunStateStore.TransactionWriteObserver {
        private final List<TransactionWrite> records = new ArrayList<>();

        @Override
        public void onWrite(
                ZkRunStateStore.GuardedMutationKind kind, int ordinal,
                int operationCount, int requestBytes) {
            records.add(new TransactionWrite(
                    kind, ordinal, operationCount, requestBytes));
        }
    }

    private static final class TransactionWrite {
        private final ZkRunStateStore.GuardedMutationKind kind;
        private final int ordinal;
        private final int operationCount;
        private final int requestBytes;

        private TransactionWrite(
                ZkRunStateStore.GuardedMutationKind kind, int ordinal,
                int operationCount, int requestBytes) {
            this.kind = kind;
            this.ordinal = ordinal;
            this.operationCount = operationCount;
            this.requestBytes = requestBytes;
        }

        @Override
        public String toString() {
            return kind + "[" + ordinal + "] ops=" + operationCount
                    + " bytes=" + requestBytes;
        }
    }

    private static final class StableMutationProbe {
        private final MutationInvocation invoke;

        private StableMutationProbe(MutationInvocation invoke) {
            this.invoke = invoke;
        }
    }

    private enum UnaryAckKind {
        CANCELLATION(ZkRunStateStore.GuardedMutationKind.CANCEL),
        DEADLINE(ZkRunStateStore.GuardedMutationKind.DEADLINE);

        private final ZkRunStateStore.GuardedMutationKind mutationKind;

        UnaryAckKind(ZkRunStateStore.GuardedMutationKind mutationKind) {
            this.mutationKind = mutationKind;
        }

        private ActiveAttempt active(ZkRunStateStore store, String runId) {
            return this == CANCELLATION
                    ? runningAttempt(store, runId, false)
                    : activeDueAttempt(store, runId, false);
        }

        private Object invoke(
                ZkRunStateStore store, ActiveAttempt attempt) {
            return this == CANCELLATION
                    ? store.requestUserCancellation(
                            attempt.snapshot.runId(), "shared-request")
                    : store.evaluateDeadline(attempt.snapshot.runId());
        }

        private boolean isMissing(Object result) {
            if (this == CANCELLATION) {
                RunCancellationResult cancellation =
                        (RunCancellationResult) result;
                return cancellation.disposition()
                                == RunCancellationResult.Disposition.NOT_FOUND
                        && cancellation.status() == null
                        && cancellation.revision() == 0L;
            }
            RunDeadlineResult deadline = (RunDeadlineResult) result;
            return deadline.disposition()
                            == RunDeadlineResult.Disposition.NOT_FOUND
                    && deadline.status() == null
                    && deadline.revision() == 0L
                    && deadline.deadlineEpochMillis() == 0L
                    && deadline.observedAuthorityTimeMillis() == 0L;
        }

        private Object resultCode(Object result) {
            return this == CANCELLATION
                    ? ((RunCancellationResult) result).disposition()
                    : ((RunDeadlineResult) result).disposition();
        }
    }

    private enum RetryAckKind {
        SCHEDULE(ZkRunStateStore.GuardedMutationKind.RETRY_SCHEDULE, 3L),
        CLAIM(ZkRunStateStore.GuardedMutationKind.RETRY_CLAIM, 4L);

        private final ZkRunStateStore.GuardedMutationKind mutationKind;
        private final long activeRevision;

        RetryAckKind(
                ZkRunStateStore.GuardedMutationKind mutationKind,
                long activeRevision) {
            this.mutationKind = mutationKind;
            this.activeRevision = activeRevision;
        }

        private ActiveAttempt original(
                ZkRunStateStore store, String runId, boolean recovered) {
            return this == SCHEDULE
                    ? activeAttempt(store, runId, 0L, recovered)
                    : retryWaitingClaimableAttempt(store, runId, recovered);
        }

        private Object invoke(
                ZkRunStateStore store, ActiveAttempt attempt,
                boolean recovered) {
            return this == SCHEDULE
                    ? scheduleRetry(store, attempt, recovered)
                    : claimRetry(store, attempt, recovered);
        }

        private void purgeOriginal(
                ZkRunStateStore store, ActiveAttempt original) {
            if (this == SCHEDULE) purgeRetryWaitingRun(store, original);
            else purgeClaimedRetryRun(store, original);
        }

        private void finish(
                ZkRunStateStore store, ActiveAttempt replacement) {
            if (this == SCHEDULE) {
                finishRetryWaitingRun(store, replacement);
            } else {
                finishClaimedRetryRun(store, replacement);
            }
        }

        private boolean isMissing(Object result) {
            if (this == SCHEDULE) {
                WholeAttemptRetryDecision schedule =
                        (WholeAttemptRetryDecision) result;
                return schedule.code()
                                == WholeAttemptRetryDecision.Code.NOT_FOUND
                        && schedule.snapshot() == null
                        && schedule.retryClass() == null
                        && schedule.observedAtMillis() == 0L;
            }
            RetryAttemptClaim claim = (RetryAttemptClaim) result;
            return claim.code() == RetryAttemptClaim.Code.NOT_FOUND
                    && claim.snapshot() == null
                    && claim.identity() == null
                    && claim.observedAtMillis() == 0L
                    && claim.notBeforeMillis() == 0L;
        }

        private Object resultCode(Object result) {
            return this == SCHEDULE
                    ? ((WholeAttemptRetryDecision) result).code()
                    : ((RetryAttemptClaim) result).code();
        }
    }

    private static final class ActiveAttempt {
        private final RunSnapshot snapshot;
        private final RunAttemptFence fence;
        private final RecoveryToken recoveryToken;

        private ActiveAttempt(
                RunSnapshot snapshot, RunAttemptFence fence,
                RecoveryToken recoveryToken) {
            this.snapshot = snapshot;
            this.fence = fence;
            this.recoveryToken = recoveryToken;
        }
    }
}
