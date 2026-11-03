package io.rowweave.cluster.registry.zk;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.run.AttemptOutputProfile;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.BatchSinkAbortDecision;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RetryAttemptClaim;
import io.rowweave.run.RetryReadyEvaluation;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStateStoreUnavailableException;
import io.rowweave.run.StageAcceptanceDecision;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageTerminalDecision;
import io.rowweave.run.StageTerminalReceipt;
import io.rowweave.run.WholeAttemptRetryAuthority;
import io.rowweave.run.WholeAttemptRetryDecision;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** real ZooKeeper bounded whole-attempt retry authority. */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class ZkWholeAttemptRetryAuthorityTest {
    private static final String TIME_ORACLE = "/rowweave/run-deadline-v1/time";

    @Test
    void preDispatchScheduleAndDueClaimUseZkServerTime() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework zk = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(zk);
            WholeAttemptRetryAuthority authority = authority(store);
            RunSnapshot first = attempt(store, "server-time", 2);

            WholeAttemptRetryDecision scheduled =
                    authority.scheduleRetry(fence(first));

            assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                    scheduled.code());
            Stat oracle = new Stat();
            zk.getData().storingStatIn(oracle).forPath(TIME_ORACLE);
            assertEquals(oracle.getMtime(), scheduled.observedAtMillis());
            assertEquals(scheduled.observedAtMillis(),
                    scheduled.snapshot().currentAttempt().updatedAtMillis());

            RetryReadyEvaluation ready;
            long timeout = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
            do {
                ready = authority.evaluateRetryReady(first.runId());
                if (ready.code() == RetryReadyEvaluation.Code.DUE) break;
                Thread.sleep(10L);
            } while (System.nanoTime() < timeout);
            assertEquals(RetryReadyEvaluation.Code.DUE, ready.code());
            assertTrue(ready.observedAtMillis() >= ready.notBeforeMillis());

            RetryAttemptClaim claimed = authority.claimDueRetry(first.runId());
            assertEquals(RetryAttemptClaim.Code.CLAIMED, claimed.code());
            assertEquals(2, claimed.snapshot().attemptCount());
            assertEquals(ready.identity(), claimed.identity());
        }
    }

    @Test
    void twoSessionsLinearizeOneScheduleAndReplayTheSameClosure() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework leftClient = client(server);
             CuratorFramework rightClient = client(server)) {
            ZkRunStateStore leftStore = new ZkRunStateStore(leftClient);
            ZkRunStateStore rightStore = new ZkRunStateStore(rightClient);
            RunSnapshot first = attempt(leftStore, "dual-session", 2);
            CountDownLatch start = new CountDownLatch(1);
            AtomicReference<WholeAttemptRetryDecision> left = new AtomicReference<>();
            AtomicReference<WholeAttemptRetryDecision> right = new AtomicReference<>();
            ExecutorService pool = Executors.newFixedThreadPool(2);
            pool.submit(() -> {
                start.await();
                left.set(authority(leftStore).scheduleRetry(fence(first)));
                return null;
            });
            pool.submit(() -> {
                start.await();
                right.set(authority(rightStore).scheduleRetry(fence(first)));
                return null;
            });
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10L, TimeUnit.SECONDS));

            assertEquals(1L, Arrays.asList(left.get(), right.get()).stream()
                    .filter(result -> result.code()
                            == WholeAttemptRetryDecision.Code.SCHEDULED)
                    .count());
            assertEquals(1L, Arrays.asList(left.get(), right.get()).stream()
                    .filter(result -> result.code()
                            == WholeAttemptRetryDecision.Code.ALREADY_SCHEDULED)
                    .count());
            assertEquals(left.get().snapshot().revision(),
                    right.get().snapshot().revision());
        }
    }

    @Test
    void scheduleCommitAckLossReconcilesOneDurableEvent() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework zk = client(server)) {
            AtomicBoolean loseAck = new AtomicBoolean();
            ZkRunStateStore store = new ZkRunStateStore(zk, () -> {
                if (loseAck.compareAndSet(true, false)) {
                    throw new KeeperException.ConnectionLossException();
                }
            });
            RunSnapshot first = attempt(store, "ack-loss", 2);
            loseAck.set(true);

            WholeAttemptRetryDecision replayed =
                    authority(store).scheduleRetry(fence(first));

            assertEquals(WholeAttemptRetryDecision.Code.ALREADY_SCHEDULED,
                    replayed.code());
            assertEquals(3, store.events(first.runId(), 0L, 20).size());
        }
    }

    @Test
    void replacementRecoveryOwnerFencesTheOldScheduler() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework zk = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(zk);
            RunSnapshot first = attempt(store, "owner-replacement", 2);
            RecoveryClaim initial = store.claim(first.runId(), first.revision(), 0L,
                    new RecoveryOwner("master-a", 1L));
            assertEquals(RecoveryClaim.Code.ACQUIRED, initial.code());
            RecoveryClaim replacement = store.claim(first.runId(), first.revision(),
                    initial.token().ownershipEpoch(),
                    new RecoveryOwner("master-b", 2L));
            assertEquals(RecoveryClaim.Code.ACQUIRED, replacement.code());

            WholeAttemptRetryAuthority authority = authority(store);
            assertEquals(WholeAttemptRetryDecision.Code.STALE_RECOVERY_OWNER,
                    authority.scheduleRetryRecovered(
                            initial.token(), fence(first)).code());
            assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                    authority.scheduleRetryRecovered(
                            replacement.token(), fence(first)).code());
        }
    }

    @Test
    void manifestWorkerLossNeedsExactCompletedAbortEvidence() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework zk = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(zk);
            RunSnapshot running = runningAssignment(
                    store, "manifest-abort",
                    AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                    StageAssignmentRole.SINK, 1);
            RecoveryToken owner = acquired(store, running, "master-a", 1L);
            recordLost(store, owner, fence(running), 0);

            assertEquals(WholeAttemptRetryDecision.Code.CLOSURE_PENDING,
                    authority(store).scheduleRetryRecovered(
                            owner, fence(running)).code());
            BatchSinkAbortRecord decided = BatchSinkAbortRecord.decided(
                    fence(running), 1L,
                    BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                    "domain", 1L, running.currentAttempt().publicationId(),
                    digest(3), "RETRY_ROLLBACK", 20L);
            assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                    store.recordBatchSinkAbortRecovered(owner, decided).code());
            assertEquals(WholeAttemptRetryDecision.Code.CLOSURE_PENDING,
                    authority(store).scheduleRetryRecovered(
                            owner, fence(running)).code());
            assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                    store.recordBatchSinkAbortRecovered(
                            owner, decided.completed(21L)).code());

            assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                    authority(store).scheduleRetryRecovered(
                            owner, fence(running)).code());
        }
    }

    @Test
    void closureResultConfirmationReclassifiesChangedAbortEvidence()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = client(server);
             CuratorFramework second = client(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            RunSnapshot running = runningAssignment(
                    writer, "closure-result-fence",
                    AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                    StageAssignmentRole.SINK, 1);
            RecoveryToken owner = acquired(
                    writer, running, "closure-owner", 1L);
            recordLost(writer, owner, fence(running), 0);
            BatchSinkAbortRecord decided = BatchSinkAbortRecord.decided(
                    fence(running), 1L,
                    BatchSinkDescriptor.CapabilityProfile
                            .MANIFEST_ATOMIC_GROUP_V1,
                    "domain", 1L,
                    running.currentAttempt().publicationId(),
                    digest(7), "RETRY_ROLLBACK", 20L);
            AtomicBoolean changed = new AtomicBoolean();
            reader.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase
                        .RESULT_BEFORE_AUTHORITY_CONFIRMATION
                        && changed.compareAndSet(false, true)) {
                    assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                            writer.recordBatchSinkAbortRecovered(
                                    owner, decided).code());
                    assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                            writer.recordBatchSinkAbortRecovered(
                                    owner, decided.completed(21L)).code());
                }
            });

            WholeAttemptRetryDecision result = reader
                    .scheduleRetryRecovered(owner, fence(running));

            assertTrue(changed.get());
            assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                    result.code(),
                    "stale CLOSURE_PENDING must be discarded after abort changes");
        }
    }

    @Test
    void closureResultConfirmationFencesLegacyAbsentAbortEvidence()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = client(server);
             CuratorFramework second = client(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            RunSnapshot running = runningAssignment(
                    writer, "closure-legacy-absent-abort",
                    AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                    StageAssignmentRole.SINK, 1);
            RecoveryToken owner = acquired(
                    writer, running, "closure-owner", 1L);
            recordLost(writer, owner, fence(running), 0);
            second.delete().forPath("/rowweave/run-state-v1/runs/"
                    + sha256Hex(running.runId()) + "/sink-abort");
            BatchSinkAbortRecord decided = BatchSinkAbortRecord.decided(
                    fence(running), 1L,
                    BatchSinkDescriptor.CapabilityProfile
                            .MANIFEST_ATOMIC_GROUP_V1,
                    "domain", 1L,
                    running.currentAttempt().publicationId(),
                    digest(8), "RETRY_ROLLBACK", 20L);
            AtomicBoolean changed = new AtomicBoolean();
            reader.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase
                        .RESULT_BEFORE_AUTHORITY_CONFIRMATION
                        && changed.compareAndSet(false, true)) {
                    assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                            writer.recordBatchSinkAbortRecovered(
                                    owner, decided).code());
                    assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                            writer.recordBatchSinkAbortRecovered(
                                    owner, decided.completed(21L)).code());
                }
            });

            WholeAttemptRetryDecision result = reader
                    .scheduleRetryRecovered(owner, fence(running));

            assertTrue(changed.get());
            assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                    result.code(),
                    "legacy physical absence must not authenticate stale closure");
        }
    }

    @Test
    void legacyAbortRepairConfirmationTransportFailsImmediately()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework zk = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(zk);
            RunSnapshot running = runningAssignment(
                    store, "closure-legacy-repair-confirmation",
                    AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                    StageAssignmentRole.SINK, 1);
            RecoveryToken owner = acquired(
                    store, running, "closure-owner", 1L);
            recordLost(store, owner, fence(running), 0);
            zk.delete().forPath("/rowweave/run-state-v1/runs/"
                    + sha256Hex(running.runId()) + "/sink-abort");
            AtomicBoolean ackLost = new AtomicBoolean();
            AtomicInteger confirmationFaults = new AtomicInteger();
            store.evidenceReadObserverForTest(phase -> {
                if (ackLost.get()
                        && phase == ZkRunStateStore.EvidenceReadPhase
                        .CORE_MISMATCH_BEFORE_CONFIRMATION) {
                    confirmationFaults.incrementAndGet();
                    throw new KeeperException.OperationTimeoutException();
                }
            });
            store.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind
                        .SINK_ABORT_GUARD
                        && moment == ZkRunStateStore.GuardedMutationMoment
                        .AFTER_COMMIT_BEFORE_ACK
                        && ackLost.compareAndSet(false, true)) {
                    throw new KeeperException.ConnectionLossException();
                }
            });

            assertThrows(RunStateStoreUnavailableException.class,
                    () -> authority(store).scheduleRetryRecovered(
                            owner, fence(running)));
            assertTrue(ackLost.get());
            assertEquals(1, confirmationFaults.get(),
                    "helper confirmation transport must fail the API once");
        }
    }

    @Test
    void legacyAbortRepairTransportKeepsOneSixteenAttemptBudget()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework zk = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(zk);
            RunSnapshot running = runningAssignment(
                    store, "closure-legacy-repair-budget",
                    AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                    StageAssignmentRole.SINK, 1);
            RecoveryToken owner = acquired(
                    store, running, "closure-owner", 1L);
            recordLost(store, owner, fence(running), 0);
            zk.delete().forPath("/rowweave/run-state-v1/runs/"
                    + sha256Hex(running.runId()) + "/sink-abort");
            AtomicInteger attempts = new AtomicInteger();
            store.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind
                        .SINK_ABORT_GUARD
                        && moment == ZkRunStateStore.GuardedMutationMoment
                        .BEFORE_COMMIT) {
                    attempts.incrementAndGet();
                    throw new KeeperException.ConnectionLossException();
                }
            });

            assertThrows(RunStateStoreUnavailableException.class,
                    () -> authority(store).scheduleRetryRecovered(
                            owner, fence(running)));
            assertEquals(16, attempts.get(),
                    "nested retries must not amplify the helper budget");
        }
    }

    @Test
    void staleEvidenceVersionCannotBeDeletedAndRetryTransactionIsBoundedAt4096()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework zk = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(zk);
            RunSnapshot running = runningAssignment(
                    store, "scale-fence", AttemptOutputProfile.DISCARD,
                    StageAssignmentRole.WHOLE_GRAPH, 4096);
            RecoveryToken owner = acquired(store, running, "master-a", 1L);
            recordLost(store, owner, fence(running), 0);
            AtomicBoolean raceOnce = new AtomicBoolean(true);
            AtomicInteger observedCommits = new AtomicInteger();
            AtomicInteger maxOperations = new AtomicInteger();
            String runPath = "/rowweave/run-state-v1/runs/"
                    + zk.getChildren().forPath("/rowweave/run-state-v1/runs").get(0);
            String terminalShard = runPath
                    + "/stage-assignment/terminal-shards/000";
            store.retryWriteObserverForTest(operationCount -> {
                observedCommits.incrementAndGet();
                maxOperations.accumulateAndGet(operationCount, Math::max);
                if (raceOnce.compareAndSet(true, false)) {
                    byte[] same = zk.getData().forPath(terminalShard);
                    zk.setData().forPath(terminalShard, same);
                }
            });

            WholeAttemptRetryDecision result = authority(store)
                    .scheduleRetryRecovered(owner, fence(running));

            assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED, result.code());
            assertTrue(observedCommits.get() >= 2,
                    "a stale evidence transaction must retry after BadVersion");
            assertTrue(maxOperations.get() <= 272,
                    "4096 assignments must remain bounded by 64 shards");
            assertEquals(null, store.assignmentSnapshot(fence(running)));
        }
    }

    @Test
    void futureBusinessEventTimeCannotBreakServerTimeRetryAuthority()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework zk = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(zk);
            long futureBusinessTime = System.currentTimeMillis() + 60_000L;
            RunSnapshot ready = accepted(store, RunCommand.createRun(
                    "create-clock-anomaly", "clock-anomaly", "definition",
                    "artifact", 2, 0L, futureBusinessTime));
            RunSnapshot first = accepted(store, RunCommand.createAttempt(
                    "attempt-clock-anomaly", ready.runId(), ready.revision(),
                    "attempt-1", "publication-1", futureBusinessTime));

            WholeAttemptRetryDecision scheduled = authority(store)
                    .scheduleRetry(fence(first));

            assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                    scheduled.code());
            assertEquals(futureBusinessTime,
                    scheduled.snapshot().currentAttempt().updatedAtMillis());
            assertTrue(scheduled.observedAtMillis() < futureBusinessTime);
        }
    }

    @Test
    void dueClaimAckLossReplaysTheSingleCreatedAttempt() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework zk = client(server)) {
            AtomicBoolean loseAck = new AtomicBoolean();
            ZkRunStateStore store = new ZkRunStateStore(zk, () -> {
                if (loseAck.compareAndSet(true, false)) {
                    throw new KeeperException.ConnectionLossException();
                }
            });
            RunSnapshot first = attempt(store, "claim-ack-loss", 2);
            WholeAttemptRetryAuthority authority = authority(store);
            authority.scheduleRetry(fence(first));
            awaitDue(authority, first.runId());
            loseAck.set(true);

            RetryAttemptClaim replayed = authority.claimDueRetry(first.runId());

            assertEquals(RetryAttemptClaim.Code.ALREADY_CLAIMED,
                    replayed.code());
            assertEquals(2, replayed.snapshot().attemptCount());
            assertEquals(4, store.events(first.runId(), 0L, 20).size());
        }
    }

    @Test
    void exhaustedClosureNeverPublishesAnIntermediateReadyWindow()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework zk = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(zk);
            RunSnapshot first = attempt(store, "exhausted", 1);
            WholeAttemptRetryAuthority authority = authority(store);

            WholeAttemptRetryDecision exhausted =
                    authority.scheduleRetry(fence(first));

            assertEquals(WholeAttemptRetryDecision.Code.EXHAUSTED,
                    exhausted.code());
            assertEquals(io.rowweave.run.RunStatus.FAILED,
                    exhausted.snapshot().status());
            assertEquals("RETRY_ATTEMPTS_EXHAUSTED",
                    exhausted.snapshot().currentAttempt().reasonCode());
            long terminalRevision = exhausted.snapshot().revision();
            assertEquals(WholeAttemptRetryDecision.Code.EXHAUSTED,
                    authority.scheduleRetry(fence(first)).code());
            assertEquals(terminalRevision, store.get(first.runId()).revision());
        }
    }

    @Test
    void retryRejectionFailsClosedWhenRecoveryCatalogIsMissing()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework zk = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(zk);
            RunSnapshot first = attempt(store, "missing-catalog", 2);
            String digest = sha256Hex(first.runId());
            zk.delete().forPath("/rowweave/run-recovery-v1/shards/"
                    + digest.substring(0, 2) + "/entries/" + digest);
            RunAttemptFence stale = new RunAttemptFence(first.runId(),
                    first.currentAttempt().attemptId(),
                    first.currentAttempt().fenceToken() + 1L);

            assertThrows(IllegalStateException.class,
                    () -> authority(store).scheduleRetry(stale));
        }
    }

    private static WholeAttemptRetryAuthority authority(ZkRunStateStore store) {
        return (WholeAttemptRetryAuthority) (Object) store;
    }

    private static RunSnapshot attempt(
            ZkRunStateStore store, String runId, int maxAttempts) {
        RunSnapshot ready = accepted(store, RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact",
                maxAttempts, 0L, 1L));
        return accepted(store, RunCommand.createAttempt(
                "attempt-" + runId, runId, ready.revision(),
                "attempt-1", "publication-1", 2L));
    }

    private static RunSnapshot runningAssignment(
            ZkRunStateStore store, String runId, AttemptOutputProfile profile,
            StageAssignmentRole firstRole, int assignmentCount) {
        RunSnapshot created = attempt(store, runId, 2);
        RunAttemptFence fence = fence(created);
        List<StageAssignmentRecord> records = new ArrayList<>(assignmentCount);
        for (int i = 0; i < assignmentCount; i++) {
            records.add(StageAssignmentRecord.create(
                    fence, 1L, i, 100L + i, i, 0,
                    i == 0 ? firstRole : StageAssignmentRole.ROUTER,
                    "worker-" + i, "localhost", 7001, -1,
                    "incarnation-" + i));
        }
        StageAssignmentSnapshot assignments = store.publish(
                new StageAssignmentManifest(fence, created.artifactId(), 1L,
                        profile, records)).snapshot();
        assertEquals(StageAcceptanceDecision.Code.ACCEPTED,
                store.accept(new StageAssignmentAcceptance(
                        assignments.token(0), digest(1), 10L)).code());
        RunSnapshot dispatching = accepted(store, RunCommand.advanceAttempt(
                "dispatch-" + runId, runId, created.revision(),
                created.currentAttempt().attemptId(),
                created.currentAttempt().fenceToken(),
                AttemptStatus.DISPATCHING, false, 3L, null, null));
        return accepted(store, RunCommand.advanceAttempt(
                "running-" + runId, runId, dispatching.revision(),
                dispatching.currentAttempt().attemptId(),
                dispatching.currentAttempt().fenceToken(),
                AttemptStatus.RUNNING, false, 4L, null, null));
    }

    private static RetryReadyEvaluation awaitDue(
            WholeAttemptRetryAuthority authority, String runId)
            throws Exception {
        long timeout = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        RetryReadyEvaluation ready;
        do {
            ready = authority.evaluateRetryReady(runId);
            if (ready.code() == RetryReadyEvaluation.Code.DUE) return ready;
            Thread.sleep(10L);
        } while (System.nanoTime() < timeout);
        return ready;
    }

    private static RecoveryToken acquired(
            ZkRunStateStore store, RunSnapshot snapshot,
            String ownerId, long term) {
        RecoveryClaim claim = store.claim(snapshot.runId(), snapshot.revision(), 0L,
                new RecoveryOwner(ownerId, term));
        assertEquals(RecoveryClaim.Code.ACQUIRED, claim.code());
        return claim.token();
    }

    private static void recordLost(
            ZkRunStateStore store, RecoveryToken owner,
            RunAttemptFence fence, int ordinal) {
        StageAssignmentAcceptance acceptance =
                store.assignmentSnapshot(fence).acceptance(ordinal);
        assertEquals(StageTerminalDecision.Code.RECORDED,
                store.recordTerminalRecovered(owner, StageTerminalReceipt.lost(
                        acceptance.token(), acceptance.commandDigest(),
                        "HOST_TERMINATED", digest(2), 0L)).code());
    }

    private static byte[] digest(int value) {
        byte[] digest = new byte[32];
        Arrays.fill(digest, (byte) value);
        return digest;
    }

    private static String sha256Hex(String value) throws Exception {
        byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        StringBuilder out = new StringBuilder();
        for (byte b : digest) {
            out.append(String.format(java.util.Locale.ROOT,
                    "%02x", b & 0xff));
        }
        return out.toString();
    }

    private static RunAttemptFence fence(RunSnapshot snapshot) {
        return new RunAttemptFence(snapshot.runId(),
                snapshot.currentAttempt().attemptId(),
                snapshot.currentAttempt().fenceToken());
    }

    private static RunSnapshot accepted(
            ZkRunStateStore store, RunCommand command) {
        RunDecision decision = store.apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }

    private static CuratorFramework client(TestingServer server) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .sessionTimeoutMs(1_000)
                .connectionTimeoutMs(250)
                .retryPolicy(new ExponentialBackoffRetry(10, 1))
                .build();
        client.start();
        assertTrue(client.blockUntilConnected(5, TimeUnit.SECONDS));
        return client;
    }
}
