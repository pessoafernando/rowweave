package io.rowweave.cluster.registry.zk;

import io.rowweave.commit.CommitManifest;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.BatchSinkAbortDecision;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.CancellationCause;
import io.rowweave.run.ManifestCommitIntent;
import io.rowweave.run.ManifestCommitJournalSnapshot;
import io.rowweave.run.ManifestIntentDecision;
import io.rowweave.run.ManifestMemberDecision;
import io.rowweave.run.ManifestMemberReceipt;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryFencedRunStateStore;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunResultSummary;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageTerminalReceipt;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.zookeeper.KeeperException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZkManifestCommitJournalAuthorityTest {

    @Test
    void twoSessionsShareJournalAndTerminalHidesRetainedEvidence() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = client(server);
             CuratorFramework secondClient = client(server)) {
            ZkRunStateStore first = new ZkRunStateStore(firstClient);
            ZkRunStateStore second = new ZkRunStateStore(secondClient);
            Fixture fixture = fixture(first, "run-zk-manifest-journal");
            ManifestMemberReceipt left = fixture.member(0, "left");
            ManifestMemberReceipt right = fixture.member(64, "right");

            assertEquals(ManifestMemberDecision.Code.RECORDED,
                    first.recordManifestMember(left).code());
            assertEquals(ManifestMemberDecision.Code.ALREADY_RECORDED,
                    second.recordManifestMember(left).code());
            assertEquals(ManifestMemberDecision.Code.RECORDED,
                    second.recordManifestMember(right).code());
            fixture.advanceToCommitting(first, null);

            ManifestCommitJournalSnapshot complete = second
                    .manifestCommitJournal(fixture.manifest.fence());
            assertEquals(2, complete.memberCount());
            ManifestCommitIntent intent = complete.buildIntent();
            assertEquals(ManifestIntentDecision.Code.COMMITTED,
                    first.commitManifestIntent(intent).code());
            assertEquals(ManifestIntentDecision.Code.ALREADY_COMMITTED,
                    second.commitManifestIntent(intent).code());
            assertEquals(intent, second.manifestCommitJournal(
                    fixture.manifest.fence()).intent());

            String assignment = runPath(fixture.run.runId()) + "/stage-assignment";
            List<String> memberShards = firstClient.getChildren()
                    .forPath(assignment + "/manifest-member-shards");
            java.util.Collections.sort(memberShards);
            assertEquals(Arrays.asList("000", "001"), memberShards);
            assertTrue(firstClient.checkExists()
                    .forPath(assignment + "/manifest-intent") != null);

            RunSnapshot current = first.get(fixture.run.runId());
            RunResultSummary summary = RunResultSummary.manifestFrom(
                    fixture.assignments, intent.generation(),
                    MessageDigest.getInstance("SHA-256")
                            .digest(complete.intendedManifest().encode()));
            RunDecision succeeded = first.apply(RunCommand.advanceAttempt(
                    "succeeded", current.runId(), current.revision(),
                    current.currentAttempt().attemptId(), current.currentAttempt().fenceToken(),
                    AttemptStatus.SUCCEEDED, false, 60L, "SUCCEEDED", null, summary));
            assertEquals(RunDecision.Code.ACCEPTED, succeeded.code());
            assertNull(second.manifestCommitJournal(fixture.manifest.fence()));
            assertTrue(firstClient.checkExists()
                    .forPath(assignment + "/manifest-intent") != null,
                    "terminal evidence remains physically retained until final purge");
            List<String> retainedMemberShards = firstClient.getChildren()
                    .forPath(assignment + "/manifest-member-shards");
            java.util.Collections.sort(retainedMemberShards);
            assertEquals(Arrays.asList("000", "001"), retainedMemberShards);
            assertEquals(ManifestMemberDecision.Code.STALE_RUN,
                    second.recordManifestMember(left).code());
            assertEquals(ManifestIntentDecision.Code.STALE_RUN,
                    second.commitManifestIntent(intent).code());
        }
    }

    @Test
    void recoveryClaimAndFirstMemberAcrossSessionsHaveOneWinner() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = client(server);
             CuratorFramework secondClient = client(server)) {
            ZkRunStateStore first = new ZkRunStateStore(firstClient);
            ZkRunStateStore second = new ZkRunStateStore(secondClient);
            Fixture fixture = fixture(first, "run-zk-member-race");
            ManifestMemberReceipt member = fixture.member(0, "left");

            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(2);
            AtomicReference<RecoveryClaim> claim = new AtomicReference<>();
            AtomicReference<ManifestMemberDecision> recorded = new AtomicReference<>();
            pool.submit(() -> {
                start.await();
                claim.set(first.claim(fixture.run.runId(), fixture.run.revision(), 0L,
                        new RecoveryOwner("master-new", 100L)));
                return null;
            });
            pool.submit(() -> {
                start.await();
                recorded.set(second.recordManifestMember(member));
                return null;
            });
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

            assertEquals(RecoveryClaim.Code.ACQUIRED, claim.get().code());
            assertTrue(recorded.get().code() == ManifestMemberDecision.Code.RECORDED
                    || recorded.get().code() == ManifestMemberDecision.Code.RECOVERY_OWNED);
            assertEquals(recorded.get().code() == ManifestMemberDecision.Code.RECORDED ? 1 : 0,
                    first.manifestCommitJournal(fixture.manifest.fence()).memberCount());
        }
    }

    @Test
    void recoveryReplacementFencesMemberAndIntentButExactReplaysSurvive() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = client(server);
             CuratorFramework secondClient = client(server)) {
            ZkRunStateStore first = new ZkRunStateStore(firstClient);
            ZkRunStateStore second = new ZkRunStateStore(secondClient);
            Fixture fixture = fixture(first, "run-zk-manifest-owner");
            RecoveryClaim initial = first.claim(
                    fixture.run.runId(), fixture.run.revision(), 0L,
                    new RecoveryOwner("master-first", 110L));
            ManifestMemberReceipt left = fixture.member(0, "left");
            ManifestMemberReceipt right = fixture.member(64, "right");
            assertEquals(ManifestMemberDecision.Code.RECORDED,
                    second.recordManifestMemberRecovered(initial.token(), left).code());

            RecoveryClaim replacement = first.claim(
                    fixture.run.runId(), fixture.run.revision(),
                    initial.token().ownershipEpoch(),
                    new RecoveryOwner("master-second", 111L));
            assertEquals(ManifestMemberDecision.Code.RECOVERY_OWNED,
                    second.recordManifestMemberRecovered(initial.token(), right).code());
            assertEquals(ManifestMemberDecision.Code.RECORDED,
                    second.recordManifestMemberRecovered(replacement.token(), right).code());
            assertEquals(ManifestMemberDecision.Code.ALREADY_RECORDED,
                    first.recordManifestMemberRecovered(initial.token(), left).code());

            fixture.advanceToCommitting(first, replacement.token());
            ManifestCommitIntent intent = first.manifestCommitJournal(
                    fixture.manifest.fence()).buildIntent();
            assertEquals(ManifestIntentDecision.Code.RECOVERY_OWNED,
                    second.commitManifestIntentRecovered(initial.token(), intent).code());
            assertEquals(ManifestIntentDecision.Code.COMMITTED,
                    second.commitManifestIntentRecovered(replacement.token(), intent).code());
            assertEquals(ManifestIntentDecision.Code.ALREADY_COMMITTED,
                    first.commitManifestIntentRecovered(initial.token(), intent).code());
        }
    }

    @Test
    void memberAndIntentCommitAckLossReconcileFromSharedBytes() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            AtomicBoolean loseMemberAck = new AtomicBoolean(true);
            AtomicBoolean loseIntentAck = new AtomicBoolean(true);
            ZkRunStateStore store = new ZkRunStateStore(
                    client, () -> { }, () -> { }, () -> { },
                    () -> {
                        if (loseMemberAck.compareAndSet(true, false)) {
                            throw new KeeperException.ConnectionLossException();
                        }
                    },
                    () -> {
                        if (loseIntentAck.compareAndSet(true, false)) {
                            throw new KeeperException.ConnectionLossException();
                        }
                    });
            Fixture fixture = fixture(store, "run-zk-manifest-ack");
            assertEquals(ManifestMemberDecision.Code.ALREADY_RECORDED,
                    store.recordManifestMember(fixture.member(0, "left")).code());
            assertEquals(ManifestMemberDecision.Code.RECORDED,
                    store.recordManifestMember(fixture.member(64, "right")).code());
            fixture.advanceToCommitting(store, null);
            ManifestCommitIntent intent = store.manifestCommitJournal(
                    fixture.manifest.fence()).buildIntent();
            assertEquals(ManifestIntentDecision.Code.ALREADY_COMMITTED,
                    store.commitManifestIntent(intent).code());
        }
    }

    @Test
    void abortDecisionIsSharedCommitExclusiveAndSurvivesTerminalEvidenceRetention()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = client(server);
             CuratorFramework secondClient = client(server)) {
            ZkRunStateStore first = new ZkRunStateStore(firstClient);
            ZkRunStateStore second = new ZkRunStateStore(secondClient);
            Fixture fixture = fixture(first, "run-zk-abort-authority");
            first.recordManifestMember(fixture.member(0, "left"));
            first.recordManifestMember(fixture.member(64, "right"));
            fixture.advanceToCommitting(first, null);
            BatchSinkAbortRecord decided = BatchSinkAbortRecord.decided(
                    fixture.manifest.fence(), fixture.manifest.generation(),
                    io.rowweave.BatchSinkDescriptor.CapabilityProfile
                            .MANIFEST_ATOMIC_GROUP_V1,
                    "domain-zk-journal", 31L, "publication-zk-journal",
                    digest(9), "PREPUBLICATION_FAILURE", 70L);

            assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                    first.recordBatchSinkAbort(decided).code());
            assertEquals(BatchSinkAbortDecision.Code.ALREADY_RECORDED,
                    second.recordBatchSinkAbort(decided).code());
            assertEquals(ManifestIntentDecision.Code.INTENT_CONFLICT,
                    second.commitManifestIntent(second.manifestCommitJournal(
                            fixture.manifest.fence()).buildIntent()).code());
            BatchSinkAbortRecord completed = decided.completed(71L);
            assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                    second.recordBatchSinkAbort(completed).code());

            RunSnapshot current = first.get(fixture.run.runId());
            RunDecision reconciling = first.apply(RunCommand.requestCancellation(
                    "abort-reconciling", current.runId(), current.revision(),
                    current.currentAttempt().attemptId(),
                    current.currentAttempt().fenceToken(), CancellationCause.FENCE,
                    72L, "ABORT", "abort decided"));
            current = reconciling.snapshot();
            assertEquals(RunDecision.Code.ACCEPTED, first.apply(
                    RunCommand.advanceAttempt(
                            "abort-terminal", current.runId(), current.revision(),
                            current.currentAttempt().attemptId(),
                            current.currentAttempt().fenceToken(), AttemptStatus.FAILED,
                            false, 73L, "FAILED", "terminal")).code());
            assertNull(second.manifestCommitJournal(fixture.manifest.fence()));
            assertEquals(completed,
                    second.batchSinkAbort(fixture.manifest.fence()));
        }
    }

    @Test
    void abortDecisionClosesTheMemberJournalAcrossSessions() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = client(server);
             CuratorFramework secondClient = client(server)) {
            ZkRunStateStore first = new ZkRunStateStore(firstClient);
            ZkRunStateStore second = new ZkRunStateStore(secondClient);
            Fixture fixture = fixture(first, "run-zk-abort-closes-members",
                    0, 32, 64);
            first.recordManifestMember(fixture.member(0, "left"));
            fixture.advanceToCommitting(first, null);
            BatchSinkAbortRecord abort = BatchSinkAbortRecord.decided(
                    fixture.manifest.fence(), fixture.manifest.generation(),
                    io.rowweave.BatchSinkDescriptor.CapabilityProfile
                            .MANIFEST_ATOMIC_GROUP_V1,
                    "domain-zk-journal", 31L, "publication-zk-journal",
                    digest(9), "PREPUBLICATION_FAILURE", 70L);
            assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                    first.recordBatchSinkAbort(abort).code());

            assertEquals(ManifestMemberDecision.Code.MEMBER_CONFLICT,
                    second.recordManifestMember(fixture.member(32, "late")).code());
        }
    }

    @Test
    void userCancelingAttemptMayDurablyDecideAbort() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            Fixture fixture = fixture(store, "run-zk-user-cancel-abort");
            for (AttemptStatus next : Arrays.asList(
                    AttemptStatus.DISPATCHING, AttemptStatus.RUNNING)) {
                RunSnapshot current = store.get(fixture.run.runId());
                assertEquals(RunDecision.Code.ACCEPTED, store.apply(
                        RunCommand.advanceAttempt(
                                "advance-" + next + "-user-cancel",
                                current.runId(), current.revision(),
                                current.currentAttempt().attemptId(),
                                current.currentAttempt().fenceToken(), next,
                                false, 40L + next.ordinal(), next.name(), null)).code());
            }
            assertEquals(io.rowweave.run.RunCancellationResult.Disposition.ACCEPTED,
                    store.requestUserCancellation(
                            fixture.run.runId(), "request-user-cancel").disposition());
            BatchSinkAbortRecord abort = BatchSinkAbortRecord.decided(
                    fixture.manifest.fence(), fixture.manifest.generation(),
                    io.rowweave.BatchSinkDescriptor.CapabilityProfile
                            .MANIFEST_ATOMIC_GROUP_V1,
                    "domain-zk-journal", 31L, "publication-zk-journal",
                    digest(9), "USER_CANCELED", 70L);

            assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                    store.recordBatchSinkAbort(abort).code());
        }
    }

    @Test
    void timeoutCancelingAttemptMayDurablyDecideAbort() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            Fixture fixture = fixture(store, "run-zk-timeout-abort");
            RunSnapshot current = store.get(fixture.run.runId());
            for (AttemptStatus next : Arrays.asList(
                    AttemptStatus.DISPATCHING, AttemptStatus.RUNNING)) {
                assertEquals(RunDecision.Code.ACCEPTED, store.apply(
                        RunCommand.advanceAttempt(
                                "advance-" + next + "-timeout-abort", current.runId(),
                                current.revision(), current.currentAttempt().attemptId(),
                                current.currentAttempt().fenceToken(), next,
                                false, 40L + next.ordinal(), next.name(), null)).code());
                current = store.get(fixture.run.runId());
            }
            assertEquals(RunDecision.Code.ACCEPTED, store.apply(
                    RunCommand.requestCancellation(
                            "request-timeout-abort", current.runId(), current.revision(),
                            current.currentAttempt().attemptId(),
                            current.currentAttempt().fenceToken(), CancellationCause.TIMEOUT,
                            100L, "RUN_DEADLINE", "deadline reached")).code());
            BatchSinkAbortRecord abort = BatchSinkAbortRecord.decided(
                    fixture.manifest.fence(), fixture.manifest.generation(),
                    io.rowweave.BatchSinkDescriptor.CapabilityProfile
                            .MANIFEST_ATOMIC_GROUP_V1,
                    "domain-zk-journal", 31L, "publication-zk-journal",
                    digest(9), "RUN_DEADLINE", 100L);

            assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                    store.recordBatchSinkAbort(abort).code());
        }
    }

    private static Fixture fixture(ZkRunStateStore store, String runId) {
        return fixture(store, runId, 0, 64);
    }

    private static Fixture fixture(
            ZkRunStateStore store, String runId, int... sinkOrdinals) {
        RunSnapshot created = store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact", 1, 0L, 1L))
                .snapshot();
        RunSnapshot run = store.apply(RunCommand.createAttempt(
                "attempt-" + runId, runId, created.revision(),
                "attempt-1", "publication-zk-journal", 2L)).snapshot();
        RunAttemptFence fence = new RunAttemptFence(run.runId(),
                run.currentAttempt().attemptId(), run.currentAttempt().fenceToken());
        List<StageAssignmentRecord> records = new ArrayList<>();
        for (int i = 0; i < 65; i++) {
            StageAssignmentRole role = StageAssignmentRole.ROUTER;
            for (int sinkOrdinal : sinkOrdinals) {
                if (i == sinkOrdinal) role = StageAssignmentRole.SINK;
            }
            records.add(StageAssignmentRecord.create(
                    fence, 1L, i, 100L + i, i, 0, role,
                    "worker-" + i, "127.0.0.1", 7001 + i, -1, "inc-" + i));
        }
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, run.artifactId(), 1L, records);
        StageAssignmentSnapshot assignments = store.publish(manifest).snapshot();
        for (int ordinal : sinkOrdinals) {
            StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                    assignments.token(ordinal), digest(ordinal + 1), 10L + ordinal);
            store.accept(acceptance);
            store.recordTerminal(StageTerminalReceipt.succeeded(
                    acceptance.token(), acceptance.commandDigest(),
                    ordinal, ordinal, 20L + ordinal));
        }
        return new Fixture(run, manifest, store.assignmentSnapshot(fence));
    }

    private static byte[] digest(int value) {
        byte[] digest = new byte[CommitManifest.SHA256_BYTES];
        Arrays.fill(digest, (byte) value);
        return digest;
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

    private static String runPath(String runId) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256")
                .digest(runId.getBytes(StandardCharsets.UTF_8));
        StringBuilder hex = new StringBuilder(64);
        for (byte value : digest) hex.append(String.format("%02x", value & 0xff));
        return "/rowweave/run-state-v1/runs/" + hex;
    }

    private static final class Fixture {
        final RunSnapshot run;
        final StageAssignmentManifest manifest;
        final StageAssignmentSnapshot assignments;

        Fixture(RunSnapshot run, StageAssignmentManifest manifest,
                StageAssignmentSnapshot assignments) {
            this.run = run;
            this.manifest = manifest;
            this.assignments = assignments;
        }

        ManifestMemberReceipt member(int ordinal, String name) {
            StageAssignmentAcceptance acceptance = assignments.acceptance(ordinal);
            return ManifestMemberReceipt.sealed(
                    acceptance.token(), acceptance.commandDigest(),
                    "domain-zk-journal", 31L, "publication-zk-journal",
                    new CommitManifest.Member(name,
                            "data/31/" + name + ".data", "schema-" + ordinal,
                            ordinal, digest(ordinal + 1)), 30L + ordinal);
        }

        void advanceToCommitting(ZkRunStateStore store,
                                 io.rowweave.run.RecoveryToken token) {
            for (AttemptStatus next : Arrays.asList(
                    AttemptStatus.DISPATCHING, AttemptStatus.RUNNING,
                    AttemptStatus.COMMITTING)) {
                RunSnapshot current = store.get(run.runId());
                RunCommand command = RunCommand.advanceAttempt(
                        "advance-" + next + "-" + run.runId(), current.runId(),
                        current.revision(), current.currentAttempt().attemptId(),
                        current.currentAttempt().fenceToken(), next, false,
                        40L + next.ordinal(), next.name(), next.name());
                RunDecision decision = token == null ? store.apply(command)
                        : ((RecoveryFencedRunStateStore) store).applyRecovered(token, command);
                assertEquals(RunDecision.Code.ACCEPTED, decision.code());
            }
        }
    }
}
