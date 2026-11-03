package io.rowweave.cluster.registry.zk;

import io.rowweave.run.AttemptStatus;
import io.rowweave.run.DurableExecutionEvidenceView;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryFencedRunStateStore;
import io.rowweave.run.RecoveryFencedStageAssignmentAuthority;
import io.rowweave.run.RecoveryOwner;
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
import io.rowweave.run.StageAssignmentToken;
import io.rowweave.run.StageManifestPublication;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZkStageAssignmentAuthorityTest {

    @Test
    void twoSessionsShareShardedReceiptsClaimFenceAndTerminalCleanup() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = client(server);
             CuratorFramework secondClient = client(server)) {
            ZkRunStateStore first = new ZkRunStateStore(firstClient);
            ZkRunStateStore second = new ZkRunStateStore(secondClient);
            RunSnapshot run = attempt(first, "run-sharded", 1);
            StageAssignmentManifest manifest = manifest(run, 65);

            assertEquals(StageManifestPublication.Code.PUBLISHED,
                    first.publish(manifest).code());
            assertEquals(StageManifestPublication.Code.ALREADY_PUBLISHED,
                    second.publish(manifest).code());
            StageAssignmentSnapshot initial = second.assignmentSnapshot(manifest.fence());
            assertEquals(2, initial.manifest().shardCount());
            assertTrue(initial.allNone());

            StageAssignmentAcceptance firstReceipt = acceptance(initial.token(0), 1, 10L);
            StageAssignmentAcceptance lastReceipt = acceptance(initial.token(64), 2, 11L);
            assertEquals(StageAcceptanceDecision.Code.ACCEPTED,
                    first.accept(firstReceipt).code());
            assertEquals(StageAcceptanceDecision.Code.ACCEPTED,
                    second.accept(lastReceipt).code());
            assertEquals(StageAcceptanceDecision.Code.ALREADY_ACCEPTED,
                    second.accept(firstReceipt).code());
            assertEquals(2, first.assignmentSnapshot(manifest.fence()).acceptedCount());

            RecoveryClaim claim = second.claim(run.runId(), run.revision(), 0L,
                    new RecoveryOwner("master-new", 99L));
            assertEquals(RecoveryClaim.Code.ACQUIRED, claim.code());
            StageAssignmentSnapshot claimed = first.assignmentSnapshot(manifest.fence());
            assertEquals(2L, claimed.acceptanceGeneration());
            assertEquals(2, claimed.acceptedCount());
            assertEquals(StageAcceptanceDecision.Code.STALE_ASSIGNMENT_GENERATION,
                    first.accept(acceptance(initial.token(1), 3, 12L)).code());
            assertEquals(StageAcceptanceDecision.Code.RECOVERY_OWNED,
                    first.accept(acceptance(claimed.token(1), 3, 12L)).code());

            RunDecision terminal = ((RecoveryFencedRunStateStore) second).applyRecovered(
                    claim.token(), RunCommand.advanceAttempt(
                            "terminal", run.runId(), run.revision(),
                            run.currentAttempt().attemptId(), run.currentAttempt().fenceToken(),
                            AttemptStatus.FAILED, false, 3L, "FAILED", "terminal"));
            assertEquals(RunDecision.Code.ACCEPTED, terminal.code());
            assertNull(first.assignmentSnapshot(manifest.fence()));

            String assignmentRoot = runPath(run.runId()) + "/stage-assignment";
            assertTrue(firstClient.checkExists().forPath(assignmentRoot + "/root") != null);
            assertEquals(DurableExecutionEvidenceView.State.PRESENT,
                    first.executionEvidence(run.runId()).state());
            assertEquals(Arrays.asList("000", "001"),
                    firstClient.getChildren().forPath(
                            assignmentRoot + "/assignment-shards"));
            assertEquals(Arrays.asList("000", "001"),
                    firstClient.getChildren().forPath(
                            assignmentRoot + "/acceptance-shards"));
        }
    }

    @Test
    void claimAndFirstAcceptanceAcrossSessionsHaveOneWinner() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = client(server);
             CuratorFramework secondClient = client(server)) {
            ZkRunStateStore first = new ZkRunStateStore(firstClient);
            ZkRunStateStore second = new ZkRunStateStore(secondClient);
            RunSnapshot run = attempt(first, "run-race", 1);
            StageAssignmentManifest manifest = manifest(run, 1);
            StageAssignmentAcceptance request = acceptance(
                    first.publish(manifest).snapshot().token(0), 7, 10L);

            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(2);
            AtomicReference<RecoveryClaim> claim = new AtomicReference<>();
            AtomicReference<StageAcceptanceDecision> accepted = new AtomicReference<>();
            pool.submit(() -> {
                start.await();
                claim.set(first.claim(run.runId(), run.revision(), 0L,
                        new RecoveryOwner("master-new", 88L)));
                return null;
            });
            pool.submit(() -> {
                start.await();
                accepted.set(second.accept(request));
                return null;
            });
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

            assertEquals(RecoveryClaim.Code.ACQUIRED, claim.get().code());
            StageAssignmentSnapshot after = second.assignmentSnapshot(manifest.fence());
            assertEquals(2L, after.acceptanceGeneration());
            assertTrue(accepted.get().code() == StageAcceptanceDecision.Code.ACCEPTED
                    || accepted.get().code()
                    == StageAcceptanceDecision.Code.STALE_ASSIGNMENT_GENERATION);
            assertEquals(accepted.get().code() == StageAcceptanceDecision.Code.ACCEPTED,
                    !after.allNone());
        }
    }

    @Test
    void assignmentCommitAckLossReconcilesAndCorruptionFailsClosed() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            AtomicBoolean losePublishAck = new AtomicBoolean(true);
            AtomicBoolean loseAcceptAck = new AtomicBoolean(true);
            ZkRunStateStore store = new ZkRunStateStore(client,
                    () -> {
                        if (losePublishAck.compareAndSet(true, false)) {
                            throw new KeeperException.ConnectionLossException();
                        }
                    },
                    () -> {
                        if (loseAcceptAck.compareAndSet(true, false)) {
                            throw new KeeperException.ConnectionLossException();
                        }
                    });
            RunSnapshot run = attempt(store, "run-ack", 1);
            StageAssignmentManifest manifest = manifest(run, 1);
            StageManifestPublication publication = store.publish(manifest);
            assertEquals(StageManifestPublication.Code.ALREADY_PUBLISHED, publication.code());
            StageAssignmentAcceptance request = acceptance(
                    publication.snapshot().token(0), 5, 10L);
            assertEquals(StageAcceptanceDecision.Code.ALREADY_ACCEPTED,
                    store.accept(request).code());

            String shard = runPath(run.runId())
                    + "/stage-assignment/assignment-shards/000";
            byte[] corrupt = client.getData().forPath(shard);
            corrupt[corrupt.length / 2] ^= 1;
            client.setData().forPath(shard, corrupt);
            assertThrows(IllegalStateException.class,
                    () -> store.assignmentSnapshot(manifest.fence()));
        }
    }

    @Test
    void backendStopFailsClosedInsteadOfReturningFalseAbsence() throws Exception {
        try (TestingServer server = new TestingServer(true);
             ZkServiceRegistry registry =
                     new ZkServiceRegistry(server.getConnectString(),
                             1_000, 250)) {
            ZkRunStateStore store = (ZkRunStateStore) registry.runStateStore();
            RunSnapshot run = attempt(store, "run-stop", 1);
            StageAssignmentManifest manifest = manifest(run, 1);
            store.publish(manifest);

            server.stop();
            assertThrows(RunStateStoreUnavailableException.class,
                    () -> store.assignmentSnapshot(manifest.fence()));
        }
    }

    @Test
    void recoveredPublicationAcrossSessionsRequiresExactOwnerToken() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = client(server);
             CuratorFramework secondClient = client(server)) {
            ZkRunStateStore first = new ZkRunStateStore(firstClient);
            ZkRunStateStore second = new ZkRunStateStore(secondClient);
            RunSnapshot run = attempt(first, "run-recovered-publish", 2);
            StageAssignmentManifest manifest = manifest(run, 1);
            RecoveryClaim claim = first.claim(run.runId(), run.revision(), 0L,
                    new RecoveryOwner("master-first", 10L));
            assertEquals(RecoveryClaim.Code.ACQUIRED, claim.code());
            Object authority = second;
            assertTrue(authority instanceof RecoveryFencedStageAssignmentAuthority);

            RecoveryFencedStageAssignmentAuthority recovered =
                    (RecoveryFencedStageAssignmentAuthority) authority;
            assertEquals(StageManifestPublication.Code.RECOVERY_OWNED,
                    second.publish(manifest).code());
            assertEquals(StageManifestPublication.Code.PUBLISHED,
                    recovered.publishRecovered(claim.token(), manifest).code());
        }
    }

    @Test
    void recoveredAcceptanceAcrossSessionsRequiresCurrentExactOwner() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = client(server);
             CuratorFramework secondClient = client(server)) {
            ZkRunStateStore first = new ZkRunStateStore(firstClient);
            ZkRunStateStore second = new ZkRunStateStore(secondClient);
            RunSnapshot run = attempt(first, "run-recovered-accept", 2);
            StageAssignmentManifest manifest = manifest(run, 1);
            RecoveryClaim initial = first.claim(run.runId(), run.revision(), 0L,
                    new RecoveryOwner("master-first", 10L));
            RecoveryFencedStageAssignmentAuthority recovered = second;
            assertEquals(StageManifestPublication.Code.PUBLISHED,
                    recovered.publishRecovered(initial.token(), manifest).code());

            RecoveryClaim replacement = first.claim(run.runId(), run.revision(),
                    initial.token().ownershipEpoch(), new RecoveryOwner("master-second", 20L));
            StageAssignmentAcceptance request = acceptance(
                    second.assignmentSnapshot(manifest.fence()).token(0), 6, 12L);
            assertEquals(StageAcceptanceDecision.Code.RECOVERY_OWNED,
                    recovered.acceptRecovered(initial.token(), request).code());
            assertEquals(StageAcceptanceDecision.Code.ACCEPTED,
                    recovered.acceptRecovered(replacement.token(), request).code());
            assertEquals(StageAcceptanceDecision.Code.ALREADY_ACCEPTED,
                    recovered.acceptRecovered(initial.token(), request).code());
        }
    }

    private static RunSnapshot attempt(ZkRunStateStore store, String runId, int maxAttempts) {
        RunDecision created = store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact", maxAttempts, 0L, 1L));
        assertEquals(RunDecision.Code.ACCEPTED, created.code());
        RunDecision attempted = store.apply(RunCommand.createAttempt(
                "attempt-" + runId, runId, created.snapshot().revision(),
                "attempt-1", "publication-1", 2L));
        assertEquals(RunDecision.Code.ACCEPTED, attempted.code());
        return attempted.snapshot();
    }

    private static StageAssignmentManifest manifest(RunSnapshot run, int count) {
        RunAttemptFence fence = new RunAttemptFence(run.runId(),
                run.currentAttempt().attemptId(), run.currentAttempt().fenceToken());
        List<StageAssignmentRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(StageAssignmentRecord.create(
                    fence, 1L, i, i + 1L, i, 0,
                    i == count - 1 ? StageAssignmentRole.SINK : StageAssignmentRole.ROUTER,
                    "worker-" + i, "127.0.0.1", 7001, -1, "inc-" + i));
        }
        return new StageAssignmentManifest(fence, run.artifactId(), 1L, records);
    }

    private static StageAssignmentAcceptance acceptance(StageAssignmentToken token,
                                                        int digestByte, long time) {
        byte[] digest = new byte[32];
        Arrays.fill(digest, (byte) digestByte);
        return new StageAssignmentAcceptance(token, digest, time);
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
}
