package io.rowweave.cluster.registry.zk;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.plan.StageExecutionTelemetry;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.DurableExecutionEvidenceView;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryFencedRunStateStore;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.PreparedSinkDecision;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.PreparedSinkReceipt;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunLifecycleRef;
import io.rowweave.run.RetentionOwner;
import io.rowweave.run.RunRetentionClaim;
import io.rowweave.run.RunPurgeProgress;
import io.rowweave.run.StageAcceptanceDecision;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageTerminalDecision;
import io.rowweave.run.StageTerminalReceipt;
import io.rowweave.run.SinkAttemptIdentity;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZkStageTerminalAuthorityTest {

    @Test
    void terminalConvergesAcrossMasterCrashBeforeAndAfterPersistence() throws Exception {
        try (TestingServer server = new TestingServer(true)) {
            RunAttemptFence fence;
            StageAssignmentAcceptance acceptance;
            StageTerminalReceipt receipt;
            CuratorFramework beforeCommit = client(server);
            try {
                ZkRunStateStore first = new ZkRunStateStore(beforeCommit);
                RunSnapshot run = attempt(first, "run-terminal-master-crash", 1);
                StageAssignmentSnapshot assignments = first.publish(manifest(run, 1)).snapshot();
                acceptance = acceptance(assignments, 0, 7);
                first.accept(acceptance);
                fence = assignments.manifest().fence();
                receipt = StageTerminalReceipt.succeeded(
                        acceptance.token(), acceptance.commandDigest(), 3L, 2L, 10L,
                        StageExecutionTelemetry.complete(11L, 22L, 3L, 4L, 55L, 66L));
                assertNull(assignments.terminal(0));
            } finally {
                beforeCommit.close();
            }

            CuratorFramework afterCrash = client(server);
            try {
                ZkRunStateStore replacement = new ZkRunStateStore(afterCrash);
                assertEquals(StageTerminalDecision.Code.RECORDED,
                        replacement.recordTerminal(receipt).code(),
                        "a replacement Master must persist a receipt absent before the crash");
                assertEquals(receipt,
                        replacement.assignmentSnapshot(fence).terminal(0));
            } finally {
                afterCrash.close();
            }

            try (CuratorFramework afterCommitCrash = client(server)) {
                ZkRunStateStore nextReplacement = new ZkRunStateStore(afterCommitCrash);
                StageTerminalDecision replay = nextReplacement.recordTerminal(receipt);
                assertEquals(StageTerminalDecision.Code.ALREADY_RECORDED, replay.code(),
                        "a replacement Master must reconcile the receipt persisted before crash");
                assertEquals(receipt, replay.receipt());
            }
        }
    }

    @Test
    void twoSessionsShareTerminalShardsOwnerFenceAndAtomicCleanup() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = client(server);
             CuratorFramework secondClient = client(server)) {
            ZkRunStateStore first = new ZkRunStateStore(firstClient);
            ZkRunStateStore second = new ZkRunStateStore(secondClient);
            RunSnapshot run = attempt(first, "run-terminal-shards", 1);
            StageAssignmentManifest manifest = manifest(run, 65);
            StageAssignmentSnapshot initial = first.publish(manifest).snapshot();
            StageAssignmentAcceptance firstAcceptance = acceptance(initial, 0, 1);
            StageAssignmentAcceptance lastAcceptance = acceptance(initial, 64, 2);
            assertEquals(StageAcceptanceDecision.Code.ACCEPTED,
                    first.accept(firstAcceptance).code());
            assertEquals(StageAcceptanceDecision.Code.ACCEPTED,
                    second.accept(lastAcceptance).code());

            StageTerminalReceipt succeeded = StageTerminalReceipt.succeeded(
                    firstAcceptance.token(), firstAcceptance.commandDigest(), 9L, 7L, 20L,
                    StageExecutionTelemetry.complete(11L, 22L, 3L, 4L, 55L, 66L));
            assertEquals(StageTerminalDecision.Code.RECORDED,
                    second.recordTerminal(succeeded).code());
            assertEquals(StageTerminalDecision.Code.ALREADY_RECORDED,
                    first.recordTerminal(succeeded).code());
            StageTerminalReceipt telemetryReplay = succeeded.withTelemetry(
                    StageExecutionTelemetry.complete(101L, 202L, 30L, 40L, 505L, 606L));
            StageTerminalDecision telemetryDuplicate =
                    first.recordTerminal(telemetryReplay);
            assertEquals(StageTerminalDecision.Code.ALREADY_RECORDED,
                    telemetryDuplicate.code());
            assertEquals(succeeded, telemetryDuplicate.receipt(),
                    "the first persisted telemetry remains authoritative");
            assertEquals(StageTerminalDecision.Code.TERMINAL_CONFLICT,
                    first.recordTerminal(StageTerminalReceipt.failed(
                            firstAcceptance.token(), firstAcceptance.commandDigest(),
                            9L, 0L, "USER_CODE", digest(8), 20L)).code());

            RecoveryClaim claim = first.claim(run.runId(), run.revision(), 0L,
                    new RecoveryOwner("master-new", 99L));
            StageTerminalReceipt lost = StageTerminalReceipt.lost(
                    lastAcceptance.token(), lastAcceptance.commandDigest(),
                    "WORKER_REPLACED", digest(3), 0L);
            assertEquals(StageTerminalDecision.Code.RECOVERY_OWNED,
                    second.recordTerminal(lost).code());
            assertEquals(StageTerminalDecision.Code.RECORDED,
                    second.recordTerminalRecovered(claim.token(), lost).code());

            StageAssignmentSnapshot loaded = first.assignmentSnapshot(manifest.fence());
            assertEquals(2, loaded.terminalCount());
            assertEquals(succeeded, loaded.terminal(0));
            assertEquals(lost, loaded.terminal(64));
            String assignmentRoot = runPath(run.runId()) + "/stage-assignment";
            assertEquals(Arrays.asList("000", "001"),
                    firstClient.getChildren().forPath(assignmentRoot + "/terminal-shards"));
            byte[] persistedTerminal = firstClient.getData().forPath(
                    assignmentRoot + "/terminal-shards/000");
            assertEquals(5, (persistedTerminal[4] & 0xff) << 8
                    | persistedTerminal[5] & 0xff,
                    "terminal shards advance to schema v5 for prepared evidence");

            RunDecision terminal = ((RecoveryFencedRunStateStore) first).applyRecovered(
                    claim.token(), RunCommand.advanceAttempt(
                            "terminal", run.runId(), run.revision(),
                            run.currentAttempt().attemptId(), run.currentAttempt().fenceToken(),
                            AttemptStatus.FAILED, false, 30L, "FAILED", "terminal"));
            assertEquals(RunDecision.Code.ACCEPTED, terminal.code());
            assertNull(second.assignmentSnapshot(manifest.fence()));
            DurableExecutionEvidenceView retained = second.executionEvidence(run.runId());
            assertEquals(DurableExecutionEvidenceView.State.PRESENT, retained.state());
            assertEquals(2, retained.assignments().terminalCount());
            assertEquals(Arrays.asList("000", "001"),
                    firstClient.getChildren().forPath(
                            assignmentRoot + "/terminal-shards"));
            assertEquals(StageAcceptanceDecision.Code.STALE_RUN,
                    second.accept(firstAcceptance).code());
            assertEquals(StageTerminalDecision.Code.STALE_RUN,
                    second.recordTerminal(succeeded).code());

            RunLifecycleRef lifecycle = first.lifecycle(run.runId());
            RunRetentionClaim retention = first.claim(
                    run.runId(), terminal.snapshot().revision(),
                    lifecycle.terminalAtMillis(), new RetentionOwner("janitor", 101L), 0L);
            assertEquals(RunRetentionClaim.Code.ACQUIRED, retention.code());
            assertEquals(DurableExecutionEvidenceView.State.PRESENT,
                    second.executionEvidence(run.runId()).state());
            RunPurgeProgress progress;
            do {
                progress = first.purge(retention.token(), 1000);
            } while (progress.code() == RunPurgeProgress.Code.IN_PROGRESS);
            assertEquals(RunPurgeProgress.Code.PURGED, progress.code());
            assertEquals(DurableExecutionEvidenceView.State.MISSING,
                    second.executionEvidence(run.runId()).state());
        }
    }

    @Test
    void preparedSinkCasPersistsAndReplaysOnlyTheExactLocator() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = client(server);
             CuratorFramework secondClient = client(server)) {
            ZkRunStateStore first = new ZkRunStateStore(firstClient);
            ZkRunStateStore second = new ZkRunStateStore(secondClient);
            RunSnapshot run = attempt(first, "run-prepared-sink", 1);
            StageAssignmentManifest manifest = manifest(run, 1);
            StageAssignmentSnapshot initial = first.publish(manifest).snapshot();
            StageAssignmentAcceptance acceptance = acceptance(initial, 0, 7);
            assertEquals(StageAcceptanceDecision.Code.ACCEPTED,
                    first.accept(acceptance).code());
            StageTerminalReceipt terminal = StageTerminalReceipt.succeeded(
                    acceptance.token(), acceptance.commandDigest(), 3L, 3L, 20L);
            BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                    1, "rowweave.manifest-file", 1,
                    String.join("", java.util.Collections.nCopies(64, "a")),
                    "domain:member", "domain",
                    BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
            SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                    manifest, manifest.assignment(0), "sink-op", descriptor);
            PreparedSinkReceipt prepared = prepared(
                    acceptance, identity, new byte[]{1, 2, 3});

            assertEquals(PreparedSinkDecision.Code.TERMINAL_REQUIRED,
                    second.recordPreparedSink(prepared).code());
            assertEquals(StageTerminalDecision.Code.RECORDED,
                    first.recordTerminal(terminal).code());
            assertEquals(PreparedSinkDecision.Code.RECORDED,
                    second.recordPreparedSink(prepared).code());
            assertEquals(PreparedSinkDecision.Code.ALREADY_RECORDED,
                    first.recordPreparedSink(prepared(
                            acceptance, identity, new byte[]{1, 2, 3})).code());
            assertEquals(PreparedSinkDecision.Code.LOCATOR_CONFLICT,
                    first.recordPreparedSink(prepared(
                            acceptance, identity, new byte[]{9, 9, 9})).code());
            assertEquals(prepared,
                    second.assignmentSnapshot(manifest.fence()).preparedSink(0));

            byte[] persistedTerminal = firstClient.getData().forPath(
                    runPath(run.runId())
                            + "/stage-assignment/terminal-shards/000");
            assertEquals(5, (persistedTerminal[4] & 0xff) << 8
                    | persistedTerminal[5] & 0xff);
        }
    }

    @Test
    void terminalCommitAckLossReconcilesAndCorruptionFailsClosed() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            AtomicBoolean loseTerminalAck = new AtomicBoolean(true);
            ZkRunStateStore store = new ZkRunStateStore(
                    client, () -> { }, () -> { }, () -> {
                if (loseTerminalAck.compareAndSet(true, false)) {
                    throw new KeeperException.ConnectionLossException();
                }
            });
            RunSnapshot run = attempt(store, "run-terminal-ack", 1);
            StageAssignmentManifest manifest = manifest(run, 1);
            StageAssignmentSnapshot assignments = store.publish(manifest).snapshot();
            StageAssignmentAcceptance acceptance = acceptance(assignments, 0, 5);
            StageTerminalReceipt receipt = StageTerminalReceipt.succeeded(
                    acceptance.token(), acceptance.commandDigest(), 1L, 1L, 20L);
            assertEquals(StageTerminalDecision.Code.ACCEPTANCE_REQUIRED,
                    store.recordTerminal(receipt).code());
            store.accept(acceptance);
            assertEquals(StageTerminalDecision.Code.ALREADY_RECORDED,
                    store.recordTerminal(receipt).code());

            String terminalShard = runPath(run.runId())
                    + "/stage-assignment/terminal-shards/000";
            byte[] corrupt = client.getData().forPath(terminalShard);
            corrupt[corrupt.length / 2] ^= 1;
            client.setData().forPath(terminalShard, corrupt);
            org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                    () -> store.assignmentSnapshot(manifest.fence()));
        }
    }

    @Test
    void claimAndFirstTerminalAcrossSessionsHaveOneWinner() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = client(server);
             CuratorFramework secondClient = client(server)) {
            ZkRunStateStore first = new ZkRunStateStore(firstClient);
            ZkRunStateStore second = new ZkRunStateStore(secondClient);
            RunSnapshot run = attempt(first, "run-terminal-race", 1);
            StageAssignmentManifest manifest = manifest(run, 1);
            StageAssignmentSnapshot initial = first.publish(manifest).snapshot();
            StageAssignmentAcceptance acceptance = acceptance(initial, 0, 1);
            first.accept(acceptance);
            StageTerminalReceipt receipt = StageTerminalReceipt.succeeded(
                    acceptance.token(), acceptance.commandDigest(), 1L, 1L, 20L);

            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(2);
            AtomicReference<RecoveryClaim> claim = new AtomicReference<>();
            AtomicReference<StageTerminalDecision> terminal = new AtomicReference<>();
            pool.submit(() -> {
                start.await();
                claim.set(first.claim(run.runId(), run.revision(), 0L,
                        new RecoveryOwner("master-new", 100L)));
                return null;
            });
            pool.submit(() -> {
                start.await();
                terminal.set(second.recordTerminal(receipt));
                return null;
            });
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

            assertEquals(RecoveryClaim.Code.ACQUIRED, claim.get().code());
            assertTrue(terminal.get().code() == StageTerminalDecision.Code.RECORDED
                    || terminal.get().code() == StageTerminalDecision.Code.RECOVERY_OWNED);
            assertEquals(terminal.get().code() == StageTerminalDecision.Code.RECORDED ? 1 : 0,
                    first.assignmentSnapshot(manifest.fence()).terminalCount());
        }
    }

    private static RunSnapshot attempt(ZkRunStateStore store, String runId, int maxAttempts) {
        RunDecision created = store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact",
                maxAttempts, 0L, 1L));
        return store.apply(RunCommand.createAttempt(
                "attempt-" + runId, runId, created.snapshot().revision(),
                "attempt-1", "publication-1", 2L)).snapshot();
    }

    private static StageAssignmentManifest manifest(RunSnapshot run, int count) {
        RunAttemptFence fence = new RunAttemptFence(run.runId(),
                run.currentAttempt().attemptId(), run.currentAttempt().fenceToken());
        List<StageAssignmentRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(StageAssignmentRecord.create(
                    fence, 1L, i, i + 1L, i, 0,
                    i == count - 1 ? StageAssignmentRole.SINK : StageAssignmentRole.ROUTER,
                    "worker-" + i, "127.0.0.1", 7001 + i, -1, "inc-" + i));
        }
        return new StageAssignmentManifest(fence, run.artifactId(), 1L, records);
    }

    private static StageAssignmentAcceptance acceptance(
            StageAssignmentSnapshot snapshot, int ordinal, int digestByte) {
        return new StageAssignmentAcceptance(
                snapshot.token(ordinal), digest(digestByte), 10L + ordinal);
    }

    private static byte[] digest(int value) {
        byte[] digest = new byte[StageTerminalReceipt.DIGEST_BYTES];
        Arrays.fill(digest, (byte) value);
        return digest;
    }

    private static PreparedSinkReceipt prepared(
            StageAssignmentAcceptance acceptance, SinkAttemptIdentity identity,
            byte[] payload) {
        PreparedSinkLocator locator = new PreparedSinkLocator(
                PreparedSinkLocator.CURRENT_VERSION, identity,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", payload, 3L, digest(5), 12L);
        return new PreparedSinkReceipt(
                acceptance.token(), acceptance.commandDigest(), locator);
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
