package io.rowweave.cluster.registry.zk;

import io.rowweave.incremental.CapturedTargetReceipt;
import io.rowweave.incremental.FrontierRecoveryToken;
import io.rowweave.incremental.IncrementalAttemptProgressCommand;
import io.rowweave.incremental.IncrementalAuthorityDecision;
import io.rowweave.incremental.IncrementalClaimRequest;
import io.rowweave.incremental.IncrementalFrontier;
import io.rowweave.incremental.IncrementalInitializeRequest;
import io.rowweave.incremental.IncrementalPendingRunDecision;
import io.rowweave.incremental.IncrementalRunMode;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** ZK Run progress is fenced by the current frontier owner. */
class ZkIncrementalProgressGateTest {

    @Test
    void progressAckLossReplaysAndReplacementFrontierOwnerFencesOldLeader()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connectedClient(server);
             CuratorFramework replacementClient = connectedClient(server)) {
            ZkRunStateStore firstRuns = new ZkRunStateStore(firstClient);
            ZkRunStateStore replacementRuns = new ZkRunStateStore(replacementClient);
            AtomicBoolean progressAck = new AtomicBoolean(true);
            ZkIncrementalFrontierStore first = new ZkIncrementalFrontierStore(
                    firstClient, firstRuns, () -> { }, () -> { }, () -> { }, () -> {
                if (progressAck.compareAndSet(true, false)) {
                    throw new KeeperException.ConnectionLossException();
                }
            });
            ZkIncrementalFrontierStore replacement =
                    new ZkIncrementalFrontierStore(replacementClient, replacementRuns);
            IncrementalClaimRequest request = request();
            assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                    first.initialize("initialize-progress", initialize()).code());
            FrontierRecoveryToken firstOwner = first.claimOwnership(
                    "feed-progress", 0L, 0L,
                    new RecoveryOwner("master-a", 1L)).token();
            assertEquals(IncrementalPendingRunDecision.Code.PUBLISHED,
                    first.publishPending(firstOwner, request).code());
            assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                    first.claimInterval("claim-progress", firstOwner, request).code());

            RunSnapshot created = replacementRuns.get(request.runId());
            IncrementalAttemptProgressCommand dispatching = progress(
                    replacement, request, created, AttemptStatus.DISPATCHING,
                    "progress-dispatching", 11L);
            RunDecision unknownAck = first.advanceProgress(firstOwner, dispatching);
            assertEquals(RunDecision.Code.ALREADY_APPLIED, unknownAck.code());
            assertEquals(AttemptStatus.DISPATCHING, replacementRuns.get(request.runId())
                    .currentAttempt().status());
            assertEquals(1L, replacement.record("feed-progress").revision());
            assertEquals(RunDecision.Code.ALREADY_APPLIED,
                    replacement.advanceProgress(firstOwner, dispatching).code());

            RunSnapshot beforeReplacement = replacementRuns.get(request.runId());
            FrontierRecoveryToken replacementOwner = replacement.claimOwnership(
                    "feed-progress", 1L, 1L,
                    new RecoveryOwner("master-b", 2L)).token();
            IncrementalAttemptProgressCommand staleOwner = progress(
                    replacement, request, beforeReplacement, AttemptStatus.RUNNING,
                    "progress-stale-owner", 12L);
            assertEquals(RunDecision.Code.STALE_RECOVERY_OWNER,
                    first.advanceProgress(firstOwner, staleOwner).code());
            assertEquals(beforeReplacement.revision(),
                    replacementRuns.get(request.runId()).revision());

            assertEquals(RunDecision.Code.ACCEPTED,
                    replacement.advanceProgress(replacementOwner, progress(
                            replacement, request, beforeReplacement,
                            AttemptStatus.RUNNING,
                            "progress-replacement", 12L)).code());
        }
    }

    @Test
    void staleFrontierRevisionAndFenceNeverMutateRun() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore runs = new ZkRunStateStore(client);
            ZkIncrementalFrontierStore store =
                    new ZkIncrementalFrontierStore(client, runs);
            IncrementalClaimRequest request = request();
            store.initialize("initialize-stale", initialize());
            FrontierRecoveryToken owner = store.claimOwnership(
                    "feed-progress", 0L, 0L,
                    new RecoveryOwner("master", 1L)).token();
            store.publishPending(owner, request);
            store.claimInterval("claim-stale", owner, request);
            RunSnapshot before = runs.get(request.runId());

            IncrementalAttemptProgressCommand staleRevision =
                    new IncrementalAttemptProgressCommand(
                            "feed-progress", request.intervalId(), 0L,
                            before.revision(), request.firstAttemptFence(),
                            AttemptStatus.DISPATCHING, "stale-revision", 11L);
            assertEquals(RunDecision.Code.STALE_REVISION,
                    store.advanceProgress(owner, staleRevision).code());
            RunAttemptFence wrongFence = new RunAttemptFence(
                    request.runId(), "wrong-attempt", 2L);
            IncrementalAttemptProgressCommand staleFence =
                    new IncrementalAttemptProgressCommand(
                            "feed-progress", request.intervalId(), 1L,
                            before.revision(), wrongFence,
                            AttemptStatus.DISPATCHING, "stale-fence", 11L);
            assertEquals(RunDecision.Code.STALE_FENCE,
                    store.advanceProgress(owner, staleFence).code());
            assertEquals(before.revision(), runs.get(request.runId()).revision());
            assertEquals(AttemptStatus.CREATED,
                    runs.get(request.runId()).currentAttempt().status());
        }
    }

    @Test
    void ownershipReplacementInsideProgressCasReturnsStaleWithoutMutation()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connectedClient(server);
             CuratorFramework replacementClient = connectedClient(server)) {
            ZkRunStateStore firstRuns = new ZkRunStateStore(firstClient);
            ZkRunStateStore replacementRuns = new ZkRunStateStore(replacementClient);
            ZkIncrementalFrontierStore first =
                    new ZkIncrementalFrontierStore(firstClient, firstRuns);
            ZkIncrementalFrontierStore replacement =
                    new ZkIncrementalFrontierStore(replacementClient, replacementRuns);
            IncrementalClaimRequest request = request();
            first.initialize("initialize-race", initialize());
            FrontierRecoveryToken firstOwner = first.claimOwnership(
                    "feed-progress", 0L, 0L,
                    new RecoveryOwner("master-a", 1L)).token();
            first.publishPending(firstOwner, request);
            first.claimInterval("claim-race", firstOwner, request);
            RunSnapshot before = firstRuns.get(request.runId());
            AtomicBoolean replace = new AtomicBoolean(true);
            firstRuns.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.APPLY_EXISTING
                        && moment == ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT
                        && replace.compareAndSet(true, false)) {
                    replacement.claimOwnership(
                            "feed-progress", 1L, 1L,
                            new RecoveryOwner("master-b", 2L));
                }
            });

            RunDecision raced = first.advanceProgress(firstOwner, progress(
                    first, request, before, AttemptStatus.DISPATCHING,
                    "progress-raced-owner", 11L));
            assertEquals(RunDecision.Code.STALE_RECOVERY_OWNER, raced.code());
            assertEquals(before.revision(), replacementRuns.get(request.runId()).revision());
            assertEquals(AttemptStatus.CREATED,
                    replacementRuns.get(request.runId()).currentAttempt().status());
        }
    }

    @Test
    void recoveredProgressRequiresBothCurrentOwnershipTokens() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server);
             CuratorFramework replacementClient = connectedClient(server)) {
            ZkRunStateStore runs = new ZkRunStateStore(client);
            ZkRunStateStore replacementRuns = new ZkRunStateStore(replacementClient);
            ZkIncrementalFrontierStore store =
                    new ZkIncrementalFrontierStore(client, runs);
            IncrementalClaimRequest request = request();
            store.initialize("initialize-recovered-progress", initialize());
            RecoveryOwner owner = new RecoveryOwner("master-a", 1L);
            FrontierRecoveryToken frontier = store.claimOwnership(
                    "feed-progress", 0L, 0L, owner).token();
            store.publishPending(frontier, request);
            store.claimInterval("claim-recovered-progress", frontier, request);
            RunSnapshot created = runs.get(request.runId());
            RecoveryClaim runClaim = store.claimRunOwnership(
                    frontier, request.runId(), created.revision(), 0L, owner);
            RecoveryToken runToken = runClaim.token();
            IncrementalAttemptProgressCommand dispatching = progress(
                    store, request, created, AttemptStatus.DISPATCHING,
                    "recovered-dispatching", 11L);

            assertEquals(RunDecision.Code.ACCEPTED,
                    store.advanceProgress(frontier, runToken, dispatching).code());
            assertEquals(RunDecision.Code.ALREADY_APPLIED,
                    store.advanceProgress(frontier, runToken, dispatching).code());
            RunSnapshot advanced = runs.get(request.runId());
            RecoveryOwner replacementOwner = new RecoveryOwner("master-b", 2L);
            RecoveryClaim replacement = replacementRuns.claim(
                    request.runId(), advanced.revision(), 1L, replacementOwner);
            assertEquals(RecoveryClaim.Code.ACQUIRED, replacement.code());
            IncrementalAttemptProgressCommand running = progress(
                    store, request, advanced, AttemptStatus.RUNNING,
                    "recovered-stale-run-owner", 12L);
            assertEquals(RunDecision.Code.STALE_RECOVERY_OWNER,
                    store.advanceProgress(frontier, runToken, running).code());
            assertEquals(advanced.revision(), runs.get(request.runId()).revision());
        }
    }

    private static IncrementalAttemptProgressCommand progress(
            ZkIncrementalFrontierStore store, IncrementalClaimRequest request,
            RunSnapshot run, AttemptStatus target, String commandId, long time) {
        return new IncrementalAttemptProgressCommand(
                request.feedId(), request.intervalId(),
                store.record(request.feedId()).revision(), run.revision(),
                request.firstAttemptFence(), target, commandId, time);
    }

    private static IncrementalInitializeRequest initialize() throws Exception {
        return new IncrementalInitializeRequest(
                "feed-progress", 1L, digest("definition"), digest("source"),
                digest("frontier"), IncrementalFrontier.Kind.INT64_SEQUENCE_V1);
    }

    private static IncrementalClaimRequest request() throws Exception {
        CapturedTargetReceipt capture = CapturedTargetReceipt.normal(
                1L, digest("definition"), digest("source"), 0L,
                IncrementalFrontier.begin(), "trigger-progress",
                IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(2L), bytes("snapshot"));
        return new IncrementalClaimRequest(
                "feed-progress", capture, digest("frontier"),
                hex(digest("artifact")), "attempt-progress", "publication-progress",
                3, 0L, 10L);
    }

    private static byte[] digest(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(bytes(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String hex(byte[] bytes) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] value = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int next = bytes[i] & 0xff;
            value[i * 2] = digits[next >>> 4];
            value[i * 2 + 1] = digits[next & 0x0f];
        }
        return new String(value);
    }

    private static CuratorFramework connectedClient(TestingServer server) {
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), new ExponentialBackoffRetry(20, 10));
        client.start();
        try {
            if (!client.blockUntilConnected(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Curator did not connect");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        }
        return client;
    }
}
