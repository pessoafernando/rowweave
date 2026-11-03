package io.rowweave.cluster.registry.zk;

import io.rowweave.incremental.CapturedTargetReceipt;
import io.rowweave.incremental.FrontierRecoveryToken;
import io.rowweave.incremental.IncrementalClaimRequest;
import io.rowweave.incremental.IncrementalFrontier;
import io.rowweave.incremental.IncrementalInitializeRequest;
import io.rowweave.incremental.IncrementalRunMode;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RunSnapshot;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Run recovery ownership is subordinate to frontier ownership. */
class ZkIncrementalRecoveryOwnershipTest {

    @Test
    void replacementFrontierOwnerAtomicallyFencesRunRecoveryClaim()
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
            first.initialize("initialize-recovery", initialize());
            RecoveryOwner ownerA = new RecoveryOwner("master-a", 1L);
            FrontierRecoveryToken frontierA = first.claimOwnership(
                    "feed-recovery", 0L, 0L, ownerA).token();
            first.publishPending(frontierA, request);
            first.claimInterval("claim-recovery", frontierA, request);
            RunSnapshot run = firstRuns.get(request.runId());

            RecoveryClaim firstClaim = first.claimRunOwnership(
                    frontierA, request.runId(), run.revision(), 0L, ownerA);
            assertEquals(RecoveryClaim.Code.ACQUIRED, firstClaim.code());
            assertEquals(ownerA, firstClaim.token().owner());
            assertEquals(RecoveryClaim.Code.REPLAYED, first.claimRunOwnership(
                    frontierA, request.runId(), run.revision(), 0L, ownerA).code());

            RecoveryOwner ownerB = new RecoveryOwner("master-b", 2L);
            FrontierRecoveryToken frontierB = replacement.claimOwnership(
                    "feed-recovery", 1L, 1L, ownerB).token();
            assertEquals(RecoveryClaim.Code.LOST_RACE, first.claimRunOwnership(
                    frontierA, request.runId(), run.revision(), 1L, ownerA).code());
            RecoveryClaim replacementClaim = replacement.claimRunOwnership(
                    frontierB, request.runId(), run.revision(), 1L, ownerB);
            assertEquals(RecoveryClaim.Code.ACQUIRED, replacementClaim.code());
            assertEquals(ownerB, replacementRuns.ownership(request.runId()).owner());
        }
    }

    @Test
    void frontierReplacementInsideRunClaimMultiLeavesRunOwnerUnchanged()
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
            RecoveryOwner ownerA = new RecoveryOwner("master-a", 1L);
            FrontierRecoveryToken frontierA = first.claimOwnership(
                    "feed-recovery", 0L, 0L, ownerA).token();
            first.publishPending(frontierA, request);
            first.claimInterval("claim-race", frontierA, request);
            RunSnapshot run = firstRuns.get(request.runId());
            AtomicBoolean replace = new AtomicBoolean(true);
            firstRuns.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.RECOVERY_CLAIM
                        && moment == ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT
                        && replace.compareAndSet(true, false)) {
                    replacement.claimOwnership(
                            "feed-recovery", 1L, 1L,
                            new RecoveryOwner("master-b", 2L));
                }
            });

            assertEquals(RecoveryClaim.Code.LOST_RACE,
                    first.claimRunOwnership(frontierA, request.runId(),
                            run.revision(), 0L, ownerA).code());
            assertEquals(null, replacementRuns.ownership(request.runId()));
        }
    }

    private static IncrementalInitializeRequest initialize() throws Exception {
        return new IncrementalInitializeRequest(
                "feed-recovery", 1L, digest("definition"), digest("source"),
                digest("frontier"), IncrementalFrontier.Kind.INT64_SEQUENCE_V1);
    }

    private static IncrementalClaimRequest request() throws Exception {
        CapturedTargetReceipt capture = CapturedTargetReceipt.normal(
                1L, digest("definition"), digest("source"), 0L,
                IncrementalFrontier.begin(), "trigger-recovery",
                IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(2L), bytes("snapshot"));
        return new IncrementalClaimRequest(
                "feed-recovery", capture, digest("frontier"),
                hex(digest("artifact")), "attempt-recovery", "publication-recovery",
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
