package io.rowweave.cluster.registry.zk;

import io.rowweave.incremental.CapturedTargetReceipt;
import io.rowweave.incremental.FrontierOwnershipClaim;
import io.rowweave.incremental.FrontierRecoveryToken;
import io.rowweave.incremental.IncrementalAuthorityDecision;
import io.rowweave.incremental.IncrementalClaimRequest;
import io.rowweave.incremental.IncrementalFrontier;
import io.rowweave.incremental.IncrementalFrontierAuthorityState;
import io.rowweave.incremental.IncrementalInitializeRequest;
import io.rowweave.incremental.IncrementalPendingRunDecision;
import io.rowweave.incremental.IncrementalRunMode;
import io.rowweave.run.RecoveryOwner;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class ZkIncrementalPendingClaimTest {

    @Test
    void pendingRunAndFirstAttemptAreDurableAndClaimAckLossReplays()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connectedClient(server);
             CuratorFramework replacementClient = connectedClient(server)) {
            ZkRunStateStore firstRuns = new ZkRunStateStore(firstClient);
            ZkRunStateStore replacementRuns = new ZkRunStateStore(replacementClient);
            AtomicBoolean claimAck = new AtomicBoolean(true);
            ZkIncrementalFrontierStore first = new ZkIncrementalFrontierStore(
                    firstClient, firstRuns, () -> { }, () -> { }, () -> {
                if (claimAck.compareAndSet(true, false)) {
                    throw new KeeperException.ConnectionLossException();
                }
            });
            ZkIncrementalFrontierStore replacement =
                    new ZkIncrementalFrontierStore(replacementClient, replacementRuns);
            IncrementalClaimRequest request = request();
            assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                    first.initialize("initialize-pending", initialize()).code());
            FrontierRecoveryToken token = first.claimOwnership(
                    "feed-pending", 0L, 0L,
                    new RecoveryOwner("master-a", 1L)).token();

            assertEquals(IncrementalPendingRunDecision.Code.PUBLISHED,
                    first.publishPending(token, request).code());
            RunSnapshot pending = replacementRuns.get(request.runId());
            assertNotNull(pending);
            assertNull(pending.currentAttempt());

            IncrementalAuthorityDecision claimed = first.claimInterval(
                    "claim-pending", token, request);
            assertEquals(IncrementalAuthorityDecision.Code.ALREADY_APPLIED,
                    claimed.code());
            assertEquals(IncrementalFrontierAuthorityState.ACTIVE,
                    replacement.record("feed-pending").state());
            assertEquals(request.intervalId(), replacement.record("feed-pending")
                    .activeInterval().identity().intervalId());
            RunSnapshot attempted = replacementRuns.get(request.runId());
            assertNotNull(attempted.currentAttempt());
            assertEquals(request.attemptId(), attempted.currentAttempt().attemptId());
            assertEquals(1L, attempted.currentAttempt().fenceToken());

            assertEquals(IncrementalAuthorityDecision.Code.ALREADY_APPLIED,
                    replacement.claimInterval("claim-pending", token, request).code());
        }
    }

    private static IncrementalInitializeRequest initialize() throws Exception {
        return new IncrementalInitializeRequest(
                "feed-pending", 1L, digest("definition"), digest("source"),
                digest("frontier"), IncrementalFrontier.Kind.INT64_SEQUENCE_V1);
    }

    private static IncrementalClaimRequest request() throws Exception {
        CapturedTargetReceipt capture = CapturedTargetReceipt.normal(
                1L, digest("definition"), digest("source"), 0L,
                IncrementalFrontier.begin(), "trigger-1",
                IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(2L), bytes("snapshot"));
        return new IncrementalClaimRequest(
                "feed-pending", capture, digest("frontier"),
                hex(digest("artifact")), "attempt-1", "publication-1",
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
