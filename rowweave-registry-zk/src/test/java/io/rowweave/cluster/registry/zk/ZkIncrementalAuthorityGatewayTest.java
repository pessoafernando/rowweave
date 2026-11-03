package io.rowweave.cluster.registry.zk;

import io.rowweave.incremental.CapturedTargetReceipt;
import io.rowweave.incremental.FrontierOwnershipClaim;
import io.rowweave.incremental.FrontierRecoveryToken;
import io.rowweave.incremental.IncrementalAttemptProgressCommand;
import io.rowweave.incremental.IncrementalAuthorityDecision;
import io.rowweave.incremental.IncrementalAuthorityGateway;
import io.rowweave.incremental.IncrementalClaimRequest;
import io.rowweave.incremental.IncrementalFrontier;
import io.rowweave.incremental.IncrementalInitializeRequest;
import io.rowweave.incremental.IncrementalPendingRunDecision;
import io.rowweave.incremental.IncrementalRunMode;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/** registry exposes one joint incremental authority gateway. */
class ZkIncrementalAuthorityGatewayTest {

    @Test
    void registryGatewayUsesTheSameZkLinearizationPointForFrontierAndRun()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             ZkServiceRegistry registry =
                     new ZkServiceRegistry(server.getConnectString())) {
            IncrementalAuthorityGateway authority =
                    registry.incrementalAuthorityGateway();
            assertSame(authority, registry.incrementalAuthorityGateway());
            IncrementalInitializeRequest initialize =
                    new IncrementalInitializeRequest(
                            "feed-gateway", 1L, digest("definition"),
                            digest("source"), digest("frontier"),
                            IncrementalFrontier.Kind.INT64_SEQUENCE_V1);
            assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                    authority.initialize("initialize-gateway", initialize)
                            .code());
            RecoveryOwner owner = new RecoveryOwner("master-a", 1L);
            FrontierOwnershipClaim ownership = authority.claimOwnership(
                    "feed-gateway", 0L, 0L, owner);
            FrontierRecoveryToken frontier = ownership.token();
            CapturedTargetReceipt capture = CapturedTargetReceipt.normal(
                    1L, digest("definition"), digest("source"), 0L,
                    IncrementalFrontier.begin(), "trigger-gateway",
                    IncrementalRunMode.SNAPSHOT_V1,
                    IncrementalFrontier.int64Sequence(2L),
                    bytes("snapshot"));
            IncrementalClaimRequest request = new IncrementalClaimRequest(
                    "feed-gateway", capture, digest("frontier"),
                    hex(digest("artifact")), "attempt-gateway",
                    "publication-gateway", 2, 0L, 10L);
            assertEquals(IncrementalPendingRunDecision.Code.PUBLISHED,
                    authority.publishPending(frontier, request).code());
            assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                    authority.claimInterval(
                            "claim-gateway", frontier, request).code());

            RunSnapshot run = authority.runSnapshot(request.runId());
            RecoveryClaim runOwnership = authority.claimRunOwnership(
                    frontier, run.runId(), run.revision(), 0L, owner);
            IncrementalAttemptProgressCommand progress =
                    new IncrementalAttemptProgressCommand(
                            "feed-gateway", request.intervalId(),
                            authority.record("feed-gateway").revision(),
                            run.revision(), request.firstAttemptFence(),
                            AttemptStatus.DISPATCHING, "dispatch-gateway", 11L);
            RunDecision decision = authority.advanceProgress(
                    frontier, runOwnership.token(), progress);
            assertEquals(RunDecision.Code.ACCEPTED, decision.code());
            assertEquals(AttemptStatus.DISPATCHING,
                    authority.runSnapshot(run.runId())
                            .currentAttempt().status());
        }
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
}
