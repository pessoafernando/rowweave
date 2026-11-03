package io.rowweave.cluster.registry.zk;

import io.rowweave.incremental.CapturedTargetReceipt;
import io.rowweave.incremental.FrontierOwnershipClaim;
import io.rowweave.incremental.IncrementalAuthorityDecision;
import io.rowweave.incremental.IncrementalAuthorityRecordCodec;
import io.rowweave.incremental.IncrementalClaimRequest;
import io.rowweave.incremental.IncrementalClaimRequestCodec;
import io.rowweave.incremental.IncrementalFrontier;
import io.rowweave.incremental.IncrementalInitializeRequest;
import io.rowweave.incremental.IncrementalPendingRunDecision;
import io.rowweave.incremental.IncrementalRunMode;
import io.rowweave.incremental.OrderedFrontierKey;
import io.rowweave.run.RecoveryOwner;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** exact serialized ZK request profile for a large claim. */
@Tag("profile")
class ZkIncrementalResourceProfileTest {
    private static final int ZK_REQUEST_LIMIT = 900 * 1024;

    @Test
    void maximumReceiptPendingAndClaimStayBelowZkRequestCeiling()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            AtomicInteger maximumOperations = new AtomicInteger();
            AtomicInteger maximumRequestBytes = new AtomicInteger();
            ZkRunStateStore runs = new ZkRunStateStore(client);
            runs.transactionWriteObserverForTest(
                    (kind, ordinal, operations, requestBytes) -> {
                        maximumOperations.accumulateAndGet(
                                operations, Math::max);
                        maximumRequestBytes.accumulateAndGet(
                                requestBytes, Math::max);
                    });
            ZkIncrementalFrontierStore store =
                    new ZkIncrementalFrontierStore(client, runs);
            String feedId = repeat('f', 256);
            byte[] definition = digest("definition");
            byte[] source = digest("source");
            byte[] frontierContract = digest("frontier-contract");
            IncrementalInitializeRequest initialize =
                    new IncrementalInitializeRequest(
                            feedId, 1L, definition, source,
                            frontierContract,
                            IncrementalFrontier.Kind.TIMESTAMP_KEY_V1);
            assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                    store.initialize(repeat('i', 256), initialize).code());
            FrontierOwnershipClaim ownership = store.claimOwnership(
                    feedId, 0L, 0L,
                    new RecoveryOwner(repeat('m', 256), 1L));
            assertEquals(FrontierOwnershipClaim.Code.ACQUIRED,
                    ownership.code());

            CapturedTargetReceipt receipt = CapturedTargetReceipt.normal(
                    1L, definition, source, 0L,
                    IncrementalFrontier.begin(), repeat('t', 256),
                    IncrementalRunMode.SNAPSHOT_V1,
                    maximumTimestampFrontier(), new byte[16 * 1024]);
            IncrementalClaimRequest request = new IncrementalClaimRequest(
                    feedId, receipt, frontierContract,
                    repeat('a', 64), repeat('x', 256),
                    repeat('p', 256), 3, 0L, 1L);
            assertEquals(IncrementalPendingRunDecision.Code.PUBLISHED,
                    store.publishPending(ownership.token(), request).code());
            assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                    store.claimInterval(repeat('c', 256),
                            ownership.token(), request).code());

            int claimBytes = IncrementalClaimRequestCodec.encode(request).length;
            int recordBytes = IncrementalAuthorityRecordCodec.encode(
                    store.record(feedId)).length;
            assertTrue(claimBytes
                    <= IncrementalClaimRequestCodec.MAX_FRAME_BYTES);
            assertTrue(recordBytes
                    <= IncrementalAuthorityRecordCodec.MAX_FRAME_BYTES);
            assertTrue(maximumOperations.get() > 0);
            assertTrue(maximumRequestBytes.get() > 0);
            assertTrue(maximumRequestBytes.get() < ZK_REQUEST_LIMIT);

            System.out.printf("[CDC-ZK-PROFILE] claim=%dB record=%dB "
                            + "maxMultiOps=%d maxMultiRequest=%dB limit=%dB%n",
                    claimBytes, recordBytes, maximumOperations.get(),
                    maximumRequestBytes.get(), ZK_REQUEST_LIMIT);
        }
    }

    private static IncrementalFrontier maximumTimestampFrontier() {
        byte[] component = new byte[1017];
        java.util.Arrays.fill(component, (byte) 1);
        OrderedFrontierKey key = OrderedFrontierKey.of(
                OrderedFrontierKey.bytes(component),
                OrderedFrontierKey.bytes(component),
                OrderedFrontierKey.bytes(component),
                OrderedFrontierKey.bytes(component));
        return IncrementalFrontier.timestampKey(Long.MAX_VALUE, key);
    }

    private static CuratorFramework connectedClient(TestingServer server) {
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), new ExponentialBackoffRetry(20, 10));
        client.start();
        try {
            assertTrue(client.blockUntilConnected(10, TimeUnit.SECONDS));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        }
        return client;
    }

    private static byte[] digest(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(
                value.getBytes(StandardCharsets.UTF_8));
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }
}
