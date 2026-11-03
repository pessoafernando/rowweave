package io.rowweave.cluster.registry.zk;

import io.rowweave.incremental.FrontierOwnershipClaim;
import io.rowweave.incremental.IncrementalAuthorityDecision;
import io.rowweave.incremental.IncrementalFrontier;
import io.rowweave.incremental.IncrementalInitializeRequest;
import io.rowweave.run.RecoveryOwner;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZkIncrementalFrontierStoreTest {

    @Test
    void initializeQueryAndOwnershipAreDurableAcrossIndependentSessions() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connectedClient(server);
             CuratorFramework secondClient = connectedClient(server)) {
            ZkIncrementalFrontierStore first = new ZkIncrementalFrontierStore(firstClient);
            ZkIncrementalFrontierStore second = new ZkIncrementalFrontierStore(secondClient);
            IncrementalInitializeRequest request = initialize("feed-zk", 1L);

            assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                    first.initialize("initialize-zk", request).code());
            assertEquals(first.record("feed-zk"), second.record("feed-zk"));
            assertEquals(IncrementalAuthorityDecision.Code.ALREADY_APPLIED,
                    second.initialize("initialize-zk", request).code());

            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(2);
            List<FrontierOwnershipClaim> claims = new ArrayList<>();
            Object monitor = new Object();
            pool.submit(() -> {
                start.await();
                FrontierOwnershipClaim claim = first.claimOwnership(
                        "feed-zk", 0L, 0L, new RecoveryOwner("master-a", 1L));
                synchronized (monitor) { claims.add(claim); }
                return null;
            });
            pool.submit(() -> {
                start.await();
                FrontierOwnershipClaim claim = second.claimOwnership(
                        "feed-zk", 0L, 0L, new RecoveryOwner("master-b", 2L));
                synchronized (monitor) { claims.add(claim); }
                return null;
            });
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10L, TimeUnit.SECONDS));
            assertEquals(1L, claims.stream()
                    .filter(claim -> claim.code() == FrontierOwnershipClaim.Code.ACQUIRED)
                    .count());
            assertEquals(1L, claims.stream()
                    .filter(claim -> claim.code() == FrontierOwnershipClaim.Code.LOST_RACE)
                    .count());
        }
    }

    @Test
    void postCommitAckLossReconcilesInitializeAndOwnership() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            AtomicBoolean initializeAck = new AtomicBoolean(true);
            AtomicBoolean ownershipAck = new AtomicBoolean(true);
            ZkIncrementalFrontierStore store = new ZkIncrementalFrontierStore(
                    client,
                    () -> {
                        if (initializeAck.compareAndSet(true, false)) {
                            throw new KeeperException.ConnectionLossException();
                        }
                    },
                    () -> {
                        if (ownershipAck.compareAndSet(true, false)) {
                            throw new KeeperException.ConnectionLossException();
                        }
                    });

            IncrementalAuthorityDecision initialized = store.initialize(
                    "initialize-ack", initialize("feed-ack", 1L));
            assertEquals(IncrementalAuthorityDecision.Code.ALREADY_APPLIED,
                    initialized.code());
            FrontierOwnershipClaim claimed = store.claimOwnership(
                    "feed-ack", 0L, 0L, new RecoveryOwner("master", 7L));
            assertEquals(FrontierOwnershipClaim.Code.REPLAYED, claimed.code());
            assertEquals(1L, claimed.token().ownershipEpoch());
            assertEquals(claimed.token(), store.frontierOwnership("feed-ack"));
            assertNull(store.frontierOwnership("missing-feed"));
        }
    }

    @Test
    void persistentGuardRejectsAbaAfterFeedRootRecreation() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkIncrementalFrontierStore store = new ZkIncrementalFrontierStore(client);
            assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                    store.initialize("initialize-first", initialize("feed-aba", 1L)).code());
            FrontierOwnershipClaim first = store.claimOwnership(
                    "feed-aba", 0L, 0L, new RecoveryOwner("master-a", 1L));

            client.delete().deletingChildrenIfNeeded()
                    .forPath(ZkIncrementalFrontierStore.feedPath("feed-aba"));
            assertEquals(IncrementalAuthorityDecision.Code.FEED_CONFLICT,
                    store.initialize("initialize-stale", initialize("feed-aba", 1L)).code());
            assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                    store.initialize("initialize-second", initialize("feed-aba", 2L)).code());
            FrontierOwnershipClaim second = store.claimOwnership(
                    "feed-aba", 0L, 0L, new RecoveryOwner("master-b", 2L));

            assertEquals(2L, second.token().feedIncarnation());
            assertNotEquals(first.token().feedRootCzxid(), second.token().feedRootCzxid());
            assertNotEquals(first.token().guardShardObservation(),
                    second.token().guardShardObservation());
        }
    }

    @Test
    void sameShardCapacityIsBoundedAndConcurrentInitializationLosesNoEntry()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connectedClient(server);
             CuratorFramework secondClient = connectedClient(server)) {
            ZkIncrementalFrontierStore first =
                    new ZkIncrementalFrontierStore(firstClient);
            ZkIncrementalFrontierStore second =
                    new ZkIncrementalFrontierStore(secondClient);
            List<String> feeds = sameShardFeeds(17);
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(2);
            List<IncrementalAuthorityDecision> decisions = new ArrayList<>();
            Object monitor = new Object();
            for (int i = 0; i < 2; i++) {
                final int index = i;
                final ZkIncrementalFrontierStore store = i == 0 ? first : second;
                pool.submit(() -> {
                    start.await();
                    IncrementalAuthorityDecision decision = store.initialize(
                            "initialize-" + index, initialize(feeds.get(index), 1L));
                    synchronized (monitor) { decisions.add(decision); }
                    return null;
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10L, TimeUnit.SECONDS));
            assertEquals(2L, decisions.stream()
                    .filter(decision -> decision.code()
                            == IncrementalAuthorityDecision.Code.ACCEPTED)
                    .count());
            for (int i = 2; i < 16; i++) {
                assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                        first.initialize("initialize-" + i,
                                initialize(feeds.get(i), 1L)).code());
            }
            assertEquals(IncrementalAuthorityDecision.Code.GUARD_SHARD_FULL,
                    second.initialize("initialize-overflow",
                            initialize(feeds.get(16), 1L)).code());
        }
    }

    @Test
    void damagedGuardFailsClosedBeforeOwnershipMutation() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkIncrementalFrontierStore store = new ZkIncrementalFrontierStore(client);
            store.initialize("initialize-damaged", initialize("feed-damaged", 1L));
            String guardPath = ZkIncrementalFrontierStore.guardPathForFeed(
                    "feed-damaged");
            byte[] guard = client.getData().forPath(guardPath);
            guard[guard.length - 1] ^= 1;
            client.setData().forPath(guardPath, guard);

            assertThrows(IllegalStateException.class,
                    () -> store.claimOwnership(
                            "feed-damaged", 0L, 0L,
                            new RecoveryOwner("master", 1L)));
            assertEquals(null, client.checkExists().forPath(
                    ZkIncrementalFrontierStore.feedPath("feed-damaged")
                            + "/recovery-owner"));
        }
    }

    private static List<String> sameShardFeeds(int count) {
        List<String> result = new ArrayList<>();
        int shard = -1;
        for (int candidate = 0; result.size() < count; candidate++) {
            String feed = "same-shard-" + candidate;
            int next = ZkIncrementalFrontierStore.guardShardIndex(feed);
            if (shard < 0) shard = next;
            if (next == shard) result.add(feed);
        }
        return result;
    }

    private static IncrementalInitializeRequest initialize(String feedId, long incarnation)
            throws Exception {
        return new IncrementalInitializeRequest(
                feedId, incarnation, digest("definition"), digest("source"),
                digest("frontier"), IncrementalFrontier.Kind.INT64_SEQUENCE_V1);
    }

    private static byte[] digest(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(StandardCharsets.UTF_8));
    }

    private static CuratorFramework connectedClient(TestingServer server) {
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), new ExponentialBackoffRetry(20, 10));
        client.start();
        try {
            if (!client.blockUntilConnected(30, TimeUnit.SECONDS)) {
                client.close();
                throw new AssertionError("Curator did not connect");
            }
        } catch (InterruptedException interrupted) {
            client.close();
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        }
        return client;
    }
}
