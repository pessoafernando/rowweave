package io.rowweave.cluster.registry.zk;

import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RecoverableRunCatalogProvider;
import io.rowweave.run.RecoverableRunRef;
import io.rowweave.run.RunRecoveryCatalogKernel;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RunRecoveryOwnershipProvider;
import io.rowweave.run.RunStatus;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.RunStateStoreProvider;
import io.rowweave.run.RunStateStoreUnavailableException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZkRunStateStoreTest {

    @Test
    void legacyRunsWithoutCatalogRequireExplicitMigration() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            client.create().creatingParentsIfNeeded()
                    .forPath("/rowweave/run-state-v1/runs/legacy-hash", new byte[0]);
            RunStateStoreUnavailableException failure = assertThrows(
                    RunStateStoreUnavailableException.class,
                    () -> new ZkRunStateStore(client));
            assertTrue(failure.getMessage().contains("initialize ZK run state root"));
            assertNull(client.checkExists().forPath("/rowweave/run-recovery-v1"));
        }
    }

    @Test
    void registryProvidesOneStoreBoundToItsClientLifecycle() throws Exception {
        try (TestingServer server = new TestingServer(true);
             ZkServiceRegistry registry = new ZkServiceRegistry(server.getConnectString())) {
            RunStateStoreProvider provider = (RunStateStoreProvider) registry;
            assertSame(provider.runStateStore(), provider.runStateStore());
            assertTrue(registry instanceof RecoverableRunCatalogProvider);
            assertSame(provider.runStateStore(),
                    ((RecoverableRunCatalogProvider) registry).recoverableRunCatalog());
            assertTrue(registry instanceof RunRecoveryOwnershipProvider);
            assertSame(provider.runStateStore(),
                    ((RunRecoveryOwnershipProvider) registry).runRecoveryOwnership());
        }
    }

    @Test
    void twoIndependentSessionsCompetingForRecoveryOwnershipHaveOneWinner() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connectedClient(server);
             CuratorFramework secondClient = connectedClient(server)) {
            ZkRunStateStore first = new ZkRunStateStore(firstClient);
            ZkRunStateStore second = new ZkRunStateStore(secondClient);
            assertEquals(RunDecision.Code.ACCEPTED, first.apply(RunCommand.createRun(
                    "create-owner-race", "owner-race", "definition", "artifact", 1, 0L, 1L)).code());
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(2);
            List<RecoveryClaim> claims = new ArrayList<>();
            Object monitor = new Object();
            pool.submit(() -> {
                start.await();
                RecoveryClaim claim = first.claim("owner-race", 1L, 0L,
                        new RecoveryOwner("master-a", 1L));
                synchronized (monitor) { claims.add(claim); }
                return null;
            });
            pool.submit(() -> {
                start.await();
                RecoveryClaim claim = second.claim("owner-race", 1L, 0L,
                        new RecoveryOwner("master-b", 2L));
                synchronized (monitor) { claims.add(claim); }
                return null;
            });
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
            assertEquals(1L, claims.stream()
                    .filter(c -> c.code() == RecoveryClaim.Code.ACQUIRED).count());
            assertEquals(1L, claims.stream()
                    .filter(c -> c.code() == RecoveryClaim.Code.LOST_RACE).count());
            assertEquals(first.ownership("owner-race"), second.ownership("owner-race"));
        }
    }

    @Test
    void ownershipCommitAckLossReconcilesThePublishedToken() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            AtomicBoolean firstOwnershipCommit = new AtomicBoolean(true);
            ZkRunStateStore store = new ZkRunStateStore(client, () -> { }, () -> {
                if (firstOwnershipCommit.compareAndSet(true, false)) {
                    throw new KeeperException.ConnectionLossException();
                }
            }, 100, RunRecoveryCatalogKernel.MAX_ENTRIES_PER_SHARD);
            store.apply(RunCommand.createRun(
                    "create-owner-ack", "owner-ack", "definition", "artifact", 1, 0L, 1L));
            RecoveryClaim claim = store.claim("owner-ack", 1L, 0L,
                    new RecoveryOwner("master-a", 11L));
            assertEquals(RecoveryClaim.Code.REPLAYED, claim.code());
            assertEquals(claim.token(), store.ownership("owner-ack"));
            assertEquals(1L, claim.token().ownershipEpoch());
        }
    }

    @Test
    void damagedOwnerAndMissingCatalogFailClosedDuringClaim() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            store.apply(RunCommand.createRun(
                    "create-owner-corrupt", "owner-corrupt", "definition", "artifact", 1, 0L, 1L));
            RecoveryClaim acquired = store.claim("owner-corrupt", 1L, 0L,
                    new RecoveryOwner("master-a", 1L));
            String runPath = "/rowweave/run-state-v1/runs/" + sha256Hex("owner-corrupt");
            byte[] owner = client.getData().forPath(runPath + "/recovery-owner");
            owner[12] ^= 1;
            client.setData().forPath(runPath + "/recovery-owner", owner);
            assertThrows(IllegalArgumentException.class,
                    () -> store.ownership("owner-corrupt"));
            assertThrows(IllegalArgumentException.class,
                    () -> store.current(acquired.token()));

            store.apply(RunCommand.createRun(
                    "create-owner-missing-catalog", "owner-missing-catalog",
                    "definition", "artifact", 1, 0L, 1L));
            client.delete().forPath(catalogEntryPath(sha256Hex("owner-missing-catalog")));
            assertThrows(IllegalStateException.class,
                    () -> store.claim("owner-missing-catalog", 1L, 0L,
                            new RecoveryOwner("master-b", 2L)));
            String missingRun = "/rowweave/run-state-v1/runs/" + sha256Hex("owner-missing-catalog");
            assertNull(client.checkExists().forPath(missingRun + "/recovery-owner"));
        }
    }

    @Test
    void twoIndependentSessionsCompetingOnOneRevisionHaveOneWinner() throws Exception {
        try (TestingServer server = new TestingServer(true);
             ZkServiceRegistry firstRegistry = new ZkServiceRegistry(server.getConnectString());
             ZkServiceRegistry secondRegistry = new ZkServiceRegistry(server.getConnectString())) {
            RunStateStore first = ((RunStateStoreProvider) firstRegistry).runStateStore();
            RunStateStore second = ((RunStateStoreProvider) secondRegistry).runStateStore();
            assertEquals(RunDecision.Code.ACCEPTED, first.apply(RunCommand.createRun(
                    "create", "run", "definition", "artifact", 2, 0L, 1L)).code());

            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(2);
            List<RunDecision> decisions = new ArrayList<>();
            Object monitor = new Object();
            for (int i = 1; i <= 2; i++) {
                final int number = i;
                final RunStateStore store = i == 1 ? first : second;
                pool.submit(() -> {
                    start.await();
                    RunDecision decision = store.apply(RunCommand.createAttempt(
                            "attempt-" + number, "run", 1L,
                            "attempt-" + number, "publication-" + number, 2L));
                    synchronized (monitor) { decisions.add(decision); }
                    return null;
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
            assertEquals(1L, decisions.stream().filter(RunDecision::accepted).count());
            assertEquals(1L, decisions.stream()
                    .filter(d -> d.code() == RunDecision.Code.STALE_REVISION).count());
            assertEquals(2L, first.get("run").revision());
            assertEquals(2, second.events("run", 0L, 100).size());
        }
    }

    @Test
    void commitAckLossIsReconciledByReceiptWithoutDuplicateEvent() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            AtomicBoolean first = new AtomicBoolean(true);
            ZkRunStateStore store = new ZkRunStateStore(client, () -> {
                if (first.compareAndSet(true, false)) throw new KeeperException.ConnectionLossException();
            });
            RunDecision decision = store.apply(RunCommand.createRun(
                    "create", "run", "definition", "artifact", 2, 0L, 1L));
            assertEquals(RunDecision.Code.ALREADY_APPLIED, decision.code());
            assertEquals(1L, decision.event().sequence());
            assertEquals(1L, store.get("run").revision());
            assertEquals(1, store.events("run", 0L, 100).size());
            String digest = sha256Hex("run");
            String entry = catalogEntryPath(digest);
            RecoverableRunRef ref = RunRecoveryCatalogKernel.decodeRef(
                    client.getData().forPath(entry));
            assertEquals("run", ref.runId());
            assertEquals(1L, ref.revision());
            assertEquals(RunStatus.READY, ref.status());
        }
    }

    @Test
    void sameShardCapacityIsSerializedAcrossIndependentSessions() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connectedClient(server);
             CuratorFramework secondClient = connectedClient(server)) {
            ZkRunStateStore first = new ZkRunStateStore(firstClient, () -> { }, 100, 1);
            ZkRunStateStore second = new ZkRunStateStore(secondClient, () -> { }, 100, 1);
            List<String> ids = sameShardIds(2);
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(2);
            List<RunDecision> decisions = new ArrayList<>();
            Object monitor = new Object();
            pool.submit(() -> {
                start.await();
                RunDecision decision = first.apply(RunCommand.createRun(
                        "create-1", ids.get(0), "definition", "artifact", 1, 0L, 1L));
                synchronized (monitor) { decisions.add(decision); }
                return null;
            });
            pool.submit(() -> {
                start.await();
                RunDecision decision = second.apply(RunCommand.createRun(
                        "create-2", ids.get(1), "definition", "artifact", 1, 0L, 1L));
                synchronized (monitor) { decisions.add(decision); }
                return null;
            });
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
            assertEquals(1L, decisions.stream().filter(RunDecision::accepted).count());
            assertEquals(1L, decisions.stream()
                    .filter(d -> d.code() == RunDecision.Code.LIMIT_EXCEEDED).count());
            int snapshots = (first.get(ids.get(0)) == null ? 0 : 1)
                    + (first.get(ids.get(1)) == null ? 0 : 1);
            assertEquals(1, snapshots);
        }
    }

    @Test
    void terminalTransitionDeletesCatalogEntryInTheAuthorityMulti() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            RunDecision created = store.apply(RunCommand.createRun(
                    "create-terminal", "terminal", "definition", "artifact", 1, 0L, 1L));
            String entry = catalogEntryPath(sha256Hex("terminal"));
            assertTrue(client.checkExists().forPath(entry) != null);
            RunDecision canceled = store.apply(RunCommand.cancelReadyRun(
                    "cancel-terminal", "terminal", created.snapshot().revision(),
                    2L, "USER_CANCEL", "stop"));
            assertEquals(RunDecision.Code.ACCEPTED, canceled.code());
            assertEquals(RunStatus.CANCELED, store.get("terminal").status());
            assertNull(client.checkExists().forPath(entry));
        }
    }

    @Test
    void missingOrTamperedCatalogEntryFailsAsAuthorityCorruption() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            store.apply(RunCommand.createRun(
                    "create-missing-catalog", "missing-catalog",
                    "definition", "artifact", 1, 0L, 1L));
            String missing = catalogEntryPath(sha256Hex("missing-catalog"));
            client.delete().forPath(missing);
            assertThrows(IllegalStateException.class, () -> store.get("missing-catalog"));

            store.apply(RunCommand.createRun(
                    "create-bad-catalog", "bad-catalog",
                    "definition", "artifact", 1, 0L, 1L));
            String bad = catalogEntryPath(sha256Hex("bad-catalog"));
            byte[] bytes = client.getData().forPath(bad);
            bytes[8] ^= 1;
            client.setData().forPath(bad, bytes);
            assertThrows(IllegalArgumentException.class, () -> store.get("bad-catalog"));
        }
    }

    @Test
    void disconnectedAuthorityFailsTypedAndPreservesCommandIdentity() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            RunCommand command = RunCommand.createRun(
                    "create-offline", "offline", "definition", "artifact", 2, 0L, 1L);
            server.stop();
            RunStateStoreUnavailableException failure = assertThrows(
                    RunStateStoreUnavailableException.class,
                    () -> store.apply(command));
            assertEquals("create-offline", failure.commandId());

            server.restart();
            assertTrue(client.blockUntilConnected(5, TimeUnit.SECONDS));
            assertNull(store.get("offline"));
            assertEquals(0, store.events("offline", 0L, 100).size());
            assertEquals(RunDecision.Code.ACCEPTED, store.apply(command).code());
            assertEquals(1, store.events("offline", 0L, 100).size());
        }
    }

    @Test
    void missingLatestEventFailsAsCorruptionInsteadOfUnavailable() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            assertEquals(RunDecision.Code.ACCEPTED, store.apply(RunCommand.createRun(
                    "create", "corrupt", "definition", "artifact", 2, 0L, 1L)).code());
            String runPath = "/rowweave/run-state-v1/runs/" + sha256Hex("corrupt");
            client.delete().forPath(runPath + "/events/00000000000000000001");
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> store.get("corrupt"));
            assertTrue(failure.getMessage().contains("corrupt ZK run authority"));
        }
    }

    @Test
    void corruptSnapshotChecksumAndDefensiveChildBoundFailClosed() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore bounded = new ZkRunStateStore(client, () -> { }, 1);
            RunDecision created = bounded.apply(RunCommand.createRun(
                    "create-bound", "bounded", "definition", "artifact", 2, 0L, 1L));
            assertEquals(RunDecision.Code.ACCEPTED, bounded.apply(RunCommand.createAttempt(
                    "attempt-bound", "bounded", created.snapshot().revision(),
                    "attempt-1", "publication-1", 2L)).code());
            assertThrows(IllegalStateException.class, () -> bounded.events("bounded", 0L, 100));

            ZkRunStateStore corrupt = new ZkRunStateStore(client);
            corrupt.apply(RunCommand.createRun(
                    "create-corrupt", "checksum", "definition", "artifact", 2, 0L, 1L));
            String snapshot = "/rowweave/run-state-v1/runs/" + sha256Hex("checksum") + "/snapshot";
            byte[] bytes = client.getData().forPath(snapshot);
            bytes[20] ^= 1;
            client.setData().forPath(snapshot, bytes);
            assertThrows(IllegalArgumentException.class, () -> corrupt.get("checksum"));
        }
    }

    @Test
    void identityReceiptAndIllegalEventChildCorruptionFailClosed() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            store.apply(RunCommand.createRun(
                    "create-identity", "identity", "definition", "artifact", 2, 0L, 1L));
            String identityRun = "/rowweave/run-state-v1/runs/" + sha256Hex("identity");
            client.setData().forPath(identityRun + "/identity",
                    "different".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            assertThrows(IllegalStateException.class, () -> store.get("identity"));

            store.apply(RunCommand.createRun(
                    "create-receipt", "receipt", "definition", "artifact", 2, 0L, 1L));
            String receiptRun = "/rowweave/run-state-v1/runs/" + sha256Hex("receipt");
            client.delete().forPath(receiptRun + "/commands/" + sha256Hex("create-receipt"));
            assertThrows(IllegalStateException.class, () -> store.get("receipt"));

            store.apply(RunCommand.createRun(
                    "create-child", "child", "definition", "artifact", 2, 0L, 1L));
            String childRun = "/rowweave/run-state-v1/runs/" + sha256Hex("child");
            client.create().forPath(childRun + "/events/not-a-sequence", new byte[0]);
            assertThrows(IllegalStateException.class, () -> store.events("child", 0L, 100));
        }
    }

    private static CuratorFramework connectedClient(TestingServer server) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .sessionTimeoutMs(1_000)
                .connectionTimeoutMs(250)
                .retryPolicy(new ExponentialBackoffRetry(10, 1))
                .build();
        client.start();
        client.blockUntilConnected();
        return client;
    }

    private static String sha256Hex(String value) throws Exception {
        byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        StringBuilder out = new StringBuilder();
        for (byte b : digest) out.append(String.format(java.util.Locale.ROOT, "%02x", b & 0xff));
        return out.toString();
    }

    private static String catalogEntryPath(String digest) {
        return "/rowweave/run-recovery-v1/shards/" + digest.substring(0, 2)
                + "/entries/" + digest;
    }

    private static List<String> sameShardIds(int count) throws Exception {
        @SuppressWarnings("unchecked")
        List<String>[] byShard = new List[256];
        for (int i = 0; ; i++) {
            String id = "zk-capacity-run-" + i;
            int shard = Integer.parseInt(sha256Hex(id).substring(0, 2), 16);
            if (byShard[shard] == null) byShard[shard] = new ArrayList<>();
            byShard[shard].add(id);
            if (byShard[shard].size() == count) return byShard[shard];
        }
    }
}
