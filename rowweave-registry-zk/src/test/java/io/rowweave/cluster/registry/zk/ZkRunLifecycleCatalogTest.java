package io.rowweave.cluster.registry.zk;

import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunLifecycleCatalog;
import io.rowweave.run.RunLifecycleCatalogKernel;
import io.rowweave.run.RunLifecycleCatalogProvider;
import io.rowweave.run.RunLifecyclePage;
import io.rowweave.run.RunLifecycleRef;
import io.rowweave.run.RunLifecycleState;
import io.rowweave.run.RunRecoveryCatalogKernel;
import io.rowweave.run.RunRetentionAuthorityProvider;
import io.rowweave.run.RunStatus;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZkRunLifecycleCatalogTest {

    @Test
    void scanOfAnUninitializedShardReturnsABoundedEmptyPage() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);

            RunLifecyclePage page = store.scanLifecycle(null, 10);

            assertTrue(page.runs().isEmpty());
            assertTrue(!page.cycleComplete());
        }
    }

    @Test
    void createAndTerminalMutationPublishLifecycleInTheAuthorityTransaction() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            assertEquals(RunDecision.Code.ACCEPTED,
                    create(store, "run", Long.MAX_VALUE - 1L).code());
            RunLifecycleRef active = store.lifecycle("run");
            assertEquals(RunLifecycleState.ACTIVE, active.state());
            assertEquals(1L, active.revision());
            assertEquals(0L, active.terminalAtMillis());

            assertEquals(RunDecision.Code.ACCEPTED,
                    store.apply(RunCommand.cancelReadyRun(
                            "cancel", "run", 1L, Long.MAX_VALUE - 1L,
                            "CANCELED", "done")).code());
            Stat stat = client.checkExists().forPath(lifecyclePath("run"));
            RunLifecycleRef terminal = store.lifecycle("run");
            assertEquals(RunLifecycleState.TERMINAL, terminal.state());
            assertEquals(RunStatus.CANCELED, terminal.status());
            assertEquals(2L, terminal.revision());
            assertEquals(stat.getMtime(), terminal.terminalAtMillis());

            String cursor = RunLifecycleCatalogKernel.encodeCursor(0L,
                    RunLifecycleCatalogKernel.shard(
                            RunLifecycleCatalogKernel.digestHex("run")), null);
            RunLifecyclePage page = store.scanLifecycle(cursor, 10);
            assertEquals(1, page.runs().size());
            assertEquals("run", page.runs().get(0).runId());
        }
    }

    @Test
    void lifecycleCapacityIsSerializedAcrossSessionsAndTerminalDoesNotReleaseIt()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connectedClient(server);
             CuratorFramework secondClient = connectedClient(server)) {
            ZkRunStateStore first = store(firstClient, 1);
            ZkRunStateStore second = store(secondClient, 1);
            List<String> ids = sameShardIds(3);
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(2);
            List<RunDecision> decisions = new ArrayList<>();
            Object monitor = new Object();
            pool.submit(() -> {
                start.await();
                RunDecision decision = create(first, ids.get(0), 1L);
                synchronized (monitor) { decisions.add(decision); }
                return null;
            });
            pool.submit(() -> {
                start.await();
                RunDecision decision = create(second, ids.get(1), 1L);
                synchronized (monitor) { decisions.add(decision); }
                return null;
            });
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
            assertEquals(1L, decisions.stream().filter(RunDecision::accepted).count());
            assertEquals(1L, decisions.stream()
                    .filter(value -> value.code() == RunDecision.Code.LIMIT_EXCEEDED).count());

            String accepted = first.get(ids.get(0)) == null ? ids.get(1) : ids.get(0);
            assertEquals(RunDecision.Code.ACCEPTED,
                    first.apply(RunCommand.cancelReadyRun(
                            "cancel-accepted", accepted, 1L, 2L,
                            "CANCELED", "done")).code());
            assertEquals(RunDecision.Code.LIMIT_EXCEEDED,
                    create(first, ids.get(2), 1L).code());
            assertNull(first.get(ids.get(2)));
        }
    }

    @Test
    void missingOrCorruptLifecycleIsAuthorityCorruption() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            create(store, "missing", 1L);
            client.delete().forPath(lifecyclePath("missing"));
            assertThrows(IllegalStateException.class, () -> store.get("missing"));

            create(store, "corrupt", 1L);
            String path = lifecyclePath("corrupt");
            byte[] bytes = client.getData().forPath(path);
            bytes[8] ^= 1;
            client.setData().forPath(path, bytes);
            assertThrows(IllegalArgumentException.class, () -> store.get("corrupt"));
        }
    }

    @Test
    void registryExposesTheSameLifecycleAuthorityInstance() throws Exception {
        try (TestingServer server = new TestingServer(true);
             ZkServiceRegistry registry = new ZkServiceRegistry(server.getConnectString())) {
            assertTrue(registry instanceof RunLifecycleCatalogProvider);
            assertSame(((io.rowweave.run.RunStateStoreProvider) registry).runStateStore(),
                    ((RunLifecycleCatalogProvider) registry).runLifecycleCatalog());
            assertTrue(registry instanceof RunRetentionAuthorityProvider);
            assertSame(((io.rowweave.run.RunStateStoreProvider) registry).runStateStore(),
                    ((RunRetentionAuthorityProvider) registry).runRetentionAuthority());
        }
    }

    private static ZkRunStateStore store(CuratorFramework client, int lifecycleCapacity) {
        return new ZkRunStateStore(client, () -> { }, 100,
                RunRecoveryCatalogKernel.MAX_ENTRIES_PER_SHARD, lifecycleCapacity);
    }

    private static RunDecision create(ZkRunStateStore store, String runId, long eventTime) {
        return store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact-" + runId,
                1, 0L, eventTime));
    }

    private static String lifecyclePath(String runId) {
        String digest = RunLifecycleCatalogKernel.digestHex(runId);
        int shard = RunLifecycleCatalogKernel.shard(digest);
        return String.format(java.util.Locale.ROOT,
                "/rowweave/run-lifecycle-v1/shards/%02x/entries/%s", shard, digest);
    }

    private static List<String> sameShardIds(int count) {
        List<String> ids = new ArrayList<>();
        int target = -1;
        for (int value = 0; ids.size() < count; value++) {
            String id = "zk-lifecycle-run-" + value;
            int shard = RunLifecycleCatalogKernel.shard(
                    RunLifecycleCatalogKernel.digestHex(id));
            if (target < 0) target = shard;
            if (shard == target) ids.add(id);
        }
        return ids;
    }

    private static CuratorFramework connectedClient(TestingServer server) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), new ExponentialBackoffRetry(10, 3));
        client.start();
        assertTrue(client.blockUntilConnected(5, TimeUnit.SECONDS));
        return client;
    }
}
