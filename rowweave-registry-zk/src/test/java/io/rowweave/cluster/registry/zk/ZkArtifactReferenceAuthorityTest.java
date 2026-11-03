package io.rowweave.cluster.registry.zk;

import io.rowweave.artifact.ArtifactReferenceAuthority;
import io.rowweave.artifact.ArtifactReferenceAuthorityProvider;
import io.rowweave.run.ArtifactReferenceAuthorityContract;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunLifecycleCatalogKernel;
import io.rowweave.run.RunRecoveryCatalogKernel;
import io.rowweave.run.RunStateStore;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Run, reference count and usage count in ZooKeeper share one {@code multi} linearization point. */
class ZkArtifactReferenceAuthorityTest
        extends ArtifactReferenceAuthorityContract {
    private static final String REFERENCE_ROOT =
            "/rowweave/artifact-references-v1/artifacts/";
    private final List<AutoCloseable> resources = new ArrayList<>();

    @Override
    protected RunStateStore newStore() {
        try {
            TestingServer server = new TestingServer(true);
            CuratorFramework client = connectedClient(server);
            resources.add(client);
            resources.add(server);
            return new ZkRunStateStore(client);
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    @AfterEach
    void closeResources() throws Exception {
        for (int i = resources.size() - 1; i >= 0; i--) {
            resources.get(i).close();
        }
        resources.clear();
    }

    @Test
    void twoSessionsCompeteWithoutLosingSharedReferences() throws Exception {
        TestingServer server = new TestingServer(true);
        CuratorFramework firstClient = connectedClient(server);
        CuratorFramework secondClient = connectedClient(server);
        resources.add(secondClient);
        resources.add(firstClient);
        resources.add(server);
        ZkRunStateStore first = new ZkRunStateStore(firstClient);
        ZkRunStateStore second = new ZkRunStateStore(secondClient);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        resources.add(() -> pool.shutdownNow());
        Future<RunDecision> one = pool.submit(() -> {
            start.await();
            return first.apply(RunCommand.createRun(
                    "create-one", "shared-one", "definition", ARTIFACT_A,
                    1, 0L, 1L));
        });
        Future<RunDecision> two = pool.submit(() -> {
            start.await();
            return second.apply(RunCommand.createRun(
                    "create-two", "shared-two", "definition", ARTIFACT_A,
                    1, 0L, 1L));
        });
        start.countDown();
        assertEquals(RunDecision.Code.ACCEPTED, one.get().code());
        assertEquals(RunDecision.Code.ACCEPTED, two.get().code());
        assertEquals(2L, first.inspect(ARTIFACT_A).referenceCount());
        assertEquals(2L, second.inspect(ARTIFACT_A).referenceCount());
    }

    @Test
    void unknownCreateAckReconcilesWithoutDoubleCounting() throws Exception {
        TestingServer server = new TestingServer(true);
        CuratorFramework client = connectedClient(server);
        resources.add(client);
        resources.add(server);
        AtomicBoolean first = new AtomicBoolean(true);
        ZkRunStateStore store = new ZkRunStateStore(client, () -> {
            if (first.compareAndSet(true, false)) {
                throw new KeeperException.ConnectionLossException();
            }
        });
        RunDecision decision = store.apply(RunCommand.createRun(
                "ack-create", "ack-run", "definition", ARTIFACT_A,
                1, 0L, 1L));
        assertEquals(RunDecision.Code.ALREADY_APPLIED, decision.code());
        assertEquals(1L, store.inspect(ARTIFACT_A).referenceCount());
    }

    @Test
    void corruptCountFailsClosed() throws Exception {
        TestingServer server = new TestingServer(true);
        CuratorFramework client = connectedClient(server);
        resources.add(client);
        resources.add(server);
        ZkRunStateStore store = new ZkRunStateStore(client);
        assertEquals(RunDecision.Code.ACCEPTED,
                store.apply(RunCommand.createRun(
                        "corrupt-create", "corrupt-ref-run", "definition",
                        ARTIFACT_A, 1, 0L, 1L)).code());
        client.setData().forPath(REFERENCE_ROOT + ARTIFACT_A,
                new byte[] { 1, 2, 3 });
        assertThrows(IllegalStateException.class,
                () -> store.inspect(ARTIFACT_A));
    }

    @Test
    void referenceShardCapacityRejectsOnlyNewRun() throws Exception {
        TestingServer server = new TestingServer(true);
        CuratorFramework client = connectedClient(server);
        resources.add(client);
        resources.add(server);
        ZkRunStateStore store = new ZkRunStateStore(client, 1);
        List<String> sameShard = sameReferenceShardRunIds(2);
        assertEquals(RunDecision.Code.ACCEPTED,
                store.apply(RunCommand.createRun(
                        "capacity-one", sameShard.get(0), "definition", ARTIFACT_A,
                        1, 0L, 1L)).code());
        assertEquals(RunDecision.Code.LIMIT_EXCEEDED,
                store.apply(RunCommand.createRun(
                        "capacity-two", sameShard.get(1), "definition", ARTIFACT_A,
                        1, 0L, 1L)).code());
        assertEquals(1L, store.inspect(ARTIFACT_A).referenceCount());
    }

    @Test
    void registryExposesItsRunStoreAsReferenceAuthority() throws Exception {
        TestingServer server = new TestingServer(true);
        resources.add(server);
        ZkServiceRegistry registry = new ZkServiceRegistry(server.getConnectString());
        resources.add(registry);
        Object provider = registry;
        ArtifactReferenceAuthority authority =
                ((ArtifactReferenceAuthorityProvider) provider)
                        .artifactReferenceAuthority();
        assertSame(registry.runStateStore(), authority);
    }

    private static CuratorFramework connectedClient(TestingServer server)
            throws Exception {
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

    private static List<String> sameReferenceShardRunIds(int count) {
        List<String> ids = new ArrayList<>();
        int target = -1;
        for (int value = 0; ids.size() < count; value++) {
            String runId = "zk-artifact-ref-capacity-" + value;
            int shard = RunRecoveryCatalogKernel.shard(
                    RunRecoveryCatalogKernel.digestHex(runId));
            if (target < 0) target = shard;
            if (shard == target) ids.add(runId);
        }
        return ids;
    }
}
