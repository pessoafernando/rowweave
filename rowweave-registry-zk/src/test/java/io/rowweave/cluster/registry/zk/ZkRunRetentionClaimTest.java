package io.rowweave.cluster.registry.zk;

import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RecoverableRunRef;
import io.rowweave.run.RunLifecycleCatalogKernel;
import io.rowweave.run.RunLifecycleRef;
import io.rowweave.run.RunRecoveryCatalogKernel;
import io.rowweave.run.RunRetentionClaim;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStatus;
import io.rowweave.run.RetentionOwner;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZkRunRetentionClaimTest {

    @Test
    void terminalSnapshotWithARecoveryCatalogEntryFailsClosed() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            RunSnapshot terminal = terminal(store, "corrupt");
            String digest = RunRecoveryCatalogKernel.digestHex("corrupt");
            int shard = RunRecoveryCatalogKernel.shard(digest);
            String path = String.format(java.util.Locale.ROOT,
                    "/rowweave/run-recovery-v1/shards/%02x/entries/%s", shard, digest);
            client.create().forPath(path, RunRecoveryCatalogKernel.encodeRef(
                    new RecoverableRunRef("corrupt", terminal.revision(), RunStatus.READY)));

            assertThrows(IllegalStateException.class, () -> store.claim(
                    "corrupt", terminal.revision(), Long.MAX_VALUE,
                    new RetentionOwner("master", 1L), 0L));
        }
    }

    @Test
    void committedClaimWithLostAckReconcilesTheExactToken() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            AtomicBoolean loseAck = new AtomicBoolean(true);
            ZkRunStateStore store = new ZkRunStateStore(
                    client, () -> { }, 100,
                    RunRecoveryCatalogKernel.MAX_ENTRIES_PER_SHARD,
                    RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD,
                    () -> {
                        if (loseAck.getAndSet(false)) {
                            throw new KeeperException.ConnectionLossException();
                        }
                    });
            RunSnapshot terminal = terminal(store, "run");
            RunLifecycleRef ref = store.lifecycle("run");

            RunRetentionClaim claim = store.claim(
                    "run", terminal.revision(), ref.terminalAtMillis(),
                    new RetentionOwner("master", 7L), 0L);

            assertEquals(RunRetentionClaim.Code.REPLAYED, claim.code());
            assertEquals(1L, claim.token().ownershipEpoch());
            assertTrue(store.current(claim.token()));
        }
    }

    private static RunSnapshot terminal(ZkRunStateStore store, String runId) {
        RunDecision created = store.apply(RunCommand.createRun(
                "create", runId, "definition", "artifact", 1, 0L, 1L));
        assertEquals(RunDecision.Code.ACCEPTED, created.code());
        RunDecision canceled = store.apply(RunCommand.cancelReadyRun(
                "cancel", runId, created.snapshot().revision(), 2L,
                "CANCELED", "terminal"));
        assertEquals(RunDecision.Code.ACCEPTED, canceled.code());
        return canceled.snapshot();
    }

    private static CuratorFramework connectedClient(TestingServer server) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), new ExponentialBackoffRetry(10, 3));
        client.start();
        assertTrue(client.blockUntilConnected(5, TimeUnit.SECONDS));
        return client;
    }
}
