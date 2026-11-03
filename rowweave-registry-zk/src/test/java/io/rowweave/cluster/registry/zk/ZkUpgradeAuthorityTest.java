package io.rowweave.cluster.registry.zk;

import io.rowweave.upgrade.NamespaceVerification;
import io.rowweave.upgrade.StoreFormatHeader;
import io.rowweave.upgrade.UpgradeAuthorityRecord;
import io.rowweave.upgrade.UpgradeCommand;
import io.rowweave.upgrade.UpgradeOwner;
import io.rowweave.upgrade.UpgradeObjectEvidence;
import io.rowweave.upgrade.UpgradePhase;
import io.rowweave.upgrade.UpgradeTransition;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ZkUpgradeAuthorityTest {
    @Test
    void shadowCopyAndCutoverReconcileAckLossAcrossSessions() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connected(server);
             CuratorFramework secondClient = connected(server)) {
            String root = "/rowweave/test-upgrade";
            firstClient.create().creatingParentsIfNeeded().forPath(
                    root + "/namespaces/legacy/shard-0/run",
                    "run-v1".getBytes(StandardCharsets.UTF_8));
            firstClient.create().forPath(root + "/namespaces/legacy/frontier",
                    "frontier-v1".getBytes(StandardCharsets.UTF_8));
            firstClient.create().forPath(root + "/namespaces/legacy/empty",
                    new byte[0]);
            UpgradeOwner owner = new UpgradeOwner(3L, "master-a", "inc-a", 5L);
            ZkUpgradeAuthority first = new ZkUpgradeAuthority(firstClient, root);
            UpgradeAuthorityRecord idle = first.bootstrap(
                    new StoreFormatHeader(1, "store-a", 1, 0, digest('a')),
                    new StoreFormatHeader(1, "store-a", 1, 2, digest('b')),
                    8L, "legacy", "shadow-8", owner, 5_000L);
            UpgradeAuthorityRecord frozen = accepted(first.apply(command(
                    idle, UpgradeCommand.Kind.FREEZE_ADMISSION,
                    "freeze", 0L, 0, 0), 1L, 1L, 1_000L));
            UpgradeAuthorityRecord quiesced = accepted(first.apply(command(
                    frozen, UpgradeCommand.Kind.QUIESCE, "q", 0L, 0, 0),
                    0L, 0L, 1_000L));
            UpgradeCommand copy = command(quiesced,
                    UpgradeCommand.Kind.BEGIN_COPY, "copy", 0L, 0, 0);
            UpgradeAuthorityRecord copying = accepted(first.apply(
                    copy, 0L, 0L, 1_000L));
            NamespaceVerification evidence = first.copyAndVerifyShadow(copy);
            UpgradeAuthorityRecord verified = accepted(first.apply(command(
                    copying, UpgradeCommand.Kind.VERIFY, "verify", 0L,
                    evidence.objectCount(), evidence.shardCount()),
                    0L, 0L, 1_000L));

            AtomicBoolean ack = new AtomicBoolean(true);
            ZkUpgradeAuthority uncertain = new ZkUpgradeAuthority(firstClient,
                    root, () -> {
                        if (ack.getAndSet(false)) {
                            throw new KeeperException.ConnectionLossException();
                        }
                    });
            UpgradeCommand cutover = command(verified,
                    UpgradeCommand.Kind.CUTOVER, "cut", 0L,
                    evidence.objectCount(), evidence.shardCount());
            assertEquals(UpgradeTransition.Code.REPLAYED,
                    uncertain.apply(cutover, 0L, 0L, 1_000L).code());

            UpgradeAuthorityRecord observed =
                    new ZkUpgradeAuthority(secondClient, root).read();
            assertEquals(UpgradePhase.CUTOVER, observed.phase());
            assertEquals("shadow-8", observed.activeNamespace());
            assertEquals(1L, observed.activePointerVersion());
            assertEquals(2L, observed.writerEpoch());
            assertFalse(observed.writesAllowed());
            assertEquals("run-v1", new String(secondClient.getData().forPath(
                    root + "/namespaces/shadow-8/shard-0/run"),
                    StandardCharsets.UTF_8));
            assertEquals(0, secondClient.getData().forPath(
                    root + "/namespaces/shadow-8/empty").length);
            UpgradeObjectEvidence runEvidence = first.copyEvidence(
                    copy, "shard-0/run");
            assertEquals("shard-0/run", runEvidence.relativePath());
            assertEquals(copy.commandId(), runEvidence.commandId());
            assertEquals(copy.commandDigest(), runEvidence.commandDigest());
            assertEquals(sha256("run-v1"), runEvidence.sourceDigest());
            assertEquals(sha256("run-v1"), runEvidence.targetDigest());
            assertEquals(evidence.objectCount(), evidence.evidenceCount());
        }
    }

    @Test
    void staleInstanceCannotWriteAcrossUpgradeRootAba() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connected(server)) {
            String root = "/rowweave/test-upgrade-aba";
            client.create().creatingParentsIfNeeded().forPath(
                    root + "/namespaces/legacy/value", new byte[]{1});
            UpgradeOwner oldOwner = new UpgradeOwner(
                    3L, "master-a", "inc-a", 5L);
            ZkUpgradeAuthority stale = new ZkUpgradeAuthority(client, root);
            UpgradeAuthorityRecord old = stale.bootstrap(
                    new StoreFormatHeader(1, "store-a", 1, 0, digest('a')),
                    new StoreFormatHeader(1, "store-a", 1, 2, digest('b')),
                    8L, "legacy", "shadow-8", oldOwner, 5_000L);
            UpgradeCommand staleCommand = command(old,
                    UpgradeCommand.Kind.QUIESCE, "stale-q", 0L, 0L, 0);

            client.delete().deletingChildrenIfNeeded().forPath(root);
            client.create().creatingParentsIfNeeded().forPath(
                    root + "/namespaces/legacy/value", new byte[]{2});
            UpgradeOwner newOwner = new UpgradeOwner(
                    4L, "master-b", "inc-b", 6L);
            ZkUpgradeAuthority replacement = new ZkUpgradeAuthority(client, root);
            UpgradeAuthorityRecord current = replacement.bootstrap(
                    new StoreFormatHeader(1, "store-a", 1, 0, digest('a')),
                    new StoreFormatHeader(1, "store-a", 1, 2, digest('b')),
                    9L, "legacy", "shadow-9", newOwner, 6_000L);

            assertThrows(IllegalStateException.class, () -> stale.apply(
                    staleCommand, 0L, 0L, 1_000L));
            assertEquals(UpgradePhase.IDLE, replacement.read().phase());
            assertEquals(current.generation(), replacement.read().generation());
        }
    }

    private static UpgradeAuthorityRecord accepted(UpgradeTransition result) {
        assertEquals(UpgradeTransition.Code.ACCEPTED, result.code());
        return result.record();
    }

    private static UpgradeCommand command(
            UpgradeAuthorityRecord record, UpgradeCommand.Kind kind, String id,
            long pointer, long objects, int shards) {
        return new UpgradeCommand(kind, id, "release-2",
                record.activeHeader().storeId(), record.generation(),
                record.sourceNamespace(), record.sourceDigest(),
                record.shadowNamespace(), record.shadowDigest(), record.owner(),
                pointer, objects, shards, record.sourceDigest(),
                record.shadowDigest(), 5_000L);
    }

    private static CuratorFramework connected(TestingServer server)
            throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), new ExponentialBackoffRetry(25, 3));
        client.start();
        if (!client.blockUntilConnected(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Curator did not connect");
        }
        return client;
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }

    private static String sha256(String value) throws Exception {
        byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(StandardCharsets.UTF_8));
        StringBuilder encoded = new StringBuilder(64);
        for (byte item : digest) encoded.append(String.format("%02x", item & 0xff));
        return encoded.toString();
    }
}
