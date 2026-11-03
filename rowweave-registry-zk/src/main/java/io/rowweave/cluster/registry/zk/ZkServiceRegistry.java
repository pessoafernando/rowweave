package io.rowweave.cluster.registry.zk;

import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.RegistryWorkerCodec;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.commit.ManifestCommitAuthority;
import io.rowweave.commit.ManifestCommitAuthorityProvider;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.RunStateStoreProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The ZooKeeper answer to {@link ServiceRegistry}. A worker registers as an ephemeral node
 * {@code /rowweave/workers/{workerId}} whose data is a serialised {@link RegistryWorker}.
 *
 * <ul>
 * <li><b>The lease is the ZK session.</b> An ephemeral node lives exactly as long as the session
 * that created it, so a crashed or disconnected process lets its session time out and ZooKeeper
 * deletes the node, which reaches us as a removal watch. That is what replaces the in-process rule
 * that a worker is alive while its process is. Curator keeps the session alive in the background,
 * so {@link #renew} has nothing to do.</li>
 * <li><b>{@code tryClaimExclusive} is the election primitive.</b> Creating an ephemeral node
 * succeeds only when the node does not exist, which is a compare-and-set that ZooKeeper guarantees
 * to a single owner. When the leader dies its session expires, the role is released, and the next
 * standby to claim wins.</li>
 * <li><b>Watches come from {@link CuratorCache}</b> over the children of the base path, so an
 * addition, a change and a deletion become the three events a caller is shown.</li>
 * </ul>
 *
 * <p>The engine depends only on the {@link ServiceRegistry} interface and Curator belongs to this
 * module alone, so a deployment that never asks for ZooKeeper never loads it.
 */
public final class ZkServiceRegistry implements ServiceRegistry, ManifestCommitAuthorityProvider,
        RunStateStoreProvider, io.rowweave.run.RecoverableRunCatalogProvider,
        io.rowweave.run.RunLifecycleCatalogProvider,
        io.rowweave.run.RunRetentionAuthorityProvider,
        io.rowweave.run.RunRecoveryOwnershipProvider,
        io.rowweave.run.DurableStageAssignmentAuthorityProvider,
        io.rowweave.run.DurableExecutionEvidenceAuthorityProvider,
        io.rowweave.incremental.IncrementalFrontierStoreProvider,
        io.rowweave.incremental.IncrementalAuthorityGatewayProvider,
        io.rowweave.artifact.ArtifactReferenceAuthorityProvider,
        io.rowweave.stream.cluster.StreamProductJobStateStoreProvider,
        io.rowweave.stream.cluster.StreamClusterRunStateStoreProvider,
        io.rowweave.stream.runtime.StreamCheckpointAuthorityProvider,
        io.rowweave.stream.runtime.StreamCheckpointCatalogProvider,
        io.rowweave.upgrade.UpgradeAuthorityStoreProvider, AutoCloseable {

    private static final String BASE_PATH = "/rowweave/workers";
    private static final int ZOOKEEPER_CLOSE_BUDGET_MILLIS = 5_000;
    private static final int DEFAULT_SESSION_TIMEOUT_MILLIS =
            CuratorFrameworkFactory.builder().getSessionTimeoutMs();
    private static final int DEFAULT_CONNECTION_TIMEOUT_MILLIS =
            CuratorFrameworkFactory.builder().getConnectionTimeoutMs();

    private final CloseTrackingZookeeperFactory zookeeperFactory;
    private final CuratorFramework client;
    private final CuratorCache cache;
    private final ManifestCommitAuthority manifestCommitAuthority;
    private final RunStateStore runStateStore;
    private final io.rowweave.incremental.IncrementalAuthorityGateway
            incrementalFrontierStore;
    private final io.rowweave.upgrade.UpgradeAuthorityStore
            upgradeAuthorityStore;
    private final List<WatchListener> listeners = new CopyOnWriteArrayList<>();
    private final boolean requireSecureRecords = "MTLS".equalsIgnoreCase(
            System.getProperty("rowweave.control.securityProfile", "LOOPBACK").trim());

    public ZkServiceRegistry(String connectString) {
        this(connectString, DEFAULT_SESSION_TIMEOUT_MILLIS,
                DEFAULT_CONNECTION_TIMEOUT_MILLIS, null);
    }

    /** Enables store-upgrade admission only for an explicitly selected root. */
    public ZkServiceRegistry(
            String connectString, String upgradeAuthorityRoot) {
        this(connectString, DEFAULT_SESSION_TIMEOUT_MILLIS,
                DEFAULT_CONNECTION_TIMEOUT_MILLIS, upgradeAuthorityRoot);
    }

    ZkServiceRegistry(String connectString, int sessionTimeoutMillis,
                      int connectionTimeoutMillis) {
        this(connectString, sessionTimeoutMillis,
                connectionTimeoutMillis, null);
    }

    private ZkServiceRegistry(
            String connectString, int sessionTimeoutMillis,
            int connectionTimeoutMillis, String upgradeAuthorityRoot) {
        this.zookeeperFactory = new CloseTrackingZookeeperFactory();
        this.client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .sessionTimeoutMs(sessionTimeoutMillis)
                .connectionTimeoutMs(connectionTimeoutMillis)
                .retryPolicy(new ExponentialBackoffRetry(100, 3))
                .waitForShutdownTimeoutMs(ZOOKEEPER_CLOSE_BUDGET_MILLIS)
                .zookeeperFactory(zookeeperFactory)
                .build();
        this.client.start();
        try {
            this.client.blockUntilConnected();
            if (client.checkExists().forPath(BASE_PATH) == null) {
                try {
                    client.create().creatingParentsIfNeeded().forPath(BASE_PATH);
                } catch (org.apache.zookeeper.KeeperException.NodeExistsException ignore) {
                    // raced with another client — fine
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("failed to connect ZK / create base path", e);
        }
        this.cache = CuratorCache.build(client, BASE_PATH);
        ZkUpgradeAuthority configuredUpgrade = upgradeAuthorityRoot == null
                ? null : new ZkUpgradeAuthority(client, upgradeAuthorityRoot);
        CuratorFramework stateClient = configuredUpgrade == null
                ? client : client.usingNamespace(
                        curatorNamespace(configuredUpgrade.activeNamespacePath()));
        this.manifestCommitAuthority = new ZkManifestCommitAuthority(stateClient);
        this.runStateStore = new ZkRunStateStore(stateClient);
        this.incrementalFrontierStore = new ZkIncrementalFrontierStore(
                stateClient, (ZkRunStateStore) this.runStateStore);
        this.upgradeAuthorityStore = configuredUpgrade;
        this.cache.listenable().addListener(CuratorCacheListener.builder()
                .forCreates(node -> fireIfWorker(node, EventType.ADDED))
                .forChanges((oldNode, node) -> fireIfWorker(node, EventType.UPDATED))
                .forDeletes(node -> fireIfWorker(node, EventType.REMOVED))
                .build());
        this.cache.start();
    }

    @Override
    public ManifestCommitAuthority manifestCommitAuthority() {
        return manifestCommitAuthority;
    }

    @Override
    public RunStateStore runStateStore() {
        return runStateStore;
    }

    @Override
    public io.rowweave.incremental.IncrementalFrontierStore
    incrementalFrontierStore() {
        return incrementalFrontierStore;
    }

    @Override
    public io.rowweave.incremental.IncrementalAuthorityGateway
    incrementalAuthorityGateway() {
        return incrementalFrontierStore;
    }

    @Override
    public io.rowweave.upgrade.UpgradeAuthorityStore upgradeAuthorityStore() {
        return upgradeAuthorityStore;
    }

    @Override
    public io.rowweave.stream.cluster.StreamProductJobStateStore
    streamProductJobStateStore(
            io.rowweave.artifact.ArtifactRepository artifactRepository) {
        if (artifactRepository == null) {
            throw new IllegalArgumentException(
                    "shared Stream Artifact repository is required");
        }
        return new io.rowweave.stream.cluster.ZkStreamProductJobStateStore(
                client, "/rowweave/stream/product-jobs", artifactRepository);
    }

    @Override
    public io.rowweave.stream.cluster.StreamClusterRunStateStore
    streamClusterRunStateStore() {
        return new io.rowweave.stream.cluster.ZkStreamClusterRunStateStore(
                client, "/rowweave/stream/run-authority");
    }

    @Override
    public io.rowweave.stream.runtime.StreamCheckpointAuthority
    streamCheckpointAuthority() {
        return new io.rowweave.stream.runtime.ZkStreamCheckpointAuthority(
                client, "/rowweave/stream/checkpoint-authority");
    }

    @Override
    public io.rowweave.stream.runtime.StreamCheckpointCatalog
    streamCheckpointCatalog() {
        return new io.rowweave.stream.runtime.ZkStreamCheckpointCatalog(
                client, "/rowweave/stream/checkpoint-catalog");
    }

    private static String curatorNamespace(String absolutePath) {
        if (absolutePath == null || absolutePath.length() < 2
                || absolutePath.charAt(0) != '/') {
            throw new IllegalStateException("active ZK namespace path is invalid");
        }
        return absolutePath.substring(1);
    }

    @Override
    public io.rowweave.run.RecoverableRunCatalog recoverableRunCatalog() {
        return (io.rowweave.run.RecoverableRunCatalog) runStateStore;
    }

    @Override
    public io.rowweave.run.RunLifecycleCatalog runLifecycleCatalog() {
        return (io.rowweave.run.RunLifecycleCatalog) runStateStore;
    }

    @Override
    public io.rowweave.run.RunRetentionAuthority runRetentionAuthority() {
        return (io.rowweave.run.RunRetentionAuthority) runStateStore;
    }

    @Override
    public io.rowweave.artifact.ArtifactReferenceAuthority
    artifactReferenceAuthority() {
        return (io.rowweave.artifact.ArtifactReferenceAuthority) runStateStore;
    }

    @Override
    public io.rowweave.run.RunRecoveryOwnership runRecoveryOwnership() {
        return (io.rowweave.run.RunRecoveryOwnership) runStateStore;
    }

    @Override
    public io.rowweave.run.DurableStageAssignmentAuthority durableStageAssignmentAuthority() {
        return (io.rowweave.run.DurableStageAssignmentAuthority) runStateStore;
    }

    @Override
    public io.rowweave.run.DurableExecutionEvidenceAuthority
    durableExecutionEvidenceAuthority() {
        return (io.rowweave.run.DurableExecutionEvidenceAuthority) runStateStore;
    }

    private void fireIfWorker(org.apache.curator.framework.recipes.cache.ChildData node, EventType type) {
        String path = node.getPath();
        if (path == null || path.equals(BASE_PATH) || !path.startsWith(BASE_PATH + "/")) {
            return; // skip the base node itself
        }
        RegistryWorker w;
        byte[] data = node.getData();
        if (data != null && data.length > 0) {
            w = decodeRecord(data);
        } else {
            // A removal can carry no data, so the id comes back from the last path element and host, port and slots stay unknown at zero.
            String id = path.substring((BASE_PATH + "/").length());
            w = new RegistryWorker(id, "", 0, 0);
        }
        WatchEvent ev = new WatchEvent(type, w);
        for (WatchListener l : listeners) {
            l.onEvent(ev);
        }
    }

    private String pathOf(String workerId) {
        return BASE_PATH + "/" + workerId;
    }

    @Override
    public boolean workerRegistrationOwnedByWorker() {
        return true;
    }

    @Override
    public void register(RegistryWorker worker) {
        upsertOwned(worker, "register");
    }

    @Override
    public boolean tryClaimExclusive(RegistryWorker worker) {
        byte[] data = encode(worker);
        try {
            // Creating an ephemeral node succeeds only when it does not exist, which is what
            // gives one owner. The lease is the session, so a dead leader releases the role.
            client.create().withMode(CreateMode.EPHEMERAL).forPath(pathOf(worker.workerId()), data);
            return true;
        } catch (org.apache.zookeeper.KeeperException.NodeExistsException e) {
            return false;  // the node was taken, so this claim loses the race
        } catch (Exception e) {
            throw new RuntimeException("ZK tryClaimExclusive failed for " + worker.workerId(), e);
        }
    }

    @Override
    public void report(RegistryWorker worker) {
        upsertOwned(worker, "report");
    }

    /** Only the session that created an ephemeral node may update it; the same id from another session fails closed. */
    private void upsertOwned(RegistryWorker worker, String operation) {
        byte[] data = encode(worker);
        String path = pathOf(worker.workerId());
        for (int attempt = 0; attempt < 4; attempt++) {
            try {
                Stat existing = client.checkExists().forPath(path);
                if (existing == null) {
                    client.create().withMode(CreateMode.EPHEMERAL).forPath(path, data);
                    return;
                }
                long sessionId = client.getZookeeperClient().getZooKeeper().getSessionId();
                if (existing.getEphemeralOwner() != sessionId) {
                    throw new IllegalStateException("workerId already owned by another ZK session: "
                            + worker.workerId());
                }
                client.setData().withVersion(existing.getVersion()).forPath(path, data);
                return;
            } catch (org.apache.zookeeper.KeeperException.NodeExistsException racedCreate) {
                // Another session won the create, so the next round checks the owner instead of writing over its record.
            } catch (org.apache.zookeeper.KeeperException.NoNodeException racedDelete) {
                // Our own node went away between the check and the write, or the session was rebuilt, so create again.
            } catch (org.apache.zookeeper.KeeperException.BadVersionException racedUpdate) {
                // A report from this session moved the version; re-reading owner and version converges.
            } catch (Exception e) {
                throw new RuntimeException("ZK " + operation + " failed for " + worker.workerId(), e);
            }
        }
        throw new IllegalStateException("ZK " + operation + " did not converge for " + worker.workerId());
    }

    @Override
    public void renew(String workerId) {
        // The lease is the session and Curator keeps it alive, so an explicit renewal is not needed.
    }

    @Override
    public void deregister(String workerId) {
        String path = pathOf(workerId);
        for (int attempt = 0; attempt < 4; attempt++) {
            try {
                Stat existing = client.checkExists().forPath(path);
                if (existing == null) {
                    return;
                }
                long sessionId = client.getZookeeperClient().getZooKeeper().getSessionId();
                if (existing.getEphemeralOwner() != sessionId) {
                    return;  // a late close from an older worker or leader gives up rather than delete the new owner's node
                }
                client.delete().withVersion(existing.getVersion()).forPath(path);  // which is what produces the removal watch
                return;
            } catch (org.apache.zookeeper.KeeperException.NoNodeException ignore) {
                return;  // already gone, so there is nothing to delete
            } catch (org.apache.zookeeper.KeeperException.BadVersionException racedUpdate) {
                // A report from the same owner moved the version; the next round re-reads it before deleting.
            } catch (Exception e) {
                throw new RuntimeException("ZK deregister failed for " + workerId, e);
            }
        }
        throw new IllegalStateException("ZK deregister did not converge for " + workerId);
    }

    @Override
    public List<RegistryWorker> liveWorkers() {
        List<RegistryWorker> out = new ArrayList<>();
        try {
            List<String> ids = client.getChildren().forPath(BASE_PATH);
            for (String id : ids) {
                byte[] data = client.getData().forPath(pathOf(id));
                if (data != null && data.length > 0) {
                    out.add(decodeRecord(data));
                }
            }
        } catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
            return out;  // no base path means no workers
        } catch (Exception e) {
            throw new RuntimeException("ZK liveWorkers failed", e);
        }
        return out;
    }

    @Override
    public RegistryWorker worker(String workerId) {
        try {
            byte[] data = client.getData().forPath(pathOf(workerId));
            return data == null || data.length == 0 ? null : decodeRecord(data);
        } catch (org.apache.zookeeper.KeeperException.NoNodeException missing) {
            return null;
        } catch (Exception e) {
            throw new RuntimeException("ZK worker lookup failed for " + workerId, e);
        }
    }

    @Override
    public void watch(WatchListener listener) {
        listeners.add(listener);
    }

    @Override
    public void close() {
        try {
            cache.close();
        } finally {
            try {
                client.close();
            } finally {
                zookeeperFactory.closeAll();
            }
        }
    }

    /**
     * Curator may replace its ZooKeeper handle after session loss. Keep every
     * handle so shutdown can fence reconnect threads from both the current and
     * replaced sessions; otherwise an old daemon can outlive this registry.
     */
    private static final class CloseTrackingZookeeperFactory
            implements ZookeeperFactory {
        private final List<ZooKeeper> handles = new ArrayList<>();

        @Override
        public synchronized ZooKeeper newZooKeeper(
                String connectString, int sessionTimeout,
                Watcher watcher, boolean canBeReadOnly)
                throws Exception {
            ZooKeeper handle = new ZooKeeper(connectString, sessionTimeout,
                    watcher, canBeReadOnly);
            handles.add(handle);
            return handle;
        }

        private synchronized void closeAll() {
            boolean interrupted = false;
            long deadline = System.nanoTime()
                    + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(
                    ZOOKEEPER_CLOSE_BUDGET_MILLIS);
            for (ZooKeeper handle : handles) {
                try {
                    handle.close(remainingMillis(deadline));
                } catch (InterruptedException failure) {
                    interrupted = true;
                }
            }
            handles.clear();
            if (interrupted) Thread.currentThread().interrupt();
        }

        private static int remainingMillis(long deadline) {
            long remainingNanos = Math.max(0L, deadline - System.nanoTime());
            long roundedUp = java.util.concurrent.TimeUnit.NANOSECONDS
                    .toMillis(remainingNanos + 999_999L);
            return (int) Math.min(Integer.MAX_VALUE, roundedUp);
        }
    }

    // ---- the wire codec for a registry record: four fields plus an optional trailing data port ----

    private byte[] encode(RegistryWorker w) {
        if (requireSecureRecords && w.securityMetadata() == null) {
            throw new SecurityException("MTLS registry record requires deployment/authz identity");
        }
        return RegistryWorkerCodec.encode(w);
    }

    private RegistryWorker decodeRecord(byte[] data) {
        return RegistryWorkerCodec.decode(data, requireSecureRecords);
    }

    /** Kept for binary/reflection compatibility with the pre-secure four-field codec tests. */
    private static RegistryWorker decode(byte[] data) {
        return RegistryWorkerCodec.decode(data, false);
    }
}
