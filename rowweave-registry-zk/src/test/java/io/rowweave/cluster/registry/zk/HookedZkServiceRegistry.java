package io.rowweave.cluster.registry.zk;

import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.artifact.ArtifactReferenceAuthority;
import io.rowweave.artifact.ArtifactReferenceAuthorityProvider;
import io.rowweave.commit.ManifestCommitAuthority;
import io.rowweave.commit.ManifestCommitAuthorityProvider;
import io.rowweave.run.DurableStageAssignmentAuthority;
import io.rowweave.run.DurableStageAssignmentAuthorityProvider;
import io.rowweave.run.RecoverableRunCatalog;
import io.rowweave.run.RecoverableRunCatalogProvider;
import io.rowweave.run.RunRecoveryOwnership;
import io.rowweave.run.RunRecoveryOwnershipProvider;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.RunStateStoreProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** Test-only registry whose run authority exposes post-commit crash cut points. */
public final class HookedZkServiceRegistry implements ServiceRegistry,
        ManifestCommitAuthorityProvider, RunStateStoreProvider,
        RecoverableRunCatalogProvider, RunRecoveryOwnershipProvider,
        DurableStageAssignmentAuthorityProvider, ArtifactReferenceAuthorityProvider,
        AutoCloseable {
    private final ZkServiceRegistry workers;
    private final CuratorFramework authorityClient;
    private final ZkRunStateStore authority;

    public HookedZkServiceRegistry(String connectString,
                                   Runnable afterAssignmentPublish,
                                   Runnable afterAcceptanceCommit) {
        this(connectString, afterAssignmentPublish,
                afterAcceptanceCommit, false);
    }

    public HookedZkServiceRegistry(
            String connectString, Runnable afterAssignmentPublish,
            Runnable afterAcceptanceCommit,
            boolean loseFirstQualityDecisionAck) {
        this(connectString, afterAssignmentPublish, afterAcceptanceCommit,
                loseFirstQualityDecisionAck, false);
    }

    public HookedZkServiceRegistry(
            String connectString, Runnable afterAssignmentPublish,
            Runnable afterAcceptanceCommit,
            boolean loseFirstQualityDecisionAck,
            boolean useOperationTimeout) {
        workers = new ZkServiceRegistry(connectString);
        authorityClient = CuratorFrameworkFactory.newClient(
                connectString, new ExponentialBackoffRetry(100, 3));
        authorityClient.start();
        try {
            authorityClient.blockUntilConnected();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted connecting test authority", interrupted);
        }
        authority = new ZkRunStateStore(authorityClient,
                afterAssignmentPublish::run, afterAcceptanceCommit::run);
        if (loseFirstQualityDecisionAck) {
            AtomicBoolean first = new AtomicBoolean(true);
            authority.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.QUALITY_DECISION
                        && moment == ZkRunStateStore.GuardedMutationMoment
                        .AFTER_COMMIT_BEFORE_ACK
                        && first.compareAndSet(true, false)) {
                    if (useOperationTimeout) {
                        throw new KeeperException.OperationTimeoutException();
                    }
                    throw new KeeperException.ConnectionLossException();
                }
            });
        }
    }

    @Override public boolean workerRegistrationOwnedByWorker() {
        return workers.workerRegistrationOwnedByWorker();
    }
    @Override public void register(RegistryWorker worker) { workers.register(worker); }
    @Override public boolean tryClaimExclusive(RegistryWorker worker) {
        return workers.tryClaimExclusive(worker);
    }
    @Override public void report(RegistryWorker worker) { workers.report(worker); }
    @Override public void renew(String workerId) { workers.renew(workerId); }
    @Override public void deregister(String workerId) { workers.deregister(workerId); }
    @Override public List<RegistryWorker> liveWorkers() { return workers.liveWorkers(); }
    @Override public RegistryWorker worker(String workerId) { return workers.worker(workerId); }
    @Override public void watch(WatchListener listener) { workers.watch(listener); }
    @Override public ManifestCommitAuthority manifestCommitAuthority() {
        return workers.manifestCommitAuthority();
    }
    @Override public RunStateStore runStateStore() { return authority; }
    @Override public RecoverableRunCatalog recoverableRunCatalog() { return authority; }
    @Override public RunRecoveryOwnership runRecoveryOwnership() { return authority; }
    @Override public DurableStageAssignmentAuthority durableStageAssignmentAuthority() {
        return authority;
    }
    @Override public ArtifactReferenceAuthority artifactReferenceAuthority() {
        return authority;
    }

    @Override
    public void close() {
        authorityClient.close();
        workers.close();
    }
}
