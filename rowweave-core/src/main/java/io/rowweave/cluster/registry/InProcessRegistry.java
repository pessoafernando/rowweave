package io.rowweave.cluster.registry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Pure-memory implementation of {@link ServiceRegistry}: no external dependency (narrow dependency boundary), used by unit tests, the local pseudo-cluster and single-machine default.
 * Instances are shared within one process, so several MasterServer objects (election) and several WorkerServer objects (registration) all see the same view.
 * This is what makes in-process dual-master failover work: a worker registration is visible to the standby.
 *
 * <p>Exclusive claiming ({@link #tryClaimExclusive}) is atomic through {@link ConcurrentHashMap#putIfAbsent}, so among concurrent claimants exactly one wins.
 * Watch callbacks fan out synchronously (the registering or deregistering thread calls the listener directly); listeners live in a {@link CopyOnWriteArrayList}, so iterating them needs no lock.
 */
public final class InProcessRegistry implements ServiceRegistry,
        io.rowweave.run.RunStateStoreProvider,
        io.rowweave.run.RecoverableRunCatalogProvider,
        io.rowweave.run.RunLifecycleCatalogProvider,
        io.rowweave.run.RunRetentionAuthorityProvider,
        io.rowweave.run.RunRecoveryOwnershipProvider,
        io.rowweave.run.DurableStageAssignmentAuthorityProvider,
        io.rowweave.run.DurableExecutionEvidenceAuthorityProvider,
        io.rowweave.artifact.ArtifactRepositoryProvider,
        io.rowweave.artifact.ArtifactReferenceAuthorityProvider,
        io.rowweave.stream.cluster.StreamProductJobStateStoreProvider,
        io.rowweave.stream.cluster.StreamClusterRunStateStoreProvider,
        io.rowweave.stream.runtime.StreamCheckpointAuthorityProvider,
        io.rowweave.stream.runtime.StreamCheckpointCatalogProvider,
        io.rowweave.incremental.IncrementalAuthorityGatewayProvider,
        io.rowweave.upgrade.UpgradeAuthorityStoreProvider {

    private final ConcurrentHashMap<String, RegistryWorker> workers = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<WatchListener> listeners = new CopyOnWriteArrayList<>();
    private final io.rowweave.run.RunStateStore runStateStore =
            new io.rowweave.run.InMemoryRunStateStore();
    private final io.rowweave.artifact.ArtifactRepository artifactRepository =
            new io.rowweave.artifact.InMemoryArtifactRepository(
                    1_024, 512L * 1024L * 1024L);
    private final io.rowweave.incremental.IncrementalAuthorityGateway
            incrementalAuthorityGateway =
            new io.rowweave.incremental.InMemoryIncrementalAuthorityStore();
    private final io.rowweave.upgrade.UpgradeAuthorityStore
            upgradeAuthorityStore;
    private final io.rowweave.stream.cluster.InMemoryStreamProductJobStateStore
            streamProductJobs =
            new io.rowweave.stream.cluster.InMemoryStreamProductJobStateStore();
    private final io.rowweave.stream.cluster.InMemoryStreamClusterRunStateStore
            streamClusterRuns =
            new io.rowweave.stream.cluster.InMemoryStreamClusterRunStateStore();
    private final io.rowweave.stream.runtime.InMemoryStreamCheckpointAuthority
            streamCheckpoints =
            new io.rowweave.stream.runtime.InMemoryStreamCheckpointAuthority();
    private final io.rowweave.stream.runtime.InMemoryStreamCheckpointCatalog
            streamCheckpointCatalog =
            new io.rowweave.stream.runtime.InMemoryStreamCheckpointCatalog();

    public InProcessRegistry() {
        this(null);
    }

    public InProcessRegistry(
            io.rowweave.upgrade.UpgradeAuthorityStore upgradeAuthorityStore) {
        this.upgradeAuthorityStore = upgradeAuthorityStore;
    }

    @Override
    public io.rowweave.run.RunStateStore runStateStore() {
        return runStateStore;
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

    @Override
    public io.rowweave.artifact.ArtifactRepository artifactRepository() {
        return artifactRepository;
    }

    @Override
    public io.rowweave.artifact.ArtifactReferenceAuthority
    artifactReferenceAuthority() {
        return (io.rowweave.artifact.ArtifactReferenceAuthority) runStateStore;
    }

    @Override
    public io.rowweave.incremental.IncrementalAuthorityGateway
    incrementalAuthorityGateway() {
        return incrementalAuthorityGateway;
    }

    @Override
    public io.rowweave.upgrade.UpgradeAuthorityStore upgradeAuthorityStore() {
        return upgradeAuthorityStore;
    }

    @Override
    public io.rowweave.stream.cluster.StreamProductJobStateStore
    streamProductJobStateStore(
            io.rowweave.artifact.ArtifactRepository ignored) {
        return streamProductJobs;
    }

    @Override
    public io.rowweave.stream.cluster.StreamClusterRunStateStore
    streamClusterRunStateStore() {
        return streamClusterRuns;
    }

    @Override
    public io.rowweave.stream.runtime.StreamCheckpointAuthority
    streamCheckpointAuthority() {
        return streamCheckpoints;
    }

    @Override
    public io.rowweave.stream.runtime.StreamCheckpointCatalog
    streamCheckpointCatalog() {
        return streamCheckpointCatalog;
    }

    @Override
    public void register(RegistryWorker worker) {
        workers.put(worker.workerId(), worker);
        fire(EventType.ADDED, worker);
    }

    @Override
    public boolean tryClaimExclusive(RegistryWorker worker) {
        RegistryWorker prev = workers.putIfAbsent(worker.workerId(), worker); // Atomic CAS: written only when absent.
        if (prev == null) {
            fire(EventType.ADDED, worker);
            return true;
        }
        return false;
    }

    @Override
    public void report(RegistryWorker worker) {
        workers.put(worker.workerId(), worker);
        fire(EventType.UPDATED, worker);
    }

    @Override
    public void renew(String workerId) {
        // Pure memory has no lease expiry (the process being alive is what makes the entry live); renew is a no-op that keeps the keep-alive semantics. Only the ZK implementation refreshes a real lease.
    }

    @Override
    public void deregister(String workerId) {
        RegistryWorker removed = workers.remove(workerId);
        if (removed != null) {
            fire(EventType.REMOVED, removed);
        }
    }

    @Override
    public List<RegistryWorker> liveWorkers() {
        return new ArrayList<>(workers.values());
    }

    @Override
    public RegistryWorker worker(String workerId) {
        return workers.get(workerId);
    }

    @Override
    public void watch(WatchListener listener) {
        listeners.add(listener);
    }

    private void fire(EventType type, RegistryWorker worker) {
        WatchEvent event = new WatchEvent(type, worker);
        for (WatchListener l : listeners) {
            l.onEvent(event);
        }
    }
}
