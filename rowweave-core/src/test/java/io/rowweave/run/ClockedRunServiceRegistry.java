package io.rowweave.run;

import io.rowweave.artifact.ArtifactReferenceAuthority;
import io.rowweave.artifact.ArtifactReferenceAuthorityProvider;
import io.rowweave.artifact.ArtifactRepository;
import io.rowweave.artifact.ArtifactRepositoryProvider;
import io.rowweave.artifact.InMemoryArtifactRepository;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;

import java.util.List;
import java.util.function.LongSupplier;

/** Test-scope registry fixture with an injectable Run authority clock. */
public final class ClockedRunServiceRegistry implements ServiceRegistry,
        RunStateStoreProvider, RecoverableRunCatalogProvider,
        RunLifecycleCatalogProvider, RunRetentionAuthorityProvider,
        RunRecoveryOwnershipProvider, DurableStageAssignmentAuthorityProvider,
        ArtifactRepositoryProvider, ArtifactReferenceAuthorityProvider {
    private final ServiceRegistry workers;
    private final InMemoryRunStateStore runs;
    private final ArtifactRepository artifacts =
            new InMemoryArtifactRepository(1_024, 64L * 1024L * 1024L);

    public ClockedRunServiceRegistry(LongSupplier authorityClock) {
        this(authorityClock, new InProcessRegistry());
    }

    public ClockedRunServiceRegistry(
            LongSupplier authorityClock, ServiceRegistry workerRegistry) {
        if (workerRegistry == null) {
            throw new IllegalArgumentException("workerRegistry is required");
        }
        workers = workerRegistry;
        runs = new InMemoryRunStateStore(
                RunRecoveryCatalogKernel.MAX_ENTRIES_PER_SHARD,
                RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD,
                authorityClock);
    }

    @Override public RunStateStore runStateStore() { return runs; }
    @Override public RecoverableRunCatalog recoverableRunCatalog() { return runs; }
    @Override public RunLifecycleCatalog runLifecycleCatalog() { return runs; }
    @Override public RunRetentionAuthority runRetentionAuthority() { return runs; }
    @Override public RunRecoveryOwnership runRecoveryOwnership() { return runs; }
    @Override public DurableStageAssignmentAuthority durableStageAssignmentAuthority() {
        return runs;
    }
    @Override public ArtifactRepository artifactRepository() { return artifacts; }
    @Override public ArtifactReferenceAuthority artifactReferenceAuthority() { return runs; }
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
}
