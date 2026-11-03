package io.rowweave.cluster;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.run.FileRunStateStore;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunExecutionEventPage;
import io.rowweave.run.RunLifecycleCatalog;
import io.rowweave.run.RunLifecycleRef;
import io.rowweave.run.RunRetentionAuthority;
import io.rowweave.run.RunRetentionClaim;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.RunStateStoreProvider;
import io.rowweave.run.RetentionOwner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class FileRunExecutionEventQueryTest {

    @Test
    void reopenedFileAuthorityServesPublicPagesWithoutLossOrDuplication(@TempDir Path root) {
        FileRunStateStore initial = new FileRunStateStore(root);
        long revision = initial.apply(RunCommand.createRun(
                "create", "run-file-events", "definition", "artifact", 1, 0L, 1L))
                .snapshot().revision();
        long terminalRevision = initial.apply(RunCommand.cancelReadyRun(
                "cancel", "run-file-events", revision, 2L, "CANCELED", "private-message"))
                .snapshot().revision();

        FileRunStateStore reopened = new FileRunStateStore(root);
        try (MasterServer master = new MasterServer(
                     60_000L, 0, new FileAuthorityRegistry(reopened));
             ClusterJobClient client = new ClusterJobClient(
                     "127.0.0.1", master.port())) {
            RunExecutionEventPage first = client.queryRunEvents(
                    "run-file-events", 0L, 1);
            RunExecutionEventPage second = client.queryRunEvents(
                    "run-file-events", first.nextAfterSequence(), 1);
            assertEquals(1L, first.events().get(0).sequence());
            assertEquals(2L, second.events().get(0).sequence());
            assertFalse(second.hasMore());
            assertEquals(0, client.queryRunEvents(
                    "run-file-events", second.nextAfterSequence(), 1).events().size());

            RunLifecycleRef lifecycle = ((RunLifecycleCatalog) reopened)
                    .lifecycle("run-file-events");
            RunRetentionClaim claim = ((RunRetentionAuthority) reopened).claim(
                    "run-file-events", terminalRevision, lifecycle.terminalAtMillis(),
                    new RetentionOwner("retention-owner", 1L), 0L);
            assertEquals(RunRetentionClaim.Code.ACQUIRED, claim.code());
            assertEquals(RunExecutionEventPage.State.RETIRING,
                    client.queryRunEvents("run-file-events", 0L, 1).state());
        }
    }

    private static final class FileAuthorityRegistry
            implements ServiceRegistry, RunStateStoreProvider {
        private final InProcessRegistry delegate = new InProcessRegistry();
        private final RunStateStore store;

        private FileAuthorityRegistry(RunStateStore store) { this.store = store; }
        @Override public RunStateStore runStateStore() { return store; }
        @Override public void register(RegistryWorker worker) { delegate.register(worker); }
        @Override public boolean tryClaimExclusive(RegistryWorker worker) {
            return delegate.tryClaimExclusive(worker);
        }
        @Override public void report(RegistryWorker worker) { delegate.report(worker); }
        @Override public void renew(String workerId) { delegate.renew(workerId); }
        @Override public void deregister(String workerId) { delegate.deregister(workerId); }
        @Override public List<RegistryWorker> liveWorkers() { return delegate.liveWorkers(); }
        @Override public RegistryWorker worker(String workerId) { return delegate.worker(workerId); }
        @Override public void watch(WatchListener listener) { delegate.watch(listener); }
    }
}

