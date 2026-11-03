package io.rowweave.cluster;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.run.ExecutionEvent;
import io.rowweave.run.InMemoryRunStateStore;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.RunStateStoreProvider;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MasterRunCancellationTest {

    @Test
    void activeLeaderReturnsAuthorityResultAndStableReplay() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        try (MasterServer master = new MasterServer(
                60_000L, 0, new QueryRegistry(store))) {
            store.apply(RunCommand.createRun(
                    "create", "run", "definition", "artifact", 1, 0L, 1L));

            RpcMessage accepted = master.handler().apply(
                    RpcMessage.cancelRun("run", "request"));
            assertResult(accepted, RunCancellationResult.Disposition.ACCEPTED, 2L);
            RpcMessage replay = master.handler().apply(
                    RpcMessage.cancelRun("run", "request"));
            assertResult(replay,
                    RunCancellationResult.Disposition.ALREADY_REQUESTED, 2L);
            assertEquals(1L, store.events("run", 0L, 10).stream()
                    .filter(event -> event.type() == ExecutionEvent.Type.CANCEL_REQUESTED)
                    .count());

            RpcMessage missing = master.handler().apply(
                    RpcMessage.cancelRun("missing", "request"));
            assertResult(missing, RunCancellationResult.Disposition.NOT_FOUND, 0L);
        }
    }

    @Test
    void missingCapabilityAndAuthorityFailureRemainRedactedFailures() {
        RunStateStore missingCapability = new RunStateStore() {
            @Override public io.rowweave.run.RunDecision apply(RunCommand command) {
                throw new UnsupportedOperationException();
            }
            @Override public io.rowweave.run.RunSnapshot get(String runId) { return null; }
            @Override public List<ExecutionEvent> events(
                    String runId, long afterSequence, int limit) {
                return java.util.Collections.emptyList();
            }
        };
        try (MasterServer master = new MasterServer(
                60_000L, 0, new QueryRegistry(missingCapability))) {
            RpcMessage response = master.handler().apply(
                    RpcMessage.cancelRun("run", "request"));
            assertFalse(response.success());
            assertTrue(response.errorMessage().contains("unavailable"));
        }
    }

    private static void assertResult(RpcMessage message,
                                     RunCancellationResult.Disposition disposition,
                                     long revision) {
        assertEquals(RpcMessage.Type.RUN_CANCELLATION_RESULT, message.type());
        assertTrue(message.success(), message.errorMessage());
        assertEquals(disposition.name(), message.runCancellationDisposition());
        assertEquals(revision, message.runCancellationRevision());
    }

    private static final class QueryRegistry
            implements ServiceRegistry, RunStateStoreProvider {
        private final InProcessRegistry delegate = new InProcessRegistry();
        private final RunStateStore store;

        private QueryRegistry(RunStateStore store) { this.store = store; }
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

