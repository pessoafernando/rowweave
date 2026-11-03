package io.rowweave.cluster;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.ExecutionEvent;
import io.rowweave.run.InMemoryRunStateStore;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunExecutionEvent;
import io.rowweave.run.RunExecutionEventPage;
import io.rowweave.run.RunExecutionEventPageCodec;
import io.rowweave.run.RunHistoryRetiringException;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.RunStateStoreProvider;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MasterRunExecutionEventQueryTest {

    @Test
    void emptyAuthorityPageIsRetriedWhenRunAppearsBeforeExistenceCheck() {
        QueryStore store = new QueryStore();
        store.createOnFirstGet = RunCommand.createRun(
                "create-race", "run-race", "definition", "artifact", 1, 0L, 1L);
        try (MasterServer master = new MasterServer(
                60_000L, 0, new QueryRegistry(store))) {
            RunExecutionEventPage result = page(master.handler().apply(
                    RpcMessage.queryRunEvents("run-race", 0L, 2)));

            assertEquals(RunExecutionEventPage.State.FOUND, result.state());
            assertEquals(1, result.events().size());
            assertEquals(1L, result.events().get(0).sequence());
            assertEquals(2, store.eventReads);
        }
    }

    @Test
    void inconsistentEmptyAuthorityPageFailsAfterBoundedReads() {
        QueryStore store = new QueryStore();
        store.apply(RunCommand.createRun(
                "create-stuck", "run-stuck", "definition", "artifact", 1, 0L, 1L));
        store.suppressEvents = true;
        try (MasterServer master = new MasterServer(
                60_000L, 0, new QueryRegistry(store))) {
            RpcMessage result = master.handler().apply(
                    RpcMessage.queryRunEvents("run-stuck", 0L, 2));

            assertFalse(result.success());
            assertTrue(result.errorMessage().contains("unavailable"));
            assertEquals(16, store.eventReads);
        }
    }

    @Test
    void pagesAfterExclusiveWithExactHasMoreAndDistinctEmptyOrMissing() {
        QueryStore store = new QueryStore();
        createThreeEvents(store, "run-events");
        try (MasterServer master = new MasterServer(
                60_000L, 0, new QueryRegistry(store))) {
            RunExecutionEventPage first = page(master.handler().apply(
                    RpcMessage.queryRunEvents("run-events", 0L, 2)));
            assertEquals(RunExecutionEventPage.State.FOUND, first.state());
            assertEquals(2, first.events().size());
            assertEquals(1L, first.events().get(0).sequence());
            assertEquals(2L, first.nextAfterSequence());
            assertTrue(first.hasMore());
            assertEquals(3, store.lastLimit);

            RpcMessage secondResponse = master.handler().apply(
                    RpcMessage.queryRunEvents("run-events", 2L, 2));
            RunExecutionEventPage second = page(secondResponse);
            assertEquals(1, second.events().size());
            assertEquals(3L, second.events().get(0).sequence());
            assertEquals(RunExecutionEvent.DiagnosticCategory.OTHER,
                    second.events().get(0).diagnosticCategory());
            assertFalse(second.hasMore());

            RpcMessage emptyResponse = master.handler().apply(
                    RpcMessage.queryRunEvents("run-events", 3L, 2));
            RunExecutionEventPage empty = page(emptyResponse);
            assertEquals(RunExecutionEventPage.State.FOUND, empty.state());
            assertTrue(empty.events().isEmpty());
            assertEquals(3L, empty.nextAfterSequence());

            RunExecutionEventPage missing = page(master.handler().apply(
                    RpcMessage.queryRunEvents("missing-run", 7L, 2)));
            assertEquals(RunExecutionEventPage.State.NOT_FOUND, missing.state());
            assertEquals(7L, missing.afterSequence());
            assertEquals(0, store.workerReads);

            String wire = new String(
                    secondResponse.runEventPageBytes(), StandardCharsets.ISO_8859_1);
            assertFalse(wire.contains("jdbc"));
            assertFalse(wire.contains("password"));
            assertFalse(wire.contains("attempt-secret"));
        }
    }

    @Test
    void retiringIsAStateWhileAuthorityFailureRemainsFailureAndRedactsCause() {
        QueryStore store = new QueryStore();
        store.retiringRun = "retiring";
        store.failedRun = "failed";
        try (MasterServer master = new MasterServer(
                60_000L, 0, new QueryRegistry(store))) {
            RpcMessage retiringResponse = master.handler().apply(
                    RpcMessage.queryRunEvents("retiring", 5L, 1));
            assertTrue(retiringResponse.success());
            assertEquals(RunExecutionEventPage.State.RETIRING,
                    page(retiringResponse).state());

            RpcMessage failed = master.handler().apply(
                    RpcMessage.queryRunEvents("failed", 0L, 1));
            assertFalse(failed.success());
            assertNull(failed.runEventPageBytes());
            assertTrue(failed.errorMessage().contains("unavailable"));
            assertFalse(failed.errorMessage().contains("jdbc"));
            assertFalse(failed.errorMessage().contains("secret"));
        }
    }

    private static RunExecutionEventPage page(RpcMessage response) {
        assertEquals(RpcMessage.Type.RUN_EVENTS_PAGE, response.type());
        assertTrue(response.success(), response.errorMessage());
        return RunExecutionEventPageCodec.decode(response.runEventPageBytes());
    }

    private static void createThreeEvents(RunStateStore store, String runId) {
        RunSnapshot run = store.apply(RunCommand.createRun(
                "create", runId, "definition", "artifact", 1, 0L, 1L)).snapshot();
        run = store.apply(RunCommand.createAttempt(
                "attempt-secret", runId, run.revision(), "attempt-secret",
                "publication", 2L)).snapshot();
        store.apply(RunCommand.advanceAttempt(
                "advance", runId, run.revision(), "attempt-secret", 1L,
                AttemptStatus.DISPATCHING, false, 3L,
                "PASSWORD_SECRET", "jdbc:password=secret"));
    }

    private static final class QueryStore implements RunStateStore {
        private final InMemoryRunStateStore delegate = new InMemoryRunStateStore();
        private String retiringRun;
        private String failedRun;
        private int lastLimit;
        private int workerReads;
        private int eventReads;
        private RunCommand createOnFirstGet;
        private boolean suppressEvents;
        private final AtomicBoolean createOnce = new AtomicBoolean(true);

        @Override public RunDecision apply(RunCommand command) { return delegate.apply(command); }
        @Override public RunSnapshot get(String runId) {
            if (createOnFirstGet != null && createOnce.compareAndSet(true, false)) {
                RunDecision created = delegate.apply(createOnFirstGet);
                if (created.code() != RunDecision.Code.ACCEPTED) {
                    throw new IllegalStateException("test CREATE_RUN was not accepted");
                }
            }
            return delegate.get(runId);
        }
        @Override public List<ExecutionEvent> events(String runId, long afterSequence, int limit) {
            lastLimit = limit;
            eventReads++;
            if (runId.equals(retiringRun)) throw new RunHistoryRetiringException(runId);
            if (runId.equals(failedRun)) {
                throw new IllegalStateException("jdbc:password=secret");
            }
            if (suppressEvents) return java.util.Collections.emptyList();
            return delegate.events(runId, afterSequence, limit);
        }
    }

    private static final class QueryRegistry implements ServiceRegistry, RunStateStoreProvider {
        private final InProcessRegistry delegate = new InProcessRegistry();
        private final QueryStore store;

        private QueryRegistry(QueryStore store) { this.store = store; }
        @Override public RunStateStore runStateStore() { return store; }
        @Override public void register(RegistryWorker worker) { delegate.register(worker); }
        @Override public boolean tryClaimExclusive(RegistryWorker worker) {
            return delegate.tryClaimExclusive(worker);
        }
        @Override public void report(RegistryWorker worker) { delegate.report(worker); }
        @Override public void renew(String workerId) { delegate.renew(workerId); }
        @Override public void deregister(String workerId) { delegate.deregister(workerId); }
        @Override public List<RegistryWorker> liveWorkers() {
            store.workerReads++;
            return delegate.liveWorkers();
        }
        @Override public RegistryWorker worker(String workerId) { return delegate.worker(workerId); }
        @Override public void watch(WatchListener listener) { delegate.watch(listener); }
    }
}

