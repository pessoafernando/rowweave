package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InMemoryRunLifecycleCatalogTest {

    @Test
    void createAndEveryAcceptedMutationPublishExactLifecycleState() {
        AtomicLong clock = new AtomicLong(1_000L);
        InMemoryRunStateStore store = new InMemoryRunStateStore(1024, 16, clock::get);
        RunLifecycleCatalog catalog = store;

        RunDecision created = store.apply(RunCommand.createRun(
                "create", "run", "definition", "artifact", 1, 0L, 900L));
        assertEquals(RunDecision.Code.ACCEPTED, created.code());
        assertLifecycle(catalog.lifecycle("run"), 1L, RunStatus.READY,
                RunLifecycleState.ACTIVE, 0L);

        RunDecision attempted = store.apply(RunCommand.createAttempt(
                "attempt", "run", 1L, "attempt-1", "publication-1", 901L));
        assertEquals(RunDecision.Code.ACCEPTED, attempted.code());
        assertLifecycle(catalog.lifecycle("run"), 2L, RunStatus.RUNNING,
                RunLifecycleState.ACTIVE, 0L);

        RunDecision failed = store.apply(RunCommand.advanceAttempt(
                "failed", "run", 2L, "attempt-1", 1L,
                AttemptStatus.FAILED, false, 902L, "FAILED", "failed"));
        assertEquals(RunDecision.Code.ACCEPTED, failed.code());
        assertLifecycle(catalog.lifecycle("run"), 3L, RunStatus.FAILED,
                RunLifecycleState.TERMINAL, 1_000L);

        String terminalCursor = RunLifecycleCatalogKernel.encodeCursor(0L,
                RunLifecycleCatalogKernel.shard(
                        RunLifecycleCatalogKernel.digestHex("run")), null);
        RunLifecyclePage page = catalog.scanLifecycle(terminalCursor, 10);
        assertEquals(1, page.runs().size());
        assertEquals("run", page.runs().get(0).runId());
        assertEquals("artifact", page.runs().get(0).artifactId());

        RunDecision replay = store.apply(RunCommand.advanceAttempt(
                "failed", "run", 2L, "attempt-1", 1L,
                AttemptStatus.FAILED, false, 902L, "FAILED", "failed"));
        assertEquals(RunDecision.Code.ALREADY_APPLIED, replay.code());
        assertEquals(1_000L, catalog.lifecycle("run").terminalAtMillis());
    }

    @Test
    void authorityClockNotBusinessEventTimeControlsTerminalAgeAndNeverMovesBackward() {
        AtomicLong clock = new AtomicLong(2_000L);
        InMemoryRunStateStore store = new InMemoryRunStateStore(1024, 16, clock::get);
        RunLifecycleCatalog catalog = store;

        createAndCancelReady(store, "future-event", Long.MAX_VALUE - 1L);
        assertEquals(2_000L, catalog.lifecycle("future-event").terminalAtMillis());

        clock.set(1_500L);
        createAndCancelReady(store, "clock-rollback", Long.MAX_VALUE - 1L);
        assertEquals(2_000L, catalog.lifecycle("clock-rollback").terminalAtMillis());
    }

    @Test
    void lifecycleCapacityIsReservedAtCreateAndTerminalDoesNotFreeIt() {
        List<String> ids = sameShardIds(3);
        InMemoryRunStateStore store = new InMemoryRunStateStore(
                1024, 2, () -> 1_000L);
        RunLifecycleCatalog catalog = store;

        assertEquals(RunDecision.Code.ACCEPTED,
                create(store, ids.get(0), "artifact-0").code());
        assertEquals(RunDecision.Code.ACCEPTED,
                create(store, ids.get(1), "artifact-1").code());
        assertEquals(RunDecision.Code.ACCEPTED,
                store.apply(RunCommand.cancelReadyRun(
                        "cancel", ids.get(0), 1L, 2L, "CANCELED", "done")).code());

        RunDecision rejected = create(store, ids.get(2), "artifact-2");
        assertEquals(RunDecision.Code.LIMIT_EXCEEDED, rejected.code());
        assertNull(rejected.snapshot());
        assertNull(store.get(ids.get(2)));
        assertNull(catalog.lifecycle(ids.get(2)));
        assertLifecycle(catalog.lifecycle(ids.get(0)), 2L, RunStatus.CANCELED,
                RunLifecycleState.TERMINAL, 1_000L);
    }

    @Test
    void scanReturnsOnlyTerminalRefsAndValidatesPointLookup() {
        InMemoryRunStateStore store = new InMemoryRunStateStore(1024, 16, () -> 5L);
        RunLifecycleCatalog catalog = store;
        create(store, "active", "artifact-active");
        createAndCancelReady(store, "terminal", 2L);

        String cursor = null;
        List<String> found = new ArrayList<>();
        boolean complete = false;
        while (!complete) {
            RunLifecyclePage page = catalog.scanLifecycle(cursor, 1);
            for (RunLifecycleRef ref : page.runs()) found.add(ref.runId());
            cursor = page.nextCursor();
            complete = page.cycleComplete();
        }
        assertEquals(java.util.Collections.singletonList("terminal"), found);
        assertFalse(catalog.lifecycle("active").status().isTerminal());
        assertThrows(IllegalArgumentException.class, () -> catalog.lifecycle(""));
    }

    private static RunDecision create(InMemoryRunStateStore store,
                                      String runId, String artifactId) {
        return store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", artifactId,
                1, 0L, 1L));
    }

    private static void createAndCancelReady(InMemoryRunStateStore store,
                                             String runId, long eventTime) {
        assertEquals(RunDecision.Code.ACCEPTED,
                store.apply(RunCommand.createRun(
                        "create-" + runId, runId, "definition", "artifact-" + runId,
                        1, 0L, eventTime)).code());
        assertEquals(RunDecision.Code.ACCEPTED,
                store.apply(RunCommand.cancelReadyRun(
                        "cancel-" + runId, runId, 1L, eventTime,
                        "CANCELED", "done")).code());
    }

    private static void assertLifecycle(RunLifecycleRef ref, long revision,
                                        RunStatus status, RunLifecycleState state,
                                        long terminalAtMillis) {
        assertEquals(revision, ref.revision());
        assertEquals(status, ref.status());
        assertEquals(state, ref.state());
        assertEquals(terminalAtMillis, ref.terminalAtMillis());
    }

    private static List<String> sameShardIds(int count) {
        List<String> ids = new ArrayList<>();
        int target = -1;
        for (int value = 0; ids.size() < count; value++) {
            String id = "capacity-run-" + value;
            int shard = RunLifecycleCatalogKernel.shard(
                    RunLifecycleCatalogKernel.digestHex(id));
            if (target < 0) target = shard;
            if (shard == target) ids.add(id);
        }
        return ids;
    }
}
