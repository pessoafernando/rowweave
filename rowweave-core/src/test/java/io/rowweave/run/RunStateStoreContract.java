package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Black-box contract that every RunStateStore backend must pass. */
public abstract class RunStateStoreContract {

    protected abstract RunStateStore newStore();

    @Test
    void identityLimitsAndEventPagingAreBackendIndependent() {
        RunStateStore store = newStore();
        assertNull(store.get("missing"));
        assertEquals(0, store.events("missing", 0L, 1).size());
        assertThrows(IllegalArgumentException.class, () -> store.events("run", -1L, 1));
        assertThrows(IllegalArgumentException.class, () -> store.events("run", 0L, 0));
        assertThrows(IllegalArgumentException.class, () -> store.events("run", 0L, 1001));

        RunSnapshot run = accepted(store, RunCommand.createRun(
                "create", "run", "definition", "artifact", 2, 0L, 1L));
        run = accepted(store, RunCommand.createAttempt(
                "attempt", "run", run.revision(), "attempt-1", "publication-1", 2L));

        List<ExecutionEvent> first = store.events("run", 0L, 1);
        List<ExecutionEvent> second = store.events("run", first.get(0).sequence(), 1);
        assertEquals(1, first.size());
        assertEquals(1L, first.get(0).sequence());
        assertEquals(1, second.size());
        assertEquals(2L, second.get(0).sequence());
        assertEquals(0, store.events("run", 2L, 1).size());
        assertThrows(UnsupportedOperationException.class, () -> first.add(second.get(0)));
    }

    @Test
    void commandReplayConflictAndRevisionOrderingAreBackendIndependent() {
        RunStateStore store = newStore();
        RunCommand create = RunCommand.createRun(
                "create", "run", "definition", "artifact", 2, 0L, 1L);
        RunDecision accepted = store.apply(create);
        assertEquals(RunDecision.Code.ACCEPTED, accepted.code());

        RunDecision replay = store.apply(create);
        assertEquals(RunDecision.Code.ALREADY_APPLIED, replay.code());
        assertNotNull(replay.event());
        assertEquals(accepted.event().sequence(), replay.event().sequence());
        assertEquals(1L, store.get("run").revision());
        assertEquals(1, store.events("run", 0L, 100).size());

        RunCommand conflicting = RunCommand.createRun(
                "create", "run", "different-definition", "artifact", 2, 0L, 1L);
        assertEquals(RunDecision.Code.COMMAND_ID_CONFLICT, store.apply(conflicting).code());

        RunCommand attempt = RunCommand.createAttempt(
                "attempt", "run", 1L, "attempt-1", "publication-1", 2L);
        assertEquals(RunDecision.Code.ACCEPTED, store.apply(attempt).code());
        assertEquals(RunDecision.Code.ALREADY_APPLIED, store.apply(attempt).code());
        assertEquals(2L, store.get("run").revision());
        assertEquals(2, store.events("run", 0L, 100).size());
    }

    @Test
    void attemptStateMatrixIsBackendIndependent() {
        List<Edge> edges = Arrays.asList(
                edge(AttemptStatus.CREATED, AttemptStatus.DISPATCHING),
                edge(AttemptStatus.CREATED, AttemptStatus.FAILED),
                edge(AttemptStatus.DISPATCHING, AttemptStatus.RUNNING),
                edge(AttemptStatus.DISPATCHING, AttemptStatus.FAILED),
                edge(AttemptStatus.RUNNING, AttemptStatus.COMMITTING),
                edge(AttemptStatus.RUNNING, AttemptStatus.FAILED),
                edge(AttemptStatus.COMMITTING, AttemptStatus.SUCCEEDED),
                edge(AttemptStatus.COMMITTING, AttemptStatus.RECONCILING),
                edge(AttemptStatus.RECONCILING, AttemptStatus.SUCCEEDED),
                edge(AttemptStatus.RECONCILING, AttemptStatus.FAILED),
                edge(AttemptStatus.CANCELING, AttemptStatus.CANCELED),
                edge(AttemptStatus.CANCELING, AttemptStatus.TIMED_OUT),
                edge(AttemptStatus.CANCELING, AttemptStatus.FENCED),
                edge(AttemptStatus.CANCELING, AttemptStatus.RECONCILING));

        int index = 0;
        for (Edge edge : edges) {
            RunStateStore store = newStore();
            String runId = "matrix-" + (++index);
            RunSnapshot run = accepted(store, RunCommand.createRun(
                    "create-" + index, runId, "definition", "artifact", 2, 0L, 1L));
            run = accepted(store, RunCommand.createAttempt(
                    "attempt-" + index, runId, run.revision(),
                    "attempt-1", "publication-1", 2L));
            run = reach(store, run, edge.from, edge.to, 3L, index);
            RunDecision decision = store.apply(RunCommand.advanceAttempt(
                    "edge-" + index, runId, run.revision(), "attempt-1", 1L,
                    edge.to, false, run.updatedAtMillis() + 1L,
                    "EDGE", edge.from + "->" + edge.to,
                    edge.to == AttemptStatus.SUCCEEDED
                            ? RunResultSummary.discard(0L) : null));
            assertEquals(RunDecision.Code.ACCEPTED, decision.code(),
                    edge.from + " -> " + edge.to);
            assertEquals(edge.to, decision.snapshot().currentAttempt().status());
            assertEquals(decision.snapshot().revision(), decision.event().sequence());
        }
    }

    @Test
    void staleRevisionFenceAndConcurrentCasDoNotPublishPartialEvents() throws Exception {
        RunStateStore store = newStore();
        accepted(store, RunCommand.createRun(
                "create", "run", "definition", "artifact", 3, 0L, 1L));

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        List<RunDecision> decisions = new ArrayList<>();
        Object monitor = new Object();
        for (int i = 1; i <= 2; i++) {
            final int number = i;
            pool.submit(() -> {
                start.await();
                RunDecision decision = store.apply(RunCommand.createAttempt(
                        "attempt-" + number, "run", 1L,
                        "attempt-" + number, "publication-" + number, 2L));
                synchronized (monitor) {
                    decisions.add(decision);
                }
                return null;
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(1L, decisions.stream().filter(RunDecision::accepted).count());
        assertEquals(1L, decisions.stream()
                .filter(d -> d.code() == RunDecision.Code.STALE_REVISION).count());
        assertEquals(2L, store.get("run").revision());
        assertEquals(2, store.events("run", 0L, 100).size());

        RunSnapshot current = store.get("run");
        RunDecision staleFence = store.apply(RunCommand.advanceAttempt(
                "stale-fence", "run", current.revision(), current.currentAttempt().attemptId(),
                current.currentAttempt().fenceToken() + 1L, AttemptStatus.DISPATCHING,
                false, 3L, null, null));
        assertEquals(RunDecision.Code.STALE_FENCE, staleFence.code());
        assertEquals(2L, store.get("run").revision());
        assertEquals(2, store.events("run", 0L, 100).size());
    }

    private static RunSnapshot reach(RunStateStore store, RunSnapshot run,
                                     AttemptStatus target, AttemptStatus destination,
                                     long nextTime, int index) {
        if (target == AttemptStatus.CREATED) return run;
        if (target == AttemptStatus.CANCELING) {
            run = advance(store, run, AttemptStatus.DISPATCHING, nextTime++, index);
            run = advance(store, run, AttemptStatus.RUNNING, nextTime++, index);
            CancellationCause cause = destination == AttemptStatus.TIMED_OUT
                    ? CancellationCause.TIMEOUT
                    : destination == AttemptStatus.FENCED
                    ? CancellationCause.FENCE : CancellationCause.USER;
            return accepted(store, RunCommand.requestCancellation(
                    "cancel-" + index, run.runId(), run.revision(), "attempt-1", 1L,
                    cause, nextTime, "STOP", "matrix"));
        }
        run = advance(store, run, AttemptStatus.DISPATCHING, nextTime++, index);
        if (target == AttemptStatus.DISPATCHING) return run;
        run = advance(store, run, AttemptStatus.RUNNING, nextTime++, index);
        if (target == AttemptStatus.RUNNING) return run;
        run = advance(store, run, AttemptStatus.COMMITTING, nextTime++, index);
        if (target == AttemptStatus.COMMITTING) return run;
        return advance(store, run, AttemptStatus.RECONCILING, nextTime, index);
    }

    private static RunSnapshot advance(RunStateStore store, RunSnapshot run,
                                       AttemptStatus target, long time, int index) {
        return accepted(store, RunCommand.advanceAttempt(
                "advance-" + index + '-' + target.name(), run.runId(), run.revision(),
                "attempt-1", 1L, target, false, time, null, null));
    }

    private static RunSnapshot accepted(RunStateStore store, RunCommand command) {
        RunDecision decision = store.apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        assertNotNull(decision.event());
        assertEquals(decision.snapshot().revision(), decision.event().sequence());
        assertEquals(decision.snapshot().revision(), store.get(command.runId).revision());
        ExecutionEvent persisted = store.events(
                command.runId, decision.event().sequence() - 1L, 1).get(0);
        assertEquals(decision.event().sequence(), persisted.sequence());
        assertArrayEquals(decision.event().commandDigest(), persisted.commandDigest());
        return decision.snapshot();
    }

    private static Edge edge(AttemptStatus from, AttemptStatus to) {
        return new Edge(from, to);
    }

    private static final class Edge {
        final AttemptStatus from;
        final AttemptStatus to;

        Edge(AttemptStatus from, AttemptStatus to) {
            this.from = from;
            this.to = to;
        }
    }
}
