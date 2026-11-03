package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RunStateMatrixTest {

    @Test
    void everyDeclaredAttemptEdgeIsAccepted() {
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
            InMemoryRunStateStore store = new InMemoryRunStateStore();
            String runId = "matrix-" + (++index);
            RunSnapshot run = apply(store, RunCommand.createRun(
                    runId, "definition", "artifact", 2, 0L, 1L));
            run = apply(store, RunCommand.createAttempt(
                    runId, run.revision(), "attempt-1", "publication-1", 2L));
            long time = 3L;
            run = reach(store, run, edge.from, edge.to, time);
            time = run.updatedAtMillis() + 1L;

            RunDecision decision = store.apply(RunCommand.advanceAttempt(
                    runId, run.revision(), "attempt-1", 1L,
                    edge.to, false, time, "EDGE", edge.from + "->" + edge.to,
                    edge.to == AttemptStatus.SUCCEEDED
                            ? RunResultSummary.discard(0L) : null));
            assertEquals(RunDecision.Code.ACCEPTED, decision.code(),
                    edge.from + " -> " + edge.to);
            assertEquals(edge.to, decision.snapshot().currentAttempt().status());
        }
    }

    @Test
    void genericRetryMutationIsRejectedWithoutConsumingRevision() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = apply(store, RunCommand.createRun(
                "limit", "definition", "artifact", 1, 0L, 1L));
        run = apply(store, RunCommand.createAttempt(
                "limit", run.revision(), "attempt-1", "publication-1", 2L));
        RunSnapshot active = run;
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> RunCommand.advanceAttempt(
                        "limit", active.revision(), "attempt-1", 1L,
                        AttemptStatus.FAILED, true, 3L,
                        "RETRY_WORKER_LOST", "retry requested"));
        assertEquals(2L, store.get("limit").revision());
        assertEquals(2, store.events("limit", 0L, 100).size());
    }

    private static RunSnapshot reach(InMemoryRunStateStore store, RunSnapshot run,
                                     AttemptStatus target, AttemptStatus destination,
                                     long nextTime) {
        if (target == AttemptStatus.CREATED) {
            return run;
        }
        if (target == AttemptStatus.CANCELING) {
            run = advance(store, run, AttemptStatus.DISPATCHING, nextTime++);
            run = advance(store, run, AttemptStatus.RUNNING, nextTime++);
            CancellationCause cause = destination == AttemptStatus.TIMED_OUT
                    ? CancellationCause.TIMEOUT
                    : destination == AttemptStatus.FENCED
                    ? CancellationCause.FENCE : CancellationCause.USER;
            return apply(store, RunCommand.requestCancellation(
                    run.runId(), run.revision(), "attempt-1", 1L,
                    cause, nextTime, "STOP", "matrix"));
        }
        run = advance(store, run, AttemptStatus.DISPATCHING, nextTime++);
        if (target == AttemptStatus.DISPATCHING) {
            return run;
        }
        run = advance(store, run, AttemptStatus.RUNNING, nextTime++);
        if (target == AttemptStatus.RUNNING) {
            return run;
        }
        run = advance(store, run, AttemptStatus.COMMITTING, nextTime++);
        if (target == AttemptStatus.COMMITTING) {
            return run;
        }
        return advance(store, run, AttemptStatus.RECONCILING, nextTime);
    }

    private static RunSnapshot advance(InMemoryRunStateStore store, RunSnapshot run,
                                       AttemptStatus target, long time) {
        return apply(store, RunCommand.advanceAttempt(
                run.runId(), run.revision(), "attempt-1", 1L,
                target, false, time, null, null));
    }

    private static RunSnapshot apply(InMemoryRunStateStore store, RunCommand command) {
        RunDecision decision = store.apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
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
