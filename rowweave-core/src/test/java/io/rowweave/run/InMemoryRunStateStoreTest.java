package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryRunStateStoreTest {

    @Test
    void validatesIdentityLimitsTimeAndBoundedEventPaging() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();

        assertThrows(IllegalArgumentException.class, () -> store.apply(
                RunCommand.createRun("bad\0run", "definition", "artifact", 2, 0L, 10L)));
        assertThrows(IllegalArgumentException.class, () -> store.apply(
                RunCommand.createRun(Normalizer.normalize("e\u0301", Normalizer.Form.NFD),
                        "definition", "artifact", 2, 0L, 10L)));
        assertThrows(IllegalArgumentException.class, () -> store.apply(
                RunCommand.createRun("run", "definition", "artifact", 0, 0L, 10L)));
        assertThrows(IllegalArgumentException.class, () -> store.apply(
                RunCommand.createRun("run", "definition", "artifact", 1001, 0L, 10L)));
        assertThrows(IllegalArgumentException.class, () -> store.apply(
                RunCommand.createRun("run", "definition", "artifact", 2, 9L, 10L)));

        RunDecision created = store.apply(
                RunCommand.createRun("run", "definition", "artifact", 2, 100L, 10L));
        assertEquals(RunDecision.Code.ACCEPTED, created.code());
        assertEquals(1L, created.snapshot().revision());
        assertEquals(1L, created.event().sequence());

        assertThrows(IllegalArgumentException.class, () -> store.events("run", 0L, 0));
        assertThrows(IllegalArgumentException.class, () -> store.events("run", 0L, 1001));
        assertEquals(1, store.events("run", 0L, 1).size());

        assertThrows(IllegalArgumentException.class, () -> store.apply(
                RunCommand.createAttempt("run", 1L, "attempt-1", "publication-1", 9L)));
        assertThrows(IllegalArgumentException.class, () -> store.apply(
                RunCommand.advanceAttempt("run", 1L, "attempt-1", 1L,
                        AttemptStatus.FAILED, true, 11L, repeat('x', 129), "failure")));
        assertThrows(IllegalArgumentException.class, () -> store.apply(
                RunCommand.advanceAttempt("run", 1L, "attempt-1", 1L,
                        AttemptStatus.FAILED, true, 11L, "FAIL", repeat('x', 4097))));
    }

    @Test
    void enforcesStateMatrixAndBlocksRetryWhileCommitOutcomeIsUnknown() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = accepted(store, RunCommand.createRun(
                "run", "definition", "artifact", 3, 0L, 1L));
        run = accepted(store, RunCommand.createAttempt(
                "run", run.revision(), "attempt-1", "publication-1", 2L));
        assertEquals(RunStatus.RUNNING, run.status());
        assertEquals(1L, run.currentAttempt().fenceToken());

        RunDecision illegal = store.apply(RunCommand.advanceAttempt(
                "run", run.revision(), "attempt-1", 1L,
                AttemptStatus.SUCCEEDED, false, 3L, null, null,
                RunResultSummary.discard(0L)));
        assertEquals(RunDecision.Code.ILLEGAL_TRANSITION, illegal.code());
        assertEquals(run.revision(), store.get("run").revision());

        run = accepted(store, RunCommand.advanceAttempt(
                "run", run.revision(), "attempt-1", 1L,
                AttemptStatus.DISPATCHING, false, 3L, null, null));
        run = accepted(store, RunCommand.advanceAttempt(
                "run", run.revision(), "attempt-1", 1L,
                AttemptStatus.RUNNING, false, 4L, null, null));
        run = accepted(store, RunCommand.advanceAttempt(
                "run", run.revision(), "attempt-1", 1L,
                AttemptStatus.COMMITTING, false, 5L, null, null));
        run = accepted(store, RunCommand.advanceAttempt(
                "run", run.revision(), "attempt-1", 1L,
                AttemptStatus.RECONCILING, false, 6L, "COMMIT_ACK_LOST", "outcome unknown"));
        assertEquals(RunStatus.RECONCILING, run.status());

        RunDecision retryBlocked = store.apply(RunCommand.createAttempt(
                "run", run.revision(), "attempt-2", "publication-2", 7L));
        assertEquals(RunDecision.Code.ILLEGAL_TRANSITION, retryBlocked.code());

        run = accepted(store, RunCommand.retryAuthorityAdvance(
                "retry-run", "run", run.revision(), "attempt-1", 1L,
                8L, WholeAttemptRetryClass.WORKER_LOST, false));
        assertEquals(RunStatus.READY, run.status());
        RetryAttemptIdentity retryIdentity = RetryAttemptIdentity.derive(
                run.runId(), run.artifactId(), 2);
        run = accepted(store, RunCommand.retryAuthorityCreateAttempt(
                retryIdentity.commandId("create"), run.runId(), run.revision(),
                retryIdentity, 9L));
        assertEquals(2L, run.currentAttempt().fenceToken());

        run = accepted(store, RunCommand.advanceAttempt(
                "run", run.revision(), retryIdentity.attemptId(), 2L,
                AttemptStatus.DISPATCHING, false, 10L, null, null));
        run = accepted(store, RunCommand.advanceAttempt(
                "run", run.revision(), retryIdentity.attemptId(), 2L,
                AttemptStatus.RUNNING, false, 11L, null, null));
        run = accepted(store, RunCommand.advanceAttempt(
                "run", run.revision(), retryIdentity.attemptId(), 2L,
                AttemptStatus.COMMITTING, false, 12L, null, null));
        run = accepted(store, RunCommand.advanceAttempt(
                "run", run.revision(), retryIdentity.attemptId(), 2L,
                AttemptStatus.SUCCEEDED, false, 13L, null, null,
                RunResultSummary.discard(0L)));
        assertEquals(RunStatus.SUCCEEDED, run.status());

        RunDecision terminal = store.apply(RunCommand.requestCancellation(
                "run", run.revision(), retryIdentity.attemptId(), 2L,
                CancellationCause.USER, 14L, "USER_CANCEL", "too late"));
        assertEquals(RunDecision.Code.ILLEGAL_TRANSITION, terminal.code());
    }

    @Test
    void cancellationAndTimeoutRequireTwoPhaseConvergence() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = accepted(store, RunCommand.createRun(
                "run-timeout", "definition", "artifact", 2, 50L, 1L));
        run = accepted(store, RunCommand.createAttempt(
                "run-timeout", run.revision(), "attempt-1", "publication-1", 2L));
        run = accepted(store, RunCommand.advanceAttempt(
                "run-timeout", run.revision(), "attempt-1", 1L,
                AttemptStatus.DISPATCHING, false, 3L, null, null));
        run = accepted(store, RunCommand.advanceAttempt(
                "run-timeout", run.revision(), "attempt-1", 1L,
                AttemptStatus.RUNNING, false, 4L, null, null));

        run = accepted(store, RunCommand.requestCancellation(
                "run-timeout", run.revision(), "attempt-1", 1L,
                CancellationCause.TIMEOUT, 50L, "DEADLINE", "deadline reached"));
        assertEquals(RunStatus.CANCELING, run.status());
        assertEquals(AttemptStatus.CANCELING, run.currentAttempt().status());
        assertEquals(CancellationCause.TIMEOUT, run.currentAttempt().cancellationCause());

        RunDecision prematureRetry = store.apply(RunCommand.createAttempt(
                "run-timeout", run.revision(), "attempt-2", "publication-2", 51L));
        assertEquals(RunDecision.Code.ILLEGAL_TRANSITION, prematureRetry.code());

        run = accepted(store, RunCommand.advanceAttempt(
                "run-timeout", run.revision(), "attempt-1", 1L,
                AttemptStatus.TIMED_OUT, false, 52L, "DRAINED", "worker stopped"));
        assertEquals(RunStatus.FAILED, run.status());
        assertEquals(AttemptStatus.TIMED_OUT, run.currentAttempt().status());

        RunSnapshot user = accepted(store, RunCommand.createRun(
                "run-cancel", "definition", "artifact", 1, 0L, 1L));
        user = accepted(store, RunCommand.createAttempt(
                "run-cancel", user.revision(), "attempt-1", "publication-1", 2L));
        user = accepted(store, RunCommand.requestCancellation(
                "run-cancel", user.revision(), "attempt-1", 1L,
                CancellationCause.USER, 3L, "USER_CANCEL", "requested"));
        user = accepted(store, RunCommand.advanceAttempt(
                "run-cancel", user.revision(), "attempt-1", 1L,
                AttemptStatus.CANCELED, false, 4L, "DRAINED", "worker stopped"));
        assertEquals(RunStatus.CANCELED, user.status());
    }

    @Test
    void readyRunCanBeCanceledWithoutReusingTerminalAttemptAuthority() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = accepted(store, RunCommand.createRun(
                "run-ready-cancel", "definition", "artifact", 2, 0L, 1L));
        run = accepted(store, RunCommand.createAttempt(
                "run-ready-cancel", run.revision(), "attempt-1", "publication-1", 2L));
        run = accepted(store, RunCommand.retryAuthorityAdvance(
                "retry-ready-cancel", "run-ready-cancel", run.revision(),
                "attempt-1", 1L, 3L,
                WholeAttemptRetryClass.PRE_DISPATCH_ABSENT, false));
        assertEquals(RunStatus.READY, run.status());

        run = accepted(store, RunCommand.cancelReadyRun(
                "run-ready-cancel", run.revision(), 4L, "USER_CANCEL", "stop retries"));
        assertEquals(RunStatus.CANCELED, run.status());
        assertEquals(AttemptStatus.FAILED, run.currentAttempt().status());
    }

    @Test
    void repeatedCancellationDuringReconciliationIsIdempotentAndEventFree() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = accepted(store, RunCommand.createRun(
                "run-reconcile", "definition", "artifact", 1, 0L, 1L));
        run = accepted(store, RunCommand.createAttempt(
                "run-reconcile", run.revision(), "attempt-1", "publication-1", 2L));
        run = accepted(store, RunCommand.advanceAttempt(
                "run-reconcile", run.revision(), "attempt-1", 1L,
                AttemptStatus.DISPATCHING, false, 3L, null, null));
        run = accepted(store, RunCommand.advanceAttempt(
                "run-reconcile", run.revision(), "attempt-1", 1L,
                AttemptStatus.RUNNING, false, 4L, null, null));
        run = accepted(store, RunCommand.advanceAttempt(
                "run-reconcile", run.revision(), "attempt-1", 1L,
                AttemptStatus.COMMITTING, false, 5L, null, null));
        run = accepted(store, RunCommand.requestCancellation(
                "run-reconcile", run.revision(), "attempt-1", 1L,
                CancellationCause.USER, 6L, "USER_CANCEL", "commit raced"));
        int eventsBeforeRepeat = store.events("run-reconcile", 0L, 100).size();

        RunDecision repeated = store.apply(RunCommand.requestCancellation(
                "run-reconcile", run.revision(), "attempt-1", 1L,
                CancellationCause.USER, 7L, "USER_CANCEL", "commit raced"));
        assertEquals(RunDecision.Code.ALREADY_APPLIED, repeated.code());
        assertEquals(run.revision(), store.get("run-reconcile").revision());
        assertEquals(eventsBeforeRepeat, store.events("run-reconcile", 0L, 100).size());
    }

    @Test
    void staleRevisionAndFenceHaveNoEventAndConcurrentCasHasOneWinner() throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = accepted(store, RunCommand.createRun(
                "run", "definition", "artifact", 3, 0L, 1L));
        long revision = run.revision();

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        List<RunDecision> decisions = new ArrayList<>();
        Object lock = new Object();
        for (int i = 1; i <= 2; i++) {
            final int attempt = i;
            pool.submit(() -> {
                start.await();
                RunDecision decision = store.apply(RunCommand.createAttempt(
                        "run", revision, "attempt-" + attempt, "publication-" + attempt, 2L));
                synchronized (lock) {
                    decisions.add(decision);
                }
                return null;
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        assertEquals(1L, decisions.stream().filter(RunDecision::accepted).count());
        assertEquals(1L, decisions.stream()
                .filter(d -> d.code() == RunDecision.Code.STALE_REVISION).count());

        run = store.get("run");
        assertEquals(2L, run.revision());
        assertEquals(2, store.events("run", 0L, 100).size());
        assertEquals(1L, run.currentAttempt().fenceToken());

        RunDecision staleFence = store.apply(RunCommand.advanceAttempt(
                "run", run.revision(), run.currentAttempt().attemptId(), 999L,
                AttemptStatus.DISPATCHING, false, 3L, null, null));
        assertEquals(RunDecision.Code.STALE_FENCE, staleFence.code());
        assertEquals(2L, store.get("run").revision());
        assertEquals(2, store.events("run", 0L, 100).size());

        List<ExecutionEvent> first = store.events("run", 0L, 1);
        List<ExecutionEvent> second = store.events("run", first.get(0).sequence(), 1);
        assertEquals(1L, first.get(0).sequence());
        assertEquals(2L, second.get(0).sequence());
        assertFalse(first == store.events("run", 0L, 1));
    }

    private static RunSnapshot accepted(InMemoryRunStateStore store, RunCommand command) {
        RunDecision result = store.apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, result.code());
        assertTrue(result.accepted());
        assertNotNull(result.snapshot());
        assertNotNull(result.event());
        assertEquals(result.snapshot().revision(), result.event().sequence());
        return result.snapshot();
    }

    private static String repeat(char value, int count) {
        StringBuilder text = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            text.append(value);
        }
        return text.toString();
    }
}
