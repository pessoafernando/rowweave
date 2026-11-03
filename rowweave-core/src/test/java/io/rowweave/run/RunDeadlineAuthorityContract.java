package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Shared black-box contract for the InMemory and File deadline authorities. */
public abstract class RunDeadlineAuthorityContract {
    protected abstract RunStateStore newStore(LongSupplier authorityClock);

    @Test
    void missingDisabledAndNotDueDoNotMutate() {
        MutableClock clock = new MutableClock(10L);
        RunStateStore store = newStore(clock);
        assertResult(authority(store).evaluateDeadline("missing"),
                RunDeadlineResult.Disposition.NOT_FOUND, null, 0L, 0L, 0L);

        create(store, "disabled", 0L);
        assertResult(authority(store).evaluateDeadline("disabled"),
                RunDeadlineResult.Disposition.NO_DEADLINE,
                RunStatus.READY, 1L, 0L, 0L);
        assertEquals(1, store.events("disabled", 0L, 10).size());

        create(store, "future", 100L);
        clock.set(99L);
        assertResult(authority(store).evaluateDeadline("future"),
                RunDeadlineResult.Disposition.NOT_DUE,
                RunStatus.READY, 1L, 100L, 99L);
        assertEquals(1, store.events("future", 0L, 10).size());
    }

    @Test
    void readyDeadlineIsInclusiveDurableAndReplayableWithoutAttempt() {
        MutableClock clock = new MutableClock(100L);
        RunStateStore store = newStore(clock);
        create(store, "ready", 100L);

        RunDeadlineResult accepted = authority(store).evaluateDeadline("ready");
        assertResult(accepted, RunDeadlineResult.Disposition.ACCEPTED,
                RunStatus.FAILED, 2L, 100L, 100L);
        RunSnapshot terminal = store.get("ready");
        assertEquals(RunStatus.FAILED, terminal.status());
        assertEquals(0, terminal.attemptCount());
        assertNull(terminal.currentAttempt());
        List<ExecutionEvent> events = store.events("ready", 0L, 10);
        assertEquals(2, events.size());
        assertEquals(ExecutionEvent.Type.TIMEOUT_REQUESTED, events.get(1).type());
        assertEquals("RUN_DEADLINE", events.get(1).reasonCode());
        assertNull(events.get(1).message());

        clock.set(101L);
        assertResult(authority(store).evaluateDeadline("ready"),
                RunDeadlineResult.Disposition.ALREADY_REQUESTED,
                RunStatus.FAILED, 2L, 100L, 101L);
        clock.set(50L); // authority clock rollback cannot erase an already durable decision.
        assertResult(authority(store).evaluateDeadline("ready"),
                RunDeadlineResult.Disposition.ALREADY_REQUESTED,
                RunStatus.FAILED, 2L, 100L, 50L);
        assertEquals(2, store.events("ready", 0L, 10).size());
    }

    @Test
    void createdDispatchingAndRunningAcceptExactlyOneTimeoutRequest() {
        for (AttemptStatus target : new AttemptStatus[] {
                AttemptStatus.CREATED, AttemptStatus.DISPATCHING, AttemptStatus.RUNNING}) {
            MutableClock clock = new MutableClock(100L);
            RunStateStore store = newStore(clock);
            String runId = target.name().toLowerCase();
            RunSnapshot snapshot = attempt(store, runId, 100L);
            if (target == AttemptStatus.DISPATCHING || target == AttemptStatus.RUNNING) {
                snapshot = advance(store, snapshot, AttemptStatus.DISPATCHING);
            }
            if (target == AttemptStatus.RUNNING) {
                snapshot = advance(store, snapshot, AttemptStatus.RUNNING);
            }

            RunDeadlineResult accepted = authority(store).evaluateDeadline(runId);
            assertResult(accepted, RunDeadlineResult.Disposition.ACCEPTED,
                    RunStatus.CANCELING, snapshot.revision() + 1L, 100L, 100L);
            AttemptSnapshot canceled = store.get(runId).currentAttempt();
            assertEquals(AttemptStatus.CANCELING, canceled.status());
            assertEquals(CancellationCause.TIMEOUT, canceled.cancellationCause());
            assertEquals(1L, store.events(runId, 0L, 20).stream()
                    .filter(event -> event.type() == ExecutionEvent.Type.TIMEOUT_REQUESTED)
                    .count());

            clock.set(101L);
            assertResult(authority(store).evaluateDeadline(runId),
                    RunDeadlineResult.Disposition.ALREADY_REQUESTED,
                    RunStatus.CANCELING, accepted.revision(), 100L, 101L);
        }
    }

    @Test
    void userCancellationCommittingReconcilingAndTerminalWinTheRace() {
        MutableClock clock = new MutableClock(100L);

        RunStateStore userStore = newStore(clock);
        RunSnapshot user = running(userStore, "user", 100L);
        RunDecision userCancel = userStore.apply(RunCommand.requestCancellation(
                "user-cancel", "user", user.revision(), "attempt", 1L,
                CancellationCause.USER, user.updatedAtMillis() + 1L,
                "USER_CANCEL", null));
        assertEquals(RunDecision.Code.ACCEPTED, userCancel.code());
        assertResult(authority(userStore).evaluateDeadline("user"),
                RunDeadlineResult.Disposition.TOO_LATE,
                RunStatus.CANCELING, userCancel.snapshot().revision(), 100L, 100L);

        RunStateStore committingStore = newStore(clock);
        RunSnapshot committing = advance(committingStore,
                running(committingStore, "committing", 100L), AttemptStatus.COMMITTING);
        assertResult(authority(committingStore).evaluateDeadline("committing"),
                RunDeadlineResult.Disposition.TOO_LATE,
                RunStatus.RUNNING, committing.revision(), 100L, 100L);

        RunDecision reconciling = committingStore.apply(RunCommand.requestCancellation(
                "fence", "committing", committing.revision(), "attempt", 1L,
                CancellationCause.FENCE, committing.updatedAtMillis() + 1L,
                "FENCE", null));
        assertEquals(RunDecision.Code.ACCEPTED, reconciling.code());
        assertResult(authority(committingStore).evaluateDeadline("committing"),
                RunDeadlineResult.Disposition.TOO_LATE,
                RunStatus.RECONCILING, reconciling.snapshot().revision(), 100L, 100L);

        RunStateStore failedStore = newStore(clock);
        RunSnapshot failed = running(failedStore, "failed", 100L);
        failed = accepted(failedStore, RunCommand.advanceAttempt(
                "failed", "failed", failed.revision(), "attempt", 1L,
                AttemptStatus.FAILED, false, failed.updatedAtMillis() + 1L,
                "FAILED", null));
        assertResult(authority(failedStore).evaluateDeadline("failed"),
                RunDeadlineResult.Disposition.ALREADY_TERMINAL,
                RunStatus.FAILED, failed.revision(), 100L, 100L);
    }

    @Test
    void invalidAuthorityTimeFailsWithoutMutation() {
        MutableClock clock = new MutableClock(0L);
        RunStateStore store = newStore(clock);
        create(store, "run", 100L);
        assertThrows(IllegalStateException.class,
                () -> authority(store).evaluateDeadline("run"));
        assertEquals(RunStatus.READY, store.get("run").status());
        assertEquals(1, store.events("run", 0L, 10).size());
    }

    protected static RunDeadlineAuthority authority(RunStateStore store) {
        return (RunDeadlineAuthority) store;
    }

    protected static RunSnapshot create(RunStateStore store, String runId, long deadline) {
        return accepted(store, RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact",
                1, deadline, 1L));
    }

    protected static RunSnapshot attempt(
            RunStateStore store, String runId, long deadline) {
        RunSnapshot created = create(store, runId, deadline);
        return accepted(store, RunCommand.createAttempt(
                "attempt-" + runId, runId, created.revision(),
                "attempt", "publication", 2L));
    }

    protected static RunSnapshot running(
            RunStateStore store, String runId, long deadline) {
        return advance(store, advance(store, attempt(store, runId, deadline),
                AttemptStatus.DISPATCHING), AttemptStatus.RUNNING);
    }

    protected static RunSnapshot advance(
            RunStateStore store, RunSnapshot snapshot, AttemptStatus target) {
        return accepted(store, RunCommand.advanceAttempt(
                "advance-" + snapshot.runId() + '-' + target,
                snapshot.runId(), snapshot.revision(), "attempt", 1L,
                target, false, snapshot.updatedAtMillis() + 1L, null, null));
    }

    protected static RunSnapshot accepted(RunStateStore store, RunCommand command) {
        RunDecision decision = store.apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }

    protected static void assertResult(
            RunDeadlineResult result, RunDeadlineResult.Disposition disposition,
            RunStatus status, long revision, long deadline, long observedTime) {
        assertEquals(disposition, result.disposition());
        assertEquals(status, result.status());
        assertEquals(revision, result.revision());
        assertEquals(deadline, result.deadlineEpochMillis());
        assertEquals(observedTime, result.observedAuthorityTimeMillis());
    }

    protected static final class MutableClock implements LongSupplier {
        private long now;

        MutableClock(long now) { this.now = now; }

        void set(long now) { this.now = now; }

        @Override
        public long getAsLong() { return now; }
    }
}
