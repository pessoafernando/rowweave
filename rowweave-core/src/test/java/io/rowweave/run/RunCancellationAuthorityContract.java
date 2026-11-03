package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Black-box contract for every public cancellation authority backend. */
public abstract class RunCancellationAuthorityContract {
    protected abstract RunStateStore newStore();

    @Test
    void validatesIdentityBeforeAuthorityLookup() {
        RunCancellationAuthority authority = authority(newStore());
        assertThrows(IllegalArgumentException.class,
                () -> authority.requestUserCancellation(null, "request"));
        assertThrows(IllegalArgumentException.class,
                () -> authority.requestUserCancellation("run", ""));
        assertThrows(IllegalArgumentException.class,
                () -> authority.requestUserCancellation("run", repeat('x', 129)));
        assertThrows(IllegalArgumentException.class,
                () -> authority.requestUserCancellation("run", "not\u0000valid"));
    }

    @Test
    void missingReadyAndStableReplayAreUnambiguous() {
        RunStateStore store = newStore();
        RunCancellationAuthority authority = authority(store);
        assertResult(authority.requestUserCancellation("missing", "request-1"),
                RunCancellationResult.Disposition.NOT_FOUND, null, 0L);

        create(store, "ready");
        RunCancellationResult first = authority.requestUserCancellation(
                "ready", "request-1");
        assertResult(first, RunCancellationResult.Disposition.ACCEPTED,
                RunStatus.CANCELED, 2L);
        List<ExecutionEvent> events = store.events("ready", 0L, 10);
        assertEquals(2, events.size());
        assertEquals(ExecutionEvent.Type.CANCEL_REQUESTED, events.get(1).type());
        assertEquals(CancellationCause.USER,
                store.get("ready").currentAttempt() == null ? CancellationCause.USER
                        : store.get("ready").currentAttempt().cancellationCause());
        assertEquals("USER_CANCEL", events.get(1).reasonCode());
        assertNull(events.get(1).message());

        assertResult(authority.requestUserCancellation("ready", "request-1"),
                RunCancellationResult.Disposition.ALREADY_REQUESTED,
                RunStatus.CANCELED, 2L);
        assertEquals(2, store.events("ready", 0L, 10).size());
        assertResult(authority.requestUserCancellation("ready", "request-2"),
                RunCancellationResult.Disposition.ALREADY_CANCELED,
                RunStatus.CANCELED, 2L);
        assertEquals(2, store.events("ready", 0L, 10).size());
    }

    @Test
    void createdDispatchingAndRunningAcceptExactlyOneUserCancellation() {
        for (AttemptStatus target : new AttemptStatus[] {
                AttemptStatus.CREATED, AttemptStatus.DISPATCHING, AttemptStatus.RUNNING}) {
            RunStateStore store = newStore();
            String runId = target.name().toLowerCase();
            RunSnapshot snapshot = attempt(store, runId);
            if (target == AttemptStatus.DISPATCHING || target == AttemptStatus.RUNNING) {
                snapshot = advance(store, snapshot, AttemptStatus.DISPATCHING);
            }
            if (target == AttemptStatus.RUNNING) {
                snapshot = advance(store, snapshot, AttemptStatus.RUNNING);
            }

            RunCancellationResult accepted = authority(store)
                    .requestUserCancellation(runId, "request");
            assertResult(accepted, RunCancellationResult.Disposition.ACCEPTED,
                    RunStatus.CANCELING, snapshot.revision() + 1L);
            AttemptSnapshot attempt = store.get(runId).currentAttempt();
            assertEquals(AttemptStatus.CANCELING, attempt.status());
            assertEquals(CancellationCause.USER, attempt.cancellationCause());
            assertEquals(1L, store.events(runId, 0L, 20).stream()
                    .filter(event -> event.type() == ExecutionEvent.Type.CANCEL_REQUESTED)
                    .count());

            assertResult(authority(store).requestUserCancellation(runId, "request"),
                    RunCancellationResult.Disposition.ALREADY_REQUESTED,
                    RunStatus.CANCELING, accepted.revision());
            assertResult(authority(store).requestUserCancellation(runId, "another-request"),
                    RunCancellationResult.Disposition.ALREADY_REQUESTED,
                    RunStatus.CANCELING, accepted.revision());
            assertEquals(1L, store.events(runId, 0L, 20).stream()
                    .filter(event -> event.type() == ExecutionEvent.Type.CANCEL_REQUESTED)
                    .count());
        }
    }

    @Test
    void committingReconcilingAndNonUserCancellationAreTooLate() {
        RunStateStore committingStore = newStore();
        RunSnapshot committing = running(committingStore, "committing");
        committing = advance(committingStore, committing, AttemptStatus.COMMITTING);
        assertResult(authority(committingStore).requestUserCancellation(
                        "committing", "request"),
                RunCancellationResult.Disposition.TOO_LATE,
                RunStatus.RUNNING, committing.revision());

        RunDecision reconciling = committingStore.apply(RunCommand.requestCancellation(
                "fence", "committing", committing.revision(), "attempt", 1L,
                CancellationCause.FENCE, committing.updatedAtMillis() + 1L,
                "FENCE", null));
        assertEquals(RunDecision.Code.ACCEPTED, reconciling.code());
        assertResult(authority(committingStore).requestUserCancellation(
                        "committing", "request-2"),
                RunCancellationResult.Disposition.TOO_LATE,
                RunStatus.RECONCILING, reconciling.snapshot().revision());

        RunStateStore timeoutStore = newStore();
        RunSnapshot running = running(timeoutStore, "timeout");
        RunDecision timeout = timeoutStore.apply(RunCommand.requestCancellation(
                "timeout-request", "timeout", running.revision(), "attempt", 1L,
                CancellationCause.TIMEOUT, running.updatedAtMillis() + 1L,
                "TIMEOUT", null));
        assertEquals(RunDecision.Code.ACCEPTED, timeout.code());
        assertResult(authority(timeoutStore).requestUserCancellation(
                        "timeout", "user-request"),
                RunCancellationResult.Disposition.TOO_LATE,
                RunStatus.CANCELING, timeout.snapshot().revision());
    }

    @Test
    void terminalOutcomesAreNotMutated() {
        RunStateStore failedStore = newStore();
        RunSnapshot failed = running(failedStore, "failed");
        failed = accepted(failedStore, RunCommand.advanceAttempt(
                "fail", "failed", failed.revision(), "attempt", 1L,
                AttemptStatus.FAILED, false, failed.updatedAtMillis() + 1L,
                "FAIL", null));
        assertResult(authority(failedStore).requestUserCancellation("failed", "request"),
                RunCancellationResult.Disposition.ALREADY_TERMINAL,
                RunStatus.FAILED, failed.revision());

        RunStateStore successStore = newStore();
        RunSnapshot success = running(successStore, "success");
        success = advance(successStore, success, AttemptStatus.COMMITTING);
        success = accepted(successStore, RunCommand.advanceAttempt(
                "succeed", "success", success.revision(), "attempt", 1L,
                AttemptStatus.SUCCEEDED, false, success.updatedAtMillis() + 1L,
                null, null, RunResultSummary.discard(0L)));
        assertResult(authority(successStore).requestUserCancellation("success", "request"),
                RunCancellationResult.Disposition.ALREADY_TERMINAL,
                RunStatus.SUCCEEDED, success.revision());
    }

    @Test
    void requestIdentityIsScopedToOneRun() {
        RunStateStore store = newStore();
        create(store, "left");
        create(store, "right");
        assertEquals(RunCancellationResult.Disposition.ACCEPTED,
                authority(store).requestUserCancellation("left", "same").disposition());
        assertEquals(RunCancellationResult.Disposition.ACCEPTED,
                authority(store).requestUserCancellation("right", "same").disposition());
    }

    @Test
    void deletingRunIsRetiringEvenForAnAcceptedRequestReplay() {
        RunStateStore store = newStore();
        create(store, "retiring");
        RunCancellationResult accepted = authority(store)
                .requestUserCancellation("retiring", "request");
        RunLifecycleRef terminal = ((RunLifecycleCatalog) store).lifecycle("retiring");
        RunRetentionClaim claim = ((RunRetentionAuthority) store).claim(
                "retiring", accepted.revision(), terminal.terminalAtMillis(),
                new RetentionOwner("master", 1L), 0L);
        assertEquals(RunRetentionClaim.Code.ACQUIRED, claim.code());

        assertResult(authority(store).requestUserCancellation("retiring", "request"),
                RunCancellationResult.Disposition.RETIRING, null, 0L);
    }

    protected static RunCancellationAuthority authority(RunStateStore store) {
        return (RunCancellationAuthority) store;
    }

    protected static RunSnapshot create(RunStateStore store, String runId) {
        return accepted(store, RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact", 1, 0L, 1L));
    }

    protected static RunSnapshot attempt(RunStateStore store, String runId) {
        RunSnapshot created = create(store, runId);
        return accepted(store, RunCommand.createAttempt(
                "attempt-" + runId, runId, created.revision(),
                "attempt", "publication", 2L));
    }

    protected static RunSnapshot running(RunStateStore store, String runId) {
        return advance(store, advance(store, attempt(store, runId),
                AttemptStatus.DISPATCHING), AttemptStatus.RUNNING);
    }

    protected static RunSnapshot advance(RunStateStore store, RunSnapshot snapshot,
                                         AttemptStatus target) {
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

    private static void assertResult(RunCancellationResult result,
                                     RunCancellationResult.Disposition disposition,
                                     RunStatus status, long revision) {
        assertEquals(disposition, result.disposition());
        assertEquals(status, result.status());
        assertEquals(revision, result.revision());
    }

    private static String repeat(char value, int count) {
        StringBuilder text = new StringBuilder(count);
        for (int i = 0; i < count; i++) text.append(value);
        return text.toString();
    }
}
