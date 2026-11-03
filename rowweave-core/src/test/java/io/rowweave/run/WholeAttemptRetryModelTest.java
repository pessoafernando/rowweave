package io.rowweave.run;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Hard retry ceiling, identity, bypass and the READY control race. */
class WholeAttemptRetryModelTest {

    @Test
    void retryPolicyIsFixedBoundedAndOverflowSafe() {
        assertEquals(3, WholeAttemptRetryPolicy.MAX_ATTEMPTS);
        assertEquals(1_000L, WholeAttemptRetryPolicy.delayAfterAttempt(1));
        assertEquals(5_000L, WholeAttemptRetryPolicy.delayAfterAttempt(2));
        assertEquals(11_000L, WholeAttemptRetryPolicy.notBefore(1, 10_000L));
        assertEquals(15_000L, WholeAttemptRetryPolicy.notBefore(2, 10_000L));
        assertThrows(IllegalArgumentException.class,
                () -> WholeAttemptRetryPolicy.delayAfterAttempt(0));
        assertThrows(IllegalArgumentException.class,
                () -> WholeAttemptRetryPolicy.delayAfterAttempt(3));
        assertThrows(IllegalArgumentException.class,
                () -> WholeAttemptRetryPolicy.notBefore(1, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> WholeAttemptRetryPolicy.notBefore(2, Long.MAX_VALUE - 1L));

        assertEquals(WholeAttemptRetryClass.PRE_DISPATCH_ABSENT,
                WholeAttemptRetryClass.valueOf("PRE_DISPATCH_ABSENT"));
        assertEquals(WholeAttemptRetryClass.WORKER_LOST,
                WholeAttemptRetryClass.valueOf("WORKER_LOST"));
        assertEquals(2, WholeAttemptRetryClass.values().length);
    }

    @Test
    void nextAttemptIdentityIsStableAndSeparatesRunArtifactAndNumber() {
        RetryAttemptIdentity first = RetryAttemptIdentity.derive(
                "run-a", "artifact-a", 2);
        RetryAttemptIdentity replay = RetryAttemptIdentity.derive(
                "run-a", "artifact-a", 2);
        RetryAttemptIdentity next = RetryAttemptIdentity.derive(
                "run-a", "artifact-a", 3);
        RetryAttemptIdentity otherArtifact = RetryAttemptIdentity.derive(
                "run-a", "artifact-b", 2);

        assertEquals(first, replay);
        assertEquals(first.hashCode(), replay.hashCode());
        assertEquals("run-a", first.runId());
        assertEquals(2, first.attemptNumber());
        assertEquals(2L, first.fenceToken());
        assertTrue(first.attemptId().startsWith("retry-attempt-v1-"));
        assertTrue(first.publicationId().startsWith("retry-publication-v1-"));
        assertEquals(first.commandId("create"), replay.commandId("create"));
        assertNotEquals(first.commandId("create"), first.commandId("dispatch"));
        assertNotEquals(first, next);
        assertNotEquals(first, otherArtifact);

        assertThrows(IllegalArgumentException.class,
                () -> RetryAttemptIdentity.derive("run-a", "artifact-a", 1));
        assertThrows(IllegalArgumentException.class,
                () -> first.commandId(""));
    }

    @Test
    void genericMutationsCannotAuthorizeOrPrematurelyCreateRetry() {
        assertThrows(IllegalArgumentException.class, () -> RunCommand.advanceAttempt(
                "generic-retry", "run", 3L, "attempt-1", 1L,
                AttemptStatus.FAILED, true, 10L,
                "RETRY_WORKER_LOST", null));

        RunSnapshot waiting = waitingRetry("run", 100L);
        RunDecision bypass = RunStateStoreKernel.evaluate(waiting,
                RunCommand.createAttempt("generic-create", "run", waiting.revision(),
                        "attempt-2", "publication-2", 101L));
        assertEquals(RunDecision.Code.ILLEGAL_TRANSITION, bypass.code());
        assertEquals(waiting.revision(), bypass.snapshot().revision());

        RunSnapshot initial = new RunSnapshot(
                "initial", "definition", "artifact", 3, 0, 1L,
                RunStatus.READY, 0L, 1L, 1L, null);
        assertEquals(RunDecision.Code.ACCEPTED,
                RunStateStoreKernel.evaluate(initial,
                        RunCommand.createAttempt("first", "initial", 1L,
                                "attempt-1", "publication-1", 2L)).code());

        RetryAttemptIdentity identity = RetryAttemptIdentity.derive(
                waiting.runId(), waiting.artifactId(), 2);
        RunCommand authorityCreate = RunCommand.retryAuthorityCreateAttempt(
                identity.commandId("create"), waiting.runId(), waiting.revision(),
                identity, 1_100L);
        RunDecision claimed = RunStateStoreKernel.evaluate(waiting, authorityCreate);
        assertEquals(RunDecision.Code.ACCEPTED, claimed.code());
        assertEquals(identity.attemptId(),
                claimed.snapshot().currentAttempt().attemptId());

        RunCommand decodedWithoutAuthority = RunStateCodec.decodeCommand(
                RunStateCodec.encodeCommand(authorityCreate));
        assertEquals(RunDecision.Code.ILLEGAL_TRANSITION,
                RunStateStoreKernel.evaluate(waiting, decodedWithoutAuthority).code());
    }

    @Test
    void onlyInProcessAuthorityMarkerCanScheduleRetryReady() {
        AttemptSnapshot runningAttempt = new AttemptSnapshot(
                1, "attempt-1", 1L, "publication-1",
                AttemptStatus.RUNNING, null, 2L, 3L, null, null);
        RunSnapshot running = new RunSnapshot(
                "run-schedule", "definition", "artifact", 3, 1, 3L,
                RunStatus.RUNNING, 0L, 1L, 3L, runningAttempt);
        RunCommand authority = RunCommand.retryAuthorityAdvance(
                "retry-authority", running.runId(), running.revision(),
                runningAttempt.attemptId(), runningAttempt.fenceToken(), 100L,
                WholeAttemptRetryClass.WORKER_LOST, false);

        RunDecision scheduled = RunStateStoreKernel.evaluate(running, authority);
        assertEquals(RunDecision.Code.ACCEPTED, scheduled.code());
        assertEquals(RunStatus.READY, scheduled.snapshot().status());
        assertEquals("RETRY_WORKER_LOST",
                scheduled.snapshot().currentAttempt().reasonCode());

        RunCommand decodedWithoutAuthority = RunStateCodec.decodeCommand(
                RunStateCodec.encodeCommand(authority));
        assertEquals(RunDecision.Code.ILLEGAL_TRANSITION,
                RunStateStoreKernel.evaluate(running, decodedWithoutAuthority).code());

        RunDecision exhausted = RunStateStoreKernel.evaluate(running,
                RunCommand.retryAuthorityAdvance(
                        "retry-exhausted", running.runId(), running.revision(),
                        runningAttempt.attemptId(), runningAttempt.fenceToken(), 100L,
                        WholeAttemptRetryClass.WORKER_LOST, true));
        assertEquals(RunStatus.FAILED, exhausted.snapshot().status());
        assertEquals("RETRY_ATTEMPTS_EXHAUSTED",
                exhausted.snapshot().currentAttempt().reasonCode());
    }

    @Test
    void cancelAndDeadlineCanTerminateReadyRetryWithoutDroppingPreviousAttempt() {
        RunSnapshot waiting = waitingRetry("cancel", 100L);
        RunDecision canceled = RunStateStoreKernel.evaluate(waiting,
                RunCommand.cancelReadyRun("cancel-ready", "cancel", waiting.revision(),
                        101L, "USER_CANCEL", null));
        assertEquals(RunDecision.Code.ACCEPTED, canceled.code());
        assertEquals(RunStatus.CANCELED, canceled.snapshot().status());
        assertEquals(waiting.currentAttempt(), canceled.snapshot().currentAttempt());

        RunSnapshot deadlineWaiting = waitingRetry("deadline", 100L, 200L);
        String commandId = RunDeadlineAuthorityKernel.commandId(deadlineWaiting);
        RunDeadlineAuthorityKernel.Plan plan = RunDeadlineAuthorityKernel.plan(
                commandId, deadlineWaiting, 200L);
        RunDecision timedOut = RunStateStoreKernel.evaluate(
                deadlineWaiting, plan.command());
        assertEquals(RunDecision.Code.ACCEPTED, timedOut.code());
        assertEquals(RunStatus.FAILED, timedOut.snapshot().status());
        assertEquals(deadlineWaiting.currentAttempt(),
                timedOut.snapshot().currentAttempt());
        assertEquals(ExecutionEvent.Type.TIMEOUT_REQUESTED,
                timedOut.event().type());
        assertEquals("attempt-1", timedOut.event().attemptId());
        assertEquals(1L, timedOut.event().fenceToken());
        assertEquals(AttemptStatus.FAILED, timedOut.event().attemptFrom());
        assertEquals(AttemptStatus.FAILED, timedOut.event().attemptTo());

        ExecutionEvent decoded = RunStateCodec.decodeEvent(
                RunStateCodec.encodeEvent(timedOut.event()));
        assertEquals(timedOut.event().sequence(), decoded.sequence());
        assertEquals(timedOut.event().attemptId(), decoded.attemptId());
        assertEquals(RunExecutionEvent.DiagnosticCategory.OTHER,
                decoded.publicView().diagnosticCategory());
    }

    private static RunSnapshot waitingRetry(String runId, long retryScheduledAt) {
        return waitingRetry(runId, retryScheduledAt, 0L);
    }

    private static RunSnapshot waitingRetry(
            String runId, long retryScheduledAt, long deadline) {
        AttemptSnapshot previous = new AttemptSnapshot(
                1, "attempt-1", 1L, "publication-1",
                AttemptStatus.FAILED, null, 2L, retryScheduledAt,
                "RETRY_WORKER_LOST", null);
        return new RunSnapshot(runId, "definition", "artifact", 3, 1, 3L,
                RunStatus.READY, deadline, 1L, retryScheduledAt, previous);
    }
}
