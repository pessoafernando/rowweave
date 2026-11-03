package io.rowweave.run;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.commit.CommitManifest;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class InMemoryWholeAttemptRetryAuthorityTest {

    @Test
    void preDispatchScheduleWaitAndClaimUseOnlyAuthorityTimeAndStableIdentity() {
        AtomicLong clock = new AtomicLong(100L);
        InMemoryRunStateStore store = store(clock);
        RunSnapshot first = attempt(store, "retry-pre-dispatch", 2);
        RunAttemptFence fence = fence(first);

        WholeAttemptRetryDecision scheduled = store.scheduleRetry(fence);
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED, scheduled.code());
        assertEquals(WholeAttemptRetryClass.PRE_DISPATCH_ABSENT,
                scheduled.retryClass());
        assertEquals(RunStatus.READY, scheduled.snapshot().status());
        assertEquals("RETRY_PRE_DISPATCH_ABSENT",
                scheduled.snapshot().currentAttempt().reasonCode());
        assertEquals(100L, scheduled.snapshot().currentAttempt().updatedAtMillis());
        long revision = scheduled.snapshot().revision();

        assertEquals(WholeAttemptRetryDecision.Code.ALREADY_SCHEDULED,
                store.scheduleRetry(fence).code());
        assertEquals(revision, store.get(first.runId()).revision());

        clock.set(1_099L);
        RetryReadyEvaluation early = store.evaluateRetryReady(first.runId());
        assertEquals(RetryReadyEvaluation.Code.NOT_DUE, early.code());
        assertEquals(1_100L, early.notBeforeMillis());
        assertEquals(1, early.remainingAttempts());
        assertEquals(RetryAttemptClaim.Code.NOT_DUE,
                store.claimDueRetry(first.runId()).code());
        assertEquals(revision, store.get(first.runId()).revision());

        clock.set(1_100L);
        assertEquals(RetryReadyEvaluation.Code.DUE,
                store.evaluateRetryReady(first.runId()).code());
        RetryAttemptClaim claimed = store.claimDueRetry(first.runId());
        RetryAttemptIdentity expected = RetryAttemptIdentity.derive(
                first.runId(), first.artifactId(), 2);
        assertEquals(RetryAttemptClaim.Code.CLAIMED, claimed.code());
        assertEquals(expected, claimed.identity());
        assertEquals(expected.attemptId(), claimed.snapshot().currentAttempt().attemptId());
        assertEquals(expected.publicationId(),
                claimed.snapshot().currentAttempt().publicationId());
        assertEquals(2, claimed.snapshot().attemptCount());

        long claimedRevision = claimed.snapshot().revision();
        assertEquals(RetryAttemptClaim.Code.ALREADY_CLAIMED,
                store.claimDueRetry(first.runId()).code());
        assertEquals(claimedRevision, store.get(first.runId()).revision());
    }

    @Test
    void discardWorkerLostNeedsCompleteReceiptClosureAndExactRecoveryOwner() {
        AtomicLong clock = new AtomicLong(200L);

        InMemoryRunStateStore pendingStore = store(clock);
        RunSnapshot pendingRun = runningAttemptWithAcceptedAssignment(
                pendingStore, "retry-pending", AttemptOutputProfile.DISCARD,
                StageAssignmentRole.WHOLE_GRAPH);
        assertEquals(WholeAttemptRetryDecision.Code.CLOSURE_PENDING,
                pendingStore.scheduleRetry(fence(pendingRun)).code());
        assertEquals(RunStatus.RUNNING, pendingStore.get(pendingRun.runId()).status());

        InMemoryRunStateStore failedStore = store(clock);
        RunSnapshot failedRun = runningAttemptWithAcceptedAssignment(
                failedStore, "retry-failed", AttemptOutputProfile.DISCARD,
                StageAssignmentRole.WHOLE_GRAPH);
        StageAssignmentSnapshot failedAssignment =
                failedStore.assignmentSnapshot(fence(failedRun));
        StageAssignmentAcceptance failedAcceptance = failedAssignment.acceptance(0);
        failedStore.recordTerminal(StageTerminalReceipt.failed(
                failedAcceptance.token(), failedAcceptance.commandDigest(), 1L, 0L,
                "WORKER_LOST", digest(9), 20L));
        assertEquals(WholeAttemptRetryDecision.Code.NOT_RETRYABLE,
                failedStore.scheduleRetry(fence(failedRun)).code());

        InMemoryRunStateStore legacyStore = store(clock);
        RunSnapshot legacyRun = runningAttemptWithAcceptedAssignment(
                legacyStore, "retry-legacy", AttemptOutputProfile.UNKNOWN_LEGACY,
                StageAssignmentRole.WHOLE_GRAPH);
        RecoveryToken legacyOwner = claimOwner(legacyStore, legacyRun);
        recordLost(legacyStore, legacyOwner, fence(legacyRun), "ARBITRARY_TEXT");
        assertEquals(WholeAttemptRetryDecision.Code.NOT_RETRYABLE,
                legacyStore.scheduleRetryRecovered(legacyOwner, fence(legacyRun)).code());

        InMemoryRunStateStore discardStore = store(clock);
        RunSnapshot discardRun = runningAttemptWithAcceptedAssignment(
                discardStore, "retry-discard", AttemptOutputProfile.DISCARD,
                StageAssignmentRole.WHOLE_GRAPH);
        RecoveryToken owner = claimOwner(discardStore, discardRun);
        recordLost(discardStore, owner, fence(discardRun), "NOT_A_RETRY_CLASS");

        assertEquals(WholeAttemptRetryDecision.Code.STALE_RECOVERY_OWNER,
                discardStore.scheduleRetry(fence(discardRun)).code());
        RecoveryToken stale = RecoveryToken.create(discardRun.runId(),
                new RecoveryOwner("stale-master", 8L), 99L, discardRun.revision());
        assertEquals(WholeAttemptRetryDecision.Code.STALE_RECOVERY_OWNER,
                discardStore.scheduleRetryRecovered(stale, fence(discardRun)).code());
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                discardStore.scheduleRetryRecovered(owner, fence(discardRun)).code());
    }

    @Test
    void manifestWorkerLostRequiresExactCompletedAbortAndIntentIsCutoff() {
        AtomicLong clock = new AtomicLong(300L);
        InMemoryRunStateStore store = store(clock);
        RunSnapshot run = runningAttemptWithAcceptedAssignment(
                store, "retry-manifest", AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                StageAssignmentRole.SINK);
        RecoveryToken owner = claimOwner(store, run);
        recordLost(store, owner, fence(run), "HOST_TERMINATED");

        assertEquals(WholeAttemptRetryDecision.Code.CLOSURE_PENDING,
                store.scheduleRetryRecovered(owner, fence(run)).code());
        BatchSinkAbortRecord decided = BatchSinkAbortRecord.decided(
                fence(run), 1L,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", 1L, run.currentAttempt().publicationId(), digest(4),
                "RETRY_ROLLBACK", 30L);
        assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                store.recordBatchSinkAbortRecovered(owner, decided).code());
        assertEquals(WholeAttemptRetryDecision.Code.CLOSURE_PENDING,
                store.scheduleRetryRecovered(owner, fence(run)).code());
        assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                store.recordBatchSinkAbortRecovered(owner, decided.completed(31L)).code());
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                store.scheduleRetryRecovered(owner, fence(run)).code());
        clock.set(1_300L);
        RetryAttemptClaim retry = store.claimDueRetryRecovered(owner, run.runId());
        assertEquals(RetryAttemptClaim.Code.CLAIMED, retry.code());
        assertEquals(2, retry.snapshot().attemptCount());
        assertNull(store.batchSinkAbort(fence(run)),
                "the prior attempt abort must not poison the new publication attempt");

        InMemoryRunStateStore committingStore = store(clock);
        RunSnapshot committing = runningAttemptWithAcceptedAssignment(
                committingStore, "retry-intent-cutoff",
                AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                StageAssignmentRole.SINK);
        RunDecision committingDecision = committingStore.apply(RunCommand.advanceAttempt(
                "commit-cutoff", committing.runId(), committing.revision(),
                committing.currentAttempt().attemptId(),
                committing.currentAttempt().fenceToken(), AttemptStatus.COMMITTING,
                false, 40L, null, null));
        assertEquals(RunDecision.Code.ACCEPTED, committingDecision.code());
        assertEquals(WholeAttemptRetryDecision.Code.TOO_LATE,
                committingStore.scheduleRetry(fence(committingDecision.snapshot())).code());
    }

    @Test
    void thirdRetryableClosureExhaustsAtomicallyWithoutAnotherReadyWindow() {
        AtomicLong clock = new AtomicLong(100L);
        InMemoryRunStateStore store = store(clock);
        RunSnapshot current = attempt(store, "retry-exhausted", 3);

        WholeAttemptRetryDecision first = store.scheduleRetry(fence(current));
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED, first.code());
        clock.set(1_100L);
        current = store.claimDueRetry(current.runId()).snapshot();
        assertEquals(2, current.attemptCount());

        clock.set(2_000L);
        WholeAttemptRetryDecision second = store.scheduleRetry(fence(current));
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED, second.code());
        clock.set(7_000L);
        current = store.claimDueRetry(current.runId()).snapshot();
        assertEquals(3, current.attemptCount());

        clock.set(8_000L);
        WholeAttemptRetryDecision exhausted = store.scheduleRetry(fence(current));
        assertEquals(WholeAttemptRetryDecision.Code.EXHAUSTED, exhausted.code());
        assertEquals(RunStatus.FAILED, exhausted.snapshot().status());
        assertEquals(AttemptStatus.FAILED,
                exhausted.snapshot().currentAttempt().status());
        assertEquals("RETRY_ATTEMPTS_EXHAUSTED",
                exhausted.snapshot().currentAttempt().reasonCode());
        assertNotEquals(RunStatus.READY, exhausted.snapshot().status());

        RunLifecycleRef terminal = store.lifecycle(current.runId());
        assertEquals(RunRetentionClaim.Code.ACQUIRED,
                store.claim(current.runId(), exhausted.snapshot().revision(),
                        terminal.terminalAtMillis(),
                        new RetentionOwner("janitor", 1L), 0L).code());
        assertEquals(WholeAttemptRetryDecision.Code.RETIRING,
                store.scheduleRetry(fence(current)).code());
        assertEquals(RetryReadyEvaluation.Code.RETIRING,
                store.evaluateRetryReady(current.runId()).code());
        assertEquals(RetryAttemptClaim.Code.RETIRING,
                store.claimDueRetry(current.runId()).code());
    }

    @Test
    void deadlineAndUserCancellationWinTheReadyRetryWindow() {
        AtomicLong deadlineClock = new AtomicLong(100L);
        InMemoryRunStateStore deadlineStore = store(deadlineClock);
        RunSnapshot deadlineRun = attempt(
                deadlineStore, "retry-deadline", 2, 105L);
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                deadlineStore.scheduleRetry(fence(deadlineRun)).code());
        deadlineClock.set(105L);
        RetryAttemptClaim timedOut =
                deadlineStore.claimDueRetry(deadlineRun.runId());
        assertEquals(RetryAttemptClaim.Code.TOO_LATE, timedOut.code());
        assertEquals(RunStatus.FAILED, timedOut.snapshot().status());
        assertEquals(1, timedOut.snapshot().attemptCount());

        AtomicLong cancelClock = new AtomicLong(200L);
        InMemoryRunStateStore cancelStore = store(cancelClock);
        RunSnapshot cancelRun = attempt(cancelStore, "retry-cancel", 2);
        RunSnapshot waiting = cancelStore.scheduleRetry(
                fence(cancelRun)).snapshot();
        assertEquals(RunCancellationResult.Disposition.ACCEPTED,
                cancelStore.requestUserCancellation(
                        waiting.runId(), "cancel-retry-window").disposition());
        RetryAttemptClaim canceled = cancelStore.claimDueRetry(waiting.runId());
        assertEquals(RetryAttemptClaim.Code.TOO_LATE, canceled.code());
        assertEquals(RunStatus.CANCELED, canceled.snapshot().status());
        assertEquals(1, canceled.snapshot().attemptCount());
    }

    @Test
    void durableManifestIntentIsAnIrreversibleRetryCutoff() {
        AtomicLong clock = new AtomicLong(500L);
        InMemoryRunStateStore store = store(clock);
        RunSnapshot created = attempt(store, "retry-durable-intent", 2);
        RunAttemptFence fence = fence(created);
        StageAssignmentRecord sink = StageAssignmentRecord.create(
                fence, 1L, 0, 101L, 0, 0, StageAssignmentRole.SINK,
                "worker-a", "localhost", 7001, -1, "inc-a");
        StageAssignmentRecord router = StageAssignmentRecord.create(
                fence, 1L, 1, 102L, 1, 0, StageAssignmentRole.ROUTER,
                "worker-b", "localhost", 7002, -1, "inc-b");
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, created.artifactId(), 1L,
                AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                java.util.Arrays.asList(sink, router));
        StageAssignmentSnapshot assignments = store.publish(manifest).snapshot();
        StageAssignmentAcceptance sinkAcceptance = new StageAssignmentAcceptance(
                assignments.token(0), digest(1), 10L);
        StageAssignmentAcceptance routerAcceptance = new StageAssignmentAcceptance(
                assignments.token(1), digest(2), 11L);
        store.accept(sinkAcceptance);
        store.accept(routerAcceptance);
        RunSnapshot running = advance(store, created, AttemptStatus.DISPATCHING, 3L);
        running = advance(store, running, AttemptStatus.RUNNING, 4L);
        store.recordTerminal(StageTerminalReceipt.succeeded(
                sinkAcceptance.token(), sinkAcceptance.commandDigest(),
                1L, 1L, 20L));
        CommitManifest.Member member = new CommitManifest.Member(
                "part-0", "data/1/part-0.data", "schema", 1L, digest(3));
        store.recordManifestMember(ManifestMemberReceipt.sealed(
                sinkAcceptance.token(), sinkAcceptance.commandDigest(),
                "domain", 1L, running.currentAttempt().publicationId(),
                member, 21L));
        RecoveryToken owner = claimOwner(store, running);
        assertEquals(StageTerminalDecision.Code.RECORDED,
                store.recordTerminalRecovered(owner, StageTerminalReceipt.lost(
                        routerAcceptance.token(), routerAcceptance.commandDigest(),
                        "HOST_TERMINATED", digest(4), 0L)).code());
        RunSnapshot committing = advanceRecovered(
                store, owner, running, AttemptStatus.COMMITTING, 30L);
        ManifestCommitIntent intent = store.manifestCommitJournal(fence).buildIntent();
        assertEquals(ManifestIntentDecision.Code.COMMITTED,
                store.commitManifestIntentRecovered(owner, intent).code());

        WholeAttemptRetryDecision cutoff =
                store.scheduleRetryRecovered(owner, fence(committing));
        assertEquals(WholeAttemptRetryDecision.Code.TOO_LATE, cutoff.code());
        assertEquals(RunStatus.RUNNING, cutoff.snapshot().status());
        assertEquals(AttemptStatus.COMMITTING,
                cutoff.snapshot().currentAttempt().status());
        assertEquals(intent, store.manifestCommitJournal(fence).intent());
    }

    private static InMemoryRunStateStore store(AtomicLong clock) {
        return new InMemoryRunStateStore(1024, 4096, clock::get);
    }

    private static RunSnapshot attempt(
            InMemoryRunStateStore store, String runId, int maxAttempts) {
        return attempt(store, runId, maxAttempts, 0L);
    }

    private static RunSnapshot attempt(
            InMemoryRunStateStore store, String runId, int maxAttempts,
            long deadlineEpochMillis) {
        RunSnapshot ready = store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact",
                maxAttempts, deadlineEpochMillis, 1L)).snapshot();
        return store.apply(RunCommand.createAttempt(
                "attempt-" + runId, runId, ready.revision(),
                "attempt-1", "publication-1", 2L)).snapshot();
    }

    private static RunSnapshot runningAttemptWithAcceptedAssignment(
            InMemoryRunStateStore store, String runId, AttemptOutputProfile profile,
            StageAssignmentRole role) {
        RunSnapshot created = attempt(store, runId, 2);
        RunAttemptFence fence = fence(created);
        StageAssignmentRecord record = StageAssignmentRecord.create(
                fence, 1L, 0, 101L, 0, 0, role,
                "worker", "localhost", 7001, -1, "incarnation");
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, created.artifactId(), 1L, profile,
                Collections.singletonList(record));
        StageAssignmentSnapshot assignments = store.publish(manifest).snapshot();
        store.accept(new StageAssignmentAcceptance(
                assignments.token(0), digest(1), 10L));
        RunSnapshot dispatching = store.apply(RunCommand.advanceAttempt(
                "dispatch-" + runId, runId, created.revision(),
                created.currentAttempt().attemptId(), created.currentAttempt().fenceToken(),
                AttemptStatus.DISPATCHING, false, 3L, null, null)).snapshot();
        return store.apply(RunCommand.advanceAttempt(
                "running-" + runId, runId, dispatching.revision(),
                dispatching.currentAttempt().attemptId(),
                dispatching.currentAttempt().fenceToken(),
                AttemptStatus.RUNNING, false, 4L, null, null)).snapshot();
    }

    private static RecoveryToken claimOwner(
            InMemoryRunStateStore store, RunSnapshot run) {
        return store.claim(run.runId(), run.revision(), 0L,
                new RecoveryOwner("replacement-master", 7L)).token();
    }

    private static RunSnapshot advance(
            InMemoryRunStateStore store, RunSnapshot current,
            AttemptStatus status, long eventTimeMillis) {
        return store.apply(RunCommand.advanceAttempt(
                "advance-" + current.runId() + '-' + status,
                current.runId(), current.revision(),
                current.currentAttempt().attemptId(),
                current.currentAttempt().fenceToken(), status, false,
                eventTimeMillis, null, null)).snapshot();
    }

    private static RunSnapshot advanceRecovered(
            InMemoryRunStateStore store, RecoveryToken owner,
            RunSnapshot current, AttemptStatus status, long eventTimeMillis) {
        return store.applyRecovered(owner, RunCommand.advanceAttempt(
                "advance-" + current.runId() + '-' + status,
                current.runId(), current.revision(),
                current.currentAttempt().attemptId(),
                current.currentAttempt().fenceToken(), status, false,
                eventTimeMillis, null, null)).snapshot();
    }

    private static void recordLost(
            InMemoryRunStateStore store, RecoveryToken owner,
            RunAttemptFence fence, String category) {
        StageAssignmentAcceptance acceptance =
                store.assignmentSnapshot(fence).acceptance(0);
        assertEquals(StageTerminalDecision.Code.RECORDED,
                store.recordTerminalRecovered(owner, StageTerminalReceipt.lost(
                        acceptance.token(), acceptance.commandDigest(),
                        category, digest(7), 0L)).code());
    }

    private static RunAttemptFence fence(RunSnapshot run) {
        return new RunAttemptFence(run.runId(), run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken());
    }

    private static byte[] digest(int value) {
        byte[] digest = new byte[32];
        java.util.Arrays.fill(digest, (byte) value);
        return digest;
    }
}
