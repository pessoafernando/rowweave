package io.rowweave.incremental;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.run.AttemptOutputProfile;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.BatchSinkAbortDecision;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.CancellationCause;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RetryAttemptClaim;
import io.rowweave.run.RetryAttemptIdentity;
import io.rowweave.run.RetryReadyEvaluation;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunDeadlineResult;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStatus;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageTerminalReceipt;
import io.rowweave.run.WholeAttemptRetryClass;
import io.rowweave.run.WholeAttemptRetryDecision;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/** cancellation, deadline and retry stay joint with frontier authority. */
class IncrementalInMemoryRetryCancellationContractTest {

    @Test
    void userCancellationAndDeadlineRequestDoNotClearFrontierBeforeAbortClosure()
            throws Exception {
        AtomicLong clock = new AtomicLong(100L);
        Fixture canceled = runningFixture(clock, 0L, "cancel");
        IncrementalAuthorityRecord cancelBefore = canceled.store.record("feed-cancel");
        RunCancellationResult requested = canceled.store.requestUserCancellation(
                canceled.token, canceled.runToken, canceled.request.runId(), "cancel-request");
        assertEquals(RunCancellationResult.Disposition.ACCEPTED,
                requested.disposition());
        assertEquals(RunStatus.CANCELING, requested.status());
        assertSame(cancelBefore, canceled.store.record("feed-cancel"));
        assertEquals(AttemptStatus.CANCELING, canceled.store.runSnapshot(
                canceled.request.runId()).currentAttempt().status());
        assertAbortClose(canceled, AttemptStatus.CANCELED, "USER_CANCEL");

        clock.set(200L);
        Fixture timedOut = runningFixture(clock, 200L, "timeout");
        IncrementalAuthorityRecord timeoutBefore = timedOut.store.record("feed-timeout");
        RunDeadlineResult timeout = timedOut.store.evaluateDeadline(
                timedOut.token, timedOut.runToken, timedOut.request.runId());
        assertEquals(RunDeadlineResult.Disposition.ACCEPTED,
                timeout.disposition());
        assertEquals(RunStatus.CANCELING, timeout.status());
        assertSame(timeoutBefore, timedOut.store.record("feed-timeout"));
        assertEquals(CancellationCause.TIMEOUT, timedOut.store.runSnapshot(
                timedOut.request.runId()).currentAttempt().cancellationCause());
        assertAbortClose(timedOut, AttemptStatus.TIMED_OUT, "RUN_DEADLINE");
    }

    @Test
    void wholeAttemptRetryReusesFrozenIntervalAndAtomicallyReplacesActiveFence()
            throws Exception {
        AtomicLong clock = new AtomicLong(300L);
        Fixture fixture = runningFixture(clock, 0L, "retry");
        recordLostAndCompletedAbort(fixture, "HOST_TERMINATED");
        IncrementalAuthorityRecord before = fixture.store.record("feed-retry");
        IncrementalActiveInterval frozen = before.activeInterval();

        WholeAttemptRetryDecision scheduled = fixture.store.scheduleRetry(
                fixture.token, fixture.runToken, fixture.request.firstAttemptFence());
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED, scheduled.code());
        assertEquals(WholeAttemptRetryClass.WORKER_LOST, scheduled.retryClass());
        assertEquals(RunStatus.READY, scheduled.snapshot().status());
        assertSame(before, fixture.store.record("feed-retry"),
                "retry wait keeps the prior attempt fence as durable closure evidence");

        clock.set(1_300L);
        RetryReadyEvaluation ready = fixture.store.evaluateRetryReady(
                fixture.token, fixture.runToken, fixture.request.runId());
        assertEquals(RetryReadyEvaluation.Code.DUE, ready.code());
        RetryAttemptClaim claimed = fixture.store.claimDueRetry(
                fixture.token, fixture.runToken, fixture.request.runId());
        assertEquals(RetryAttemptClaim.Code.CLAIMED, claimed.code());
        RetryAttemptIdentity expected = RetryAttemptIdentity.derive(
                fixture.request.runId(), fixture.request.artifactId(), 2);
        assertEquals(expected, claimed.identity());

        IncrementalAuthorityRecord after = fixture.store.record("feed-retry");
        IncrementalActiveInterval retried = after.activeInterval();
        assertEquals(before.revision() + 1L, after.revision());
        assertEquals(frozen.identity(), retried.identity());
        assertEquals(frozen.capturedTarget(), retried.capturedTarget());
        assertEquals(frozen.artifactId(), retried.artifactId());
        assertEquals(frozen.claimedRevision(), retried.claimedRevision());
        assertEquals(2, retried.attemptOrdinal());
        assertEquals(expected.attemptId(), retried.fence().attemptId());
        assertEquals(expected.fenceToken(), retried.fence().fenceToken());
        assertEquals(IncrementalAttemptCommitPhase.CLAIMED, retried.phase());
        assertNull(fixture.store.batchSinkAbort(
                fixture.request.firstAttemptFence()));

        long frontierRevision = after.revision();
        assertEquals(RetryAttemptClaim.Code.ALREADY_CLAIMED,
                fixture.store.claimDueRetry(
                        fixture.token, fixture.runToken,
                        fixture.request.runId()).code());
        assertEquals(frontierRevision,
                fixture.store.record("feed-retry").revision());
    }

    @Test
    void retryAndControlRequestsRequireCurrentDualOwnershipAndRejectIntentCutoff()
            throws Exception {
        AtomicLong clock = new AtomicLong(400L);
        Fixture fixture = runningFixture(clock, 450L, "fenced");
        recordLostAndCompletedAbort(fixture, "HOST_TERMINATED");
        RecoveryToken staleRun = RecoveryToken.create(
                fixture.request.runId(), new RecoveryOwner("stale", 9L),
                99L, fixture.store.runSnapshot(fixture.request.runId()).revision());
        assertEquals(WholeAttemptRetryDecision.Code.STALE_RECOVERY_OWNER,
                fixture.store.scheduleRetry(
                        fixture.token, staleRun,
                        fixture.request.firstAttemptFence()).code());
        assertEquals(RunCancellationResult.Disposition.TOO_LATE,
                fixture.store.requestUserCancellation(
                        fixture.token, staleRun, fixture.request.runId(),
                        "stale-cancel").disposition());
        clock.set(450L);
        assertEquals(RunDeadlineResult.Disposition.TOO_LATE,
                fixture.store.evaluateDeadline(
                        fixture.token, staleRun,
                        fixture.request.runId()).disposition());
        assertEquals(RunStatus.RUNNING,
                fixture.store.runSnapshot(fixture.request.runId()).status());

        Fixture noDeadline = runningFixture(clock, 0L, "no-deadline-fenced");
        RecoveryToken staleNoDeadline = RecoveryToken.create(
                noDeadline.request.runId(), new RecoveryOwner("stale", 10L),
                100L, noDeadline.store.runSnapshot(
                        noDeadline.request.runId()).revision());
        assertEquals(RunDeadlineResult.Disposition.NO_DEADLINE,
                noDeadline.store.evaluateDeadline(
                        noDeadline.token, staleNoDeadline,
                        noDeadline.request.runId()).disposition());
    }

    @Test
    void cancellationAndDeadlineCloseTheReadyRetryWindowAtomically()
            throws Exception {
        AtomicLong cancelClock = new AtomicLong(600L);
        Fixture canceled = runningFixture(cancelClock, 0L, "retry-cancel");
        recordLostAndCompletedAbort(canceled, "HOST_TERMINATED");
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                canceled.store.scheduleRetry(
                        canceled.token, canceled.runToken,
                        canceled.request.firstAttemptFence()).code());
        RunCancellationResult cancellation = canceled.store.requestUserCancellation(
                canceled.token, canceled.runToken, canceled.request.runId(),
                "cancel-ready-retry");
        assertEquals(RunCancellationResult.Disposition.ACCEPTED,
                cancellation.disposition());
        assertEquals(RunStatus.CANCELED, cancellation.status());
        assertEquals(IncrementalFrontierAuthorityState.READY,
                canceled.store.record(canceled.feedId).state());
        assertNull(canceled.store.record(canceled.feedId).activeInterval());
        assertEquals(IncrementalFrontier.begin(),
                canceled.store.record(canceled.feedId).committedFrontier());

        AtomicLong deadlineClock = new AtomicLong(700L);
        Fixture timedOut = runningFixture(
                deadlineClock, 705L, "retry-timeout");
        recordLostAndCompletedAbort(timedOut, "HOST_TERMINATED");
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                timedOut.store.scheduleRetry(
                        timedOut.token, timedOut.runToken,
                        timedOut.request.firstAttemptFence()).code());
        deadlineClock.set(705L);
        RunDeadlineResult deadline = timedOut.store.evaluateDeadline(
                timedOut.token, timedOut.runToken, timedOut.request.runId());
        assertEquals(RunDeadlineResult.Disposition.ACCEPTED,
                deadline.disposition());
        assertEquals(RunStatus.FAILED, deadline.status());
        assertEquals(IncrementalFrontierAuthorityState.READY,
                timedOut.store.record(timedOut.feedId).state());
        assertNull(timedOut.store.record(timedOut.feedId).activeInterval());
        assertEquals(IncrementalFrontier.begin(),
                timedOut.store.record(timedOut.feedId).committedFrontier());
    }

    @Test
    void retryClaimFaultsExposeOnlyJointBeforeOrAfterFence() throws Exception {
        AtomicLong beforeClock = new AtomicLong(800L);
        AtomicBoolean beforeArmed = new AtomicBoolean();
        InMemoryIncrementalAuthorityStore beforeStore =
                new InMemoryIncrementalAuthorityStore(phase -> {
                    if (beforeArmed.get()
                            && phase == IncrementalJointSuccessPhase.BEFORE_COMMIT) {
                        throw new InjectedJointFault();
                    }
                }, beforeClock::get);
        Fixture before = runningFixture(beforeStore, 0L, "retry-before-fault");
        recordLostAndCompletedAbort(before, "HOST_TERMINATED");
        before.store.scheduleRetry(
                before.token, before.runToken,
                before.request.firstAttemptFence());
        beforeClock.set(1_800L);
        IncrementalAuthorityRecord beforeRecord = before.store.record(before.feedId);
        RunSnapshot beforeRun = before.store.runSnapshot(before.request.runId());
        beforeArmed.set(true);
        org.junit.jupiter.api.Assertions.assertThrows(
                InjectedJointFault.class,
                () -> before.store.claimDueRetry(
                        before.token, before.runToken,
                        before.request.runId()));
        assertSame(beforeRecord, before.store.record(before.feedId));
        assertSame(beforeRun, before.store.runSnapshot(before.request.runId()));

        AtomicLong afterClock = new AtomicLong(900L);
        AtomicBoolean afterArmed = new AtomicBoolean();
        InMemoryIncrementalAuthorityStore afterStore =
                new InMemoryIncrementalAuthorityStore(phase -> {
                    if (afterArmed.get() && phase
                            == IncrementalJointSuccessPhase.AFTER_COMMIT_BEFORE_ACK) {
                        throw new InjectedJointFault();
                    }
                }, afterClock::get);
        Fixture after = runningFixture(afterStore, 0L, "retry-after-fault");
        recordLostAndCompletedAbort(after, "HOST_TERMINATED");
        after.store.scheduleRetry(
                after.token, after.runToken,
                after.request.firstAttemptFence());
        afterClock.set(1_900L);
        afterArmed.set(true);
        org.junit.jupiter.api.Assertions.assertThrows(
                InjectedJointFault.class,
                () -> after.store.claimDueRetry(
                        after.token, after.runToken,
                        after.request.runId()));
        IncrementalAuthorityRecord retried = after.store.record(after.feedId);
        RunSnapshot retriedRun = after.store.runSnapshot(after.request.runId());
        assertEquals(2, retried.activeInterval().attemptOrdinal());
        assertEquals(retried.activeInterval().fence().attemptId(),
                retriedRun.currentAttempt().attemptId());
        afterArmed.set(false);
        assertEquals(RetryAttemptClaim.Code.ALREADY_CLAIMED,
                after.store.claimDueRetry(
                        after.token, after.runToken,
                        after.request.runId()).code());
        assertSame(retried, after.store.record(after.feedId));
        assertSame(retriedRun, after.store.runSnapshot(after.request.runId()));
    }

    private static void assertAbortClose(
            Fixture fixture, AttemptStatus terminal, String reasonCode)
            throws Exception {
        recordCompletedAbort(fixture, reasonCode);
        VerifiedIncrementalAbortClosureProof proof = fixture.store.verifyAbortClosure(
                fixture.token, fixture.runToken,
                fixture.request.firstAttemptFence(), terminal);
        IncrementalAuthorityRecord before = fixture.store.record(fixture.feedId);
        IncrementalAuthorityCommand close = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.CLOSE_ABORTED_RUN,
                fixture.feedId, "close-" + terminal.name(), before.revision(), 1L,
                fixture.request.intervalId(), fixture.request.firstAttemptFence(),
                proof.proofDigest());
        IncrementalAuthorityDecision decision = fixture.store.apply(
                IncrementalAuthorityMutation.closeAborted(
                        close, fixture.token, fixture.runToken, proof));
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED, decision.code());
        assertEquals(IncrementalFrontierAuthorityState.READY,
                decision.record().state());
        assertNull(decision.record().activeInterval());
        assertEquals(IncrementalFrontier.begin(),
                decision.record().committedFrontier());
        assertEquals(terminal, decision.runSnapshot().currentAttempt().status());
        assertEquals(terminal == AttemptStatus.CANCELED
                        ? RunStatus.CANCELED : RunStatus.FAILED,
                decision.runSnapshot().status());
    }

    private static void recordLostAndCompletedAbort(
            Fixture fixture, String reasonCode) throws Exception {
        StageAssignmentSnapshot assignments = fixture.store.assignmentSnapshot(
                fixture.request.firstAttemptFence());
        StageAssignmentAcceptance acceptance = assignments.acceptance(0);
        assertEquals(io.rowweave.run.StageTerminalDecision.Code.RECORDED,
                fixture.store.recordTerminalRecovered(
                        fixture.token, fixture.runToken,
                        StageTerminalReceipt.lost(
                                acceptance.token(), acceptance.commandDigest(),
                                reasonCode, sha256("lost-detail"), 0L)).code());
        recordCompletedAbort(fixture, "RETRY_ROLLBACK");
    }

    private static void recordCompletedAbort(
            Fixture fixture, String reasonCode) throws Exception {
        BatchSinkAbortRecord decided = BatchSinkAbortRecord.decided(
                fixture.request.firstAttemptFence(), 1L,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", 9L, fixture.request.publicationId(),
                sha256("abort-" + fixture.feedId), reasonCode, 500L);
        assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                fixture.store.recordBatchSinkAbortRecovered(
                        fixture.token, fixture.runToken, decided).code());
        assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                fixture.store.recordBatchSinkAbortRecovered(
                        fixture.token, fixture.runToken,
                        decided.completed(501L)).code());
    }

    private static Fixture runningFixture(
            AtomicLong clock, long deadline, String suffix) throws Exception {
        return runningFixture(
                new InMemoryIncrementalAuthorityStore(clock::get),
                deadline, suffix);
    }

    private static Fixture runningFixture(
            InMemoryIncrementalAuthorityStore store,
            long deadline, String suffix) throws Exception {
        String feedId = "feed-" + suffix;
        IncrementalInitializeRequest initialize = new IncrementalInitializeRequest(
                feedId, 1L, sha256("definition"), sha256("source-contract"),
                sha256("frontier-contract"),
                IncrementalFrontier.Kind.INT64_SEQUENCE_V1);
        IncrementalAuthorityCommand initializeCommand =
                new IncrementalAuthorityCommand(
                        IncrementalAuthorityCommandKind.INITIALIZE,
                        feedId, "initialize-" + suffix, 0L, 1L,
                        null, null, initialize.transitionDigest());
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                store.apply(IncrementalAuthorityMutation.initialize(
                        initializeCommand, initialize)).code());
        FrontierOwnershipClaim ownership = store.claimOwnership(
                feedId, 0L, 0L, new RecoveryOwner("master", 1L));

        CapturedTargetReceipt capture = CapturedTargetReceipt.normal(
                1L, sha256("definition"), sha256("source-contract"), 0L,
                IncrementalFrontier.begin(), "trigger-" + suffix,
                IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(10L),
                bytes("snapshot-" + suffix));
        IncrementalClaimRequest request = new IncrementalClaimRequest(
                feedId, capture, sha256("frontier-contract"),
                "artifact-" + suffix, "attempt-1", "publication-1",
                3, deadline, 10L);
        store.publishPending(ownership.token(), request);
        IncrementalAuthorityCommand claim = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.CLAIM, feedId,
                "claim-" + suffix, 0L, 1L, request.intervalId(),
                request.firstAttemptFence(), request.transitionDigest());
        store.apply(IncrementalAuthorityMutation.claim(
                claim, ownership.token(), request));

        StageAssignmentRecord assignment = StageAssignmentRecord.create(
                request.firstAttemptFence(), 1L, 0, 101L, 0, 0,
                StageAssignmentRole.SINK, "worker", "localhost", 7001,
                -1, "worker-incarnation");
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                request.firstAttemptFence(), request.artifactId(), 1L,
                AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                Collections.singletonList(assignment));
        StageAssignmentSnapshot assignments = store.publish(manifest).snapshot();
        StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                assignments.token(0), sha256("worker-command"), 20L);
        store.accept(acceptance);
        RunSnapshot run = store.runSnapshot(request.runId());
        for (AttemptStatus target : new AttemptStatus[]{
                AttemptStatus.DISPATCHING, AttemptStatus.RUNNING}) {
            RunDecision decision = store.advanceProgress(
                    ownership.token(), new IncrementalAttemptProgressCommand(
                            feedId, request.intervalId(),
                            store.record(feedId).revision(), run.revision(),
                            request.firstAttemptFence(), target,
                            "progress-" + suffix + '-' + target,
                            run.updatedAtMillis() + 1L));
            assertEquals(RunDecision.Code.ACCEPTED, decision.code());
            run = decision.snapshot();
        }
        RecoveryClaim runOwnership = store.claim(
                request.runId(), run.revision(), 0L, ownership.token().owner());
        return new Fixture(
                store, feedId, ownership.token(), runOwnership.token(), request);
    }

    private static byte[] sha256(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(bytes(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final class Fixture {
        private final InMemoryIncrementalAuthorityStore store;
        private final String feedId;
        private final FrontierRecoveryToken token;
        private final RecoveryToken runToken;
        private final IncrementalClaimRequest request;

        private Fixture(
                InMemoryIncrementalAuthorityStore store, String feedId,
                FrontierRecoveryToken token, RecoveryToken runToken,
                IncrementalClaimRequest request) {
            this.store = store;
            this.feedId = feedId;
            this.token = token;
            this.runToken = runToken;
            this.request = request;
        }
    }

    private static final class InjectedJointFault extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
