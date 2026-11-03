package io.rowweave.incremental;

import io.rowweave.run.AttemptStatus;
import io.rowweave.run.AttemptOutputProfile;
import io.rowweave.run.BatchSinkAbortDecision;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.BatchSinkAbortRecordCodec;
import io.rowweave.run.InMemoryRunStateStore;
import io.rowweave.run.ManifestCommitIntent;
import io.rowweave.run.ManifestCommitJournalCodec;
import io.rowweave.run.ManifestCommitJournalSnapshot;
import io.rowweave.run.ManifestIntentDecision;
import io.rowweave.run.ManifestMemberDecision;
import io.rowweave.run.ManifestMemberReceipt;
import io.rowweave.run.PreparedSinkDecision;
import io.rowweave.run.PreparedSinkReceipt;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RetryAttemptClaim;
import io.rowweave.run.RetryReadyEvaluation;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunCancellationAuthorityKernel;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDeadlineResult;
import io.rowweave.run.RunDeadlineAuthorityKernel;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunResultSummary;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStateStoreKernel;
import io.rowweave.run.RunStatus;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.StageAcceptanceDecision;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageAssignmentManifestCodec;
import io.rowweave.run.StageManifestPublication;
import io.rowweave.run.StageTerminalDecision;
import io.rowweave.run.StageTerminalReceipt;
import io.rowweave.run.WholeAttemptRetryAuthorityKernel;
import io.rowweave.run.WholeAttemptRetryDecision;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/** Backend-neutral semantics exercised in-process with one atomic frontier/Run monitor. */
public final class InMemoryIncrementalAuthorityStore
        implements IncrementalLocalAuthority, IncrementalAuthorityGateway {
    public static final int MAX_GUARD_SHARD_FRAME_BYTES =
            FeedGuardCatalog.MAX_SHARD_FRAME_BYTES;

    private final FeedGuardCatalog guardCatalog = new FeedGuardCatalog();
    private final Map<String, FeedState> feeds = new HashMap<>();
    private final Map<String, String> runFeeds = new HashMap<>();
    private final InMemoryRunStateStore runs;
    private final IncrementalJointSuccessObserver jointSuccessObserver;
    private final LongSupplier authorityClock;
    private long nextRootIdentity = 1L;

    public InMemoryIncrementalAuthorityStore() {
        this(phase -> { }, System::currentTimeMillis);
    }

    InMemoryIncrementalAuthorityStore(LongSupplier authorityClock) {
        this(phase -> { }, authorityClock);
    }

    InMemoryIncrementalAuthorityStore(
            IncrementalJointSuccessObserver jointSuccessObserver) {
        this(jointSuccessObserver, System::currentTimeMillis);
    }

    InMemoryIncrementalAuthorityStore(
            IncrementalJointSuccessObserver jointSuccessObserver,
            LongSupplier authorityClock) {
        if (jointSuccessObserver == null) {
            throw new IllegalArgumentException("joint success observer is required");
        }
        if (authorityClock == null) {
            throw new IllegalArgumentException("authority clock is required");
        }
        this.jointSuccessObserver = jointSuccessObserver;
        this.authorityClock = authorityClock;
        this.runs = new InMemoryRunStateStore(authorityClock);
    }

    @Override
    public synchronized IncrementalAuthorityDecision apply(
            IncrementalAuthorityMutation mutation) {
        if (mutation == null) throw new IllegalArgumentException("authority mutation is required");
        switch (mutation.command().kind()) {
            case INITIALIZE:
                return initialize(mutation);
            case CLAIM:
                return claim(mutation);
            case COMMIT:
            case COMPLETE_BACKFILL:
                return jointSuccess(mutation);
            case CLOSE_ABORTED_RUN:
                return closeAborted(mutation);
            case MARK_CONFLICT:
                return markConflict(mutation);
            default:
                return IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                        record(mutation.command().feedId()), null);
        }
    }

    @Override
    public synchronized IncrementalAuthorityDecision initialize(
            String commandId, IncrementalInitializeRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("initialize request is required");
        }
        IncrementalAuthorityCommand command = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.INITIALIZE,
                request.feedId(), commandId, 0L, request.feedIncarnation(),
                null, null, request.transitionDigest());
        return apply(IncrementalAuthorityMutation.initialize(command, request));
    }

    public synchronized FrontierOwnershipClaim claimOwnership(
            String feedId, long expectedRevision, long expectedOwnershipEpoch,
            RecoveryOwner owner) {
        FeedState state = feeds.get(feedId);
        if (state == null) {
            return FrontierOwnershipClaim.rejected(
                    FrontierOwnershipClaim.Code.FEED_NOT_FOUND);
        }
        if (owner == null || expectedRevision != state.record.revision()) {
            return FrontierOwnershipClaim.rejected(
                    FrontierOwnershipClaim.Code.STALE_REVISION);
        }
        FeedGuardShardObservation observation = guardCatalog.observation(feedId);
        if (state.token != null && expectedOwnershipEpoch + 1L == state.ownershipEpoch
                && owner.equals(state.token.owner())
                && observation.equals(state.token.guardShardObservation())) {
            return FrontierOwnershipClaim.replayed(state.token);
        }
        if (expectedOwnershipEpoch != state.ownershipEpoch) {
            return FrontierOwnershipClaim.rejected(
                    FrontierOwnershipClaim.Code.LOST_RACE);
        }
        long nextEpoch;
        try {
            nextEpoch = Math.addExact(state.ownershipEpoch, 1L);
        } catch (ArithmeticException exhausted) {
            return FrontierOwnershipClaim.rejected(
                    FrontierOwnershipClaim.Code.LOST_RACE);
        }
        FrontierRecoveryToken token = FrontierRecoveryToken.create(
                feedId, owner, nextEpoch, state.record.revision(),
                state.record.feedIncarnation(), observation, state.rootIdentity);
        state.ownershipEpoch = nextEpoch;
        state.token = token;
        return FrontierOwnershipClaim.acquired(token);
    }

    @Override
    public synchronized FrontierRecoveryToken frontierOwnership(String feedId) {
        FeedState state = feeds.get(feedId);
        return state == null ? null : state.token;
    }

    public synchronized IncrementalPendingRunDecision publishPending(
            FrontierRecoveryToken token, IncrementalClaimRequest request) {
        return publishPending(token, request, null);
    }

    synchronized IncrementalPendingRunDecision publishPending(
            FrontierRecoveryToken token, IncrementalClaimRequest request,
            RunSnapshot historicalRun) {
        if (request == null) throw new IllegalArgumentException("claim request is required");
        if (historicalRun != null
                && !request.runId().equals(historicalRun.runId())) {
            throw new IllegalArgumentException(
                    "historical Run identity does not match pending request");
        }
        FeedState state = feeds.get(request.feedId());
        if (state == null) {
            return pending(IncrementalPendingRunDecision.Code.FEED_NOT_FOUND, null);
        }
        if (!owns(state, token)) {
            return pending(IncrementalPendingRunDecision.Code.STALE_OWNERSHIP, null);
        }
        if (state.record.activeInterval() != null) {
            return pending(IncrementalPendingRunDecision.Code.ACTIVE_OR_PENDING_EXISTS, null);
        }
        if (state.pending != null) {
            return state.pending.equals(request)
                    ? pending(IncrementalPendingRunDecision.Code.ALREADY_PUBLISHED,
                    runs.get(request.runId()))
                    : pending(IncrementalPendingRunDecision.Code.PENDING_CONFLICT,
                    runs.get(state.pending.runId()));
        }
        IncrementalPendingRunDecision.Code admission =
                pendingAdmission(state.record, request);
        if (admission != null) return pending(admission, null);
        RunSnapshot existing = runs.get(request.runId());
        if (existing == null) existing = historicalRun;
        if (existing != null) {
            return pending(IncrementalPendingRunDecision.Code.RUN_CONFLICT, existing);
        }
        RunDecision created = runs.apply(RunCommand.createRun(
                "incremental-pending-" + request.intervalId(), request.runId(),
                hex(request.capturedTarget().definitionDigest()), request.artifactId(),
                request.maxAttempts(), request.deadlineEpochMillis(),
                request.eventTimeMillis()));
        if (created.code() != RunDecision.Code.ACCEPTED) {
            return pending(IncrementalPendingRunDecision.Code.RUN_CONFLICT,
                    created.snapshot());
        }
        state.pending = request;
        runFeeds.put(request.runId(), request.feedId());
        return pending(IncrementalPendingRunDecision.Code.PUBLISHED, created.snapshot());
    }

    public synchronized IncrementalAuthorityRecord record(String feedId) {
        FeedState state = feeds.get(feedId);
        return state == null ? null : state.record;
    }

    @Override
    public synchronized IncrementalAuthorityDecision claimInterval(
            String commandId, FrontierRecoveryToken token,
            IncrementalClaimRequest request) {
        if (token == null || request == null) {
            throw new IllegalArgumentException(
                    "frontier token and claim request are required");
        }
        FeedState state = feeds.get(request.feedId());
        if (state == null) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.FEED_NOT_FOUND,
                    null, runs.get(request.runId()));
        }
        IncrementalAuthorityCommand command = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.CLAIM, request.feedId(),
                commandId, state.record.revision(),
                state.record.feedIncarnation(), request.intervalId(),
                request.firstAttemptFence(), request.transitionDigest());
        return apply(IncrementalAuthorityMutation.claim(command, token, request));
    }

    public synchronized IncrementalClaimRequest pending(String feedId) {
        FeedState state = feeds.get(feedId);
        return state == null ? null : state.pending;
    }

    public synchronized RunSnapshot runSnapshot(String runId) {
        return runs.get(runId);
    }

    @Override
    public synchronized IncrementalAuthorityRecord activeRecordForRun(
            String runId) {
        String feedId = runFeeds.get(runId);
        FeedState state = feedId == null ? null : feeds.get(feedId);
        IncrementalActiveInterval active = state == null
                ? null : state.record.activeInterval();
        return active != null && active.fence().runId().equals(runId)
                ? state.record : null;
    }

    @Override
    public synchronized RunCancellationResult requestUserCancellation(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            String runId, String requestId) {
        FeedState state = feedForRun(frontierToken, runToken, runId);
        RunSnapshot current = runs.get(runId);
        if (state == null) {
            return new RunCancellationResult(
                    runId, current == null
                    ? RunCancellationResult.Disposition.NOT_FOUND
                    : RunCancellationResult.Disposition.TOO_LATE,
                    current == null ? null : current.status(),
                    current == null ? 0L : current.revision());
        }
        IncrementalActiveInterval active = state.record.activeInterval();
        if (active != null
                && active.phase() != IncrementalAttemptCommitPhase.COMMIT_DECIDED
                && current != null && current.status() == RunStatus.READY
                && WholeAttemptRetryAuthorityKernel.retryWait(current) != null) {
            final IncrementalAuthorityRecord closed;
            try {
                closed = abortedRecord(state.record);
            } catch (ArithmeticException exhausted) {
                return new RunCancellationResult(
                        runId, RunCancellationResult.Disposition.TOO_LATE,
                        current.status(), current.revision());
            }
            RunCancellationResult result =
                    observeThenCancelReady(runId, requestId);
            if (result.disposition()
                    == RunCancellationResult.Disposition.ACCEPTED) {
                state.record = closed;
                clearLastFrontierCommand(state);
                state.lastTerminalRunId = runId;
                runFeeds.remove(runId);
                jointSuccessObserver.observe(
                        IncrementalJointSuccessPhase.AFTER_COMMIT_BEFORE_ACK);
            }
            return result;
        }
        if (active != null
                && active.phase() == IncrementalAttemptCommitPhase.COMMIT_DECIDED
                && current != null && current.currentAttempt() != null
                && current.currentAttempt().status() == AttemptStatus.COMMITTING) {
            String commandId = RunCancellationAuthorityKernel.commandId(
                    RunCancellationAuthorityKernel.validateRunId(runId),
                    RunCancellationAuthorityKernel.validateRequestId(requestId));
            RunDecision decision = runs.applyRecovered(runToken,
                    RunCommand.requestCancellation(
                            commandId, runId, current.revision(),
                            current.currentAttempt().attemptId(),
                            current.currentAttempt().fenceToken(),
                            io.rowweave.run.CancellationCause.USER,
                            Math.max(current.updatedAtMillis(),
                                    authorityClock.getAsLong()),
                            RunCancellationAuthorityKernel.REASON_CODE, null));
            RunSnapshot after = decision.snapshot();
            return new RunCancellationResult(
                    runId, decision.code() == RunDecision.Code.ACCEPTED
                    ? RunCancellationResult.Disposition.ACCEPTED
                    : RunCancellationResult.Disposition.ALREADY_REQUESTED,
                    after.status(), after.revision());
        }
        return runs.requestUserCancellation(runId, requestId);
    }

    @Override
    public synchronized RunDeadlineResult evaluateDeadline(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            String runId) {
        FeedState state = feedForRun(frontierToken, runToken, runId);
        RunSnapshot current = runs.get(runId);
        if (state == null) {
            long observed = authorityClock.getAsLong();
            RunDeadlineResult.Disposition disposition = current == null
                    ? RunDeadlineResult.Disposition.NOT_FOUND
                    : current.deadlineEpochMillis() == 0L
                    ? RunDeadlineResult.Disposition.NO_DEADLINE
                    : RunDeadlineResult.Disposition.TOO_LATE;
            return new RunDeadlineResult(
                    runId, disposition,
                    current == null ? null : current.status(),
                    current == null ? 0L : current.revision(),
                    current == null ? 0L : current.deadlineEpochMillis(),
                    current == null || disposition
                            == RunDeadlineResult.Disposition.NO_DEADLINE
                            ? 0L : observed);
        }
        IncrementalActiveInterval active = state.record.activeInterval();
        long observed = authorityClock.getAsLong();
        if (active != null
                && active.phase() != IncrementalAttemptCommitPhase.COMMIT_DECIDED
                && current != null && current.status() == RunStatus.READY
                && WholeAttemptRetryAuthorityKernel.retryWait(current) != null
                && current.deadlineEpochMillis() > 0L
                && observed >= current.deadlineEpochMillis()) {
            final IncrementalAuthorityRecord closed;
            try {
                closed = abortedRecord(state.record);
            } catch (ArithmeticException exhausted) {
                return new RunDeadlineResult(
                        runId, RunDeadlineResult.Disposition.TOO_LATE,
                        current.status(), current.revision(),
                        current.deadlineEpochMillis(), observed);
            }
            jointSuccessObserver.observe(IncrementalJointSuccessPhase.BEFORE_COMMIT);
            RunDeadlineResult result = runs.evaluateDeadline(runId);
            if (result.disposition() == RunDeadlineResult.Disposition.ACCEPTED) {
                state.record = closed;
                clearLastFrontierCommand(state);
                state.lastTerminalRunId = runId;
                runFeeds.remove(runId);
                jointSuccessObserver.observe(
                        IncrementalJointSuccessPhase.AFTER_COMMIT_BEFORE_ACK);
            }
            return result;
        }
        if (active != null
                && active.phase() == IncrementalAttemptCommitPhase.COMMIT_DECIDED
                && current != null && current.deadlineEpochMillis() > 0L
                && observed >= current.deadlineEpochMillis()
                && current.currentAttempt() != null
                && current.currentAttempt().status() == AttemptStatus.COMMITTING) {
            RunDecision decision = runs.applyRecovered(runToken,
                    RunCommand.requestCancellation(
                            RunDeadlineAuthorityKernel.commandId(current),
                            runId, current.revision(),
                            current.currentAttempt().attemptId(),
                            current.currentAttempt().fenceToken(),
                            io.rowweave.run.CancellationCause.TIMEOUT,
                            Math.max(current.updatedAtMillis(), observed),
                            RunDeadlineAuthorityKernel.REASON_CODE, null));
            RunSnapshot after = decision.snapshot();
            return new RunDeadlineResult(
                    runId, decision.code() == RunDecision.Code.ACCEPTED
                    ? RunDeadlineResult.Disposition.ACCEPTED
                    : RunDeadlineResult.Disposition.ALREADY_REQUESTED,
                    after.status(), after.revision(),
                    after.deadlineEpochMillis(), observed);
        }
        return runs.evaluateDeadline(runId);
    }

    @Override
    public synchronized WholeAttemptRetryDecision scheduleRetry(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            RunAttemptFence fence) {
        RunSnapshot current = fence == null ? null : runs.get(fence.runId());
        FeedState state = fence == null ? null
                : feedForRun(frontierToken, runToken, fence.runId());
        if (state == null || state.record.state()
                != IncrementalFrontierAuthorityState.ACTIVE
                || state.record.activeInterval() == null
                || !state.record.activeInterval().fence().equals(fence)) {
            return WholeAttemptRetryAuthorityKernel.staleRecoverySchedule(current);
        }
        if (state.record.activeInterval().phase()
                == IncrementalAttemptCommitPhase.COMMIT_DECIDED) {
            return WholeAttemptRetryAuthorityKernel.tooLateSchedule(current);
        }
        IncrementalAuthorityRecord exhaustedClosure = null;
        if (current != null
                && current.attemptCount() >= current.maxAttempts()) {
            try {
                exhaustedClosure = abortedRecord(state.record);
            } catch (ArithmeticException exhausted) {
                return WholeAttemptRetryAuthorityKernel.unavailableSchedule(current);
            }
        }
        if (exhaustedClosure != null) {
            jointSuccessObserver.observe(IncrementalJointSuccessPhase.BEFORE_COMMIT);
        }
        WholeAttemptRetryDecision decision =
                runs.scheduleRetryRecovered(runToken, fence);
        if (decision.code() == WholeAttemptRetryDecision.Code.EXHAUSTED) {
            if (exhaustedClosure == null) {
                throw new IllegalStateException(
                        "retry exhausted without reserved frontier closure");
            }
            state.record = exhaustedClosure;
            clearLastFrontierCommand(state);
            state.lastTerminalRunId = fence.runId();
            runFeeds.remove(fence.runId());
            jointSuccessObserver.observe(
                    IncrementalJointSuccessPhase.AFTER_COMMIT_BEFORE_ACK);
        }
        return decision;
    }

    @Override
    public synchronized RetryReadyEvaluation evaluateRetryReady(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            String runId) {
        FeedState state = feedForRun(frontierToken, runToken, runId);
        if (state == null || state.record.state()
                != IncrementalFrontierAuthorityState.ACTIVE) {
            return WholeAttemptRetryAuthorityKernel.staleRecoveryReady(
                    runs.get(runId));
        }
        return runs.evaluateRetryReady(runId);
    }

    @Override
    public synchronized RetryAttemptClaim claimDueRetry(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            String runId) {
        FeedState state = feedForRun(frontierToken, runToken, runId);
        RunSnapshot before = runs.get(runId);
        if (state == null || state.record.state()
                != IncrementalFrontierAuthorityState.ACTIVE
                || state.record.activeInterval() == null) {
            return WholeAttemptRetryAuthorityKernel.staleRecoveryClaim(before);
        }
        IncrementalActiveInterval active = state.record.activeInterval();
        if (state.record.revision() == Long.MAX_VALUE
                || before == null || before.revision() == Long.MAX_VALUE) {
            return WholeAttemptRetryAuthorityKernel.unavailableClaim(before);
        }
        if (before != null && before.currentAttempt() != null) {
            RunAttemptFence currentFence = new RunAttemptFence(
                    runId, before.currentAttempt().attemptId(),
                    before.currentAttempt().fenceToken());
            if (active.fence().equals(currentFence)
                    && before.status() == RunStatus.RUNNING
                    && before.attemptCount() > 1) {
                return runs.claimDueRetryRecovered(runToken, runId);
            }
        }
        RetryReadyEvaluation readiness = runs.evaluateRetryReady(runId);
        if (readiness.code() == RetryReadyEvaluation.Code.TOO_LATE) {
            evaluateDeadline(frontierToken, runToken, runId);
            return WholeAttemptRetryAuthorityKernel.tooLateClaim(runs.get(runId));
        }
        IncrementalAuthorityRecord projected = null;
        RunAttemptFence projectedFence = null;
        if (readiness.code() == RetryReadyEvaluation.Code.DUE
                && readiness.identity() != null) {
            projectedFence = new RunAttemptFence(
                    runId, readiness.identity().attemptId(),
                    readiness.identity().fenceToken());
            try {
                IncrementalActiveInterval retried = new IncrementalActiveInterval(
                        active.identity(), active.capturedTarget(), active.artifactId(),
                        active.claimedRevision(), projectedFence,
                        active.attemptOrdinal() + 1,
                        IncrementalAttemptCommitPhase.CLAIMED, null);
                projected = withActive(state.record, retried);
            } catch (ArithmeticException | IllegalArgumentException exhausted) {
                return WholeAttemptRetryAuthorityKernel.unavailableClaim(before);
            }
        }
        if (projected != null) {
            jointSuccessObserver.observe(IncrementalJointSuccessPhase.BEFORE_COMMIT);
        }
        RetryAttemptClaim claim = runs.claimDueRetryRecovered(runToken, runId);
        if ((claim.code() == RetryAttemptClaim.Code.CLAIMED
                || claim.code() == RetryAttemptClaim.Code.ALREADY_CLAIMED)
                && claim.snapshot() != null
                && claim.snapshot().currentAttempt() != null) {
            RunAttemptFence nextFence = new RunAttemptFence(
                    runId, claim.snapshot().currentAttempt().attemptId(),
                    claim.snapshot().currentAttempt().fenceToken());
            if (!active.fence().equals(nextFence)) {
                if (projected == null || !nextFence.equals(projectedFence)) {
                    throw new IllegalStateException(
                            "retry claim differs from its reserved frontier projection");
                }
                state.record = projected;
                clearLastFrontierCommand(state);
                jointSuccessObserver.observe(
                        IncrementalJointSuccessPhase.AFTER_COMMIT_BEFORE_ACK);
            }
        }
        return claim;
    }

    @Override
    public synchronized RunDecision advanceProgress(
            FrontierRecoveryToken token,
            IncrementalAttemptProgressCommand command) {
        return advanceProgress(token, null, command, false);
    }

    @Override
    public synchronized RunDecision advanceProgress(
            FrontierRecoveryToken token, RecoveryToken runToken,
            IncrementalAttemptProgressCommand command) {
        if (runToken == null) {
            throw new IllegalArgumentException("Run recovery token is required");
        }
        return advanceProgress(token, runToken, command, true);
    }

    private RunDecision advanceProgress(
            FrontierRecoveryToken token, RecoveryToken runToken,
            IncrementalAttemptProgressCommand command, boolean recovered) {
        if (command == null) {
            throw new IllegalArgumentException("progress command is required");
        }
        FeedState state = feeds.get(command.feedId());
        RunSnapshot current = runs.get(command.fence().runId());
        if (state == null) {
            return current == null
                    ? RunStateStoreKernel.evaluate(null, runProgress(command))
                    : RunStateStoreKernel.staleRecoveryOwner(current);
        }
        if (current == null) {
            return RunStateStoreKernel.evaluate(current, runProgress(command));
        }
        if (!owns(state, token)) {
            return RunStateStoreKernel.staleRecoveryOwner(current);
        }
        if (recovered && !recoveryOwns(state, token, runToken)) {
            return RunStateStoreKernel.staleRecoveryOwner(current);
        }
        if (command.expectedFrontierRevision() != state.record.revision()) {
            return staleRunRevision(current, command);
        }
        IncrementalActiveInterval active = state.record.activeInterval();
        if (state.record.state() != IncrementalFrontierAuthorityState.ACTIVE
                || active == null
                || !active.identity().intervalId().equals(command.intervalId())
                || !active.fence().equals(command.fence())) {
            return staleRunFence(current, command);
        }
        if (runs.batchSinkAbort(command.fence()) != null) {
            return RunStateStoreKernel.illegalTransition(current);
        }
        RunDecision decision = recovered
                ? runs.applyRecovered(runToken, runProgress(command))
                : runs.apply(runProgress(command));
        if (decision.code() == RunDecision.Code.ACCEPTED) {
            clearLastFrontierCommand(state);
        }
        return decision;
    }

    @Override
    public synchronized StageManifestPublication publish(
            StageAssignmentManifest manifest) {
        return publish(null, null, manifest, false);
    }

    @Override
    public synchronized StageManifestPublication publishRecovered(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageAssignmentManifest manifest) {
        requireRecoveryTokens(frontierToken, runToken);
        return publish(frontierToken, runToken, manifest, true);
    }

    private StageManifestPublication publish(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageAssignmentManifest manifest, boolean recovered) {
        if (manifest == null) throw new IllegalArgumentException("manifest is required");
        FeedState recoveredState = recovered
                ? recoveredFeed(frontierToken, runToken) : null;
        if (recovered && recoveredState == null) {
            return StageManifestPublication.rejected(
                    StageManifestPublication.Code.RECOVERY_OWNED);
        }
        if (manifest.outputProfile()
                != AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1) {
            return StageManifestPublication.rejected(
                    StageManifestPublication.Code.CONFLICT);
        }
        FeedState state = activeFeed(manifest.fence());
        if (state == null || !state.record.activeInterval().artifactId()
                .equals(manifest.artifactId())) {
            return StageManifestPublication.rejected(
                    StageManifestPublication.Code.STALE_RUN);
        }
        if (recovered && state != recoveredState) {
            return StageManifestPublication.rejected(
                    StageManifestPublication.Code.RECOVERY_OWNED);
        }
        return recovered
                ? runs.publishRecovered(runToken, manifest) : runs.publish(manifest);
    }

    @Override
    public synchronized StageAssignmentSnapshot assignmentSnapshot(
            io.rowweave.run.RunAttemptFence fence) {
        return runs.assignmentSnapshot(fence);
    }

    @Override
    public synchronized StageAcceptanceDecision accept(
            StageAssignmentAcceptance acceptance) {
        return accept(null, null, acceptance, false);
    }

    @Override
    public synchronized StageAcceptanceDecision acceptRecovered(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageAssignmentAcceptance acceptance) {
        requireRecoveryTokens(frontierToken, runToken);
        return accept(frontierToken, runToken, acceptance, true);
    }

    private StageAcceptanceDecision accept(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageAssignmentAcceptance acceptance, boolean recovered) {
        if (acceptance == null) throw new IllegalArgumentException("acceptance is required");
        FeedState recoveredState = recovered
                ? recoveredFeed(frontierToken, runToken) : null;
        if (recovered && recoveredState == null) {
            return StageAcceptanceDecision.rejected(
                    StageAcceptanceDecision.Code.RECOVERY_OWNED);
        }
        FeedState state = activeFeed(acceptance.token().fence());
        if (state == null) {
            return StageAcceptanceDecision.rejected(
                    StageAcceptanceDecision.Code.STALE_RUN);
        }
        if (recovered && state != recoveredState) {
            return StageAcceptanceDecision.rejected(
                    StageAcceptanceDecision.Code.RECOVERY_OWNED);
        }
        return recovered
                ? runs.acceptRecovered(runToken, acceptance) : runs.accept(acceptance);
    }

    @Override
    public synchronized StageTerminalDecision recordTerminal(
            StageTerminalReceipt receipt) {
        return recordTerminal(null, null, receipt, false);
    }

    @Override
    public synchronized StageTerminalDecision recordTerminalRecovered(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageTerminalReceipt receipt) {
        requireRecoveryTokens(frontierToken, runToken);
        return recordTerminal(frontierToken, runToken, receipt, true);
    }

    private StageTerminalDecision recordTerminal(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageTerminalReceipt receipt, boolean recovered) {
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        FeedState recoveredState = recovered
                ? recoveredFeed(frontierToken, runToken) : null;
        if (recovered && recoveredState == null) {
            return StageTerminalDecision.rejected(
                    StageTerminalDecision.Code.RECOVERY_OWNED);
        }
        FeedState state = activeFeed(receipt.token().fence());
        if (state == null) {
            return StageTerminalDecision.rejected(
                    StageTerminalDecision.Code.STALE_RUN);
        }
        if (recovered && state != recoveredState) {
            return StageTerminalDecision.rejected(
                    StageTerminalDecision.Code.RECOVERY_OWNED);
        }
        return recovered
                ? runs.recordTerminalRecovered(runToken, receipt)
                : runs.recordTerminal(receipt);
    }

    @Override
    public synchronized PreparedSinkDecision recordPreparedSink(
            PreparedSinkReceipt receipt) {
        return recordPreparedSink(null, null, receipt, false);
    }

    @Override
    public synchronized PreparedSinkDecision recordPreparedSinkRecovered(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            PreparedSinkReceipt receipt) {
        requireRecoveryTokens(frontierToken, runToken);
        return recordPreparedSink(frontierToken, runToken, receipt, true);
    }

    private PreparedSinkDecision recordPreparedSink(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            PreparedSinkReceipt receipt, boolean recovered) {
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        FeedState recoveredState = recovered
                ? recoveredFeed(frontierToken, runToken) : null;
        if (recovered && recoveredState == null) {
            return PreparedSinkDecision.rejected(
                    PreparedSinkDecision.Code.RECOVERY_OWNED);
        }
        FeedState state = activeFeed(receipt.token().fence());
        if (state == null) {
            return PreparedSinkDecision.rejected(
                    PreparedSinkDecision.Code.STALE_RUN);
        }
        if (recovered && state != recoveredState) {
            return PreparedSinkDecision.rejected(
                    PreparedSinkDecision.Code.RECOVERY_OWNED);
        }
        if (phaseProjectionExhausted(state.record)) {
            return PreparedSinkDecision.rejected(
                    PreparedSinkDecision.Code.UNAVAILABLE);
        }
        PreparedSinkDecision decision = recovered
                ? runs.recordPreparedSinkRecovered(runToken, receipt)
                : runs.recordPreparedSink(receipt);
        if ((decision.code() == PreparedSinkDecision.Code.RECORDED
                || decision.code() == PreparedSinkDecision.Code.ALREADY_RECORDED)
                && runs.batchSinkAbort(receipt.token().fence()) == null) {
            projectPreparedIfComplete(state, receipt.token().fence());
        }
        return decision;
    }

    @Override
    public synchronized ManifestCommitJournalSnapshot manifestCommitJournal(
            io.rowweave.run.RunAttemptFence fence) {
        return runs.manifestCommitJournal(fence);
    }

    @Override
    public synchronized BatchSinkAbortRecord batchSinkAbort(
            io.rowweave.run.RunAttemptFence fence) {
        return runs.batchSinkAbort(fence);
    }

    @Override
    public synchronized BatchSinkAbortDecision recordBatchSinkAbort(
            BatchSinkAbortRecord record) {
        return recordBatchSinkAbort(null, null, record, false);
    }

    @Override
    public synchronized BatchSinkAbortDecision recordBatchSinkAbortRecovered(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            BatchSinkAbortRecord record) {
        requireRecoveryTokens(frontierToken, runToken);
        return recordBatchSinkAbort(frontierToken, runToken, record, true);
    }

    private BatchSinkAbortDecision recordBatchSinkAbort(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            BatchSinkAbortRecord record, boolean recovered) {
        if (record == null) {
            throw new IllegalArgumentException("abort record is required");
        }
        FeedState recoveredState = recovered
                ? recoveredFeed(frontierToken, runToken) : null;
        if (recovered && recoveredState == null) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.RECOVERY_OWNED);
        }
        FeedState state = activeFeed(record.fence());
        if (state == null) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.STALE_RUN);
        }
        if (recovered && state != recoveredState) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.RECOVERY_OWNED);
        }
        RunSnapshot run = runs.get(record.fence().runId());
        if (run == null) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.STALE_RUN);
        }
        if (runs.batchSinkAbort(record.fence()) == null
                && !hasAbortClosureCapacity(state.record, run)) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.RECORD_CONFLICT);
        }
        return recovered
                ? runs.recordBatchSinkAbortRecovered(runToken, record)
                : runs.recordBatchSinkAbort(record);
    }

    private static boolean hasAbortClosureCapacity(
            IncrementalAuthorityRecord record, RunSnapshot run) {
        return record.revision() != Long.MAX_VALUE
                && run.revision() != Long.MAX_VALUE
                && run.updatedAtMillis() != Long.MAX_VALUE;
    }

    @Override
    public synchronized ManifestMemberDecision recordManifestMember(
            ManifestMemberReceipt receipt) {
        return recordManifestMember(null, null, receipt, false);
    }

    @Override
    public synchronized ManifestMemberDecision recordManifestMemberRecovered(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            ManifestMemberReceipt receipt) {
        requireRecoveryTokens(frontierToken, runToken);
        return recordManifestMember(frontierToken, runToken, receipt, true);
    }

    private ManifestMemberDecision recordManifestMember(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            ManifestMemberReceipt receipt, boolean recovered) {
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        FeedState recoveredState = recovered
                ? recoveredFeed(frontierToken, runToken) : null;
        if (recovered && recoveredState == null) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.RECOVERY_OWNED);
        }
        FeedState state = activeFeed(receipt.token().fence());
        if (state == null) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.STALE_RUN);
        }
        if (recovered && state != recoveredState) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.RECOVERY_OWNED);
        }
        if (phaseProjectionExhausted(state.record)) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.CORRUPT);
        }
        ManifestMemberDecision decision = recovered
                ? runs.recordManifestMemberRecovered(runToken, receipt)
                : runs.recordManifestMember(receipt);
        if ((decision.code() == ManifestMemberDecision.Code.RECORDED
                || decision.code() == ManifestMemberDecision.Code.ALREADY_RECORDED)
                && runs.batchSinkAbort(receipt.token().fence()) == null) {
            projectPreparedIfComplete(state, receipt.token().fence());
        }
        return decision;
    }

    @Override
    public synchronized ManifestIntentDecision commitManifestIntent(
            ManifestCommitIntent intent) {
        return commitManifestIntent(null, null, intent, false);
    }

    @Override
    public synchronized ManifestIntentDecision commitManifestIntentRecovered(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            ManifestCommitIntent intent) {
        requireRecoveryTokens(frontierToken, runToken);
        return commitManifestIntent(frontierToken, runToken, intent, true);
    }

    private ManifestIntentDecision commitManifestIntent(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            ManifestCommitIntent intent, boolean recovered) {
        if (intent == null) throw new IllegalArgumentException("intent is required");
        FeedState recoveredState = recovered
                ? recoveredFeed(frontierToken, runToken) : null;
        if (recovered && recoveredState == null) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.RECOVERY_OWNED);
        }
        FeedState state = activeFeed(intent.fence());
        if (state == null) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.STALE_RUN);
        }
        if (recovered && state != recoveredState) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.RECOVERY_OWNED);
        }
        IncrementalActiveInterval active = state.record.activeInterval();
        RunSnapshot run = runs.get(intent.fence().runId());
        ManifestCommitJournalSnapshot journal = runs.manifestCommitJournal(intent.fence());
        if (active.phase() != IncrementalAttemptCommitPhase.PREPARED
                && active.phase() != IncrementalAttemptCommitPhase.COMMIT_DECIDED) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.INCOMPLETE_MEMBERS);
        }
        if (run == null || run.currentAttempt() == null
                || run.currentAttempt().status() != AttemptStatus.COMMITTING) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.ATTEMPT_NOT_COMMITTING);
        }
        if (active.phase() == IncrementalAttemptCommitPhase.PREPARED
                && state.record.revision() >= Long.MAX_VALUE - 1L) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.CORRUPT);
        }
        if (!completePreparedClosure(journal)) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.INCOMPLETE_MEMBERS);
        }
        if (active.phase() == IncrementalAttemptCommitPhase.PREPARED) {
            try {
                if (run.revision() == Long.MAX_VALUE) {
                    throw new ArithmeticException("Run revision exhausted");
                }
                Math.addExact(run.updatedAtMillis(), 1L);
                RunResultSummary.manifestFrom(
                        journal.assignments(), intent.generation(),
                        intent.manifestHash());
            } catch (ArithmeticException | IllegalArgumentException exhausted) {
                return ManifestIntentDecision.rejected(
                        ManifestIntentDecision.Code.CORRUPT);
            }
        }
        byte[] intentDigest = IncrementalAuthorityValue.sha256(
                ManifestCommitJournalCodec.encodeIntent(intent));
        IncrementalAuthorityRecord decided = active.phase()
                == IncrementalAttemptCommitPhase.COMMIT_DECIDED
                ? state.record : withActivePhase(
                state.record, IncrementalAttemptCommitPhase.COMMIT_DECIDED,
                intentDigest);
        ManifestIntentDecision decision = recovered
                ? runs.commitManifestIntentRecovered(runToken, intent)
                : runs.commitManifestIntent(intent);
        if (decision.code() == ManifestIntentDecision.Code.COMMITTED) {
            state.record = decided;
            clearLastFrontierCommand(state);
        } else if (decision.code() == ManifestIntentDecision.Code.ALREADY_COMMITTED
                && active.phase() != IncrementalAttemptCommitPhase.COMMIT_DECIDED) {
            throw new IllegalStateException(
                    "durable intent exists without its active phase projection");
        }
        return decision;
    }

    @Override
    public synchronized RecoveryToken ownership(String runId) {
        return runs.ownership(runId);
    }

    @Override
    public synchronized RecoveryClaim claim(
            String runId, long expectedRevision,
            long expectedOwnershipEpoch, RecoveryOwner owner) {
        return runs.claim(runId, expectedRevision, expectedOwnershipEpoch, owner);
    }

    @Override
    public synchronized boolean current(RecoveryToken token) {
        return runs.current(token);
    }

    @Override
    public synchronized RecoveryToken runOwnership(String runId) {
        return ownership(runId);
    }

    @Override
    public synchronized RecoveryClaim claimRunOwnership(
            FrontierRecoveryToken frontierToken, String runId,
            long expectedRunRevision, long expectedRunOwnershipEpoch,
            RecoveryOwner owner) {
        if (frontierToken == null || owner == null
                || !frontierToken.owner().equals(owner)) {
            throw new IllegalArgumentException(
                    "matching frontier and Run recovery owners are required");
        }
        FeedState state = feeds.get(frontierToken.feedId());
        IncrementalActiveInterval active = state == null
                ? null : state.record.activeInterval();
        if (!owns(state, frontierToken)) return RecoveryClaim.lostRace();
        if (active == null || !active.fence().runId().equals(runId)) {
            return RecoveryClaim.staleRun();
        }
        return runs.claim(runId, expectedRunRevision,
                expectedRunOwnershipEpoch, owner);
    }

    @Override
    public synchronized StageManifestPublication publishAssignments(
            FrontierRecoveryToken frontierToken,
            StageAssignmentManifest manifest) {
        if (!owns(feeds.get(frontierToken.feedId()), frontierToken)) {
            return StageManifestPublication.rejected(
                    StageManifestPublication.Code.RECOVERY_OWNED);
        }
        return publish(manifest);
    }

    @Override
    public synchronized StageManifestPublication publishAssignments(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageAssignmentManifest manifest) {
        return publishRecovered(frontierToken, runToken, manifest);
    }

    @Override
    public synchronized StageAcceptanceDecision acceptAssignment(
            FrontierRecoveryToken frontierToken,
            StageAssignmentAcceptance acceptance) {
        if (!owns(feeds.get(frontierToken.feedId()), frontierToken)) {
            return StageAcceptanceDecision.rejected(
                    StageAcceptanceDecision.Code.RECOVERY_OWNED);
        }
        return accept(acceptance);
    }

    @Override
    public synchronized StageAcceptanceDecision acceptAssignment(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageAssignmentAcceptance acceptance) {
        return acceptRecovered(frontierToken, runToken, acceptance);
    }

    @Override
    public synchronized StageTerminalDecision recordStageTerminal(
            FrontierRecoveryToken frontierToken, StageTerminalReceipt receipt) {
        if (!owns(feeds.get(frontierToken.feedId()), frontierToken)) {
            return StageTerminalDecision.rejected(
                    StageTerminalDecision.Code.RECOVERY_OWNED);
        }
        return recordTerminal(receipt);
    }

    @Override
    public synchronized StageTerminalDecision recordStageTerminal(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageTerminalReceipt receipt) {
        return recordTerminalRecovered(frontierToken, runToken, receipt);
    }

    @Override
    public synchronized PreparedSinkDecision recordPreparedSink(
            FrontierRecoveryToken frontierToken, PreparedSinkReceipt receipt) {
        if (!owns(feeds.get(frontierToken.feedId()), frontierToken)) {
            return PreparedSinkDecision.rejected(
                    PreparedSinkDecision.Code.RECOVERY_OWNED);
        }
        return recordPreparedSink(receipt);
    }

    @Override
    public synchronized PreparedSinkDecision recordPreparedSink(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            PreparedSinkReceipt receipt) {
        return recordPreparedSinkRecovered(frontierToken, runToken, receipt);
    }

    @Override
    public synchronized ManifestMemberDecision recordManifestMember(
            FrontierRecoveryToken frontierToken, ManifestMemberReceipt receipt) {
        if (!owns(feeds.get(frontierToken.feedId()), frontierToken)) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.RECOVERY_OWNED);
        }
        return recordManifestMember(receipt);
    }

    @Override
    public synchronized ManifestMemberDecision recordManifestMember(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            ManifestMemberReceipt receipt) {
        return recordManifestMemberRecovered(
                frontierToken, runToken, receipt);
    }

    @Override
    public synchronized ManifestIntentDecision commitManifestIntent(
            FrontierRecoveryToken frontierToken, ManifestCommitIntent intent) {
        if (!owns(feeds.get(frontierToken.feedId()), frontierToken)) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.RECOVERY_OWNED);
        }
        return commitManifestIntent(intent);
    }

    @Override
    public synchronized ManifestIntentDecision commitManifestIntent(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            ManifestCommitIntent intent) {
        return commitManifestIntentRecovered(frontierToken, runToken, intent);
    }

    @Override
    public synchronized BatchSinkAbortDecision recordBatchSinkAbort(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            BatchSinkAbortRecord record) {
        return recordBatchSinkAbortRecovered(frontierToken, runToken, record);
    }

    @Override
    public synchronized IncrementalAuthorityDecision jointSuccess(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            IncrementalAuthorityCommand command,
            VerifiedIncrementalJointCommitProof proof) {
        return apply(IncrementalAuthorityMutation.jointTerminal(
                command, frontierToken, runToken, proof));
    }

    @Override
    public synchronized IncrementalAuthorityDecision markConflict(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            IncrementalAuthorityCommand command,
            VerifiedIncrementalConflictProof proof) {
        return apply(IncrementalAuthorityMutation.markConflict(
                command, frontierToken, runToken, proof));
    }

    @Override
    public synchronized IncrementalAuthorityDecision closeAborted(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            IncrementalAuthorityCommand command,
            VerifiedIncrementalAbortClosureProof proof) {
        return apply(IncrementalAuthorityMutation.closeAborted(
                command, frontierToken, runToken, proof));
    }

    @Override
    public synchronized boolean currentlyOwns(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken) {
        return frontierToken != null && runToken != null
                && jointOwns(feeds.get(frontierToken.feedId()), frontierToken,
                runToken, runToken.runId());
    }

    public synchronized int guardEntryCount() { return guardCatalog.entryCount(); }
    public synchronized int guardShardEntryCount(int shard) {
        return guardCatalog.shardEntryCount(shard);
    }
    public synchronized int guardShardFrameBytes(int shard) {
        return guardCatalog.shardFrameBytes(shard);
    }

    synchronized FileBootstrapCheckpoint fileBootstrapCheckpoint() {
        FileReadyCheckpoint ready = fileReadyCheckpoint();
        if (ready == null) return null;
        for (FileBootstrapFeed feed : ready.feeds) {
            if (feed.record.lastCommitReceipt() != null
                    || feed.record.lastBackfillReceipt() != null
                    || feed.lastTerminalRunId != null) {
                return null;
            }
        }
        return new FileBootstrapCheckpoint(
                ready.nextRootIdentity, ready.feeds);
    }

    synchronized FileReadyCheckpoint fileReadyCheckpoint() {
        List<FileBootstrapFeed> checkpoint = new ArrayList<>(feeds.size());
        Map<String, FileCheckpointRun> retainedRuns = new HashMap<>();
        for (FeedState state : feeds.values()) {
            if (state.pending != null || state.record.activeInterval() != null
                    || state.record.state()
                    != IncrementalFrontierAuthorityState.READY) {
                return null;
            }
            checkpoint.add(new FileBootstrapFeed(
                    state.record, state.rootIdentity, state.ownershipEpoch,
                    state.token, state.lastCommandId, state.lastCommandDigest,
                    state.lastTerminalRunId));
            retainCheckpointRun(
                    state.record.lastCommitReceipt(), retainedRuns);
            retainCheckpointRun(
                    state.record.lastBackfillReceipt(), retainedRuns);
            retainCheckpointRun(state.lastTerminalRunId, retainedRuns);
        }
        checkpoint.sort((left, right) -> Long.compare(
                left.rootIdentity, right.rootIdentity));
        List<FileCheckpointRun> runs = new ArrayList<>(retainedRuns.values());
        runs.sort((left, right) -> left.snapshot.runId().compareTo(
                right.snapshot.runId()));
        return new FileReadyCheckpoint(nextRootIdentity, checkpoint, runs);
    }

    private void retainCheckpointRun(
            IncrementalCommitReceipt receipt,
            Map<String, FileCheckpointRun> retainedRuns) {
        if (receipt == null) return;
        retainCheckpointRun(receipt.fence().runId(), retainedRuns);
    }

    private void retainCheckpointRun(
            String runId, Map<String, FileCheckpointRun> retainedRuns) {
        if (runId == null) return;
        RunSnapshot snapshot = runs.get(runId);
        RecoveryToken token = runs.ownership(runId);
        if (snapshot == null || !snapshot.status().isTerminal()
                || token == null) {
            throw new IllegalStateException(
                    "retained receipt Run projection is unavailable");
        }
        retainedRuns.put(runId, new FileCheckpointRun(snapshot, token));
    }

    synchronized void restoreFileBootstrapCheckpoint(
            FileBootstrapCheckpoint checkpoint) {
        restoreFileReadyCheckpoint(new FileReadyCheckpoint(
                checkpoint.nextRootIdentity, checkpoint.feeds,
                java.util.Collections.emptyList()));
    }

    synchronized void restoreFileReadyCheckpoint(
            FileReadyCheckpoint checkpoint) {
        if (checkpoint == null || !feeds.isEmpty() || guardCatalog.entryCount() != 0) {
            throw new IllegalArgumentException(
                    "bootstrap checkpoint requires an empty authority");
        }
        long priorRoot = 0L;
        for (FileBootstrapFeed restored : checkpoint.feeds) {
            if (restored.record == null || restored.rootIdentity <= priorRoot
                    || restored.record.activeInterval() != null
                    || restored.record.state()
                    != IncrementalFrontierAuthorityState.READY
                    || feeds.containsKey(restored.record.feedId())) {
                throw new IllegalArgumentException(
                        "bootstrap checkpoint feed is invalid");
            }
            if ((restored.token == null) != (restored.ownershipEpoch == 0L)
                    || restored.token != null
                    && (restored.ownershipEpoch
                    != restored.token.ownershipEpoch()
                    || restored.rootIdentity
                    != restored.token.feedRootCzxid()
                    || !restored.record.feedId().equals(
                    restored.token.feedId())
                    || restored.record.feedIncarnation()
                    != restored.token.feedIncarnation())) {
                throw new IllegalArgumentException(
                        "bootstrap checkpoint ownership is invalid");
            }
            if ((restored.lastCommandId == null)
                    != (restored.lastCommandDigest == null)) {
                throw new IllegalArgumentException(
                        "bootstrap checkpoint command identity is invalid");
            }
            guardCatalog.initialize(
                    restored.record.feedId(),
                    restored.record.feedIncarnation());
            FeedState state = new FeedState(
                    restored.record, restored.rootIdentity);
            state.ownershipEpoch = restored.ownershipEpoch;
            state.token = restored.token;
            state.lastCommandId = restored.lastCommandId;
            state.lastCommandDigest = restored.lastCommandDigest == null
                    ? null : restored.lastCommandDigest.clone();
            state.lastTerminalRunId = restored.lastTerminalRunId;
            feeds.put(restored.record.feedId(), state);
            priorRoot = restored.rootIdentity;
        }
        if (checkpoint.nextRootIdentity <= priorRoot) {
            throw new IllegalArgumentException(
                    "bootstrap checkpoint next root identity is stale");
        }
        nextRootIdentity = checkpoint.nextRootIdentity;
        Map<String, Boolean> expectedRuns = new HashMap<>();
        for (FileBootstrapFeed feed : checkpoint.feeds) {
            if (feed.record.lastCommitReceipt() != null) {
                expectedRuns.put(
                        feed.record.lastCommitReceipt().fence().runId(),
                        Boolean.TRUE);
            }
            if (feed.record.lastBackfillReceipt() != null) {
                expectedRuns.put(
                        feed.record.lastBackfillReceipt().fence().runId(),
                        Boolean.TRUE);
            }
            if (feed.lastTerminalRunId != null) {
                expectedRuns.put(feed.lastTerminalRunId, Boolean.TRUE);
            }
        }
        if (checkpoint.runs.size() != expectedRuns.size()) {
            throw new IllegalArgumentException(
                    "ready checkpoint Run projection count is invalid");
        }
        for (FileCheckpointRun run : checkpoint.runs) {
            if (run == null || run.snapshot == null
                    || !expectedRuns.containsKey(run.snapshot.runId())) {
                throw new IllegalArgumentException(
                        "ready checkpoint Run projection is unreferenced");
            }
            runs.restoreTerminalCheckpoint(run.snapshot, run.token);
        }
    }
    public static int guardShardIndex(String feedId) {
        return FeedGuardCatalog.shardIndex(feedId);
    }

    private IncrementalAuthorityDecision initialize(IncrementalAuthorityMutation mutation) {
        IncrementalAuthorityCommand command = mutation.command();
        IncrementalInitializeRequest request = mutation.initializeRequest();
        if (request == null) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT, null, null);
        }
        FeedState existing = feeds.get(command.feedId());
        if (existing != null) {
            IncrementalAuthorityDecision replay = replay(existing, command);
            if (replay != null) return replay;
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.FEED_CONFLICT,
                    existing.record, null);
        }
        if (!guardCatalog.hasCapacity(command.feedId())) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.GUARD_SHARD_FULL, null, null);
        }
        if (command.expectedRevision() != 0L
                || command.feedIncarnation() != guardCatalog.nextIncarnation(command.feedId())) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.FEED_CONFLICT, null, null);
        }
        FeedGuardShardObservation observation = guardCatalog.initialize(
                command.feedId(), command.feedIncarnation());
        IncrementalAuthorityRecord record = new IncrementalAuthorityRecord(
                0L, request.feedId(), request.feedIncarnation(),
                request.definitionDigest(), request.sourceContractDigest(),
                request.frontierContractDigest(), request.frontierKind(),
                IncrementalFrontier.begin(), null, null, 0L, null,
                IncrementalFrontierAuthorityState.READY, null);
        FeedState state = new FeedState(record, nextRootIdentity++);
        state.lastCommandId = command.commandId();
        state.lastCommandDigest = IncrementalAuthorityCommandCodec.digest(command);
        feeds.put(command.feedId(), state);
        if (!observation.equals(guardCatalog.observation(command.feedId()))) {
            throw new IllegalStateException("feed guard observation changed during initialize");
        }
        return IncrementalAuthorityDecision.accepted(record, null);
    }

    private IncrementalAuthorityDecision claim(IncrementalAuthorityMutation mutation) {
        IncrementalAuthorityCommand command = mutation.command();
        FeedState state = feeds.get(command.feedId());
        if (state == null) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.FEED_NOT_FOUND, null, null);
        }
        RunSnapshot pendingRun = state.pending == null
                ? null : runs.get(state.pending.runId());
        if (!owns(state, mutation.recoveryToken())) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.STALE_OWNERSHIP,
                    state.record, pendingRun);
        }
        IncrementalAuthorityDecision replay = replay(state, command);
        if (replay != null) return replay;
        if (command.expectedRevision() != state.record.revision()) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.STALE_REVISION,
                    state.record, pendingRun);
        }
        IncrementalClaimRequest request = mutation.claimRequest();
        if (request == null || pendingAdmission(state.record, request) != null) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                    state.record, pendingRun);
        }
        if (state.record.activeInterval() != null) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.ACTIVE_EXISTS,
                    state.record, pendingRun);
        }
        if (state.pending == null) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.PENDING_NOT_FOUND,
                    state.record, null);
        }
        if (!state.pending.equals(request)) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.PENDING_CONFLICT,
                    state.record, pendingRun);
        }
        if (!pendingRunMatches(pendingRun, request)) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.RUN_CONFLICT,
                    state.record, pendingRun);
        }
        IncrementalActiveInterval active = new IncrementalActiveInterval(
                IncrementalIntervalIdentity.from(request.capturedTarget()),
                request.capturedTarget(), request.artifactId(), state.record.revision(),
                request.firstAttemptFence(), 1,
                IncrementalAttemptCommitPhase.CLAIMED, null);
        IncrementalAuthorityRecord claimed = new IncrementalAuthorityRecord(
                state.record.revision() + 1L, state.record.feedId(),
                state.record.feedIncarnation(), state.record.definitionDigest(),
                state.record.sourceContractDigest(), state.record.frontierContractDigest(),
                state.record.frontierKind(), state.record.committedFrontier(), active,
                state.record.lastCommitReceipt(),
                state.record.lastCompletedBackfillOrdinal(),
                state.record.lastBackfillReceipt(),
                IncrementalFrontierAuthorityState.ACTIVE, null);
        RunDecision attempted = runs.apply(RunCommand.createAttempt(
                command.commandId(), request.runId(), pendingRun.revision(),
                request.attemptId(), request.publicationId(), request.eventTimeMillis()));
        if (attempted.code() != RunDecision.Code.ACCEPTED
                || !runAttemptMatches(attempted.snapshot(), request)) {
            throw new IllegalStateException(
                    "joint first-attempt mutation violated validated Run state");
        }
        state.record = claimed;
        state.pending = null;
        state.lastCommandId = command.commandId();
        state.lastCommandDigest = IncrementalAuthorityCommandCodec.digest(command);
        return IncrementalAuthorityDecision.accepted(claimed, attempted.snapshot());
    }

    private IncrementalAuthorityDecision jointSuccess(
            IncrementalAuthorityMutation mutation) {
        IncrementalAuthorityCommand command = mutation.command();
        FeedState state = feeds.get(command.feedId());
        if (state == null) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.FEED_NOT_FOUND, null, null);
        }
        VerifiedIncrementalJointCommitProof proof = mutation.jointCommitProof();
        RunSnapshot run = runs.get(command.fence().runId());
        if (!jointOwns(state, mutation.recoveryToken(),
                mutation.runRecoveryToken(), command.fence().runId())) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.STALE_OWNERSHIP,
                    state.record, run);
        }
        byte[] commandDigest = IncrementalAuthorityCommandCodec.digest(command);
        IncrementalCommitReceipt normalReceipt = state.record.lastCommitReceipt();
        IncrementalCommitReceipt backfillReceipt = state.record.lastBackfillReceipt();
        IncrementalCommitReceipt retained = command.kind()
                == IncrementalAuthorityCommandKind.COMPLETE_BACKFILL
                ? backfillReceipt : normalReceipt;
        boolean normalIdMatch = normalReceipt != null
                && command.commandId().equals(normalReceipt.commandId());
        boolean backfillIdMatch = backfillReceipt != null
                && command.commandId().equals(backfillReceipt.commandId());
        if (normalIdMatch || backfillIdMatch) {
            boolean selectedIdMatch = retained != null
                    && command.commandId().equals(retained.commandId());
            if (!selectedIdMatch || normalIdMatch && backfillIdMatch
                    || !MessageDigest.isEqual(
                    commandDigest, retained.commandDigest())) {
                return IncrementalAuthorityDecision.rejected(
                        IncrementalAuthorityDecision.Code.COMMAND_ID_CONFLICT,
                        state.record, run);
            }
            return IncrementalAuthorityDecision.replayed(state.record, run);
        }
        if (command.expectedRevision() != state.record.revision()) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.STALE_REVISION,
                    state.record, run);
        }
        IncrementalActiveInterval active = state.record.activeInterval();
        if (proof == null || state.record.state()
                != IncrementalFrontierAuthorityState.ACTIVE
                || active == null || run == null
                || !jointIdentityMatches(state.record, active, run, command, proof)) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                    state.record, run);
        }
        VerifiedIncrementalSinkCommitDecision freshDecision;
        try {
            freshDecision = IncrementalSinkIntentStrictAdapter.verify(active, runs);
        } catch (IllegalArgumentException invalidIntent) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                    state.record, run);
        }
        if (!freshDecisionMatchesProof(freshDecision, proof)) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                    state.record, run);
        }

        final IncrementalCommitReceipt receipt;
        final IncrementalAuthorityRecord committed;
        final RunResultSummary summary;
        final long eventTime;
        try {
            receipt = IncrementalCommitReceipt.committed(
                    active, proof.publicationId(), proof.generation(),
                    proof.manifestHash(), command.commandId(),
                    commandDigest);
            committed = committedRecord(state.record, active, receipt);
            summary = RunResultSummary.manifestFrom(
                    runs.assignmentSnapshot(active.fence()),
                    proof.generation(), proof.manifestHash());
            if (run.revision() == Long.MAX_VALUE) {
                throw new ArithmeticException("Run revision exhausted");
            }
            eventTime = Math.addExact(run.updatedAtMillis(), 1L);
        } catch (ArithmeticException | IllegalArgumentException invalidProjection) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                    state.record, run);
        }
        RunCommand terminal = RunCommand.advanceAttempt(
                command.commandId(), run.runId(), run.revision(),
                active.fence().attemptId(), active.fence().fenceToken(),
                AttemptStatus.SUCCEEDED, false, eventTime,
                "SUCCEEDED", null, summary);
        jointSuccessObserver.observe(IncrementalJointSuccessPhase.BEFORE_COMMIT);
        RunDecision terminalDecision =
                runs.applyRecovered(mutation.runRecoveryToken(), terminal);
        if (terminalDecision.code() != RunDecision.Code.ACCEPTED) {
            IncrementalAuthorityDecision.Code code = terminalDecision.code()
                    == RunDecision.Code.STALE_RECOVERY_OWNER
                    ? IncrementalAuthorityDecision.Code.STALE_OWNERSHIP
                    : IncrementalAuthorityDecision.Code.RUN_CONFLICT;
            return IncrementalAuthorityDecision.rejected(
                    code, state.record, terminalDecision.snapshot());
        }
        state.record = committed;
        state.lastCommandId = command.commandId();
        state.lastCommandDigest = commandDigest;
        state.lastTerminalRunId = run.runId();
        runFeeds.remove(run.runId());
        jointSuccessObserver.observe(
                IncrementalJointSuccessPhase.AFTER_COMMIT_BEFORE_ACK);
        return IncrementalAuthorityDecision.accepted(
                committed, terminalDecision.snapshot());
    }

    @Override
    public synchronized VerifiedIncrementalAbortClosureProof verifyAbortClosure(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            io.rowweave.run.RunAttemptFence fence,
            AttemptStatus terminalStatus) {
        requireRecoveryTokens(frontierToken, runToken);
        FeedState state = recoveredFeed(frontierToken, runToken);
        IncrementalActiveInterval active = state == null
                ? null : state.record.activeInterval();
        RunSnapshot run = fence == null ? null : runs.get(fence.runId());
        BatchSinkAbortRecord abort = fence == null
                ? null : runs.batchSinkAbort(fence);
        ManifestCommitJournalSnapshot journal = fence == null
                ? null : runs.manifestCommitJournal(fence);
        if (state == null
                || state.record.state() != IncrementalFrontierAuthorityState.ACTIVE
                || active == null || !active.fence().equals(fence)
                || run == null || abort == null
                || !abortIdentityMatches(state.record, active, run, abort)
                || abort.state() != BatchSinkAbortRecord.State.ABORTED
                || journal == null || journal.intent() != null) {
            throw new IllegalArgumentException(
                    "current abort closure and no-intent evidence are required");
        }
        RunCommand terminal = abortTerminalCommand(
                "verify-abort-" + active.identity().intervalId(), run,
                active, terminalStatus, abort.reasonCode());
        if (RunStateStoreKernel.evaluate(run, terminal).code()
                != RunDecision.Code.ACCEPTED) {
            throw new IllegalArgumentException(
                    "abort terminal does not match current Run state");
        }
        byte[] abortDigest = IncrementalAuthorityValue.sha256(
                BatchSinkAbortRecordCodec.encode(abort));
        byte[] noIntentDigest = IncrementalProofDigest.noSinkIntent(
                active, StageAssignmentManifestCodec.encodeRoot(
                        journal.assignments().manifest()));
        return VerifiedIncrementalAbortClosureProof.authorityVerified(
                state.record, active, terminalStatus, abortDigest,
                noIntentDigest, frontierToken, runToken);
    }

    private IncrementalAuthorityDecision closeAborted(
            IncrementalAuthorityMutation mutation) {
        IncrementalAuthorityCommand command = mutation.command();
        FeedState state = feeds.get(command.feedId());
        RunSnapshot run = runs.get(command.fence().runId());
        if (state == null) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.FEED_NOT_FOUND, null, run);
        }
        if (!jointOwns(state, mutation.recoveryToken(),
                mutation.runRecoveryToken(), command.fence().runId())) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.STALE_OWNERSHIP,
                    state.record, run);
        }
        if (retainedCommandIdMatches(state.record, command.commandId())) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.COMMAND_ID_CONFLICT,
                    state.record, run);
        }
        IncrementalAuthorityDecision replay = replay(state, command);
        if (replay != null) return replay;
        if (command.expectedRevision() != state.record.revision()) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.STALE_REVISION,
                    state.record, run);
        }
        VerifiedIncrementalAbortClosureProof proof = mutation.abortClosureProof();
        if (proof == null) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                    state.record, run);
        }
        VerifiedIncrementalAbortClosureProof fresh;
        try {
            fresh = verifyAbortClosure(
                    mutation.recoveryToken(), mutation.runRecoveryToken(),
                    command.fence(), proof.terminalStatus());
        } catch (IllegalArgumentException invalidEvidence) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                    state.record, run);
        }
        if (!MessageDigest.isEqual(fresh.proofDigest(), proof.proofDigest())) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                    state.record, run);
        }

        final IncrementalAuthorityRecord closed;
        final RunCommand terminal;
        final byte[] commandDigest;
        try {
            closed = abortedRecord(state.record);
            BatchSinkAbortRecord abort = runs.batchSinkAbort(command.fence());
            terminal = abortTerminalCommand(
                    command.commandId(), run, state.record.activeInterval(),
                    proof.terminalStatus(), abort.reasonCode());
            commandDigest = IncrementalAuthorityCommandCodec.digest(command);
        } catch (ArithmeticException | IllegalArgumentException invalidProjection) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                    state.record, run);
        }
        jointSuccessObserver.observe(IncrementalJointSuccessPhase.BEFORE_COMMIT);
        RunDecision terminalDecision =
                runs.applyRecovered(mutation.runRecoveryToken(), terminal);
        if (terminalDecision.code() != RunDecision.Code.ACCEPTED) {
            IncrementalAuthorityDecision.Code code = terminalDecision.code()
                    == RunDecision.Code.STALE_RECOVERY_OWNER
                    ? IncrementalAuthorityDecision.Code.STALE_OWNERSHIP
                    : IncrementalAuthorityDecision.Code.RUN_CONFLICT;
            return IncrementalAuthorityDecision.rejected(
                    code, state.record, terminalDecision.snapshot());
        }
        state.record = closed;
        state.lastCommandId = command.commandId();
        state.lastCommandDigest = commandDigest;
        state.lastTerminalRunId = run.runId();
        runFeeds.remove(run.runId());
        jointSuccessObserver.observe(
                IncrementalJointSuccessPhase.AFTER_COMMIT_BEFORE_ACK);
        return IncrementalAuthorityDecision.accepted(
                closed, terminalDecision.snapshot());
    }

    private IncrementalAuthorityDecision markConflict(
            IncrementalAuthorityMutation mutation) {
        IncrementalAuthorityCommand command = mutation.command();
        FeedState state = feeds.get(command.feedId());
        RunSnapshot run = command.fence() == null
                ? null : runs.get(command.fence().runId());
        if (state == null) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.FEED_NOT_FOUND, null, run);
        }
        if (!jointOwns(state, mutation.recoveryToken(),
                mutation.runRecoveryToken(), command.fence() == null
                        ? null : command.fence().runId())) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.STALE_OWNERSHIP,
                    state.record, run);
        }
        IncrementalAuthorityDecision replay = replay(state, command);
        if (replay != null) return replay;
        if (retainedCommandIdMatches(state.record, command.commandId())) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.COMMAND_ID_CONFLICT,
                    state.record, run);
        }
        if (command.expectedRevision() != state.record.revision()) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.STALE_REVISION,
                    state.record, run);
        }
        VerifiedIncrementalConflictProof proof = mutation.conflictProof();
        IncrementalActiveInterval active = state.record.activeInterval();
        if (proof == null
                || state.record.state()
                != IncrementalFrontierAuthorityState.ACTIVE
                || active == null || run == null
                || !conflictIdentityMatches(
                state.record, active, run, command, proof)) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                    state.record, run);
        }
        VerifiedIncrementalSinkCommitDecision freshDecision;
        try {
            freshDecision = IncrementalSinkIntentStrictAdapter.verify(active, runs);
        } catch (IllegalArgumentException invalidIntent) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                    state.record, run);
        }
        if (!conflictDecisionMatches(freshDecision, proof)) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                    state.record, run);
        }

        final IncrementalAuthorityRecord frozen;
        final RunCommand reconciling;
        final byte[] commandDigest;
        try {
            frozen = conflictRecord(state.record, proof.proofDigest());
            if (run.currentAttempt().status() != AttemptStatus.RECONCILING
                    && run.revision() == Long.MAX_VALUE) {
                throw new ArithmeticException("Run revision exhausted");
            }
            long eventTime = run.currentAttempt().status()
                    == AttemptStatus.RECONCILING
                    ? run.updatedAtMillis()
                    : Math.addExact(run.updatedAtMillis(), 1L);
            reconciling = run.currentAttempt().status()
                    == AttemptStatus.RECONCILING ? null : RunCommand.advanceAttempt(
                    command.commandId(), run.runId(), run.revision(),
                    active.fence().attemptId(), active.fence().fenceToken(),
                    AttemptStatus.RECONCILING, false, eventTime,
                    "CDC_AUTHORITY_CONFLICT", null);
            commandDigest = IncrementalAuthorityCommandCodec.digest(command);
        } catch (ArithmeticException | IllegalArgumentException invalidProjection) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                    state.record, run);
        }
        jointSuccessObserver.observe(IncrementalJointSuccessPhase.BEFORE_COMMIT);
        RunDecision runDecision = reconciling == null ? null
                : runs.applyRecovered(mutation.runRecoveryToken(), reconciling);
        if (runDecision != null
                && runDecision.code() != RunDecision.Code.ACCEPTED) {
            return IncrementalAuthorityDecision.rejected(
                    runDecision.code() == RunDecision.Code.STALE_RECOVERY_OWNER
                            ? IncrementalAuthorityDecision.Code.STALE_OWNERSHIP
                            : IncrementalAuthorityDecision.Code.RUN_CONFLICT,
                    state.record, runDecision.snapshot());
        }
        state.record = frozen;
        state.lastCommandId = command.commandId();
        state.lastCommandDigest = commandDigest;
        jointSuccessObserver.observe(
                IncrementalJointSuccessPhase.AFTER_COMMIT_BEFORE_ACK);
        return IncrementalAuthorityDecision.accepted(
                frozen, runDecision == null ? run : runDecision.snapshot());
    }

    private static boolean retainedCommandIdMatches(
            IncrementalAuthorityRecord record, String commandId) {
        IncrementalCommitReceipt normal = record.lastCommitReceipt();
        IncrementalCommitReceipt backfill = record.lastBackfillReceipt();
        return normal != null && commandId.equals(normal.commandId())
                || backfill != null && commandId.equals(backfill.commandId());
    }

    private RunCancellationResult observeThenCancelReady(
            String runId, String requestId) {
        jointSuccessObserver.observe(IncrementalJointSuccessPhase.BEFORE_COMMIT);
        return runs.requestUserCancellation(runId, requestId);
    }

    private static boolean abortIdentityMatches(
            IncrementalAuthorityRecord record, IncrementalActiveInterval active,
            RunSnapshot run, BatchSinkAbortRecord abort) {
        return run.runId().equals(active.fence().runId())
                && run.artifactId().equals(active.artifactId())
                && run.definitionId().equals(hex(record.definitionDigest()))
                && run.currentAttempt() != null
                && run.currentAttempt().attemptNumber() == active.attemptOrdinal()
                && run.currentAttempt().attemptId().equals(
                active.fence().attemptId())
                && run.currentAttempt().fenceToken()
                == active.fence().fenceToken()
                && run.currentAttempt().publicationId().equals(
                abort.publicationId());
    }

    private static boolean conflictIdentityMatches(
            IncrementalAuthorityRecord record, IncrementalActiveInterval active,
            RunSnapshot run, IncrementalAuthorityCommand command,
            VerifiedIncrementalConflictProof proof) {
        return active.phase() == IncrementalAttemptCommitPhase.COMMIT_DECIDED
                && command.feedIncarnation() == record.feedIncarnation()
                && command.intervalId().equals(active.identity().intervalId())
                && command.fence().equals(active.fence())
                && proof.feedId().equals(record.feedId())
                && proof.feedIncarnation() == record.feedIncarnation()
                && proof.intervalId().equals(active.identity().intervalId())
                && proof.artifactId().equals(active.artifactId())
                && proof.fence().equals(active.fence())
                && run.runId().equals(active.fence().runId())
                && run.artifactId().equals(active.artifactId())
                && run.definitionId().equals(hex(record.definitionDigest()))
                && run.currentAttempt() != null
                && run.currentAttempt().attemptNumber() == active.attemptOrdinal()
                && run.currentAttempt().attemptId().equals(
                active.fence().attemptId())
                && run.currentAttempt().fenceToken()
                == active.fence().fenceToken()
                && (run.currentAttempt().status() == AttemptStatus.COMMITTING
                || run.currentAttempt().status() == AttemptStatus.RECONCILING);
    }

    private static boolean conflictDecisionMatches(
            VerifiedIncrementalSinkCommitDecision decision,
            VerifiedIncrementalConflictProof proof) {
        return decision.domainId().equals(proof.domainId())
                && decision.fence().equals(proof.fence())
                && decision.publicationId().equals(proof.publicationId())
                && decision.generation() == proof.generation()
                && MessageDigest.isEqual(
                decision.manifestHash(), proof.manifestHash())
                && MessageDigest.isEqual(
                decision.commitDecisionDigest(), proof.commitDecisionDigest());
    }

    private static RunCommand abortTerminalCommand(
            String commandId, RunSnapshot run, IncrementalActiveInterval active,
            AttemptStatus terminalStatus, String reasonCode) {
        if (run.revision() == Long.MAX_VALUE) {
            throw new ArithmeticException("Run revision exhausted");
        }
        long eventTime = Math.addExact(run.updatedAtMillis(), 1L);
        return RunCommand.verifiedAbortClosureTerminal(
                commandId, run.runId(), run.revision(),
                active.fence().attemptId(), active.fence().fenceToken(),
                terminalStatus, eventTime, reasonCode);
    }

    private static IncrementalAuthorityRecord abortedRecord(
            IncrementalAuthorityRecord current) {
        return new IncrementalAuthorityRecord(
                Math.addExact(current.revision(), 1L), current.feedId(),
                current.feedIncarnation(), current.definitionDigest(),
                current.sourceContractDigest(), current.frontierContractDigest(),
                current.frontierKind(), current.committedFrontier(), null,
                current.lastCommitReceipt(),
                current.lastCompletedBackfillOrdinal(),
                current.lastBackfillReceipt(),
                IncrementalFrontierAuthorityState.READY, null);
    }

    private static IncrementalAuthorityRecord conflictRecord(
            IncrementalAuthorityRecord current, byte[] conflictDigest) {
        return new IncrementalAuthorityRecord(
                Math.addExact(current.revision(), 1L), current.feedId(),
                current.feedIncarnation(), current.definitionDigest(),
                current.sourceContractDigest(), current.frontierContractDigest(),
                current.frontierKind(), current.committedFrontier(),
                current.activeInterval(), current.lastCommitReceipt(),
                current.lastCompletedBackfillOrdinal(),
                current.lastBackfillReceipt(),
                IncrementalFrontierAuthorityState.CORRUPT_OR_CONFLICT,
                conflictDigest);
    }

    private static boolean jointIdentityMatches(
            IncrementalAuthorityRecord record, IncrementalActiveInterval active,
            RunSnapshot run, IncrementalAuthorityCommand command,
            VerifiedIncrementalJointCommitProof proof) {
        return active.phase() == IncrementalAttemptCommitPhase.COMMIT_DECIDED
                && command.feedIncarnation() == record.feedIncarnation()
                && command.intervalId().equals(active.identity().intervalId())
                && command.fence().equals(active.fence())
                && proof.feedId().equals(record.feedId())
                && proof.feedIncarnation() == record.feedIncarnation()
                && proof.intervalId().equals(active.identity().intervalId())
                && proof.capturedTargetReceiptDigest().equals(
                active.identity().capturedTargetReceiptDigest())
                && proof.mode() == active.capturedTarget().mode()
                && MessageDigest.isEqual(
                proof.definitionFingerprint(), active.capturedTarget().definitionDigest())
                && proof.artifactId().equals(active.artifactId())
                && proof.fence().equals(active.fence())
                && run.runId().equals(active.fence().runId())
                && run.artifactId().equals(active.artifactId())
                && run.definitionId().equals(hex(record.definitionDigest()))
                && run.currentAttempt() != null
                && run.currentAttempt().attemptNumber() == active.attemptOrdinal()
                && run.currentAttempt().attemptId().equals(active.fence().attemptId())
                && run.currentAttempt().fenceToken() == active.fence().fenceToken()
                && run.currentAttempt().publicationId().equals(proof.publicationId())
                && (run.currentAttempt().status() == AttemptStatus.COMMITTING
                || run.currentAttempt().status() == AttemptStatus.RECONCILING);
    }

    private static boolean freshDecisionMatchesProof(
            VerifiedIncrementalSinkCommitDecision decision,
            VerifiedIncrementalJointCommitProof proof) {
        return decision.domainId().equals(proof.domainId())
                && decision.fence().equals(proof.fence())
                && decision.publicationId().equals(proof.publicationId())
                && decision.generation() == proof.generation()
                && MessageDigest.isEqual(
                decision.manifestHash(), proof.manifestHash())
                && MessageDigest.isEqual(
                decision.commitDecisionDigest(), proof.commitDecisionDigest());
    }

    private static IncrementalAuthorityRecord committedRecord(
            IncrementalAuthorityRecord current, IncrementalActiveInterval active,
            IncrementalCommitReceipt receipt) {
        boolean backfill = active.capturedTarget().mode()
                == IncrementalRunMode.BACKFILL_V1;
        return new IncrementalAuthorityRecord(
                Math.addExact(current.revision(), 1L), current.feedId(),
                current.feedIncarnation(), current.definitionDigest(),
                current.sourceContractDigest(), current.frontierContractDigest(),
                current.frontierKind(), backfill
                ? current.committedFrontier() : active.capturedTarget().target(),
                null, backfill ? current.lastCommitReceipt() : receipt,
                backfill ? active.capturedTarget().expectedSequence()
                        : current.lastCompletedBackfillOrdinal(),
                backfill ? receipt : current.lastBackfillReceipt(),
                IncrementalFrontierAuthorityState.READY, null);
    }

    private IncrementalAuthorityDecision replay(
            FeedState state, IncrementalAuthorityCommand command) {
        if (!command.commandId().equals(state.lastCommandId)) return null;
        if (!MessageDigest.isEqual(
                IncrementalAuthorityCommandCodec.digest(command),
                state.lastCommandDigest)) {
            return IncrementalAuthorityDecision.rejected(
                    IncrementalAuthorityDecision.Code.COMMAND_ID_CONFLICT,
                    state.record, currentRun(state));
        }
        return IncrementalAuthorityDecision.replayed(
                state.record, command.fence() == null
                ? currentRun(state) : runs.get(command.fence().runId()));
    }

    private RunSnapshot currentRun(FeedState state) {
        if (state.record.activeInterval() != null) {
            return runs.get(state.record.activeInterval().identity().candidateRunId());
        }
        return state.pending == null ? null : runs.get(state.pending.runId());
    }

    private boolean owns(FeedState state, FrontierRecoveryToken token) {
        return token != null && token.equals(state.token)
                && state.record.feedId().equals(token.feedId())
                && state.record.feedIncarnation() == token.feedIncarnation()
                && state.rootIdentity == token.feedRootCzxid()
                && guardCatalog.observation(state.record.feedId())
                .equals(token.guardShardObservation());
    }

    private FeedState recoveredFeed(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken) {
        FeedState state = feeds.get(frontierToken.feedId());
        return recoveryOwns(state, frontierToken, runToken) ? state : null;
    }

    private boolean recoveryOwns(
            FeedState state, FrontierRecoveryToken frontierToken,
            RecoveryToken runToken) {
        if (state == null) return false;
        IncrementalActiveInterval active = state.record.activeInterval();
        return active != null && owns(state, frontierToken)
                && active.fence().runId().equals(runToken.runId())
                && runs.current(runToken)
                && runToken.owner().equals(frontierToken.owner());
    }

    private boolean jointOwns(
            FeedState state, FrontierRecoveryToken frontierToken,
            RecoveryToken runToken, String runId) {
        return state != null && runToken != null && runId != null
                && owns(state, frontierToken)
                && runId.equals(runToken.runId())
                && runs.current(runToken)
                && runToken.owner().equals(frontierToken.owner());
    }

    private FeedState feedForRun(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            String runId) {
        if (frontierToken == null || runToken == null || runId == null
                || !runId.equals(runToken.runId())) {
            return null;
        }
        FeedState state = feeds.get(frontierToken.feedId());
        return jointOwns(state, frontierToken, runToken, runId)
                && frontierToken.feedId().equals(runFeeds.get(runId))
                ? state : null;
    }

    private static void requireRecoveryTokens(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken) {
        if (frontierToken == null || runToken == null) {
            throw new IllegalArgumentException(
                    "frontier and Run recovery tokens are required");
        }
    }

    private static boolean matchesFrozenContract(
            IncrementalAuthorityRecord record, IncrementalClaimRequest request) {
        CapturedTargetReceipt capture = request.capturedTarget();
        if (!record.feedId().equals(request.feedId())
                || capture.feedIncarnation() != record.feedIncarnation()
                || !MessageDigest.isEqual(
                capture.definitionDigest(), record.definitionDigest())
                || !MessageDigest.isEqual(
                capture.sourceContractDigest(), record.sourceContractDigest())
                || !MessageDigest.isEqual(
                request.frontierContractDigest(), record.frontierContractDigest())) {
            return false;
        }
        return capture.target().kind() == record.frontierKind();
    }

    private static boolean matchesPosition(
            IncrementalAuthorityRecord record, CapturedTargetReceipt capture) {
        if (capture.mode() == IncrementalRunMode.BACKFILL_V1) {
            try {
                return capture.expectedSequence() == Math.addExact(
                        record.lastCompletedBackfillOrdinal(), 1L);
            } catch (ArithmeticException exhausted) {
                return false;
            }
        }
        return capture.expectedSequence() == record.revision()
                && capture.from().equals(record.committedFrontier());
    }

    private static boolean isIncrementalNoOp(CapturedTargetReceipt capture) {
        return capture.mode() == IncrementalRunMode.INCREMENTAL_V1
                && capture.from().equals(capture.target());
    }

    static IncrementalPendingRunDecision.Code pendingAdmission(
            IncrementalAuthorityRecord record, IncrementalClaimRequest request) {
        if (record == null || request == null
                || !matchesFrozenContract(record, request)
                || !matchesPosition(record, request.capturedTarget())) {
            return IncrementalPendingRunDecision.Code.CONTRACT_CONFLICT;
        }
        return isIncrementalNoOp(request.capturedTarget())
                ? IncrementalPendingRunDecision.Code.NO_OP : null;
    }

    private static boolean pendingRunMatches(
            RunSnapshot run, IncrementalClaimRequest request) {
        return run != null && run.status() == RunStatus.READY
                && run.attemptCount() == 0 && run.currentAttempt() == null
                && run.runId().equals(request.runId())
                && run.artifactId().equals(request.artifactId())
                && run.definitionId().equals(hex(
                request.capturedTarget().definitionDigest()));
    }

    private static boolean runAttemptMatches(
            RunSnapshot run, IncrementalClaimRequest request) {
        return run != null && run.currentAttempt() != null
                && run.runId().equals(request.runId())
                && run.currentAttempt().attemptId().equals(request.attemptId())
                && run.currentAttempt().publicationId().equals(request.publicationId())
                && run.currentAttempt().fenceToken()
                == request.firstAttemptFence().fenceToken();
    }

    private static IncrementalPendingRunDecision pending(
            IncrementalPendingRunDecision.Code code, RunSnapshot run) {
        return new IncrementalPendingRunDecision(code, run);
    }

    private static RunCommand runProgress(
            IncrementalAttemptProgressCommand command) {
        return RunCommand.advanceAttempt(
                command.commandId(), command.fence().runId(),
                command.expectedRunRevision(), command.fence().attemptId(),
                command.fence().fenceToken(), command.targetStatus(), false,
                command.eventTimeMillis(), command.targetStatus().name(), null);
    }

    private static RunDecision staleRunRevision(
            RunSnapshot current, IncrementalAttemptProgressCommand command) {
        long stale = current.revision() == Long.MAX_VALUE
                ? current.revision() - 1L : current.revision() + 1L;
        return RunStateStoreKernel.evaluate(current, RunCommand.advanceAttempt(
                command.commandId(), current.runId(), stale,
                command.fence().attemptId(), command.fence().fenceToken(),
                command.targetStatus(), false, command.eventTimeMillis(),
                command.targetStatus().name(), null));
    }

    private static RunDecision staleRunFence(
            RunSnapshot current, IncrementalAttemptProgressCommand command) {
        long staleFence = current.currentAttempt() == null
                ? 1L : current.currentAttempt().fenceToken() + 1L;
        return RunStateStoreKernel.evaluate(current, RunCommand.advanceAttempt(
                command.commandId(), current.runId(), current.revision(),
                "stale-incremental-progress", staleFence,
                command.targetStatus(), false, command.eventTimeMillis(),
                command.targetStatus().name(), null));
    }

    private FeedState activeFeed(io.rowweave.run.RunAttemptFence fence) {
        if (fence == null) return null;
        String feedId = runFeeds.get(fence.runId());
        FeedState state = feedId == null ? null : feeds.get(feedId);
        if (state == null
                || state.record.state() != IncrementalFrontierAuthorityState.ACTIVE
                || state.record.activeInterval() == null
                || !state.record.activeInterval().fence().equals(fence)) {
            return null;
        }
        return state;
    }

    private void projectPreparedIfComplete(
            FeedState state, io.rowweave.run.RunAttemptFence fence) {
        IncrementalActiveInterval active = state.record.activeInterval();
        if (active == null || active.phase() != IncrementalAttemptCommitPhase.CLAIMED) {
            return;
        }
        ManifestCommitJournalSnapshot journal = runs.manifestCommitJournal(fence);
        if (completePreparedClosure(journal)) {
            state.record = withActivePhase(
                    state.record, IncrementalAttemptCommitPhase.PREPARED, null);
            clearLastFrontierCommand(state);
        }
    }

    private static boolean completePreparedClosure(
            ManifestCommitJournalSnapshot journal) {
        if (journal == null || !journal.allExpectedMembersSealed()) return false;
        StageAssignmentSnapshot assignments = journal.assignments();
        for (int i = 0; i < assignments.manifest().assignmentCount(); i++) {
            StageAssignmentRole role = assignments.manifest().assignment(i).role();
            if (role != StageAssignmentRole.SINK
                    && role != StageAssignmentRole.WHOLE_GRAPH
                    && role != StageAssignmentRole.SYSTEM_CONTROL) {
                continue;
            }
            PreparedSinkReceipt prepared = assignments.preparedSink(i);
            ManifestMemberReceipt member = journal.member(i);
            if (prepared == null || member == null || !member.hasPreparedLocator()
                    || !prepared.locator().equals(member.preparedLocator())) {
                return false;
            }
        }
        return true;
    }

    private static boolean phaseProjectionExhausted(
            IncrementalAuthorityRecord record) {
        return record.revision() == Long.MAX_VALUE
                && record.activeInterval() != null
                && record.activeInterval().phase()
                == IncrementalAttemptCommitPhase.CLAIMED;
    }

    private static IncrementalAuthorityRecord withActivePhase(
            IncrementalAuthorityRecord record,
            IncrementalAttemptCommitPhase phase, byte[] commitDecisionDigest) {
        IncrementalActiveInterval active = record.activeInterval();
        IncrementalActiveInterval projected = new IncrementalActiveInterval(
                active.identity(), active.capturedTarget(), active.artifactId(),
                active.claimedRevision(), active.fence(), active.attemptOrdinal(),
                phase, commitDecisionDigest);
        return new IncrementalAuthorityRecord(
                Math.addExact(record.revision(), 1L), record.feedId(),
                record.feedIncarnation(), record.definitionDigest(),
                record.sourceContractDigest(), record.frontierContractDigest(),
                record.frontierKind(), record.committedFrontier(), projected,
                record.lastCommitReceipt(), record.lastCompletedBackfillOrdinal(),
                record.lastBackfillReceipt(), IncrementalFrontierAuthorityState.ACTIVE,
                null);
    }

    private static IncrementalAuthorityRecord withActive(
            IncrementalAuthorityRecord record,
            IncrementalActiveInterval active) {
        return new IncrementalAuthorityRecord(
                Math.addExact(record.revision(), 1L), record.feedId(),
                record.feedIncarnation(), record.definitionDigest(),
                record.sourceContractDigest(), record.frontierContractDigest(),
                record.frontierKind(), record.committedFrontier(), active,
                record.lastCommitReceipt(), record.lastCompletedBackfillOrdinal(),
                record.lastBackfillReceipt(),
                IncrementalFrontierAuthorityState.ACTIVE, null);
    }

    private static void clearLastFrontierCommand(FeedState state) {
        state.lastCommandId = null;
        state.lastCommandDigest = null;
    }

    private static String hex(byte[] value) {
        char[] output = new char[value.length * 2];
        char[] digits = "0123456789abcdef".toCharArray();
        for (int i = 0; i < value.length; i++) {
            int unsigned = value[i] & 0xff;
            output[i * 2] = digits[unsigned >>> 4];
            output[i * 2 + 1] = digits[unsigned & 0xf];
        }
        return new String(output);
    }

    private static final class FeedState {
        private IncrementalAuthorityRecord record;
        private final long rootIdentity;
        private long ownershipEpoch;
        private FrontierRecoveryToken token;
        private IncrementalClaimRequest pending;
        private String lastCommandId;
        private byte[] lastCommandDigest;
        private String lastTerminalRunId;

        private FeedState(IncrementalAuthorityRecord record, long rootIdentity) {
            this.record = record;
            this.rootIdentity = rootIdentity;
        }
    }

    static final class FileBootstrapCheckpoint {
        final long nextRootIdentity;
        final List<FileBootstrapFeed> feeds;

        FileBootstrapCheckpoint(
                long nextRootIdentity, List<FileBootstrapFeed> feeds) {
            this.nextRootIdentity = nextRootIdentity;
            this.feeds = new ArrayList<>(feeds);
        }
    }

    static final class FileReadyCheckpoint {
        final long nextRootIdentity;
        final List<FileBootstrapFeed> feeds;
        final List<FileCheckpointRun> runs;

        FileReadyCheckpoint(
                long nextRootIdentity, List<FileBootstrapFeed> feeds,
                List<FileCheckpointRun> runs) {
            this.nextRootIdentity = nextRootIdentity;
            this.feeds = new ArrayList<>(feeds);
            this.runs = new ArrayList<>(runs);
        }
    }

    static final class FileCheckpointRun {
        final RunSnapshot snapshot;
        final RecoveryToken token;

        FileCheckpointRun(RunSnapshot snapshot, RecoveryToken token) {
            this.snapshot = snapshot;
            this.token = token;
        }
    }

    static final class FileBootstrapFeed {
        final IncrementalAuthorityRecord record;
        final long rootIdentity;
        final long ownershipEpoch;
        final FrontierRecoveryToken token;
        final String lastCommandId;
        final byte[] lastCommandDigest;
        final String lastTerminalRunId;

        FileBootstrapFeed(
                IncrementalAuthorityRecord record, long rootIdentity,
                long ownershipEpoch, FrontierRecoveryToken token,
                String lastCommandId, byte[] lastCommandDigest) {
            this(record, rootIdentity, ownershipEpoch, token,
                    lastCommandId, lastCommandDigest, null);
        }

        FileBootstrapFeed(
                IncrementalAuthorityRecord record, long rootIdentity,
                long ownershipEpoch, FrontierRecoveryToken token,
                String lastCommandId, byte[] lastCommandDigest,
                String lastTerminalRunId) {
            this.record = record;
            this.rootIdentity = rootIdentity;
            this.ownershipEpoch = ownershipEpoch;
            this.token = token;
            this.lastCommandId = lastCommandId;
            this.lastCommandDigest = lastCommandDigest == null
                    ? null : lastCommandDigest.clone();
            this.lastTerminalRunId = lastTerminalRunId;
        }
    }
}
