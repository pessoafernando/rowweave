package io.rowweave.run;

import io.rowweave.QualityGateDecision;
import io.rowweave.QualityGateDecisionCodec;
import io.rowweave.QualityGateDecisionResult;
import io.rowweave.artifact.ArtifactBackfillStatus;
import io.rowweave.artifact.ArtifactReferenceAuthority;
import io.rowweave.artifact.ArtifactReferenceRef;
import io.rowweave.artifact.ArtifactReferenceView;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.LongSupplier;

/**
 * Reference store. It proves a single linearization point for state and events, but carries no process-level durability.
 */
public final class InMemoryRunStateStore implements RunStateStore, RecoverableRunCatalog,
        RunRecoveryOwnership, RecoveryFencedRunStateStore,
        RecoveryFencedStageAssignmentAuthority, RunLifecycleCatalog,
        RunRetentionAuthority, ArtifactReferenceAuthority, RunCancellationAuthority,
        RunDeadlineAuthority, WholeAttemptRetryAuthority,
        DurableExecutionEvidenceAuthority {
    private static final int MAX_PAGE_SIZE = 1000;

    private final Map<String, Entry> runs = new LinkedHashMap<>();
    private final Map<Integer, Map<String, RecoverableRunRef>> recoverable = new LinkedHashMap<>();
    private final Map<Integer, Map<String, RunLifecycleRef>> lifecycle = new LinkedHashMap<>();
    private final Map<String, Map<Integer, Map<String, ArtifactReferenceRef>>>
            artifactReferences = new LinkedHashMap<>();
    private final int maxRecoverablePerShard;
    private final int maxLifecyclePerShard;
    private final int maxArtifactReferencesPerShard;
    private final LongSupplier authorityClock;
    private long lastAuthorityTime;

    public InMemoryRunStateStore() {
        this(RunRecoveryCatalogKernel.MAX_ENTRIES_PER_SHARD,
                RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD,
                System::currentTimeMillis, 4096);
    }

    /** Same backend semantics with an injected authority clock for a composed authority. */
    public InMemoryRunStateStore(LongSupplier authorityClock) {
        this(RunRecoveryCatalogKernel.MAX_ENTRIES_PER_SHARD,
                RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD,
                authorityClock, 4096);
    }

    InMemoryRunStateStore(int maxRecoverablePerShard) {
        this(maxRecoverablePerShard,
                RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD,
                System::currentTimeMillis, 4096);
    }

    InMemoryRunStateStore(int maxRecoverablePerShard, int maxLifecyclePerShard,
                          LongSupplier authorityClock) {
        this(maxRecoverablePerShard, maxLifecyclePerShard, authorityClock, 4096);
    }

    InMemoryRunStateStore(int maxRecoverablePerShard, int maxLifecyclePerShard,
                          LongSupplier authorityClock,
                          int maxArtifactReferencesPerShard) {
        if (maxRecoverablePerShard < 1
                || maxRecoverablePerShard > RunRecoveryCatalogKernel.MAX_ENTRIES_PER_SHARD) {
            throw new IllegalArgumentException("maxRecoverablePerShard out of bounds");
        }
        if (maxLifecyclePerShard < 1
                || maxLifecyclePerShard > RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD) {
            throw new IllegalArgumentException("maxLifecyclePerShard out of bounds");
        }
        if (authorityClock == null) {
            throw new IllegalArgumentException("authorityClock is required");
        }
        if (maxArtifactReferencesPerShard < 1
                || maxArtifactReferencesPerShard > 4096) {
            throw new IllegalArgumentException(
                    "maxArtifactReferencesPerShard out of bounds");
        }
        this.maxRecoverablePerShard = maxRecoverablePerShard;
        this.maxLifecyclePerShard = maxLifecyclePerShard;
        this.authorityClock = authorityClock;
        this.maxArtifactReferencesPerShard = maxArtifactReferencesPerShard;
    }

    @Override
    public synchronized RunDecision apply(RunCommand command) {
        return apply(command, null, false);
    }

    @Override
    public synchronized RunDecision applyRecovered(RecoveryToken token, RunCommand command) {
        if (token == null) throw new IllegalArgumentException("token is required");
        if (command == null) throw new IllegalArgumentException("command is required");
        if (!token.runId().equals(command.runId)) {
            throw new IllegalArgumentException("recovery token and command runId mismatch");
        }
        return apply(command, token, true);
    }

    @Override
    public synchronized RunCancellationResult requestUserCancellation(
            String runId, String requestId) {
        runId = RunCancellationAuthorityKernel.validateRunId(runId);
        requestId = RunCancellationAuthorityKernel.validateRequestId(requestId);
        Entry entry = runs.get(runId);
        RunSnapshot current = entry == null ? null : entry.snapshot;
        if (current != null) {
            requireLifecycleConsistency(current);
            if (lifecycle(runId).state() == RunLifecycleState.DELETING) {
                return RunCancellationAuthorityKernel.retiring(runId);
            }
        }
        String commandId = RunCancellationAuthorityKernel.commandId(runId, requestId);
        if (entry != null) {
            CommandRecord prior = entry.commands.get(commandId);
            if (prior != null) {
                return RunCancellationAuthorityKernel.replayed(
                        runId, current, prior.event, commandId);
            }
        }
        RunCancellationAuthorityKernel.Plan plan =
                RunCancellationAuthorityKernel.plan(runId, commandId, current);
        if (plan.result() != null) return plan.result();
        RunDecision decision = apply(plan.command(), null, false, true);
        if (decision.code() != RunDecision.Code.ACCEPTED) {
            throw new IllegalStateException(
                    "atomic public cancellation rejected: " + decision.code());
        }
        return RunCancellationAuthorityKernel.accepted(decision.snapshot());
    }

    @Override
    public synchronized RunDeadlineResult evaluateDeadline(String runId) {
        runId = RunDeadlineAuthorityKernel.validateRunId(runId);
        Entry entry = runs.get(runId);
        RunSnapshot current = entry == null ? null : entry.snapshot;
        if (current == null) return RunDeadlineAuthorityKernel.missing(runId);
        requireLifecycleConsistency(current);
        if (lifecycle(runId).state() == RunLifecycleState.DELETING) {
            return RunDeadlineAuthorityKernel.retiring(runId);
        }
        if (current.deadlineEpochMillis() == 0L) {
            return RunDeadlineAuthorityKernel.plan(null, current, 0L).result();
        }
        long observed = authorityClock.getAsLong();
        if (observed <= 0L) {
            throw new IllegalStateException("authority clock must be > 0");
        }
        String commandId = RunDeadlineAuthorityKernel.commandId(current);
        CommandRecord prior = entry.commands.get(commandId);
        if (prior != null) {
            return RunDeadlineAuthorityKernel.replayed(
                    current, prior.event, commandId, observed);
        }
        RunDeadlineAuthorityKernel.Plan plan =
                RunDeadlineAuthorityKernel.plan(commandId, current, observed);
        if (plan.result() != null) return plan.result();
        RunDecision decision = apply(plan.command(), null, false, true);
        if (decision.code() != RunDecision.Code.ACCEPTED) {
            throw new IllegalStateException(
                    "atomic deadline mutation rejected: " + decision.code());
        }
        return RunDeadlineAuthorityKernel.accepted(decision.snapshot(), observed);
    }

    @Override
    public synchronized WholeAttemptRetryDecision scheduleRetry(
            RunAttemptFence fence) {
        return scheduleRetry(fence, null, false);
    }

    @Override
    public synchronized WholeAttemptRetryDecision scheduleRetryRecovered(
            RecoveryToken token, RunAttemptFence fence) {
        if (token == null) throw new IllegalArgumentException("token is required");
        if (fence == null) throw new IllegalArgumentException("fence is required");
        if (!token.runId().equals(fence.runId())) {
            throw new IllegalArgumentException(
                    "recovery token and retry fence runId mismatch");
        }
        return scheduleRetry(fence, token, true);
    }

    private WholeAttemptRetryDecision scheduleRetry(
            RunAttemptFence fence, RecoveryToken token, boolean recovered) {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        Entry entry = runs.get(fence.runId());
        if (entry == null) {
            return retryDecision(WholeAttemptRetryDecision.Code.NOT_FOUND,
                    null, null, 0L);
        }
        RunSnapshot current = entry.snapshot;
        requireLifecycleConsistency(current);
        if (lifecycle(fence.runId()).state() == RunLifecycleState.DELETING) {
            return retryDecision(WholeAttemptRetryDecision.Code.RETIRING,
                    current, null, 0L);
        }
        if ((!recovered && entry.recoveryOwner != null)
                || (recovered && !token.equals(entry.recoveryOwner))) {
            return retryDecision(
                    WholeAttemptRetryDecision.Code.STALE_RECOVERY_OWNER,
                    current, null, 0L);
        }
        if (entry.retrySchedule != null
                && entry.retrySchedule.fence.equals(fence)
                && WholeAttemptRetryAuthorityKernel.scheduleStillCurrent(
                entry.retrySchedule.code, entry.retrySchedule.retryClass,
                current)) {
            WholeAttemptRetryDecision.Code replayCode =
                    entry.retrySchedule.code
                            == WholeAttemptRetryDecision.Code.SCHEDULED
                            ? WholeAttemptRetryDecision.Code.ALREADY_SCHEDULED
                            : entry.retrySchedule.code;
            return retryDecision(replayCode,
                    current, entry.retrySchedule.retryClass,
                    entry.retrySchedule.observedAtMillis);
        }
        if (!matchesCurrentAttempt(entry, fence)) {
            if (current.status().isTerminal()) {
                return retryDecision(WholeAttemptRetryDecision.Code.TOO_LATE,
                        current, null, 0L);
            }
            return retryDecision(WholeAttemptRetryDecision.Code.STALE_RUN,
                    current, null, 0L);
        }
        AttemptStatus status = current.currentAttempt().status();
        if (status == AttemptStatus.COMMITTING
                || status == AttemptStatus.RECONCILING
                || status == AttemptStatus.CANCELING
                || status.isTerminal()) {
            return retryDecision(WholeAttemptRetryDecision.Code.TOO_LATE,
                    current, null, 0L);
        }
        long observed = observeAuthorityTime(current);
        if (deadlineDue(current, observed)) {
            RunSnapshot afterDeadline = applyDeadlineAt(entry, current, observed);
            return retryDecision(WholeAttemptRetryDecision.Code.TOO_LATE,
                    afterDeadline, null, observed);
        }
        StageAssignmentSnapshot assignments = entry.stageAssignments == null
                ? null : entry.stageAssignments.snapshot();
        ManifestCommitJournalSnapshot journal = entry.stageAssignments == null
                ? null : new ManifestCommitJournalSnapshot(
                assignments, entry.stageAssignments.manifestMembers,
                entry.stageAssignments.qualityDecision,
                entry.stageAssignments.manifestIntent);
        WholeAttemptRetryAuthorityKernel.Closure closure =
                WholeAttemptRetryAuthorityKernel.classify(
                        current, assignments, journal, entry.abortRecord);
        if (closure.code() != null) {
            return retryDecision(closure.code(), current, null, observed);
        }
        boolean exhausted = current.attemptCount() >= current.maxAttempts();
        String commandId = WholeAttemptRetryAuthorityKernel.scheduleCommandId(
                fence, closure.retryClass(), assignments, entry.abortRecord);
        RunCommand command = RunCommand.retryAuthorityAdvance(
                commandId, current.runId(), current.revision(),
                fence.attemptId(), fence.fenceToken(), observed,
                closure.retryClass(), exhausted);
        RunDecision applied = apply(command, token, recovered);
        if (applied.code() != RunDecision.Code.ACCEPTED) {
            throw new IllegalStateException(
                    "atomic retry schedule rejected: " + applied.code());
        }
        WholeAttemptRetryDecision.Code code = exhausted
                ? WholeAttemptRetryDecision.Code.EXHAUSTED
                : WholeAttemptRetryDecision.Code.SCHEDULED;
        entry.retrySchedule = new RetryScheduleRecord(
                fence, closure.retryClass(), commandId, code, observed);
        lastAuthorityTime = Math.max(lastAuthorityTime, observed);
        return retryDecision(code, applied.snapshot(), closure.retryClass(), observed);
    }

    @Override
    public synchronized RetryReadyEvaluation evaluateRetryReady(String runId) {
        runId = RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
        Entry entry = runs.get(runId);
        if (entry == null) {
            return retryReady(RetryReadyEvaluation.Code.NOT_FOUND,
                    null, null, 0L, 0L, 0);
        }
        RunSnapshot current = entry.snapshot;
        requireLifecycleConsistency(current);
        if (lifecycle(runId).state() == RunLifecycleState.DELETING) {
            return retryReady(RetryReadyEvaluation.Code.RETIRING,
                    current, null, 0L, 0L, 0);
        }
        if (current.status().isTerminal()) {
            return retryReady(RetryReadyEvaluation.Code.TOO_LATE,
                    current, null, 0L, 0L, 0);
        }
        WholeAttemptRetryAuthorityKernel.Wait retryWait =
                WholeAttemptRetryAuthorityKernel.retryWait(current);
        if (retryWait == null) {
            return retryReady(RetryReadyEvaluation.Code.NOT_RETRY_WAIT,
                    current, null, 0L, 0L, 0);
        }
        long observed = observeAuthorityTime(current);
        if (deadlineDue(current, observed)) {
            return retryReady(RetryReadyEvaluation.Code.TOO_LATE,
                    current, retryWait.identity(), observed,
                    retryWait.notBeforeMillis(), retryWait.remainingAttempts());
        }
        return retryReady(observed >= retryWait.notBeforeMillis()
                        ? RetryReadyEvaluation.Code.DUE
                        : RetryReadyEvaluation.Code.NOT_DUE,
                current, retryWait.identity(), observed,
                retryWait.notBeforeMillis(), retryWait.remainingAttempts());
    }

    @Override
    public synchronized RetryAttemptClaim claimDueRetry(String runId) {
        return claimDueRetry(null, runId, false);
    }

    @Override
    public synchronized RetryAttemptClaim claimDueRetryRecovered(
            RecoveryToken token, String runId) {
        if (token == null) throw new IllegalArgumentException("token is required");
        runId = RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
        if (!token.runId().equals(runId)) {
            throw new IllegalArgumentException(
                    "recovery token and retry claim runId mismatch");
        }
        return claimDueRetry(token, runId, true);
    }

    private RetryAttemptClaim claimDueRetry(
            RecoveryToken token, String runId, boolean recovered) {
        runId = RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
        Entry entry = runs.get(runId);
        if (entry == null) {
            return retryClaim(RetryAttemptClaim.Code.NOT_FOUND,
                    null, null, 0L, 0L);
        }
        RunSnapshot current = entry.snapshot;
        requireLifecycleConsistency(current);
        if (lifecycle(runId).state() == RunLifecycleState.DELETING) {
            return retryClaim(RetryAttemptClaim.Code.RETIRING,
                    current, null, 0L, 0L);
        }
        if ((!recovered && entry.recoveryOwner != null)
                || (recovered && !token.equals(entry.recoveryOwner))) {
            return retryClaim(RetryAttemptClaim.Code.STALE_RECOVERY_OWNER,
                    current, null, 0L, 0L);
        }
        if (current.status().isTerminal()) {
            return retryClaim(RetryAttemptClaim.Code.TOO_LATE,
                    current, null, 0L, 0L);
        }
        RetryAttemptIdentity existingIdentity = claimedRetryIdentity(entry, current);
        if (existingIdentity != null) {
            return retryClaim(RetryAttemptClaim.Code.ALREADY_CLAIMED,
                    current, existingIdentity, 0L, 0L);
        }
        WholeAttemptRetryAuthorityKernel.Wait retryWait =
                WholeAttemptRetryAuthorityKernel.retryWait(current);
        if (retryWait == null) {
            return retryClaim(RetryAttemptClaim.Code.NOT_RETRY_WAIT,
                    current, null, 0L, 0L);
        }
        long observed = observeAuthorityTime(current);
        if (deadlineDue(current, observed)) {
            RunSnapshot afterDeadline = applyDeadlineAt(entry, current, observed);
            return retryClaim(RetryAttemptClaim.Code.TOO_LATE,
                    afterDeadline, retryWait.identity(), observed,
                    retryWait.notBeforeMillis());
        }
        if (observed < retryWait.notBeforeMillis()) {
            return retryClaim(RetryAttemptClaim.Code.NOT_DUE,
                    current, retryWait.identity(), observed,
                    retryWait.notBeforeMillis());
        }
        String commandId = retryWait.identity().commandId("claim");
        RunCommand command = RunCommand.retryAuthorityCreateAttempt(
                commandId, runId, current.revision(), retryWait.identity(), observed);
        RunDecision applied = apply(command, token, recovered);
        if (applied.code() != RunDecision.Code.ACCEPTED
                && applied.code() != RunDecision.Code.ALREADY_APPLIED) {
            throw new IllegalStateException(
                    "atomic retry claim rejected: " + applied.code());
        }
        entry.retrySchedule = null;
        lastAuthorityTime = Math.max(lastAuthorityTime, observed);
        return retryClaim(applied.code() == RunDecision.Code.ACCEPTED
                        ? RetryAttemptClaim.Code.CLAIMED
                        : RetryAttemptClaim.Code.ALREADY_CLAIMED,
                applied.snapshot(), retryWait.identity(), observed,
                retryWait.notBeforeMillis());
    }

    private long observeAuthorityTime(RunSnapshot current) {
        long observed = authorityClock.getAsLong();
        if (observed <= 0L) {
            throw new IllegalStateException("authority clock must be > 0");
        }
        observed = Math.max(observed, lastAuthorityTime);
        if (current != null) observed = Math.max(observed, current.updatedAtMillis());
        lastAuthorityTime = observed;
        return observed;
    }

    private static boolean deadlineDue(RunSnapshot snapshot, long observed) {
        return snapshot.deadlineEpochMillis() > 0L
                && observed >= snapshot.deadlineEpochMillis();
    }

    private RunSnapshot applyDeadlineAt(
            Entry entry, RunSnapshot current, long observed) {
        String commandId = RunDeadlineAuthorityKernel.commandId(current);
        CommandRecord prior = entry.commands.get(commandId);
        if (prior != null) return current;
        RunDeadlineAuthorityKernel.Plan plan =
                RunDeadlineAuthorityKernel.plan(commandId, current, observed);
        if (plan.command() == null) return current;
        RunDecision decision = apply(plan.command(), null, false, true);
        if (decision.code() != RunDecision.Code.ACCEPTED
                && decision.code() != RunDecision.Code.ALREADY_APPLIED) {
            throw new IllegalStateException(
                    "atomic deadline mutation rejected: " + decision.code());
        }
        return decision.snapshot();
    }

    private static RetryAttemptIdentity claimedRetryIdentity(
            Entry entry, RunSnapshot snapshot) {
        if (snapshot.status() != RunStatus.RUNNING
                || snapshot.currentAttempt() == null
                || snapshot.attemptCount() < 2
                || snapshot.attemptCount() > WholeAttemptRetryPolicy.MAX_ATTEMPTS) {
            return null;
        }
        RetryAttemptIdentity identity = RetryAttemptIdentity.derive(
                snapshot.runId(), snapshot.artifactId(), snapshot.attemptCount());
        CommandRecord receipt = entry.commands.get(identity.commandId("claim"));
        return WholeAttemptRetryAuthorityKernel.claimedRetryIdentity(
                snapshot, receipt != null
                && receipt.event.type() == ExecutionEvent.Type.ATTEMPT_CREATED);
    }

    private static WholeAttemptRetryDecision retryDecision(
            WholeAttemptRetryDecision.Code code, RunSnapshot snapshot,
            WholeAttemptRetryClass retryClass, long observed) {
        return new WholeAttemptRetryDecision(code, snapshot,
                retryClass, observed);
    }

    private static RetryReadyEvaluation retryReady(
            RetryReadyEvaluation.Code code, RunSnapshot snapshot,
            RetryAttemptIdentity identity, long observed, long notBefore,
            int remainingAttempts) {
        return new RetryReadyEvaluation(code, snapshot, identity,
                observed, notBefore, remainingAttempts);
    }

    private static RetryAttemptClaim retryClaim(
            RetryAttemptClaim.Code code, RunSnapshot snapshot,
            RetryAttemptIdentity identity, long observed, long notBefore) {
        return new RetryAttemptClaim(code, snapshot, identity,
                observed, notBefore);
    }

    private RunDecision apply(RunCommand command, RecoveryToken token, boolean recovered) {
        return apply(command, token, recovered, false);
    }

    private RunDecision apply(RunCommand command, RecoveryToken token,
                              boolean recovered, boolean publicCancellation) {
        if (command == null) {
            throw new IllegalArgumentException("command is required");
        }
        Entry currentEntry = runs.get(command.runId);
        RunSnapshot current = currentEntry == null ? null : currentEntry.snapshot;
        if (current != null) requireLifecycleConsistency(current);
        if (current != null && lifecycle(command.runId).state() == RunLifecycleState.DELETING) {
            return RunDecision.rejected(RunDecision.Code.RETENTION_IN_PROGRESS, current);
        }
        RecoveryToken currentOwner = currentEntry == null ? null : currentEntry.recoveryOwner;
        if (!recovered && !publicCancellation && currentOwner != null) {
            return RunDecision.rejected(RunDecision.Code.RECOVERY_OWNED, current);
        }
        if (recovered && !token.equals(currentOwner)) {
            return RunDecision.rejected(RunDecision.Code.STALE_RECOVERY_OWNER, current);
        }
        if (currentEntry != null) {
            CommandRecord prior = currentEntry.commands.get(command.commandId);
            if (prior != null) {
                return MessageDigest.isEqual(prior.digest, command.commandDigestUnsafe())
                        ? RunDecision.replayed(current, prior.event)
                        : RunDecision.rejected(RunDecision.Code.COMMAND_ID_CONFLICT, current);
            }
        }
        RunDecision decision = RunStateMachine.apply(current, command);
        if (!decision.accepted()) {
            return decision;
        }
        String digest = RunRecoveryCatalogKernel.digestHex(command.runId);
        int shard = RunRecoveryCatalogKernel.shard(digest);
        Map<String, RecoverableRunRef> shardEntries = recoverable.get(shard);
        Map<String, RunLifecycleRef> lifecycleEntries = lifecycle.get(shard);
        if (current == null && decision.snapshot().status().isTerminal()) {
            throw new IllegalStateException("new Run cannot be terminal");
        }
        if (current == null && lifecycleEntries != null
                && lifecycleEntries.size() >= maxLifecyclePerShard) {
            return RunDecision.rejected(RunDecision.Code.LIMIT_EXCEEDED, null);
        }
        if (current == null && shardEntries != null
                && shardEntries.size() >= maxRecoverablePerShard) {
            return RunDecision.rejected(RunDecision.Code.LIMIT_EXCEEDED, null);
        }
        ArtifactReferenceRef nextArtifactReference = prepareArtifactReference(
                current, decision.snapshot());
        if (current == null && nextArtifactReference != null
                && artifactReferenceShardSize(nextArtifactReference.artifactId(),
                referenceShard(nextArtifactReference.runId()))
                >= maxArtifactReferencesPerShard) {
            return RunDecision.rejected(RunDecision.Code.LIMIT_EXCEEDED, null);
        }
        if (current != null && !current.status().isTerminal()
                && (shardEntries == null || !shardEntries.containsKey(digest))) {
            throw new IllegalStateException("recoverable run catalog entry is missing");
        }
        final RunLifecycleRef nextLifecycle;
        final long nextAuthorityTime;
        if (decision.snapshot().status().isTerminal()) {
            long observed = authorityClock.getAsLong();
            if (observed <= 0L) {
                throw new IllegalStateException("authority clock must be > 0");
            }
            nextAuthorityTime = RunRetentionPolicy.clampAuthorityTime(
                    observed, lastAuthorityTime);
            nextLifecycle = RunLifecycleRef.terminal(command.runId,
                    decision.snapshot().revision(), decision.snapshot().status(),
                    decision.snapshot().artifactId(), nextAuthorityTime);
        } else {
            nextAuthorityTime = lastAuthorityTime;
            nextLifecycle = RunLifecycleRef.active(command.runId,
                    decision.snapshot().revision(), decision.snapshot().status(),
                    decision.snapshot().artifactId());
        }
        if (currentEntry == null) {
            currentEntry = new Entry();
            runs.put(command.runId, currentEntry);
        }
        // Snapshot and event are published inside one synchronized block, so a reader can never observe half a mutation.
        currentEntry.events.add(decision.event());
        currentEntry.commands.put(command.commandId,
                new CommandRecord(command.commandDigestUnsafe(), decision.event()));
        boolean newAttempt = current != null
                && current.currentAttempt() != null
                && decision.snapshot().currentAttempt() != null
                && (!current.currentAttempt().attemptId().equals(
                decision.snapshot().currentAttempt().attemptId())
                || current.currentAttempt().fenceToken()
                != decision.snapshot().currentAttempt().fenceToken());
        currentEntry.snapshot = decision.snapshot();
        if (newAttempt) {
            // The completed abort is evidence for closing the prior attempt only. Keeping that
            // singleton record attached to the new attempt would reject every new manifest
            // member/intent as a conflict after a legitimate whole-attempt retry.
            currentEntry.abortRecord = null;
        }
        if (lifecycleEntries == null) {
            lifecycleEntries = new LinkedHashMap<>();
            lifecycle.put(shard, lifecycleEntries);
        }
        lifecycleEntries.put(digest, nextLifecycle);
        if (nextArtifactReference != null) {
            putArtifactReference(nextArtifactReference);
        }
        lastAuthorityTime = nextAuthorityTime;
        if (decision.snapshot().status() == RunStatus.READY) {
            currentEntry.stageAssignments = null;
        }
        if (decision.snapshot().status().isTerminal()) {
            shardEntries.remove(digest);
            if (shardEntries.isEmpty()) recoverable.remove(shard);
        } else {
            if (shardEntries == null) {
                shardEntries = new LinkedHashMap<>();
                recoverable.put(shard, shardEntries);
            }
            shardEntries.put(digest, new RecoverableRunRef(command.runId,
                    decision.snapshot().revision(), decision.snapshot().status()));
        }
        return decision;
    }

    @Override
    public synchronized RunSnapshot get(String runId) {
        RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
        Entry entry = runs.get(runId);
        return entry == null ? null : entry.snapshot;
    }

    /** Internal durable-authority restore of one retained terminal projection. */
    public synchronized void restoreTerminalCheckpoint(
            RunSnapshot snapshot, RecoveryToken recoveryToken) {
        if (snapshot == null || !snapshot.status().isTerminal()
                || recoveryToken == null
                || !snapshot.runId().equals(recoveryToken.runId())
                || runs.containsKey(snapshot.runId())) {
            throw new IllegalArgumentException(
                    "terminal Run checkpoint is invalid");
        }
        Entry entry = new Entry();
        entry.snapshot = snapshot;
        entry.recoveryOwner = recoveryToken;
        runs.put(snapshot.runId(), entry);
        String digest = RunLifecycleCatalogKernel.digestHex(snapshot.runId());
        int shardNumber = RunLifecycleCatalogKernel.shard(digest);
        Map<String, RunLifecycleRef> shard = lifecycle.computeIfAbsent(
                shardNumber, ignored -> new LinkedHashMap<>());
        if (!shard.containsKey(digest)
                && shard.size() >= maxLifecyclePerShard) {
            throw new IllegalArgumentException(
                    "terminal Run checkpoint exceeds lifecycle capacity");
        }
        long terminalAt = Math.max(1L, snapshot.updatedAtMillis());
        shard.put(digest, RunLifecycleRef.terminal(
                snapshot.runId(), snapshot.revision(), snapshot.status(),
                snapshot.artifactId(), terminalAt));
        lastAuthorityTime = Math.max(lastAuthorityTime, terminalAt);
    }

    @Override
    public synchronized DurableExecutionEvidenceView executionEvidence(String runId) {
        RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
        Entry entry = runs.get(runId);
        if (entry == null) return DurableExecutionEvidenceView.missing();
        requireLifecycleConsistency(entry.snapshot);
        if (entry.stageAssignments == null) {
            return DurableExecutionEvidenceView.snapshotOnly(entry.snapshot);
        }
        return DurableExecutionEvidenceView.present(
                entry.snapshot, entry.stageAssignments.snapshot());
    }

    @Override
    public synchronized List<ExecutionEvent> events(String runId, long afterSequence, int limit) {
        RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
        if (afterSequence < 0L) {
            throw new IllegalArgumentException("afterSequence must be >= 0");
        }
        if (limit < 1 || limit > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("limit must be in [1,1000]");
        }
        Entry entry = runs.get(runId);
        if (entry == null) {
            return Collections.emptyList();
        }
        RunLifecycleRef ref = lifecycle(runId);
        if (ref != null && ref.state() == RunLifecycleState.DELETING) {
            throw new RunHistoryRetiringException(runId);
        }
        List<ExecutionEvent> page = new ArrayList<>(Math.min(limit, entry.events.size()));
        for (ExecutionEvent event : entry.events) {
            if (event.sequence() > afterSequence) {
                page.add(event);
                if (page.size() == limit) {
                    break;
                }
            }
        }
        return Collections.unmodifiableList(page);
    }

    @Override
    public synchronized RunLifecycleRef lifecycle(String runId) {
        RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
        String digest = RunLifecycleCatalogKernel.digestHex(runId);
        Map<String, RunLifecycleRef> shard = lifecycle.get(
                RunLifecycleCatalogKernel.shard(digest));
        RunLifecycleRef ref = shard == null ? null : shard.get(digest);
        if (ref != null && !runId.equals(ref.runId())) {
            throw new IllegalStateException("lifecycle identity/hash mismatch");
        }
        return ref;
    }

    @Override
    public synchronized RunLifecyclePage scanLifecycle(String cursor, int limit) {
        RunLifecycleCatalogKernel.Cursor position =
                RunLifecycleCatalogKernel.decodeCursor(cursor);
        Map<String, RunLifecycleRef> shard = lifecycle.get(position.shard());
        List<RunLifecycleRef> refs = new ArrayList<>();
        if (shard != null) {
            if (shard.size() > maxLifecyclePerShard) {
                throw new IllegalStateException("lifecycle shard exceeds configured capacity");
            }
            for (RunLifecycleRef ref : shard.values()) {
                if (ref.state() != RunLifecycleState.ACTIVE) refs.add(ref);
            }
        }
        return RunLifecycleCatalogKernel.page(cursor, limit, refs);
    }

    @Override
    public synchronized RunRetentionClaim claim(
            String runId, long expectedTerminalRevision, long cutoffMillis,
            RetentionOwner owner, long observedOwnershipEpoch) {
        String digest = RunLifecycleCatalogKernel.digestHex(runId);
        int shardNumber = RunLifecycleCatalogKernel.shard(digest);
        Entry entry = runs.get(runId);
        Map<String, RunLifecycleRef> shard = lifecycle.get(shardNumber);
        RunLifecycleRef ref = shard == null ? null : shard.get(digest);
        Map<String, RecoverableRunRef> recoveryShard = recoverable.get(shardNumber);
        RunRetentionAuthorityKernel.Transition transition =
                RunRetentionAuthorityKernel.evaluate(
                        runId, entry == null ? null : entry.snapshot, ref,
                        recoveryShard != null && recoveryShard.containsKey(digest),
                        expectedTerminalRevision, cutoffMillis, owner,
                        observedOwnershipEpoch);
        if (transition.nextLifecycle() != null) {
            shard.put(digest, transition.nextLifecycle());
        }
        return transition.claim();
    }

    @Override
    public synchronized boolean current(RetentionToken token) {
        if (token == null) throw new IllegalArgumentException("token is required");
        return RunRetentionAuthorityKernel.current(lifecycle(token.runId()), token);
    }

    @Override
    public synchronized RunPurgeProgress purge(
            RetentionToken token, int maxDeletes) {
        validatePurge(token, maxDeletes);
        RunLifecycleRef ref = lifecycle(token.runId());
        if (!RunRetentionAuthorityKernel.current(ref, token)) {
            return RunPurgeProgress.staleOwner();
        }
        Entry entry = runs.get(token.runId());
        if (entry == null || !entry.snapshot.status().isTerminal()
                || entry.snapshot.revision() != token.terminalRevision()
                || ref.terminalAtMillis() != token.terminalAtMillis()) {
            return RunPurgeProgress.corrupt();
        }
        String digest = RunLifecycleCatalogKernel.digestHex(token.runId());
        int shardNumber = RunLifecycleCatalogKernel.shard(digest);
        Map<String, RecoverableRunRef> recoveryShard = recoverable.get(shardNumber);
        if (recoveryShard != null && recoveryShard.containsKey(digest)) {
            return RunPurgeProgress.corrupt();
        }
        ArtifactReferenceRef artifactReference = null;
        if (ArtifactReferenceRef.isContentAddressed(entry.snapshot.artifactId())) {
            artifactReference = artifactReference(
                    entry.snapshot.runId(), entry.snapshot.artifactId());
            if (artifactReference == null
                    || artifactReference.lifecycleRevision()
                    != entry.snapshot.revision()) {
                return RunPurgeProgress.corrupt();
            }
        }
        runs.remove(token.runId());
        Map<String, RunLifecycleRef> lifecycleShard = lifecycle.get(shardNumber);
        lifecycleShard.remove(digest);
        if (lifecycleShard.isEmpty()) lifecycle.remove(shardNumber);
        if (artifactReference != null) removeArtifactReference(artifactReference);
        return RunPurgeProgress.purged();
    }

    @Override
    public synchronized ArtifactReferenceView inspect(String artifactId) {
        String id = ArtifactReferenceRef.requireContentAddressed(artifactId);
        Map<Integer, Map<String, ArtifactReferenceRef>> shards =
                artifactReferences.get(id);
        if (shards == null) return ArtifactReferenceView.unreferenced();
        long count = 0L;
        for (Map<String, ArtifactReferenceRef> shard : shards.values()) {
            count = Math.addExact(count, shard.size());
        }
        return count == 0L
                ? ArtifactReferenceView.unreferenced()
                : ArtifactReferenceView.referenced(count);
    }

    @Override
    public ArtifactBackfillStatus backfillStatus() {
        return ArtifactBackfillStatus.ready();
    }

    @Override
    public ArtifactBackfillStatus advanceBackfill(int maxEntries) {
        if (maxEntries < 1 || maxEntries > 1000) {
            throw new IllegalArgumentException("maxEntries must be in [1,1000]");
        }
        return ArtifactBackfillStatus.ready();
    }

    private ArtifactReferenceRef prepareArtifactReference(
            RunSnapshot current, RunSnapshot next) {
        if (!ArtifactReferenceRef.isContentAddressed(next.artifactId())) {
            return null;
        }
        ArtifactReferenceRef existing = artifactReference(
                next.runId(), next.artifactId());
        if (current == null) {
            if (existing != null) {
                throw new IllegalStateException(
                        "Artifact reference exists without Run snapshot");
            }
        } else if (existing == null
                || existing.lifecycleRevision() != current.revision()
                || !existing.artifactId().equals(current.artifactId())) {
            throw new IllegalStateException(
                    "Run snapshot lacks exact Artifact reference");
        }
        return new ArtifactReferenceRef(
                next.runId(), next.artifactId(), next.revision());
    }

    private ArtifactReferenceRef artifactReference(String runId, String artifactId) {
        Map<Integer, Map<String, ArtifactReferenceRef>> byShard =
                artifactReferences.get(artifactId);
        if (byShard == null) return null;
        Map<String, ArtifactReferenceRef> shard = byShard.get(referenceShard(runId));
        if (shard == null) return null;
        String digest = RunLifecycleCatalogKernel.digestHex(runId);
        ArtifactReferenceRef ref = shard.get(digest);
        if (ref != null && !runId.equals(ref.runId())) {
            throw new IllegalStateException("Artifact reference identity/hash mismatch");
        }
        return ref;
    }

    private int artifactReferenceShardSize(String artifactId, int shard) {
        Map<Integer, Map<String, ArtifactReferenceRef>> byShard =
                artifactReferences.get(artifactId);
        if (byShard == null) return 0;
        Map<String, ArtifactReferenceRef> entries = byShard.get(shard);
        return entries == null ? 0 : entries.size();
    }

    private void putArtifactReference(ArtifactReferenceRef ref) {
        int shardNumber = referenceShard(ref.runId());
        Map<Integer, Map<String, ArtifactReferenceRef>> byShard =
                artifactReferences.computeIfAbsent(
                        ref.artifactId(), ignored -> new LinkedHashMap<>());
        Map<String, ArtifactReferenceRef> shard = byShard.computeIfAbsent(
                shardNumber, ignored -> new LinkedHashMap<>());
        shard.put(RunLifecycleCatalogKernel.digestHex(ref.runId()), ref);
    }

    private void removeArtifactReference(ArtifactReferenceRef ref) {
        int shardNumber = referenceShard(ref.runId());
        Map<Integer, Map<String, ArtifactReferenceRef>> byShard =
                artifactReferences.get(ref.artifactId());
        Map<String, ArtifactReferenceRef> shard = byShard.get(shardNumber);
        shard.remove(RunLifecycleCatalogKernel.digestHex(ref.runId()));
        if (shard.isEmpty()) byShard.remove(shardNumber);
        if (byShard.isEmpty()) artifactReferences.remove(ref.artifactId());
    }

    private static int referenceShard(String runId) {
        return RunLifecycleCatalogKernel.shard(
                RunLifecycleCatalogKernel.digestHex(runId));
    }

    private static void validatePurge(RetentionToken token, int maxDeletes) {
        if (token == null) throw new IllegalArgumentException("token is required");
        if (maxDeletes < 1 || maxDeletes > 1000) {
            throw new IllegalArgumentException("maxDeletes must be in [1,1000]");
        }
    }

    @Override
    public synchronized RecoverableRunPage scan(String cursor, int limit) {
        RunRecoveryCatalogKernel.Cursor decoded = RunRecoveryCatalogKernel.decodeCursor(cursor);
        Map<String, RecoverableRunRef> shard = recoverable.get(decoded.shard());
        List<RecoverableRunRef> refs = shard == null
                ? Collections.emptyList() : new ArrayList<>(shard.values());
        return RunRecoveryCatalogKernel.page(cursor, limit, refs);
    }

    @Override
    public synchronized RecoveryToken ownership(String runId) {
        RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
        Entry entry = runs.get(runId);
        return entry == null ? null : entry.recoveryOwner;
    }

    @Override
    public synchronized RecoveryClaim claim(String runId, long expectedRevision,
                                            long expectedOwnershipEpoch,
                                            RecoveryOwner owner) {
        RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
        if (expectedRevision <= 0L) {
            throw new IllegalArgumentException("expectedRevision must be > 0");
        }
        if (expectedOwnershipEpoch < 0L) {
            throw new IllegalArgumentException("expectedOwnershipEpoch must be >= 0");
        }
        if (owner == null) throw new IllegalArgumentException("owner is required");

        Entry entry = runs.get(runId);
        if (entry == null || entry.snapshot.status().isTerminal()
                || entry.snapshot.revision() != expectedRevision) {
            return RecoveryClaim.staleRun();
        }
        requireCatalogConsistency(entry.snapshot);
        RecoveryToken current = entry.recoveryOwner;
        if (current != null && current.owner().equals(owner)) {
            return RecoveryClaim.replayed(current);
        }
        if (entry.stageAssignments != null
                && entry.stageAssignments.acceptanceGeneration == Long.MAX_VALUE) {
            throw new IllegalStateException("acceptance generation exhausted");
        }
        if (current == null) {
            if (expectedOwnershipEpoch != 0L) return RecoveryClaim.lostRace();
            entry.recoveryOwner = RecoveryToken.create(runId, owner, 1L, expectedRevision);
        } else {
            if (current.ownershipEpoch() != expectedOwnershipEpoch) {
                return RecoveryClaim.lostRace();
            }
            if (current.ownershipEpoch() == Long.MAX_VALUE) {
                throw new IllegalStateException("recovery ownership epoch exhausted");
            }
            entry.recoveryOwner = RecoveryToken.create(runId, owner,
                    current.ownershipEpoch() + 1L, expectedRevision);
        }
        if (entry.stageAssignments != null) {
            entry.stageAssignments.advanceAcceptanceGeneration();
        }
        return RecoveryClaim.acquired(entry.recoveryOwner);
    }

    @Override
    public synchronized boolean current(RecoveryToken token) {
        if (token == null) throw new IllegalArgumentException("token is required");
        Entry entry = runs.get(token.runId());
        return entry != null && token.equals(entry.recoveryOwner);
    }

    @Override
    public synchronized StageManifestPublication publish(StageAssignmentManifest manifest) {
        return publish(manifest, null, false);
    }

    @Override
    public synchronized StageManifestPublication publishRecovered(
            RecoveryToken token, StageAssignmentManifest manifest) {
        if (token == null) throw new IllegalArgumentException("token is required");
        if (manifest == null) throw new IllegalArgumentException("manifest is required");
        if (!token.runId().equals(manifest.fence().runId())) {
            throw new IllegalArgumentException("recovery token and manifest runId mismatch");
        }
        return publish(manifest, token, true);
    }

    private StageManifestPublication publish(StageAssignmentManifest manifest,
                                             RecoveryToken token,
                                             boolean recovered) {
        if (manifest == null) throw new IllegalArgumentException("manifest is required");
        Entry entry = runs.get(manifest.fence().runId());
        if (!matchesCurrentAttempt(entry, manifest.fence())
                || !publishable(entry.snapshot.currentAttempt().status())
                || !entry.snapshot.artifactId().equals(manifest.artifactId())) {
            return StageManifestPublication.rejected(StageManifestPublication.Code.STALE_RUN);
        }
        if ((!recovered && entry.recoveryOwner != null)
                || (recovered && !token.equals(entry.recoveryOwner))) {
            return StageManifestPublication.rejected(StageManifestPublication.Code.RECOVERY_OWNED);
        }
        if (entry.stageAssignments != null) {
            return sameManifest(entry.stageAssignments.manifest, manifest)
                    ? StageManifestPublication.replayed(entry.stageAssignments.snapshot())
                    : StageManifestPublication.rejected(StageManifestPublication.Code.CONFLICT);
        }
        entry.stageAssignments = new AssignmentState(manifest);
        return StageManifestPublication.published(entry.stageAssignments.snapshot());
    }

    private static boolean publishable(AttemptStatus status) {
        return status == AttemptStatus.CREATED || status == AttemptStatus.DISPATCHING;
    }

    @Override
    public synchronized StageAssignmentSnapshot assignmentSnapshot(RunAttemptFence fence) {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        Entry entry = runs.get(fence.runId());
        return matchesCurrentAttempt(entry, fence) && entry.stageAssignments != null
                && entry.stageAssignments.manifest.fence().equals(fence)
                ? entry.stageAssignments.snapshot() : null;
    }

    @Override
    public synchronized StageAcceptanceDecision accept(StageAssignmentAcceptance request) {
        return accept(request, null, false);
    }

    @Override
    public synchronized StageAcceptanceDecision acceptRecovered(
            RecoveryToken recoveryToken, StageAssignmentAcceptance request) {
        if (recoveryToken == null) throw new IllegalArgumentException("token is required");
        if (request == null) throw new IllegalArgumentException("acceptance is required");
        if (!recoveryToken.runId().equals(request.token().fence().runId())) {
            throw new IllegalArgumentException("recovery token and acceptance runId mismatch");
        }
        return accept(request, recoveryToken, true);
    }

    private StageAcceptanceDecision accept(StageAssignmentAcceptance request,
                                           RecoveryToken recoveryToken,
                                           boolean recovered) {
        if (request == null) throw new IllegalArgumentException("acceptance is required");
        StageAssignmentToken token = request.token();
        Entry entry = runs.get(token.fence().runId());
        if (!matchesCurrentAttempt(entry, token.fence()) || entry.stageAssignments == null) {
            return StageAcceptanceDecision.rejected(StageAcceptanceDecision.Code.STALE_RUN);
        }
        AssignmentState state = entry.stageAssignments;
        StageAssignmentManifest manifest = state.manifest;
        int ordinal = token.physicalOrdinal();
        if (token.manifestGeneration() != manifest.generation()
                || ordinal < 0 || ordinal >= manifest.assignmentCount()) {
            return StageAcceptanceDecision.rejected(
                    StageAcceptanceDecision.Code.ASSIGNMENT_CONFLICT);
        }
        StageAssignmentRecord record = manifest.assignment(ordinal);
        if (!record.stageExecutionId().equals(token.stageExecutionId())
                || record.runtimeStageId() != token.runtimeStageId()
                || !record.workerId().equals(token.workerId())
                || !record.workerIncarnationId().equals(token.workerIncarnationId())) {
            return StageAcceptanceDecision.rejected(
                    StageAcceptanceDecision.Code.ASSIGNMENT_CONFLICT);
        }
        StageAssignmentAcceptance existing = state.acceptances.get(ordinal);
        if (existing != null) {
            return existing.token().equals(token)
                    && MessageDigest.isEqual(existing.commandDigest(), request.commandDigest())
                    ? StageAcceptanceDecision.alreadyAccepted(existing)
                    : StageAcceptanceDecision.rejected(
                    StageAcceptanceDecision.Code.ASSIGNMENT_CONFLICT);
        }
        if (entry.abortRecord != null) {
            return StageAcceptanceDecision.rejected(
                    StageAcceptanceDecision.Code.ASSIGNMENT_CONFLICT);
        }
        if (token.acceptanceGeneration() != state.acceptanceGeneration) {
            return StageAcceptanceDecision.staleGeneration();
        }
        if ((!recovered && entry.recoveryOwner != null)
                || (recovered && !recoveryToken.equals(entry.recoveryOwner))) {
            return StageAcceptanceDecision.rejected(StageAcceptanceDecision.Code.RECOVERY_OWNED);
        }
        state.acceptances.set(ordinal, request);
        return StageAcceptanceDecision.accepted(request);
    }

    @Override
    public synchronized StageTerminalDecision recordTerminal(StageTerminalReceipt receipt) {
        return recordTerminal(receipt, null, false);
    }

    @Override
    public synchronized StageTerminalDecision recordTerminalRecovered(
            RecoveryToken recoveryToken, StageTerminalReceipt receipt) {
        if (recoveryToken == null) throw new IllegalArgumentException("token is required");
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        if (!recoveryToken.runId().equals(receipt.token().fence().runId())) {
            throw new IllegalArgumentException("recovery token and receipt runId mismatch");
        }
        return recordTerminal(receipt, recoveryToken, true);
    }

    private StageTerminalDecision recordTerminal(StageTerminalReceipt receipt,
                                                 RecoveryToken recoveryToken,
                                                 boolean recovered) {
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        StageAssignmentToken token = receipt.token();
        Entry entry = runs.get(token.fence().runId());
        if (!matchesCurrentAttempt(entry, token.fence()) || entry.stageAssignments == null) {
            return StageTerminalDecision.rejected(StageTerminalDecision.Code.STALE_RUN);
        }
        AssignmentState state = entry.stageAssignments;
        StageAssignmentManifest manifest = state.manifest;
        int ordinal = token.physicalOrdinal();
        if (token.manifestGeneration() != manifest.generation()
                || ordinal < 0 || ordinal >= manifest.assignmentCount()) {
            return StageTerminalDecision.rejected(
                    StageTerminalDecision.Code.ASSIGNMENT_CONFLICT);
        }
        StageAssignmentRecord record = manifest.assignment(ordinal);
        if (!record.stageExecutionId().equals(token.stageExecutionId())
                || record.runtimeStageId() != token.runtimeStageId()
                || !record.workerId().equals(token.workerId())
                || !record.workerIncarnationId().equals(token.workerIncarnationId())) {
            return StageTerminalDecision.rejected(
                    StageTerminalDecision.Code.ASSIGNMENT_CONFLICT);
        }
        StageAssignmentAcceptance acceptance = state.acceptances.get(ordinal);
        if (acceptance == null) {
            return StageTerminalDecision.rejected(
                    StageTerminalDecision.Code.ACCEPTANCE_REQUIRED);
        }
        if (!acceptance.token().equals(token)
                || !MessageDigest.isEqual(
                acceptance.commandDigest(), receipt.commandDigest())) {
            return StageTerminalDecision.rejected(
                    StageTerminalDecision.Code.ASSIGNMENT_CONFLICT);
        }
        StageTerminalReceipt existing = state.terminals.get(ordinal);
        if (existing != null) {
            return existing.hasSameTerminalBase(receipt)
                    ? StageTerminalDecision.alreadyRecorded(existing)
                    : StageTerminalDecision.rejected(
                    StageTerminalDecision.Code.TERMINAL_CONFLICT);
        }
        if ((!recovered && entry.recoveryOwner != null)
                || (recovered && !recoveryToken.equals(entry.recoveryOwner))) {
            return StageTerminalDecision.rejected(StageTerminalDecision.Code.RECOVERY_OWNED);
        }
        if (receipt.outcome() == StageTerminalOutcome.LOST && !recovered) {
            return StageTerminalDecision.rejected(
                    StageTerminalDecision.Code.LOST_REQUIRES_RECOVERY);
        }
        state.terminals.set(ordinal, receipt);
        return StageTerminalDecision.recorded(receipt);
    }

    @Override
    public synchronized PreparedSinkDecision recordPreparedSink(
            PreparedSinkReceipt receipt) {
        return recordPreparedSink(receipt, null, false);
    }

    @Override
    public synchronized PreparedSinkDecision recordPreparedSinkRecovered(
            RecoveryToken recoveryToken, PreparedSinkReceipt receipt) {
        if (recoveryToken == null) throw new IllegalArgumentException("token is required");
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        if (!recoveryToken.runId().equals(receipt.token().fence().runId())) {
            throw new IllegalArgumentException(
                    "recovery token and prepared receipt runId mismatch");
        }
        return recordPreparedSink(receipt, recoveryToken, true);
    }

    private PreparedSinkDecision recordPreparedSink(
            PreparedSinkReceipt receipt, RecoveryToken recoveryToken,
            boolean recovered) {
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        StageAssignmentToken token = receipt.token();
        Entry entry = runs.get(token.fence().runId());
        if (!matchesCurrentAttempt(entry, token.fence())
                || entry.stageAssignments == null) {
            return PreparedSinkDecision.rejected(
                    PreparedSinkDecision.Code.STALE_RUN);
        }
        AssignmentState state = entry.stageAssignments;
        int ordinal = token.physicalOrdinal();
        if (ordinal < 0 || ordinal >= state.manifest.assignmentCount()
                || token.manifestGeneration() != state.manifest.generation()) {
            return PreparedSinkDecision.rejected(
                    PreparedSinkDecision.Code.ASSIGNMENT_CONFLICT);
        }
        StageAssignmentAcceptance acceptance = state.acceptances.get(ordinal);
        StageTerminalReceipt terminal = state.terminals.get(ordinal);
        if (terminal == null) {
            return PreparedSinkDecision.rejected(
                    PreparedSinkDecision.Code.TERMINAL_REQUIRED);
        }
        if (!terminal.succeeded() || acceptance == null
                || !receipt.token().equals(terminal.token())
                || !receipt.token().equals(acceptance.token())
                || !MessageDigest.isEqual(
                receipt.commandDigest(), terminal.commandDigest())
                || !MessageDigest.isEqual(
                receipt.commandDigest(), acceptance.commandDigest())) {
            return PreparedSinkDecision.rejected(
                    PreparedSinkDecision.Code.ASSIGNMENT_CONFLICT);
        }
        PreparedSinkReceipt existing = state.preparedSinks.get(ordinal);
        if (existing != null) {
            return existing.equals(receipt)
                    ? PreparedSinkDecision.alreadyRecorded(existing)
                    : PreparedSinkDecision.rejected(
                    PreparedSinkDecision.Code.LOCATOR_CONFLICT);
        }
        if (entry.abortRecord != null) {
            return PreparedSinkDecision.rejected(
                    PreparedSinkDecision.Code.LOCATOR_CONFLICT);
        }
        if ((!recovered && entry.recoveryOwner != null)
                || recovered && !recoveryToken.equals(entry.recoveryOwner)) {
            return PreparedSinkDecision.rejected(
                    PreparedSinkDecision.Code.RECOVERY_OWNED);
        }
        state.preparedSinks.set(ordinal, receipt);
        return PreparedSinkDecision.recorded(receipt);
    }

    @Override
    public synchronized ManifestCommitJournalSnapshot manifestCommitJournal(
            RunAttemptFence fence) {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        Entry entry = runs.get(fence.runId());
        return matchesCurrentAttempt(entry, fence) && entry.stageAssignments != null
                && entry.stageAssignments.manifest.fence().equals(fence)
                ? entry.stageAssignments.journalSnapshot() : null;
    }

    @Override
    public synchronized QualityGateCommitJournalSnapshot qualityGateCommitJournal(
            RunAttemptFence fence) {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        Entry entry = runs.get(fence.runId());
        return matchesCurrentAttempt(entry, fence) && entry.stageAssignments != null
                && entry.stageAssignments.manifest.fence().equals(fence)
                ? entry.stageAssignments.qualityJournalSnapshot() : null;
    }

    @Override
    public synchronized QualityGateDecision qualityGateDecision(
            RunAttemptFence fence) {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        Entry entry = runs.get(fence.runId());
        return sameAttemptIdentity(entry, fence)
                && entry.stageAssignments != null
                && entry.stageAssignments.manifest.fence().equals(fence)
                ? entry.stageAssignments.qualityDecision : null;
    }

    @Override
    public synchronized QualityGateDecisionResult recordQualityGateDecision(
            QualityGateDecision decision) {
        return recordQualityGateDecision(decision, null, false);
    }

    @Override
    public synchronized QualityGateDecisionResult recordQualityGateDecisionRecovered(
            RecoveryToken recoveryToken, QualityGateDecision decision) {
        if (recoveryToken == null) throw new IllegalArgumentException("token is required");
        if (decision == null) throw new IllegalArgumentException("decision is required");
        if (!recoveryToken.runId().equals(decision.fence().runId())) {
            throw new IllegalArgumentException(
                    "recovery token and quality decision runId mismatch");
        }
        return recordQualityGateDecision(decision, recoveryToken, true);
    }

    private QualityGateDecisionResult recordQualityGateDecision(
            QualityGateDecision decision, RecoveryToken recoveryToken,
            boolean recovered) {
        if (decision == null) throw new IllegalArgumentException("decision is required");
        Entry entry = runs.get(decision.fence().runId());
        if (!matchesCurrentAttempt(entry, decision.fence())
                || entry.stageAssignments == null) {
            return QualityGateDecisionResult.rejected(
                    QualityGateDecisionResult.Code.STALE_RUN);
        }
        AssignmentState state = entry.stageAssignments;
        if (state.qualityDecision != null) {
            return state.qualityDecision.equals(decision)
                    ? QualityGateDecisionResult.alreadyRecorded(state.qualityDecision)
                    : QualityGateDecisionResult.rejected(
                    QualityGateDecisionResult.Code.DECISION_CONFLICT);
        }
        if (entry.abortRecord != null) {
            return QualityGateDecisionResult.rejected(
                    QualityGateDecisionResult.Code.ABORT_ALREADY_DECIDED);
        }
        if (state.manifestIntent != null) {
            return QualityGateDecisionResult.rejected(
                    QualityGateDecisionResult.Code.DECISION_CONFLICT);
        }
        if (entry.snapshot.currentAttempt().status() != AttemptStatus.COMMITTING) {
            return QualityGateDecisionResult.rejected(
                    QualityGateDecisionResult.Code.ATTEMPT_NOT_COMMITTING);
        }
        if (!decision.fence().equals(state.manifest.fence())
                || !decision.artifactId().equals(state.manifest.artifactId())
                || !decision.publicationId().equals(
                entry.snapshot.currentAttempt().publicationId())
                || state.manifest.qualityGateBinding() == null
                || !MessageDigest.isEqual(
                state.manifest.qualityGateBinding().policyDigest(),
                decision.policyDigest())) {
            return QualityGateDecisionResult.rejected(
                    QualityGateDecisionResult.Code.IDENTITY_CONFLICT);
        }
        QualityGateCommitJournalSnapshot journal = state.qualityJournalSnapshot();
        if (journal == null || !journal.allMembersSealed()
                || journal.memberCount() != QualityGateAssignmentBinding.MEMBER_COUNT) {
            return QualityGateDecisionResult.rejected(
                    QualityGateDecisionResult.Code.INCOMPLETE_MEMBERS);
        }
        try {
            journal.withQualityDecision(decision);
        } catch (IllegalArgumentException mismatch) {
            return QualityGateDecisionResult.rejected(
                    QualityGateDecisionResult.Code.EVIDENCE_CONFLICT);
        }
        if ((!recovered && entry.recoveryOwner != null)
                || (recovered && !recoveryToken.equals(entry.recoveryOwner))) {
            return QualityGateDecisionResult.rejected(
                    QualityGateDecisionResult.Code.RECOVERY_OWNED);
        }
        state.qualityDecision = decision;
        return QualityGateDecisionResult.recorded(decision);
    }

    @Override
    public synchronized BatchSinkAbortRecord batchSinkAbort(RunAttemptFence fence) {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        Entry entry = runs.get(fence.runId());
        return entry != null && entry.abortRecord != null
                && entry.abortRecord.fence().equals(fence)
                ? entry.abortRecord : null;
    }

    @Override
    public synchronized BatchSinkAbortDecision recordBatchSinkAbort(
            BatchSinkAbortRecord record) {
        return recordBatchSinkAbort(record, null, false);
    }

    @Override
    public synchronized BatchSinkAbortDecision recordBatchSinkAbortRecovered(
            RecoveryToken token, BatchSinkAbortRecord record) {
        if (token == null) throw new IllegalArgumentException("token is required");
        if (record == null) throw new IllegalArgumentException("abort record is required");
        if (!token.runId().equals(record.fence().runId())) {
            throw new IllegalArgumentException("recovery token and abort runId mismatch");
        }
        return recordBatchSinkAbort(record, token, true);
    }

    private BatchSinkAbortDecision recordBatchSinkAbort(
            BatchSinkAbortRecord record, RecoveryToken recoveryToken,
            boolean recovered) {
        if (record == null) throw new IllegalArgumentException("abort record is required");
        Entry entry = runs.get(record.fence().runId());
        if (!matchesCurrentAttempt(entry, record.fence())
                || entry.stageAssignments == null) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.STALE_RUN);
        }
        AssignmentState state = entry.stageAssignments;
        if (state.manifestIntent != null) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.COMMIT_ALREADY_DECIDED);
        }
        if (state.qualityDecision != null
                && state.qualityDecision.outcome() == QualityGateDecision.Outcome.FAIL
                && (!"QUALITY_GATE_REJECTED".equals(record.reasonCode())
                || !MessageDigest.isEqual(record.identityDigest(),
                QualityGateDecisionCodec.digest(state.qualityDecision)))) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.RECORD_CONFLICT);
        }
        AttemptStatus attemptStatus = entry.snapshot.currentAttempt().status();
        boolean abortableCanceling = attemptStatus == AttemptStatus.CANCELING
                && (entry.snapshot.currentAttempt().cancellationCause()
                == CancellationCause.USER
                || entry.snapshot.currentAttempt().cancellationCause()
                == CancellationCause.TIMEOUT);
        if (attemptStatus != AttemptStatus.RUNNING
                && attemptStatus != AttemptStatus.COMMITTING
                && attemptStatus != AttemptStatus.RECONCILING
                && !abortableCanceling) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.ATTEMPT_NOT_ABORTABLE);
        }
        if (record.assignmentGeneration() != state.manifest.generation()
                || !record.publicationId().equals(
                entry.snapshot.currentAttempt().publicationId())) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.RECORD_CONFLICT);
        }
        Iterable<ManifestMemberReceipt> identityMembers =
                state.manifest.hasQualityGateBinding()
                        ? state.qualityMembers() : state.manifestMembers;
        for (ManifestMemberReceipt member : identityMembers) {
            if (member != null && (!record.commitDomainId().equals(member.domainId())
                    || record.generation() != member.generation()
                    || !record.publicationId().equals(member.publicationId()))) {
                return BatchSinkAbortDecision.rejected(
                        BatchSinkAbortDecision.Code.RECORD_CONFLICT);
            }
        }
        BatchSinkAbortRecord existing = entry.abortRecord;
        if ((!recovered && entry.recoveryOwner != null)
                || (recovered && !recoveryToken.equals(entry.recoveryOwner))) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.RECOVERY_OWNED);
        }
        if (existing != null) {
            if (existing.equals(record)) {
                return BatchSinkAbortDecision.alreadyRecorded(existing);
            }
            if (existing.state() == BatchSinkAbortRecord.State.ABORT_DECIDED
                    && record.state() == BatchSinkAbortRecord.State.ABORTED
                    && existing.sameDecision(record)) {
                entry.abortRecord = record;
                return BatchSinkAbortDecision.recorded(record);
            }
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.RECORD_CONFLICT);
        }
        if (record.state() != BatchSinkAbortRecord.State.ABORT_DECIDED) {
            return BatchSinkAbortDecision.rejected(
                    BatchSinkAbortDecision.Code.RECORD_CONFLICT);
        }
        entry.abortRecord = record;
        return BatchSinkAbortDecision.recorded(record);
    }

    @Override
    public synchronized ManifestMemberDecision recordManifestMember(
            ManifestMemberReceipt receipt) {
        return recordManifestMember(receipt, null, false);
    }

    @Override
    public synchronized ManifestMemberDecision recordManifestMemberRecovered(
            RecoveryToken recoveryToken, ManifestMemberReceipt receipt) {
        if (recoveryToken == null) throw new IllegalArgumentException("token is required");
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        if (!recoveryToken.runId().equals(receipt.token().fence().runId())) {
            throw new IllegalArgumentException("recovery token and member runId mismatch");
        }
        return recordManifestMember(receipt, recoveryToken, true);
    }

    private ManifestMemberDecision recordManifestMember(
            ManifestMemberReceipt receipt, RecoveryToken recoveryToken,
            boolean recovered) {
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        StageAssignmentToken token = receipt.token();
        Entry entry = runs.get(token.fence().runId());
        if (!matchesCurrentAttempt(entry, token.fence()) || entry.stageAssignments == null) {
            return ManifestMemberDecision.rejected(ManifestMemberDecision.Code.STALE_RUN);
        }
        AssignmentState state = entry.stageAssignments;
        StageAssignmentManifest manifest = state.manifest;
        int ordinal = token.physicalOrdinal();
        if (token.manifestGeneration() != manifest.generation()
                || ordinal < 0 || ordinal >= manifest.assignmentCount()) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.ASSIGNMENT_CONFLICT);
        }
        StageAssignmentRecord record = manifest.assignment(ordinal);
        if (!record.stageExecutionId().equals(token.stageExecutionId())
                || record.runtimeStageId() != token.runtimeStageId()
                || !record.workerId().equals(token.workerId())
                || !record.workerIncarnationId().equals(token.workerIncarnationId())
                || !ManifestCommitJournalSnapshot.outputRole(record.role())) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.ASSIGNMENT_CONFLICT);
        }
        StageAssignmentAcceptance acceptance = state.acceptances.get(ordinal);
        StageTerminalReceipt terminal = state.terminals.get(ordinal);
        if (acceptance == null || terminal == null) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.TERMINAL_REQUIRED);
        }
        if (!acceptance.token().equals(token)
                || !terminal.token().equals(token)
                || !MessageDigest.isEqual(
                acceptance.commandDigest(), receipt.commandDigest())
                || !MessageDigest.isEqual(
                terminal.commandDigest(), receipt.commandDigest())) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.ASSIGNMENT_CONFLICT);
        }
        if (!terminal.succeeded()) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.TERMINAL_NOT_SUCCESSFUL);
        }
        if (!entry.snapshot.currentAttempt().publicationId()
                .equals(receipt.publicationId())) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.COMMIT_IDENTITY_CONFLICT);
        }
        if (manifest.hasQualityGateBinding()
                && (!receipt.hasPreparedLocator()
                || !manifest.qualityGateBinding().matches(ordinal,
                receipt.preparedLocator().attemptIdentity()))) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.ASSIGNMENT_CONFLICT);
        }
        ManifestMemberReceipt existing = state.manifestMembers.get(ordinal);
        if (existing != null) {
            return existing.equals(receipt)
                    ? ManifestMemberDecision.alreadyRecorded(existing)
                    : ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.MEMBER_CONFLICT);
        }
        if (state.manifestIntent != null) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.INTENT_ALREADY_COMMITTED);
        }
        if (state.qualityDecision != null) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.MEMBER_CONFLICT);
        }
        if (entry.abortRecord != null) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.MEMBER_CONFLICT);
        }
        Iterable<ManifestMemberReceipt> identityMembers =
                manifest.hasQualityGateBinding()
                        ? state.qualityMembers() : state.manifestMembers;
        for (ManifestMemberReceipt member : identityMembers) {
            if (member == null) continue;
            if (!member.domainId().equals(receipt.domainId())
                    || member.generation() != receipt.generation()
                    || !member.publicationId().equals(receipt.publicationId())) {
                return ManifestMemberDecision.rejected(
                        ManifestMemberDecision.Code.COMMIT_IDENTITY_CONFLICT);
            }
            if (member.member().name().equals(receipt.member().name())) {
                return ManifestMemberDecision.rejected(
                        ManifestMemberDecision.Code.MEMBER_CONFLICT);
            }
        }
        if ((!recovered && entry.recoveryOwner != null)
                || (recovered && !recoveryToken.equals(entry.recoveryOwner))) {
            return ManifestMemberDecision.rejected(
                    ManifestMemberDecision.Code.RECOVERY_OWNED);
        }
        state.manifestMembers.set(ordinal, receipt);
        return ManifestMemberDecision.recorded(receipt);
    }

    @Override
    public synchronized ManifestIntentDecision commitManifestIntent(
            ManifestCommitIntent intent) {
        return commitManifestIntent(intent, null, false);
    }

    @Override
    public synchronized ManifestIntentDecision commitManifestIntentRecovered(
            RecoveryToken recoveryToken, ManifestCommitIntent intent) {
        if (recoveryToken == null) throw new IllegalArgumentException("token is required");
        if (intent == null) throw new IllegalArgumentException("intent is required");
        if (!recoveryToken.runId().equals(intent.fence().runId())) {
            throw new IllegalArgumentException("recovery token and intent runId mismatch");
        }
        return commitManifestIntent(intent, recoveryToken, true);
    }

    private ManifestIntentDecision commitManifestIntent(
            ManifestCommitIntent intent, RecoveryToken recoveryToken,
            boolean recovered) {
        if (intent == null) throw new IllegalArgumentException("intent is required");
        Entry entry = runs.get(intent.fence().runId());
        if (!matchesCurrentAttempt(entry, intent.fence())
                || entry.stageAssignments == null) {
            return ManifestIntentDecision.rejected(ManifestIntentDecision.Code.STALE_RUN);
        }
        AssignmentState state = entry.stageAssignments;
        if (state.manifestIntent != null) {
            return state.manifestIntent.equals(intent)
                    ? ManifestIntentDecision.alreadyCommitted(state.manifestIntent)
                    : ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.INTENT_CONFLICT);
        }
        if (state.qualityDecision == null && isReservedQualityJournal(state)) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.INTENT_CONFLICT);
        }
        if (state.qualityDecision == null && intent.hasQualityDecision()) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.INTENT_CONFLICT);
        }
        if (state.qualityDecision != null
                && (!intent.hasQualityDecision()
                || state.qualityDecision.outcome()
                != QualityGateDecision.Outcome.PASS
                || !state.qualityDecision.equals(intent.qualityDecision()))) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.INTENT_CONFLICT);
        }
        if (entry.abortRecord != null) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.INTENT_CONFLICT);
        }
        if (entry.snapshot.currentAttempt().status() != AttemptStatus.COMMITTING) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.ATTEMPT_NOT_COMMITTING);
        }
        if (intent.assignmentGeneration() != state.manifest.generation()
                || !intent.fence().equals(state.manifest.fence())
                || !intent.publicationId().equals(
                entry.snapshot.currentAttempt().publicationId())) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.INTENT_CONFLICT);
        }
        ManifestCommitIntent expected;
        if (state.manifest.hasQualityGateBinding()) {
            QualityGateCommitJournalSnapshot journal =
                    state.qualityJournalSnapshot();
            if (journal == null || !journal.allMembersSealed()) {
                return ManifestIntentDecision.rejected(
                        ManifestIntentDecision.Code.INCOMPLETE_MEMBERS);
            }
            expected = journal.buildIntent();
        } else {
            ManifestCommitJournalSnapshot journal = state.journalSnapshot();
            if (!journal.allExpectedMembersSealed()) {
                return ManifestIntentDecision.rejected(
                        ManifestIntentDecision.Code.INCOMPLETE_MEMBERS);
            }
            expected = journal.buildIntent();
        }
        if (!expected.equals(intent)) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.INTENT_CONFLICT);
        }
        if ((!recovered && entry.recoveryOwner != null)
                || (recovered && !recoveryToken.equals(entry.recoveryOwner))) {
            return ManifestIntentDecision.rejected(
                    ManifestIntentDecision.Code.RECOVERY_OWNED);
        }
        state.manifestIntent = intent;
        return ManifestIntentDecision.committed(intent);
    }

    private static boolean matchesCurrentAttempt(Entry entry, RunAttemptFence fence) {
        if (entry == null || entry.snapshot == null || entry.snapshot.status().isTerminal()) {
            return false;
        }
        AttemptSnapshot attempt = entry.snapshot.currentAttempt();
        return attempt != null
                && entry.snapshot.runId().equals(fence.runId())
                && attempt.attemptId().equals(fence.attemptId())
                && attempt.fenceToken() == fence.fenceToken();
    }

    private static boolean sameAttemptIdentity(Entry entry, RunAttemptFence fence) {
        if (entry == null || entry.snapshot == null) return false;
        AttemptSnapshot attempt = entry.snapshot.currentAttempt();
        return attempt != null
                && entry.snapshot.runId().equals(fence.runId())
                && attempt.attemptId().equals(fence.attemptId())
                && attempt.fenceToken() == fence.fenceToken();
    }

    private static boolean isReservedQualityJournal(AssignmentState state) {
        return state.manifest.hasQualityGateBinding();
    }

    private static boolean sameManifest(StageAssignmentManifest left,
                                        StageAssignmentManifest right) {
        if (!MessageDigest.isEqual(StageAssignmentManifestCodec.encodeRoot(left),
                StageAssignmentManifestCodec.encodeRoot(right))) {
            return false;
        }
        if (left.shardCount() != right.shardCount()) return false;
        for (int i = 0; i < left.shardCount(); i++) {
            if (!MessageDigest.isEqual(StageAssignmentManifestCodec.encodeShard(left, i),
                    StageAssignmentManifestCodec.encodeShard(right, i))) {
                return false;
            }
        }
        return true;
    }

    private void requireCatalogConsistency(RunSnapshot snapshot) {
        String digest = RunRecoveryCatalogKernel.digestHex(snapshot.runId());
        Map<String, RecoverableRunRef> shard = recoverable.get(
                RunRecoveryCatalogKernel.shard(digest));
        RecoverableRunRef ref = shard == null ? null : shard.get(digest);
        if (ref == null || !ref.runId().equals(snapshot.runId())
                || ref.revision() != snapshot.revision()
                || ref.status() != snapshot.status()) {
            throw new IllegalStateException("recoverable run catalog is inconsistent");
        }
    }

    private void requireLifecycleConsistency(RunSnapshot snapshot) {
        String digest = RunLifecycleCatalogKernel.digestHex(snapshot.runId());
        Map<String, RunLifecycleRef> shard = lifecycle.get(
                RunLifecycleCatalogKernel.shard(digest));
        RunLifecycleRef ref = shard == null ? null : shard.get(digest);
        boolean expectedState = snapshot.status().isTerminal()
                ? ref != null && (ref.state() == RunLifecycleState.TERMINAL
                || ref.state() == RunLifecycleState.DELETING)
                : ref != null && ref.state() == RunLifecycleState.ACTIVE;
        if (ref == null || !ref.runId().equals(snapshot.runId())
                || ref.revision() != snapshot.revision()
                || ref.status() != snapshot.status()
                || !ref.artifactId().equals(snapshot.artifactId())
                || !expectedState) {
            throw new IllegalStateException("run lifecycle catalog is inconsistent");
        }
    }

    private static final class Entry {
        RunSnapshot snapshot;
        RecoveryToken recoveryOwner;
        AssignmentState stageAssignments;
        BatchSinkAbortRecord abortRecord;
        RetryScheduleRecord retrySchedule;
        final List<ExecutionEvent> events = new ArrayList<>();
        final Map<String, CommandRecord> commands = new LinkedHashMap<>();
    }

    private static final class RetryScheduleRecord {
        final RunAttemptFence fence;
        final WholeAttemptRetryClass retryClass;
        final String commandId;
        final WholeAttemptRetryDecision.Code code;
        final long observedAtMillis;

        private RetryScheduleRecord(
                RunAttemptFence fence, WholeAttemptRetryClass retryClass,
                String commandId, WholeAttemptRetryDecision.Code code,
                long observedAtMillis) {
            this.fence = fence;
            this.retryClass = retryClass;
            this.commandId = commandId;
            this.code = code;
            this.observedAtMillis = observedAtMillis;
        }
    }

    private static final class AssignmentState {
        final StageAssignmentManifest manifest;
        long acceptanceGeneration = 1L;
        final List<StageAssignmentAcceptance> acceptances;
        final List<StageTerminalReceipt> terminals;
        final List<PreparedSinkReceipt> preparedSinks;
        final List<ManifestMemberReceipt> manifestMembers;
        QualityGateDecision qualityDecision;
        ManifestCommitIntent manifestIntent;

        AssignmentState(StageAssignmentManifest manifest) {
            this.manifest = manifest;
            this.acceptances = new ArrayList<>(Collections.nCopies(
                    manifest.assignmentCount(), (StageAssignmentAcceptance) null));
            this.terminals = new ArrayList<>(Collections.nCopies(
                    manifest.assignmentCount(), (StageTerminalReceipt) null));
            this.preparedSinks = new ArrayList<>(Collections.nCopies(
                    manifest.assignmentCount(), (PreparedSinkReceipt) null));
            this.manifestMembers = new ArrayList<>(Collections.nCopies(
                    manifest.assignmentCount(), (ManifestMemberReceipt) null));
        }

        void advanceAcceptanceGeneration() {
            if (acceptanceGeneration == Long.MAX_VALUE) {
                throw new IllegalStateException("acceptance generation exhausted");
            }
            acceptanceGeneration++;
        }

        StageAssignmentSnapshot snapshot() {
            return new StageAssignmentSnapshot(
                    manifest, acceptanceGeneration, acceptances, terminals,
                    preparedSinks);
        }

        ManifestCommitJournalSnapshot journalSnapshot() {
            return new ManifestCommitJournalSnapshot(
                    snapshot(), manifestMembers, qualityDecision, manifestIntent);
        }

        List<ManifestMemberReceipt> qualityMembers() {
            QualityGateAssignmentBinding binding = manifest.qualityGateBinding();
            if (binding == null) return Collections.emptyList();
            List<ManifestMemberReceipt> selected = new ArrayList<>(
                    QualityGateAssignmentBinding.MEMBER_COUNT);
            for (QualityGateAssignmentBinding.MemberBinding member
                    : binding.members()) {
                selected.add(manifestMembers.get(member.physicalOrdinal()));
            }
            return selected;
        }

        QualityGateCommitJournalSnapshot qualityJournalSnapshot() {
            QualityGateAssignmentBinding binding = manifest.qualityGateBinding();
            if (binding == null) return null;
            List<StageAssignmentAcceptance> selectedAcceptances =
                    new ArrayList<>(QualityGateAssignmentBinding.MEMBER_COUNT);
            List<StageTerminalReceipt> selectedTerminals =
                    new ArrayList<>(QualityGateAssignmentBinding.MEMBER_COUNT);
            List<ManifestMemberReceipt> selectedMembers =
                    new ArrayList<>(QualityGateAssignmentBinding.MEMBER_COUNT);
            List<Integer> selectedShards = new ArrayList<>(
                    QualityGateAssignmentBinding.MEMBER_COUNT);
            for (QualityGateAssignmentBinding.MemberBinding member
                    : binding.members()) {
                int ordinal = member.physicalOrdinal();
                selectedAcceptances.add(acceptances.get(ordinal));
                selectedTerminals.add(terminals.get(ordinal));
                selectedMembers.add(manifestMembers.get(ordinal));
                int shard = ordinal / StageAssignmentManifest.MAX_RECORDS_PER_SHARD;
                if (!selectedShards.contains(shard)) selectedShards.add(shard);
            }
            Collections.sort(selectedShards);
            List<byte[]> shardDigests = new ArrayList<>(selectedShards.size());
            for (Integer shard : selectedShards) {
                int first = shard * StageAssignmentManifest.MAX_RECORDS_PER_SHARD;
                int count = Math.min(StageAssignmentManifest.MAX_RECORDS_PER_SHARD,
                        manifest.assignmentCount() - first);
                List<ManifestMemberReceipt> shardMembers =
                        new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    shardMembers.add(manifestMembers.get(first + i));
                }
                shardDigests.add(sha256(ManifestCommitJournalCodec.encodeMemberShard(
                        manifest, shard, shardMembers)));
            }
            return new QualityGateCommitJournalSnapshot(
                    manifest.fence(), manifest.artifactId(), manifest.generation(),
                    acceptanceGeneration, binding, selectedAcceptances,
                    selectedTerminals, selectedMembers, shardDigests,
                    qualityDecision, manifestIntent);
        }

        private static byte[] sha256(byte[] bytes) {
            try {
                return MessageDigest.getInstance("SHA-256").digest(bytes);
            } catch (NoSuchAlgorithmException impossible) {
                throw new IllegalStateException("SHA-256 unavailable", impossible);
            }
        }
    }

    private static final class CommandRecord {
        final byte[] digest;
        final ExecutionEvent event;

        CommandRecord(byte[] digest, ExecutionEvent event) {
            this.digest = digest.clone();
            this.event = event;
        }
    }
}
