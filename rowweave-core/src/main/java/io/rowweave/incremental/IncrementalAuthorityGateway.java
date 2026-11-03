package io.rowweave.incremental;

import io.rowweave.run.AttemptStatus;
import io.rowweave.run.BatchSinkAbortDecision;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.ManifestCommitIntent;
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
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunDeadlineResult;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.StageAcceptanceDecision;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageManifestPublication;
import io.rowweave.run.StageTerminalDecision;
import io.rowweave.run.StageTerminalReceipt;
import io.rowweave.run.WholeAttemptRetryDecision;

/**
 * Deployment gateway whose mutations preserve the joint frontier, Run and Sink
 * authority linearization point. Cluster control code must not reconstruct this
 * surface from independent backend providers.
 */
public interface IncrementalAuthorityGateway extends IncrementalFrontierStore {
    IncrementalPendingRunDecision publishPending(
            FrontierRecoveryToken token, IncrementalClaimRequest request);

    IncrementalAuthorityDecision claimInterval(
            String commandId, FrontierRecoveryToken token,
            IncrementalClaimRequest request);

    RecoveryClaim claimRunOwnership(
            FrontierRecoveryToken frontierToken, String runId,
            long expectedRunRevision, long expectedRunOwnershipEpoch,
            RecoveryOwner owner);

    RecoveryToken runOwnership(String runId);

    RunSnapshot runSnapshot(String runId);

    /** Bounded lookup of the sole ACTIVE frontier record bound to {@code runId}. */
    IncrementalAuthorityRecord activeRecordForRun(String runId);

    RunDecision advanceProgress(
            FrontierRecoveryToken frontierToken,
            IncrementalAttemptProgressCommand command);

    RunDecision advanceProgress(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            IncrementalAttemptProgressCommand command);

    StageManifestPublication publishAssignments(
            FrontierRecoveryToken frontierToken,
            StageAssignmentManifest manifest);

    StageManifestPublication publishAssignments(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageAssignmentManifest manifest);

    StageAssignmentSnapshot assignmentSnapshot(RunAttemptFence fence);

    StageAcceptanceDecision acceptAssignment(
            FrontierRecoveryToken frontierToken,
            StageAssignmentAcceptance acceptance);

    StageAcceptanceDecision acceptAssignment(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageAssignmentAcceptance acceptance);

    StageTerminalDecision recordStageTerminal(
            FrontierRecoveryToken frontierToken, StageTerminalReceipt receipt);

    StageTerminalDecision recordStageTerminal(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageTerminalReceipt receipt);

    PreparedSinkDecision recordPreparedSink(
            FrontierRecoveryToken frontierToken, PreparedSinkReceipt receipt);

    PreparedSinkDecision recordPreparedSink(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            PreparedSinkReceipt receipt);

    ManifestMemberDecision recordManifestMember(
            FrontierRecoveryToken frontierToken, ManifestMemberReceipt receipt);

    ManifestMemberDecision recordManifestMember(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            ManifestMemberReceipt receipt);

    ManifestIntentDecision commitManifestIntent(
            FrontierRecoveryToken frontierToken, ManifestCommitIntent intent);

    ManifestIntentDecision commitManifestIntent(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            ManifestCommitIntent intent);

    BatchSinkAbortDecision recordBatchSinkAbort(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            BatchSinkAbortRecord record);

    ManifestCommitJournalSnapshot manifestCommitJournal(RunAttemptFence fence);

    BatchSinkAbortRecord batchSinkAbort(RunAttemptFence fence);

    WholeAttemptRetryDecision scheduleRetry(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            RunAttemptFence fence);

    RetryReadyEvaluation evaluateRetryReady(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            String runId);

    RetryAttemptClaim claimDueRetry(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            String runId);

    RunCancellationResult requestUserCancellation(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            String runId, String requestId);

    RunDeadlineResult evaluateDeadline(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            String runId);

    VerifiedIncrementalAbortClosureProof verifyAbortClosure(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            RunAttemptFence fence, AttemptStatus terminalStatus);

    IncrementalAuthorityDecision jointSuccess(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            IncrementalAuthorityCommand command,
            VerifiedIncrementalJointCommitProof proof);

    IncrementalAuthorityDecision markConflict(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            IncrementalAuthorityCommand command,
            VerifiedIncrementalConflictProof proof);

    IncrementalAuthorityDecision closeAborted(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            IncrementalAuthorityCommand command,
            VerifiedIncrementalAbortClosureProof proof);

    boolean currentlyOwns(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken);
}
