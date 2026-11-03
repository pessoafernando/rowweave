package io.rowweave.incremental;

import io.rowweave.run.AttemptStatus;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RetryAttemptClaim;
import io.rowweave.run.RetryReadyEvaluation;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunDeadlineResult;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunRecoveryOwnership;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.WholeAttemptRetryDecision;

/** Backend-neutral authority surface required by the local interval runtime. */
interface IncrementalLocalAuthority
        extends IncrementalPreparationAuthority,
        IncrementalRunCommitAuthority,
        RecoveryFencedIncrementalSinkAuthority,
        RunRecoveryOwnership {
    RunSnapshot runSnapshot(String runId);

    RunDecision advanceProgress(
            FrontierRecoveryToken frontierToken,
            IncrementalAttemptProgressCommand command);

    RunDecision advanceProgress(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            IncrementalAttemptProgressCommand command);

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
}
