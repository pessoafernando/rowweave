package io.rowweave.run;

/** Joint Run/assignment/output authority for bounded whole-attempt retry. */
public interface WholeAttemptRetryAuthority {
    WholeAttemptRetryDecision scheduleRetry(RunAttemptFence fence);

    WholeAttemptRetryDecision scheduleRetryRecovered(
            RecoveryToken token, RunAttemptFence fence);

    RetryReadyEvaluation evaluateRetryReady(String runId);

    RetryAttemptClaim claimDueRetry(String runId);

    RetryAttemptClaim claimDueRetryRecovered(RecoveryToken token, String runId);
}
