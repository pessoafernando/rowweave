package io.rowweave.incremental;

/** Minimal authority surface used before an interval is dispatched. */
interface IncrementalPreparationAuthority {
    IncrementalAuthorityRecord record(String feedId);

    IncrementalPendingRunDecision publishPending(
            FrontierRecoveryToken token, IncrementalClaimRequest request);

    IncrementalAuthorityDecision apply(IncrementalAuthorityMutation mutation);
}
