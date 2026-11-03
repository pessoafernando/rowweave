package io.rowweave.incremental;

import io.rowweave.run.RecoveryOwner;

/** Durable backend boundary for feed frontier bootstrap, query and ownership. */
public interface IncrementalFrontierStore {
    IncrementalAuthorityDecision initialize(
            String commandId, IncrementalInitializeRequest request);

    IncrementalAuthorityRecord record(String feedId);

    /** Current durable owner, or {@code null} when the feed has no owner. */
    FrontierRecoveryToken frontierOwnership(String feedId);

    FrontierOwnershipClaim claimOwnership(
            String feedId, long expectedRevision, long expectedOwnershipEpoch,
            RecoveryOwner owner);
}
