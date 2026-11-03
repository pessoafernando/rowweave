package io.rowweave.incremental;

/** Internal single mutation surface for joint Run/frontier/Sink authority. */
interface IncrementalRunCommitAuthority {
    IncrementalAuthorityDecision apply(IncrementalAuthorityMutation mutation);
}
