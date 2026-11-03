package io.rowweave.run;

/** Registry capability for atomic durable execution-evidence queries. */
public interface DurableExecutionEvidenceAuthorityProvider {
    DurableExecutionEvidenceAuthority durableExecutionEvidenceAuthority();
}
