package io.rowweave.run;

/** Only registries whose backend shares the Run linearization point expose this capability. */
public interface DurableStageAssignmentAuthorityProvider {
    DurableStageAssignmentAuthority durableStageAssignmentAuthority();
}
