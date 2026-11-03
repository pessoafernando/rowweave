package io.rowweave.run;

/** Durable recovery ownership validated against the Run authority and catalog atomically. */
public interface RunRecoveryOwnership {
    RecoveryToken ownership(String runId);

    RecoveryClaim claim(String runId, long expectedRevision,
                        long expectedOwnershipEpoch, RecoveryOwner owner);

    boolean current(RecoveryToken token);
}
