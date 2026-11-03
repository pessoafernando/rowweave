package io.rowweave.run;

/** Implemented only by a backend that can fence a Run recovery takeover atomically. */
public interface RunRecoveryOwnershipProvider {
    RunRecoveryOwnership runRecoveryOwnership();
}
