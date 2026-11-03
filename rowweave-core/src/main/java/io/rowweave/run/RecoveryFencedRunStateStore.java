package io.rowweave.run;

/** Run mutation capability that consumes the recovery token at the same backend linearization point. */
public interface RecoveryFencedRunStateStore {
    RunDecision applyRecovered(RecoveryToken token, RunCommand command);
}
