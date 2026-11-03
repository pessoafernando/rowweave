package io.rowweave.run;

/**
 * The minimal controlled entry an independent durable backend needs to reuse the single state machine.
 * A backend may not construct the next snapshot or event itself.
 */
public final class RunStateStoreKernel {
    private RunStateStoreKernel() { }

    public static RunDecision evaluate(RunSnapshot current, RunCommand command) {
        if (command == null) throw new IllegalArgumentException("command is required");
        return RunStateMachine.apply(current, command);
    }

    public static RunDecision replayed(RunSnapshot current, ExecutionEvent originalEvent) {
        if (originalEvent == null) throw new IllegalArgumentException("originalEvent is required");
        return RunDecision.replayed(current, originalEvent);
    }

    public static RunDecision commandIdConflict(RunSnapshot current) {
        return RunDecision.rejected(RunDecision.Code.COMMAND_ID_CONFLICT, current);
    }

    public static RunDecision limitExceeded(RunSnapshot current) {
        return RunDecision.rejected(RunDecision.Code.LIMIT_EXCEEDED, current);
    }

    public static RunDecision retentionInProgress(RunSnapshot current) {
        return RunDecision.rejected(
                RunDecision.Code.RETENTION_IN_PROGRESS, current);
    }

    public static RunDecision recoveryOwned(RunSnapshot current) {
        return RunDecision.rejected(RunDecision.Code.RECOVERY_OWNED, current);
    }

    public static RunDecision staleRecoveryOwner(RunSnapshot current) {
        return RunDecision.rejected(RunDecision.Code.STALE_RECOVERY_OWNER, current);
    }

    public static RunDecision illegalTransition(RunSnapshot current) {
        return RunDecision.rejected(RunDecision.Code.ILLEGAL_TRANSITION, current);
    }
}
