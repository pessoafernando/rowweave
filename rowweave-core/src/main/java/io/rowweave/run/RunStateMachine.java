package io.rowweave.run;

/** Single implementation of the Run lifecycle transitions; a backend only stores and loads atomically. */
final class RunStateMachine {
    private RunStateMachine() {
    }

    static RunDecision apply(RunSnapshot current, RunCommand command) {
        if (command.kind == RunCommand.Kind.CREATE_RUN) {
            return createRun(current, command);
        }
        if (current == null) {
            return RunDecision.rejected(RunDecision.Code.RUN_NOT_FOUND, null);
        }
        if (current.revision() != command.expectedRevision) {
            return RunDecision.rejected(RunDecision.Code.STALE_REVISION, current);
        }
        if (command.eventTimeMillis < current.updatedAtMillis()) {
            throw new IllegalArgumentException("eventTimeMillis must not move backwards");
        }
        switch (command.kind) {
            case CREATE_ATTEMPT:
                return createAttempt(current, command);
            case ADVANCE_ATTEMPT:
                return advanceAttempt(current, command);
            case REQUEST_CANCELLATION:
                return requestCancellation(current, command);
            case CANCEL_READY_RUN:
                return cancelReadyRun(current, command);
            case TIMEOUT_READY_RUN:
                return timeoutReadyRun(current, command);
            default:
                throw new IllegalArgumentException("unknown run command");
        }
    }

    private static RunDecision createRun(RunSnapshot current, RunCommand command) {
        if (current != null) {
            return RunDecision.rejected(RunDecision.Code.RUN_ALREADY_EXISTS, current);
        }
        RunSnapshot next = new RunSnapshot(command.runId, command.definitionId, command.artifactId,
                command.maxAttempts, 0, 1L, RunStatus.READY,
                command.deadlineEpochMillis, command.eventTimeMillis, command.eventTimeMillis, null);
        ExecutionEvent event = event(next, ExecutionEvent.Type.RUN_CREATED,
                null, 0L, null, RunStatus.READY, null, null, command);
        return RunDecision.accepted(next, event);
    }

    private static RunDecision createAttempt(RunSnapshot current, RunCommand command) {
        if (current.status() != RunStatus.READY) {
            return RunDecision.rejected(RunDecision.Code.ILLEGAL_TRANSITION, current);
        }
        if (current.attemptCount() > 0 && !command.retryAuthorityCommand) {
            return RunDecision.rejected(RunDecision.Code.ILLEGAL_TRANSITION, current);
        }
        if (current.attemptCount() >= current.maxAttempts()) {
            return RunDecision.rejected(RunDecision.Code.LIMIT_EXCEEDED, current);
        }
        int attemptNumber = current.attemptCount() + 1;
        long fenceToken = attemptNumber;
        AttemptSnapshot attempt = new AttemptSnapshot(attemptNumber, command.attemptId, fenceToken,
                command.publicationId, AttemptStatus.CREATED, null,
                command.eventTimeMillis, command.eventTimeMillis, null, null);
        RunSnapshot next = current.next(RunStatus.RUNNING, attemptNumber, attempt, command.eventTimeMillis);
        ExecutionEvent event = event(next, ExecutionEvent.Type.ATTEMPT_CREATED,
                command.attemptId, fenceToken, current.status(), next.status(),
                null, AttemptStatus.CREATED, command);
        return RunDecision.accepted(next, event);
    }

    private static RunDecision advanceAttempt(RunSnapshot current, RunCommand command) {
        if (command.retryable && !command.retryAuthorityCommand) {
            return RunDecision.rejected(RunDecision.Code.ILLEGAL_TRANSITION, current);
        }
        RunDecision identityFailure = validateAttemptIdentity(current, command);
        if (identityFailure != null) {
            return identityFailure;
        }
        AttemptSnapshot attempt = current.currentAttempt();
        if (attempt.status() == command.targetAttemptStatus) {
            return RunDecision.rejected(RunDecision.Code.ALREADY_APPLIED, current);
        }
        if (!isLegalAttemptTransition(
                attempt.status(), command.targetAttemptStatus,
                command.retryAuthorityCommand)) {
            return RunDecision.rejected(RunDecision.Code.ILLEGAL_TRANSITION, current);
        }
        RunStatus nextRunStatus = runStatusAfter(current, attempt, command);
        if (nextRunStatus == null) {
            return RunDecision.rejected(RunDecision.Code.ILLEGAL_TRANSITION, current);
        }
        AttemptSnapshot nextAttempt = attempt.transition(command.targetAttemptStatus,
                attempt.cancellationCause(), command.eventTimeMillis,
                command.reasonCode, command.message, command.resultSummary);
        RunSnapshot next = current.next(nextRunStatus, current.attemptCount(),
                nextAttempt, command.eventTimeMillis);
        ExecutionEvent event = event(next, ExecutionEvent.Type.ATTEMPT_ADVANCED,
                attempt.attemptId(), attempt.fenceToken(), current.status(), next.status(),
                attempt.status(), nextAttempt.status(), command);
        return RunDecision.accepted(next, event);
    }

    private static RunDecision requestCancellation(RunSnapshot current, RunCommand command) {
        RunDecision identityFailure = validateAttemptIdentity(current, command);
        if (identityFailure != null) {
            return identityFailure;
        }
        if (current.status().isTerminal()) {
            return RunDecision.rejected(RunDecision.Code.ILLEGAL_TRANSITION, current);
        }
        AttemptSnapshot attempt = current.currentAttempt();
        if ((attempt.status() == AttemptStatus.CANCELING
                || attempt.status() == AttemptStatus.RECONCILING)
                && attempt.cancellationCause() == command.cancellationCause) {
            return RunDecision.rejected(RunDecision.Code.ALREADY_APPLIED, current);
        }
        AttemptStatus nextAttemptStatus;
        RunStatus nextRunStatus;
        if (attempt.status() == AttemptStatus.COMMITTING
                || attempt.status() == AttemptStatus.RECONCILING) {
            nextAttemptStatus = AttemptStatus.RECONCILING;
            nextRunStatus = RunStatus.RECONCILING;
        } else if (!attempt.status().isTerminal()) {
            nextAttemptStatus = AttemptStatus.CANCELING;
            nextRunStatus = RunStatus.CANCELING;
        } else {
            return RunDecision.rejected(RunDecision.Code.ILLEGAL_TRANSITION, current);
        }
        AttemptSnapshot nextAttempt = attempt.transition(nextAttemptStatus,
                command.cancellationCause, command.eventTimeMillis,
                command.reasonCode, command.message);
        RunSnapshot next = current.next(nextRunStatus, current.attemptCount(),
                nextAttempt, command.eventTimeMillis);
        ExecutionEvent.Type type = command.cancellationCause == CancellationCause.USER
                ? ExecutionEvent.Type.CANCEL_REQUESTED
                : command.cancellationCause == CancellationCause.TIMEOUT
                ? ExecutionEvent.Type.TIMEOUT_REQUESTED : ExecutionEvent.Type.FENCE_REQUESTED;
        ExecutionEvent event = event(next, type, attempt.attemptId(), attempt.fenceToken(),
                current.status(), next.status(), attempt.status(), nextAttempt.status(), command);
        return RunDecision.accepted(next, event);
    }

    private static RunDecision cancelReadyRun(RunSnapshot current, RunCommand command) {
        if (current.status() == RunStatus.CANCELED) {
            return RunDecision.rejected(RunDecision.Code.ALREADY_APPLIED, current);
        }
        if (current.status() != RunStatus.READY) {
            return RunDecision.rejected(RunDecision.Code.ILLEGAL_TRANSITION, current);
        }
        RunSnapshot next = current.next(RunStatus.CANCELED, current.attemptCount(),
                current.currentAttempt(), command.eventTimeMillis);
        AttemptSnapshot latest = current.currentAttempt();
        ExecutionEvent event = event(next, ExecutionEvent.Type.CANCEL_REQUESTED,
                latest == null ? null : latest.attemptId(),
                latest == null ? 0L : latest.fenceToken(),
                current.status(), next.status(),
                latest == null ? null : latest.status(),
                latest == null ? null : latest.status(), command);
        return RunDecision.accepted(next, event);
    }

    private static RunDecision timeoutReadyRun(RunSnapshot current, RunCommand command) {
        if (current.status() == RunStatus.FAILED) {
            return RunDecision.rejected(RunDecision.Code.ALREADY_APPLIED, current);
        }
        AttemptSnapshot latest = current.currentAttempt();
        if (current.status() != RunStatus.READY
                || (latest != null && !latest.status().isTerminal())
                || command.cancellationCause != CancellationCause.TIMEOUT) {
            return RunDecision.rejected(RunDecision.Code.ILLEGAL_TRANSITION, current);
        }
        RunSnapshot next = current.next(RunStatus.FAILED, current.attemptCount(),
                latest, command.eventTimeMillis);
        ExecutionEvent event = event(next, ExecutionEvent.Type.TIMEOUT_REQUESTED,
                latest == null ? null : latest.attemptId(),
                latest == null ? 0L : latest.fenceToken(),
                current.status(), next.status(),
                latest == null ? null : latest.status(),
                latest == null ? null : latest.status(), command);
        return RunDecision.accepted(next, event);
    }

    private static RunDecision validateAttemptIdentity(RunSnapshot current, RunCommand command) {
        AttemptSnapshot attempt = current.currentAttempt();
        if (attempt == null || !attempt.attemptId().equals(command.attemptId)
                || attempt.fenceToken() != command.fenceToken) {
            return RunDecision.rejected(RunDecision.Code.STALE_FENCE, current);
        }
        return null;
    }

    private static boolean isLegalAttemptTransition(
            AttemptStatus from, AttemptStatus to,
            boolean authorityVerified) {
        switch (from) {
            case CREATED:
                return to == AttemptStatus.DISPATCHING || to == AttemptStatus.FAILED;
            case DISPATCHING:
                return to == AttemptStatus.RUNNING || to == AttemptStatus.FAILED;
            case RUNNING:
                return to == AttemptStatus.COMMITTING || to == AttemptStatus.FAILED;
            case COMMITTING:
                return to == AttemptStatus.SUCCEEDED
                        || to == AttemptStatus.RECONCILING
                        || authorityVerified && to == AttemptStatus.FAILED;
            case RECONCILING:
                return to == AttemptStatus.SUCCEEDED || to == AttemptStatus.FAILED;
            case CANCELING:
                return to == AttemptStatus.CANCELED || to == AttemptStatus.TIMED_OUT
                        || to == AttemptStatus.FENCED || to == AttemptStatus.RECONCILING;
            default:
                return false;
        }
    }

    private static RunStatus runStatusAfter(RunSnapshot current, AttemptSnapshot attempt,
                                            RunCommand command) {
        AttemptStatus target = command.targetAttemptStatus;
        if (target == AttemptStatus.SUCCEEDED) {
            return RunStatus.SUCCEEDED;
        }
        if (target == AttemptStatus.RECONCILING) {
            return RunStatus.RECONCILING;
        }
        if (target == AttemptStatus.DISPATCHING || target == AttemptStatus.RUNNING
                || target == AttemptStatus.COMMITTING) {
            return current.status() == RunStatus.RUNNING ? RunStatus.RUNNING : null;
        }
        if (target == AttemptStatus.FAILED) {
            return retryOrFailed(current, command.retryable);
        }
        if (attempt.status() == AttemptStatus.CANCELING) {
            CancellationCause cause = attempt.cancellationCause();
            if (target == AttemptStatus.CANCELED && cause == CancellationCause.USER) {
                return RunStatus.CANCELED;
            }
            if (target == AttemptStatus.TIMED_OUT && cause == CancellationCause.TIMEOUT) {
                return retryOrFailed(current, command.retryable);
            }
            if (target == AttemptStatus.FENCED && cause == CancellationCause.FENCE) {
                return retryOrFailed(current, command.retryable);
            }
        }
        return null;
    }

    private static RunStatus retryOrFailed(RunSnapshot current, boolean retryable) {
        return retryable && current.attemptCount() < current.maxAttempts()
                ? RunStatus.READY : RunStatus.FAILED;
    }

    private static ExecutionEvent event(RunSnapshot next, ExecutionEvent.Type type,
                                        String attemptId, long fenceToken,
                                        RunStatus runFrom, RunStatus runTo,
                                        AttemptStatus attemptFrom, AttemptStatus attemptTo,
                                        RunCommand command) {
        return new ExecutionEvent(next.revision(), type, next.runId(), attemptId, fenceToken,
                runFrom, runTo, attemptFrom, attemptTo, command.eventTimeMillis,
                command.reasonCode, command.message, command.commandId, command.commandDigestUnsafe());
    }
}
