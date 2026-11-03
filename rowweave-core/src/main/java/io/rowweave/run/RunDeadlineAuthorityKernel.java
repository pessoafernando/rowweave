package io.rowweave.run;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Deadline state matrix, command identity and replay validation shared by the backends. */
public final class RunDeadlineAuthorityKernel {
    public static final String REASON_CODE = "RUN_DEADLINE";
    private static final String COMMAND_PREFIX = "run-deadline-v1-";

    private RunDeadlineAuthorityKernel() {
    }

    public static String validateRunId(String runId) {
        return RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
    }

    public static String commandId(RunSnapshot snapshot) {
        if (snapshot == null || snapshot.deadlineEpochMillis() <= 0L) {
            throw new IllegalArgumentException("deadline snapshot is required");
        }
        AttemptSnapshot attempt = snapshot.currentAttempt();
        String identity = snapshot.runId() + '\0' + snapshot.deadlineEpochMillis()
                + '\0' + (attempt == null ? "READY"
                : attempt.attemptId() + '\0' + attempt.fenceToken());
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(identity.getBytes(StandardCharsets.UTF_8));
            return COMMAND_PREFIX + RunCommandCanonical.hex(digest);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    public static Plan plan(String commandId, RunSnapshot snapshot, long observedTime) {
        if (snapshot == null) {
            throw new IllegalArgumentException("deadline snapshot is required");
        }
        long deadline = snapshot.deadlineEpochMillis();
        if (deadline == 0L) {
            return Plan.result(result(snapshot, RunDeadlineResult.Disposition.NO_DEADLINE, 0L));
        }
        if (observedTime <= 0L) {
            throw new IllegalStateException("authority clock must be > 0");
        }
        RunStatus status = snapshot.status();
        if (status.isTerminal()) {
            return Plan.result(result(snapshot,
                    RunDeadlineResult.Disposition.ALREADY_TERMINAL, observedTime));
        }
        if (observedTime < deadline) {
            return Plan.result(result(snapshot,
                    RunDeadlineResult.Disposition.NOT_DUE, observedTime));
        }
        long eventTime = Math.max(observedTime, snapshot.updatedAtMillis());
        if (status == RunStatus.READY) {
            return Plan.command(RunCommand.timeoutReadyRun(
                    commandId, snapshot.runId(), snapshot.revision(),
                    eventTime, REASON_CODE, null));
        }
        if (status == RunStatus.RECONCILING) {
            return Plan.result(result(snapshot,
                    RunDeadlineResult.Disposition.TOO_LATE, observedTime));
        }
        AttemptSnapshot attempt = snapshot.currentAttempt();
        if (attempt == null) {
            throw new IllegalStateException("non-READY Run is missing current attempt");
        }
        if (attempt.status() == AttemptStatus.CANCELING) {
            return Plan.result(result(snapshot,
                    attempt.cancellationCause() == CancellationCause.TIMEOUT
                            ? RunDeadlineResult.Disposition.ALREADY_REQUESTED
                            : RunDeadlineResult.Disposition.TOO_LATE,
                    observedTime));
        }
        if (attempt.status() == AttemptStatus.COMMITTING
                || attempt.status() == AttemptStatus.RECONCILING
                || attempt.status().isTerminal()) {
            return Plan.result(result(snapshot,
                    RunDeadlineResult.Disposition.TOO_LATE, observedTime));
        }
        if (attempt.status() != AttemptStatus.CREATED
                && attempt.status() != AttemptStatus.DISPATCHING
                && attempt.status() != AttemptStatus.RUNNING) {
            throw new IllegalStateException(
                    "unsupported deadline attempt state: " + attempt.status());
        }
        return Plan.command(RunCommand.requestCancellation(
                commandId, snapshot.runId(), snapshot.revision(),
                attempt.attemptId(), attempt.fenceToken(), CancellationCause.TIMEOUT,
                eventTime, REASON_CODE, null));
    }

    public static RunDeadlineResult accepted(RunSnapshot snapshot, long observedTime) {
        return result(snapshot, RunDeadlineResult.Disposition.ACCEPTED, observedTime);
    }

    public static RunDeadlineResult replayed(
            RunSnapshot snapshot, ExecutionEvent event,
            String commandId, long observedTime) {
        if (!isExactDeadline(event, commandId)) {
            throw new IllegalStateException(
                    "deadline command identity conflicts with an existing command");
        }
        return result(snapshot,
                RunDeadlineResult.Disposition.ALREADY_REQUESTED, observedTime);
    }

    public static RunDeadlineResult missing(String runId) {
        return new RunDeadlineResult(runId, RunDeadlineResult.Disposition.NOT_FOUND,
                null, 0L, 0L, 0L);
    }

    public static RunDeadlineResult retiring(String runId) {
        return new RunDeadlineResult(runId, RunDeadlineResult.Disposition.RETIRING,
                null, 0L, 0L, 0L);
    }

    private static boolean isExactDeadline(ExecutionEvent event, String commandId) {
        if (event == null || event.type() != ExecutionEvent.Type.TIMEOUT_REQUESTED
                || !commandId.equals(event.commandId())
                || !REASON_CODE.equals(event.reasonCode()) || event.message() != null) {
            return false;
        }
        RunCommand reconstructed = event.attemptId() == null
                || event.runFrom() == RunStatus.READY
                ? RunCommand.timeoutReadyRun(commandId, event.runId(),
                event.sequence() - 1L, event.eventTimeMillis(), REASON_CODE, null)
                : RunCommand.requestCancellation(commandId, event.runId(),
                event.sequence() - 1L, event.attemptId(), event.fenceToken(),
                CancellationCause.TIMEOUT, event.eventTimeMillis(), REASON_CODE, null);
        return MessageDigest.isEqual(
                reconstructed.commandDigestUnsafe(), event.commandDigest());
    }

    private static RunDeadlineResult result(
            RunSnapshot snapshot, RunDeadlineResult.Disposition disposition,
            long observedTime) {
        return new RunDeadlineResult(snapshot.runId(), disposition,
                snapshot.status(), snapshot.revision(),
                snapshot.deadlineEpochMillis(), observedTime);
    }

    public static final class Plan {
        private final RunCommand command;
        private final RunDeadlineResult result;

        private Plan(RunCommand command, RunDeadlineResult result) {
            this.command = command;
            this.result = result;
        }

        static Plan command(RunCommand command) { return new Plan(command, null); }
        static Plan result(RunDeadlineResult result) { return new Plan(null, result); }
        public RunCommand command() { return command; }
        public RunDeadlineResult result() { return result; }
    }
}
