package io.rowweave.run;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Published cancellation state matrix and canonical identity shared by the backends. */
public final class RunCancellationAuthorityKernel {
    public static final int MAX_REQUEST_ID_BYTES = 128;
    public static final String REASON_CODE = "USER_CANCEL";
    private static final String COMMAND_PREFIX = "run-cancel-v1-";

    private RunCancellationAuthorityKernel() {
    }

    public static String validateRunId(String runId) {
        return RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
    }

    public static String validateRequestId(String requestId) {
        return RunText.required(requestId, "requestId", MAX_REQUEST_ID_BYTES);
    }

    public static String commandId(String runId, String requestId) {
        validateRunId(runId);
        validateRequestId(requestId);
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(runId.getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
            byte[] bytes = digest.digest(requestId.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(bytes.length * 2);
            for (byte value : bytes) {
                hex.append(Character.forDigit((value >>> 4) & 0xf, 16));
                hex.append(Character.forDigit(value & 0xf, 16));
            }
            return COMMAND_PREFIX + hex;
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    public static Plan plan(String runId, String commandId, RunSnapshot snapshot) {
        if (snapshot == null) {
            return Plan.result(result(runId,
                    RunCancellationResult.Disposition.NOT_FOUND, null));
        }
        RunStatus status = snapshot.status();
        if (status == RunStatus.CANCELED) {
            return Plan.result(result(runId,
                    RunCancellationResult.Disposition.ALREADY_CANCELED, snapshot));
        }
        if (status == RunStatus.SUCCEEDED || status == RunStatus.FAILED) {
            return Plan.result(result(runId,
                    RunCancellationResult.Disposition.ALREADY_TERMINAL, snapshot));
        }
        if (status == RunStatus.RECONCILING) {
            return Plan.result(result(runId,
                    RunCancellationResult.Disposition.TOO_LATE, snapshot));
        }
        if (status == RunStatus.READY) {
            return Plan.command(RunCommand.cancelReadyRun(
                    commandId, runId, snapshot.revision(), eventTime(snapshot),
                    REASON_CODE, null));
        }
        AttemptSnapshot attempt = snapshot.currentAttempt();
        if (attempt == null) {
            throw new IllegalStateException("non-READY Run is missing current attempt");
        }
        if (attempt.status() == AttemptStatus.CANCELING) {
            return Plan.result(result(runId,
                    attempt.cancellationCause() == CancellationCause.USER
                            ? RunCancellationResult.Disposition.ALREADY_REQUESTED
                            : RunCancellationResult.Disposition.TOO_LATE,
                    snapshot));
        }
        if (attempt.status() == AttemptStatus.COMMITTING
                || attempt.status() == AttemptStatus.RECONCILING
                || attempt.status().isTerminal()) {
            return Plan.result(result(runId,
                    RunCancellationResult.Disposition.TOO_LATE, snapshot));
        }
        if (attempt.status() != AttemptStatus.CREATED
                && attempt.status() != AttemptStatus.DISPATCHING
                && attempt.status() != AttemptStatus.RUNNING) {
            throw new IllegalStateException(
                    "unsupported cancellable attempt state: " + attempt.status());
        }
        return Plan.command(RunCommand.requestCancellation(
                commandId, runId, snapshot.revision(), attempt.attemptId(),
                attempt.fenceToken(), CancellationCause.USER, eventTime(snapshot),
                REASON_CODE, null));
    }

    public static RunCancellationResult accepted(RunSnapshot snapshot) {
        return result(snapshot.runId(), RunCancellationResult.Disposition.ACCEPTED, snapshot);
    }

    public static RunCancellationResult replayed(String runId, RunSnapshot snapshot,
                                           ExecutionEvent event, String commandId) {
        if (!isExactPublicCancellation(event, commandId)) {
            throw new IllegalStateException(
                    "public cancellation command identity conflicts with an existing command");
        }
        return result(runId, RunCancellationResult.Disposition.ALREADY_REQUESTED, snapshot);
    }

    public static RunCancellationResult retiring(String runId) {
        return result(runId, RunCancellationResult.Disposition.RETIRING, null);
    }

    private static boolean isExactPublicCancellation(ExecutionEvent event, String commandId) {
        if (event == null || event.type() != ExecutionEvent.Type.CANCEL_REQUESTED
                || !commandId.equals(event.commandId())
                || !REASON_CODE.equals(event.reasonCode()) || event.message() != null) {
            return false;
        }
        RunCommand reconstructed = event.attemptId() == null
                ? RunCommand.cancelReadyRun(commandId, event.runId(), event.sequence() - 1L,
                event.eventTimeMillis(), REASON_CODE, null)
                : RunCommand.requestCancellation(commandId, event.runId(),
                event.sequence() - 1L, event.attemptId(), event.fenceToken(),
                CancellationCause.USER, event.eventTimeMillis(), REASON_CODE, null);
        return MessageDigest.isEqual(
                reconstructed.commandDigestUnsafe(), event.commandDigest());
    }

    private static long eventTime(RunSnapshot snapshot) {
        return Math.max(snapshot.updatedAtMillis(), System.currentTimeMillis());
    }

    private static RunCancellationResult result(
            String runId, RunCancellationResult.Disposition disposition,
            RunSnapshot snapshot) {
        return new RunCancellationResult(runId, disposition,
                snapshot == null ? null : snapshot.status(),
                snapshot == null ? 0L : snapshot.revision());
    }

    public static final class Plan {
        private final RunCommand command;
        private final RunCancellationResult result;

        private Plan(RunCommand command, RunCancellationResult result) {
            this.command = command;
            this.result = result;
        }

        static Plan command(RunCommand command) { return new Plan(command, null); }
        static Plan result(RunCancellationResult result) { return new Plan(null, result); }
        public RunCommand command() { return command; }
        public RunCancellationResult result() { return result; }
    }
}
