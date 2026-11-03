package io.rowweave.run;

/** Bounded immutable mutation command; a caller cannot submit an arbitrary next snapshot. */
public final class RunCommand {
    enum Kind {
        CREATE_RUN,
        CREATE_ATTEMPT,
        ADVANCE_ATTEMPT,
        REQUEST_CANCELLATION,
        CANCEL_READY_RUN,
        TIMEOUT_READY_RUN
    }

    final Kind kind;
    final String runId;
    final String definitionId;
    final String artifactId;
    final int maxAttempts;
    final long deadlineEpochMillis;
    final long expectedRevision;
    final String attemptId;
    final String publicationId;
    final long fenceToken;
    final AttemptStatus targetAttemptStatus;
    final boolean retryable;
    final CancellationCause cancellationCause;
    final long eventTimeMillis;
    final String reasonCode;
    final String message;
    final RunResultSummary resultSummary;
    final int canonicalVersion;
    final boolean retryAuthorityCommand;
    final String commandId;
    private final byte[] commandDigest;

    private RunCommand(String explicitCommandId,
                       Kind kind, String runId, String definitionId, String artifactId,
                       int maxAttempts, long deadlineEpochMillis, long expectedRevision,
                       String attemptId, String publicationId, long fenceToken,
                       AttemptStatus targetAttemptStatus, boolean retryable,
                       CancellationCause cancellationCause, long eventTimeMillis,
                       String reasonCode, String message, RunResultSummary resultSummary,
                       int canonicalVersion) {
        this(explicitCommandId, kind, runId, definitionId, artifactId,
                maxAttempts, deadlineEpochMillis, expectedRevision, attemptId,
                publicationId, fenceToken, targetAttemptStatus, retryable,
                cancellationCause, eventTimeMillis, reasonCode, message,
                resultSummary, canonicalVersion, false);
    }

    private RunCommand(String explicitCommandId,
                       Kind kind, String runId, String definitionId, String artifactId,
                       int maxAttempts, long deadlineEpochMillis, long expectedRevision,
                       String attemptId, String publicationId, long fenceToken,
                       AttemptStatus targetAttemptStatus, boolean retryable,
                       CancellationCause cancellationCause, long eventTimeMillis,
                       String reasonCode, String message, RunResultSummary resultSummary,
                       int canonicalVersion, boolean retryAuthorityCommand) {
        this.kind = kind;
        this.runId = RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
        this.definitionId = definitionId;
        this.artifactId = artifactId;
        this.maxAttempts = maxAttempts;
        this.deadlineEpochMillis = deadlineEpochMillis;
        this.expectedRevision = expectedRevision;
        this.attemptId = attemptId;
        this.publicationId = publicationId;
        this.fenceToken = fenceToken;
        this.targetAttemptStatus = targetAttemptStatus;
        this.retryable = retryable;
        this.cancellationCause = cancellationCause;
        if (eventTimeMillis < 0L) {
            throw new IllegalArgumentException("eventTimeMillis must be >= 0");
        }
        this.eventTimeMillis = eventTimeMillis;
        this.reasonCode = RunText.optional(reasonCode, "reasonCode", RunText.MAX_REASON_CODE_BYTES);
        this.message = RunText.optional(message, "message", RunText.MAX_MESSAGE_BYTES);
        this.resultSummary = resultSummary;
        this.canonicalVersion = canonicalVersion;
        this.retryAuthorityCommand = retryAuthorityCommand;
        if (canonicalVersion < 1 || canonicalVersion > 3) {
            throw new IllegalArgumentException("unsupported canonical command version");
        }
        if ((targetAttemptStatus == AttemptStatus.SUCCEEDED) != (resultSummary != null)
                && !(canonicalVersion == 1
                && targetAttemptStatus == AttemptStatus.SUCCEEDED
                && resultSummary == null)) {
            throw new IllegalArgumentException(
                    "result summary is required exactly for SUCCEEDED mutation");
        }
        this.commandDigest = RunCommandCanonical.digest(this);
        this.commandId = explicitCommandId == null
                ? "legacy-command-" + RunCommandCanonical.hex(commandDigest)
                : RunText.required(explicitCommandId, "commandId", RunText.MAX_ID_BYTES);
    }

    public static RunCommand createRun(String runId, String definitionId, String artifactId,
                                       int maxAttempts, long deadlineEpochMillis, long eventTimeMillis) {
        return createRun(null, runId, definitionId, artifactId,
                maxAttempts, deadlineEpochMillis, eventTimeMillis);
    }

    public static RunCommand createRun(String commandId, String runId,
                                       String definitionId, String artifactId,
                                       int maxAttempts, long deadlineEpochMillis, long eventTimeMillis) {
        RunText.required(definitionId, "definitionId", RunText.MAX_ID_BYTES);
        RunText.required(artifactId, "artifactId", RunText.MAX_ID_BYTES);
        if (maxAttempts < 1 || maxAttempts > 1000) {
            throw new IllegalArgumentException("maxAttempts must be in [1,1000]");
        }
        if (deadlineEpochMillis < 0L
                || (deadlineEpochMillis != 0L && deadlineEpochMillis < eventTimeMillis)) {
            throw new IllegalArgumentException("deadline must be zero or >= event time");
        }
        return new RunCommand(commandId, Kind.CREATE_RUN, runId, definitionId, artifactId,
                maxAttempts, deadlineEpochMillis, 0L, null, null, 0L,
                null, false, null, eventTimeMillis, null, null, null, 2);
    }

    public static RunCommand createAttempt(String runId, long expectedRevision,
                                           String attemptId, String publicationId,
                                           long eventTimeMillis) {
        return createAttempt(null, runId, expectedRevision, attemptId, publicationId, eventTimeMillis);
    }

    public static RunCommand createAttempt(String commandId, String runId, long expectedRevision,
                                           String attemptId, String publicationId,
                                           long eventTimeMillis) {
        requireExpectedRevision(expectedRevision);
        RunText.required(attemptId, "attemptId", RunText.MAX_ID_BYTES);
        RunText.required(publicationId, "publicationId", RunText.MAX_ID_BYTES);
        return new RunCommand(commandId, Kind.CREATE_ATTEMPT, runId, null, null,
                0, 0L, expectedRevision, attemptId, publicationId, 0L,
                null, false, null, eventTimeMillis, null, null, null, 2);
    }

    public static RunCommand advanceAttempt(String runId, long expectedRevision,
                                            String attemptId, long fenceToken,
                                            AttemptStatus targetStatus, boolean retryable,
                                            long eventTimeMillis, String reasonCode, String message) {
        return advanceAttempt(null, runId, expectedRevision, attemptId, fenceToken,
                targetStatus, retryable, eventTimeMillis, reasonCode, message);
    }

    public static RunCommand advanceAttempt(String runId, long expectedRevision,
                                            String attemptId, long fenceToken,
                                            AttemptStatus targetStatus, boolean retryable,
                                            long eventTimeMillis, String reasonCode, String message,
                                            RunResultSummary resultSummary) {
        return advanceAttempt(null, runId, expectedRevision, attemptId, fenceToken,
                targetStatus, retryable, eventTimeMillis, reasonCode, message, resultSummary);
    }

    public static RunCommand advanceAttempt(String commandId, String runId, long expectedRevision,
                                            String attemptId, long fenceToken,
                                            AttemptStatus targetStatus, boolean retryable,
                                            long eventTimeMillis, String reasonCode, String message) {
        return advanceAttempt(commandId, runId, expectedRevision, attemptId, fenceToken,
                targetStatus, retryable, eventTimeMillis, reasonCode, message, null);
    }

    public static RunCommand advanceAttempt(String commandId, String runId, long expectedRevision,
                                            String attemptId, long fenceToken,
                                            AttemptStatus targetStatus, boolean retryable,
                                            long eventTimeMillis, String reasonCode, String message,
                                            RunResultSummary resultSummary) {
        requireAttempt(expectedRevision, attemptId, fenceToken);
        if (retryable) {
            throw new IllegalArgumentException(
                    "retryable attempt transition requires retry authority");
        }
        if (targetStatus == null) {
            throw new IllegalArgumentException("targetStatus is required");
        }
        return new RunCommand(commandId, Kind.ADVANCE_ATTEMPT, runId, null, null,
                0, 0L, expectedRevision, attemptId, null, fenceToken,
                targetStatus, retryable, null, eventTimeMillis, reasonCode, message,
                resultSummary, canonicalVersion(resultSummary));
    }

    static RunCommand retryAuthorityAdvance(
            String commandId, String runId, long expectedRevision,
            String attemptId, long fenceToken, long eventTimeMillis,
            WholeAttemptRetryClass retryClass, boolean exhausted) {
        requireAttempt(expectedRevision, attemptId, fenceToken);
        if (retryClass == null) {
            throw new IllegalArgumentException("retryClass is required");
        }
        return new RunCommand(commandId, Kind.ADVANCE_ATTEMPT, runId, null, null,
                0, 0L, expectedRevision, attemptId, null, fenceToken,
                AttemptStatus.FAILED, !exhausted, null, eventTimeMillis,
                exhausted ? "RETRY_ATTEMPTS_EXHAUSTED" : retryClass.reasonCode(),
                null, null, 2, true);
    }

    /** Authority-only terminal used after durable no-publication abort closure is proven. */
    public static RunCommand verifiedAbortClosureTerminal(
            String commandId, String runId, long expectedRevision,
            String attemptId, long fenceToken, AttemptStatus terminalStatus,
            long eventTimeMillis, String reasonCode) {
        requireAttempt(expectedRevision, attemptId, fenceToken);
        if (terminalStatus != AttemptStatus.FAILED
                && terminalStatus != AttemptStatus.CANCELED
                && terminalStatus != AttemptStatus.TIMED_OUT
                && terminalStatus != AttemptStatus.FENCED) {
            throw new IllegalArgumentException(
                    "abort closure requires a non-success terminal status");
        }
        return new RunCommand(commandId, Kind.ADVANCE_ATTEMPT, runId,
                null, null, 0, 0L, expectedRevision, attemptId, null,
                fenceToken, terminalStatus, false, null, eventTimeMillis,
                reasonCode, null, null, 2, true);
    }

    static RunCommand retryAuthorityCreateAttempt(
            String commandId, String runId, long expectedRevision,
            RetryAttemptIdentity identity, long eventTimeMillis) {
        if (identity == null || !runId.equals(identity.runId())) {
            throw new IllegalArgumentException("retry identity does not match runId");
        }
        requireExpectedRevision(expectedRevision);
        return new RunCommand(commandId, Kind.CREATE_ATTEMPT, runId, null, null,
                0, 0L, expectedRevision, identity.attemptId(),
                identity.publicationId(), 0L, null, false, null,
                eventTimeMillis, null, null, null, 2, true);
    }

    public static RunCommand requestCancellation(String runId, long expectedRevision,
                                                 String attemptId, long fenceToken,
                                                 CancellationCause cause, long eventTimeMillis,
                                                 String reasonCode, String message) {
        return requestCancellation(null, runId, expectedRevision, attemptId, fenceToken,
                cause, eventTimeMillis, reasonCode, message);
    }

    public static RunCommand requestCancellation(String commandId, String runId, long expectedRevision,
                                                 String attemptId, long fenceToken,
                                                 CancellationCause cause, long eventTimeMillis,
                                                 String reasonCode, String message) {
        requireAttempt(expectedRevision, attemptId, fenceToken);
        if (cause == null) {
            throw new IllegalArgumentException("cancellation cause is required");
        }
        return new RunCommand(commandId, Kind.REQUEST_CANCELLATION, runId, null, null,
                0, 0L, expectedRevision, attemptId, null, fenceToken,
                null, false, cause, eventTimeMillis, reasonCode, message, null, 2);
    }

    /** No active authority sits between the two attempts, so cancellation only fences later attempt creation. */
    public static RunCommand cancelReadyRun(String runId, long expectedRevision,
                                            long eventTimeMillis,
                                            String reasonCode, String message) {
        return cancelReadyRun(null, runId, expectedRevision, eventTimeMillis, reasonCode, message);
    }

    public static RunCommand cancelReadyRun(String commandId, String runId, long expectedRevision,
                                            long eventTimeMillis,
                                            String reasonCode, String message) {
        requireExpectedRevision(expectedRevision);
        return new RunCommand(commandId, Kind.CANCEL_READY_RUN, runId, null, null,
                0, 0L, expectedRevision, null, null, 0L,
                null, false, CancellationCause.USER, eventTimeMillis, reasonCode, message, null, 2);
    }

    static RunCommand timeoutReadyRun(String commandId, String runId, long expectedRevision,
                                      long eventTimeMillis,
                                      String reasonCode, String message) {
        requireExpectedRevision(expectedRevision);
        return new RunCommand(commandId, Kind.TIMEOUT_READY_RUN, runId, null, null,
                0, 0L, expectedRevision, null, null, 0L,
                null, false, CancellationCause.TIMEOUT, eventTimeMillis,
                reasonCode, message, null, 2);
    }

    static RunCommand decodedCanonical(
            int version, String commandId, Kind kind, String runId,
            String definitionId, String artifactId, int maxAttempts,
            long deadlineEpochMillis, long expectedRevision, String attemptId,
            String publicationId, long fenceToken, AttemptStatus targetAttemptStatus,
            boolean retryable, CancellationCause cancellationCause, long eventTimeMillis,
            String reasonCode, String message, RunResultSummary resultSummary) {
        switch (kind) {
            case CREATE_RUN:
                RunText.required(definitionId, "definitionId", RunText.MAX_ID_BYTES);
                RunText.required(artifactId, "artifactId", RunText.MAX_ID_BYTES);
                if (maxAttempts < 1 || maxAttempts > 1000
                        || deadlineEpochMillis < 0L
                        || (deadlineEpochMillis != 0L
                        && deadlineEpochMillis < eventTimeMillis)) {
                    throw new IllegalArgumentException("invalid create run command");
                }
                break;
            case CREATE_ATTEMPT:
                requireExpectedRevision(expectedRevision);
                RunText.required(attemptId, "attemptId", RunText.MAX_ID_BYTES);
                RunText.required(publicationId, "publicationId", RunText.MAX_ID_BYTES);
                break;
            case ADVANCE_ATTEMPT:
                requireAttempt(expectedRevision, attemptId, fenceToken);
                if (targetAttemptStatus == null) {
                    throw new IllegalArgumentException("targetStatus is required");
                }
                break;
            case REQUEST_CANCELLATION:
                requireAttempt(expectedRevision, attemptId, fenceToken);
                if (cancellationCause == null) {
                    throw new IllegalArgumentException("cancellation cause is required");
                }
                break;
            case CANCEL_READY_RUN:
                requireExpectedRevision(expectedRevision);
                break;
            case TIMEOUT_READY_RUN:
                requireExpectedRevision(expectedRevision);
                if (definitionId != null || artifactId != null || maxAttempts != 0
                        || deadlineEpochMillis != 0L || attemptId != null
                        || publicationId != null || fenceToken != 0L
                        || targetAttemptStatus != null || retryable
                        || cancellationCause != CancellationCause.TIMEOUT
                        || resultSummary != null) {
                    throw new IllegalArgumentException("timeout-ready cause mismatch");
                }
                break;
            default:
                throw new IllegalArgumentException("unknown command kind");
        }
        return new RunCommand(commandId, kind, runId, definitionId, artifactId,
                maxAttempts, deadlineEpochMillis, expectedRevision, attemptId,
                publicationId, fenceToken, targetAttemptStatus, retryable,
                cancellationCause, eventTimeMillis, reasonCode, message,
                resultSummary, version);
    }

    private static void requireAttempt(long expectedRevision, String attemptId, long fenceToken) {
        requireExpectedRevision(expectedRevision);
        RunText.required(attemptId, "attemptId", RunText.MAX_ID_BYTES);
        if (fenceToken <= 0L) {
            throw new IllegalArgumentException("fenceToken must be > 0");
        }
    }

    private static int canonicalVersion(RunResultSummary summary) {
        return summary != null && summary.sinkIdentityDigestUnsafe() != null ? 3 : 2;
    }

    private static void requireExpectedRevision(long expectedRevision) {
        if (expectedRevision <= 0L) {
            throw new IllegalArgumentException("expectedRevision must be > 0");
        }
    }

    public String commandId() { return commandId; }

    public String runId() { return runId; }

    public byte[] commandDigest() { return commandDigest.clone(); }

    byte[] commandDigestUnsafe() { return commandDigest; }
}
