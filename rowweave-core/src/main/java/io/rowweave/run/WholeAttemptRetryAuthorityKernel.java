package io.rowweave.run;

import io.rowweave.BatchSinkDescriptor;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Backend-neutral closure, identity and retry-wait rules. */
public final class WholeAttemptRetryAuthorityKernel {
    private WholeAttemptRetryAuthorityKernel() {
    }

    public static String validateRunId(String runId) {
        return RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
    }

    public static Closure classify(
            RunSnapshot current, StageAssignmentSnapshot assignments,
            ManifestCommitJournalSnapshot journal,
            BatchSinkAbortRecord abort) {
        if (current == null || current.currentAttempt() == null) {
            throw new IllegalArgumentException("current attempt is required");
        }
        AttemptStatus status = current.currentAttempt().status();
        if (assignments == null) {
            return status == AttemptStatus.CREATED
                    || status == AttemptStatus.DISPATCHING
                    ? Closure.retryable(WholeAttemptRetryClass.PRE_DISPATCH_ABSENT)
                    : Closure.rejected(
                    WholeAttemptRetryDecision.Code.NOT_RETRYABLE);
        }
        if (journal == null
                || !sameAssignmentRoot(
                journal.assignments(), assignments)) {
            return Closure.rejected(
                    WholeAttemptRetryDecision.Code.NOT_RETRYABLE);
        }
        if (status != AttemptStatus.DISPATCHING
                && status != AttemptStatus.RUNNING) {
            return Closure.rejected(WholeAttemptRetryDecision.Code.TOO_LATE);
        }
        boolean lost = false;
        boolean absent = false;
        boolean canceled = false;
        for (int i = 0; i < assignments.manifest().assignmentCount(); i++) {
            StageAssignmentAcceptance acceptance = assignments.acceptance(i);
            StageTerminalReceipt terminal = assignments.terminal(i);
            if (acceptance == null) {
                if (terminal != null) {
                    return Closure.rejected(
                            WholeAttemptRetryDecision.Code.NOT_RETRYABLE);
                }
                absent = true;
                continue;
            }
            if (terminal == null) {
                return Closure.rejected(
                        WholeAttemptRetryDecision.Code.CLOSURE_PENDING);
            }
            if (terminal.outcome() == StageTerminalOutcome.FAILED) {
                return Closure.rejected(
                        WholeAttemptRetryDecision.Code.NOT_RETRYABLE);
            }
            if (terminal.outcome() == StageTerminalOutcome.LOST) lost = true;
            if (terminal.outcome() == StageTerminalOutcome.CANCELED) {
                canceled = true;
            }
        }
        if (!lost && !absent && !canceled) {
            return Closure.rejected(
                    WholeAttemptRetryDecision.Code.NOT_RETRYABLE);
        }
        AttemptOutputProfile profile = assignments.manifest().outputProfile();
        if (profile == AttemptOutputProfile.UNKNOWN_LEGACY) {
            return Closure.rejected(
                    WholeAttemptRetryDecision.Code.NOT_RETRYABLE);
        }
        if (profile == AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1) {
            if (journal.intent() != null) {
                return Closure.rejected(WholeAttemptRetryDecision.Code.TOO_LATE);
            }
            if (abort == null
                    || abort.state() == BatchSinkAbortRecord.State.ABORT_DECIDED) {
                return Closure.rejected(
                        WholeAttemptRetryDecision.Code.CLOSURE_PENDING);
            }
            StageAssignmentManifest manifest = assignments.manifest();
            if (!abort.fence().equals(manifest.fence())
                    || abort.assignmentGeneration() != manifest.generation()
                    || abort.capabilityProfile()
                    != BatchSinkDescriptor.CapabilityProfile
                    .MANIFEST_ATOMIC_GROUP_V1
                    || !abort.publicationId().equals(
                    current.currentAttempt().publicationId())) {
                return Closure.rejected(
                        WholeAttemptRetryDecision.Code.NOT_RETRYABLE);
            }
        }
        // A RUNNING attempt whose accepted commands are all coordinator-canceled
        // has the same replay-safe closure as worker loss. User/deadline cancellation
        // cannot enter here because those attempts are already CANCELING.
        return Closure.retryable(lost || (!absent && canceled)
                ? WholeAttemptRetryClass.WORKER_LOST
                : WholeAttemptRetryClass.PRE_DISPATCH_ABSENT);
    }

    private static boolean sameAssignmentRoot(
            StageAssignmentSnapshot left, StageAssignmentSnapshot right) {
        return MessageDigest.isEqual(
                StageAssignmentManifestCodec.encodeRoot(
                        left.manifest(), left.acceptanceGeneration()),
                StageAssignmentManifestCodec.encodeRoot(
                        right.manifest(), right.acceptanceGeneration()));
    }

    public static boolean scheduleStillCurrent(
            WholeAttemptRetryDecision.Code code,
            WholeAttemptRetryClass retryClass, RunSnapshot snapshot) {
        if (code == WholeAttemptRetryDecision.Code.SCHEDULED) {
            return snapshot.status() == RunStatus.READY
                    && snapshot.currentAttempt() != null
                    && snapshot.currentAttempt().status() == AttemptStatus.FAILED
                    && retryClass.reasonCode().equals(
                    snapshot.currentAttempt().reasonCode());
        }
        return code == WholeAttemptRetryDecision.Code.EXHAUSTED
                && snapshot.status() == RunStatus.FAILED
                && snapshot.currentAttempt() != null
                && "RETRY_ATTEMPTS_EXHAUSTED".equals(
                snapshot.currentAttempt().reasonCode());
    }

    public static WholeAttemptRetryDecision staleRecoverySchedule(
            RunSnapshot snapshot) {
        return new WholeAttemptRetryDecision(
                WholeAttemptRetryDecision.Code.STALE_RECOVERY_OWNER,
                snapshot, null, 0L);
    }

    public static WholeAttemptRetryDecision tooLateSchedule(
            RunSnapshot snapshot) {
        return new WholeAttemptRetryDecision(
                WholeAttemptRetryDecision.Code.TOO_LATE,
                snapshot, null, 0L);
    }

    public static WholeAttemptRetryDecision unavailableSchedule(
            RunSnapshot snapshot) {
        return new WholeAttemptRetryDecision(
                WholeAttemptRetryDecision.Code.UNAVAILABLE,
                snapshot, null, 0L);
    }

    public static RetryReadyEvaluation staleRecoveryReady(
            RunSnapshot snapshot) {
        return new RetryReadyEvaluation(
                RetryReadyEvaluation.Code.STALE_RECOVERY_OWNER,
                snapshot, null, 0L, 0L, 0);
    }

    public static RetryAttemptClaim staleRecoveryClaim(
            RunSnapshot snapshot) {
        return new RetryAttemptClaim(
                RetryAttemptClaim.Code.STALE_RECOVERY_OWNER,
                snapshot, null, 0L, 0L);
    }

    public static RetryAttemptClaim unavailableClaim(
            RunSnapshot snapshot) {
        return new RetryAttemptClaim(
                RetryAttemptClaim.Code.UNAVAILABLE,
                snapshot, null, 0L, 0L);
    }

    public static RetryAttemptClaim tooLateClaim(
            RunSnapshot snapshot) {
        return new RetryAttemptClaim(
                RetryAttemptClaim.Code.TOO_LATE,
                snapshot, null, 0L, 0L);
    }

    public static String scheduleCommandId(
            RunAttemptFence fence, WholeAttemptRetryClass retryClass,
            StageAssignmentSnapshot assignments,
            BatchSinkAbortRecord abort) {
        StringBuilder evidence = new StringBuilder(256);
        evidence.append("retry-schedule-v1\0")
                .append(fence.runId()).append('\0')
                .append(fence.attemptId()).append('\0')
                .append(fence.fenceToken()).append('\0')
                .append(retryClass.name());
        if (assignments != null) {
            StageAssignmentManifest manifest = assignments.manifest();
            evidence.append('\0').append(manifest.generation())
                    .append('\0').append(manifest.outputProfile().name())
                    .append('\0').append(sha256Hex(
                    StageAssignmentManifestCodec.encodeRoot(
                            manifest, assignments.acceptanceGeneration())));
            for (int i = 0; i < manifest.assignmentCount(); i++) {
                StageAssignmentAcceptance acceptance = assignments.acceptance(i);
                StageTerminalReceipt terminal = assignments.terminal(i);
                evidence.append('\0').append(i).append(':')
                        .append(acceptance == null ? 'N' : 'A').append(':')
                        .append(terminal == null
                                ? "NONE" : terminal.outcome().name());
                if (acceptance != null) {
                    evidence.append(':').append(
                            RunCommandCanonical.hex(acceptance.commandDigest()));
                }
                if (terminal != null) {
                    evidence.append(':').append(sha256Hex(
                            StageAssignmentManifestCodec.encodeTerminalReceipt(
                                    terminal)));
                }
            }
            evidence.append("\0abort=").append(abort == null
                    ? "NONE" : sha256Hex(BatchSinkAbortRecordCodec.encode(abort)));
        } else {
            evidence.append("\0ABSENT");
        }
        return "retry-schedule-v1-" + sha256Hex(evidence.toString());
    }

    public static Wait retryWait(RunSnapshot snapshot) {
        if (snapshot.status() != RunStatus.READY
                || snapshot.currentAttempt() == null
                || snapshot.currentAttempt().status() != AttemptStatus.FAILED
                || WholeAttemptRetryClass.fromReasonCode(
                snapshot.currentAttempt().reasonCode()) == null
                || snapshot.attemptCount() >= snapshot.maxAttempts()) {
            return null;
        }
        final long notBefore;
        try {
            notBefore = WholeAttemptRetryPolicy.notBefore(
                    snapshot.currentAttempt().attemptNumber(),
                    snapshot.currentAttempt().updatedAtMillis());
        } catch (IllegalArgumentException invalid) {
            return null;
        }
        RetryAttemptIdentity identity = RetryAttemptIdentity.derive(
                snapshot.runId(), snapshot.artifactId(),
                snapshot.attemptCount() + 1);
        return new Wait(identity, notBefore,
                snapshot.maxAttempts() - snapshot.attemptCount());
    }

    public static RetryAttemptIdentity claimedRetryIdentity(
            RunSnapshot snapshot, boolean exactClaimReceiptExists) {
        if (!exactClaimReceiptExists
                || snapshot.status() != RunStatus.RUNNING
                || snapshot.currentAttempt() == null
                || snapshot.attemptCount() < 2
                || snapshot.attemptCount() > WholeAttemptRetryPolicy.MAX_ATTEMPTS) {
            return null;
        }
        RetryAttemptIdentity identity = RetryAttemptIdentity.derive(
                snapshot.runId(), snapshot.artifactId(), snapshot.attemptCount());
        return identity.attemptId().equals(snapshot.currentAttempt().attemptId())
                && identity.publicationId().equals(
                snapshot.currentAttempt().publicationId())
                && snapshot.currentAttempt().fenceToken() == identity.fenceToken()
                ? identity : null;
    }

    private static String sha256Hex(String value) {
        return sha256Hex(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String sha256Hex(byte[] value) {
        try {
            return RunCommandCanonical.hex(MessageDigest.getInstance("SHA-256")
                    .digest(value));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    public static RunDecision evaluateSchedule(
            String commandId, RunSnapshot current, RunAttemptFence fence,
            long observedAtMillis, WholeAttemptRetryClass retryClass,
            boolean exhausted) {
        if (current == null || fence == null) {
            throw new IllegalArgumentException("retry snapshot and fence are required");
        }
        RunCommand command = RunCommand.retryAuthorityAdvance(
                commandId, current.runId(), current.revision(),
                fence.attemptId(), fence.fenceToken(),
                Math.max(observedAtMillis, current.updatedAtMillis()),
                retryClass, exhausted);
        return RunStateStoreKernel.evaluate(current, command);
    }

    public static RunDecision evaluateClaim(
            RunSnapshot current, RetryAttemptIdentity identity,
            long observedAtMillis) {
        if (current == null || identity == null) {
            throw new IllegalArgumentException(
                    "retry snapshot and identity are required");
        }
        RunCommand command = RunCommand.retryAuthorityCreateAttempt(
                identity.commandId("claim"), current.runId(),
                current.revision(), identity, observedAtMillis);
        return RunStateStoreKernel.evaluate(current, command);
    }

    public static WholeAttemptRetryDecision decision(
            WholeAttemptRetryDecision.Code code, RunSnapshot snapshot,
            WholeAttemptRetryClass retryClass, long observedAtMillis) {
        return new WholeAttemptRetryDecision(
                code, snapshot, retryClass, observedAtMillis);
    }

    public static RetryReadyEvaluation ready(
            RetryReadyEvaluation.Code code, RunSnapshot snapshot,
            RetryAttemptIdentity identity, long observedAtMillis,
            long notBeforeMillis, int remainingAttempts) {
        return new RetryReadyEvaluation(code, snapshot, identity,
                observedAtMillis, notBeforeMillis, remainingAttempts);
    }

    public static RetryAttemptClaim claim(
            RetryAttemptClaim.Code code, RunSnapshot snapshot,
            RetryAttemptIdentity identity, long observedAtMillis,
            long notBeforeMillis) {
        return new RetryAttemptClaim(code, snapshot, identity,
                observedAtMillis, notBeforeMillis);
    }

    public static final class Closure {
        private final WholeAttemptRetryDecision.Code code;
        private final WholeAttemptRetryClass retryClass;

        private Closure(WholeAttemptRetryDecision.Code code,
                        WholeAttemptRetryClass retryClass) {
            this.code = code;
            this.retryClass = retryClass;
        }

        static Closure retryable(WholeAttemptRetryClass retryClass) {
            return new Closure(null, retryClass);
        }

        static Closure rejected(WholeAttemptRetryDecision.Code code) {
            return new Closure(code, null);
        }

        public WholeAttemptRetryDecision.Code code() { return code; }
        public WholeAttemptRetryClass retryClass() { return retryClass; }
    }

    public static final class Wait {
        private final RetryAttemptIdentity identity;
        private final long notBeforeMillis;
        private final int remainingAttempts;

        private Wait(RetryAttemptIdentity identity, long notBeforeMillis,
                     int remainingAttempts) {
            this.identity = identity;
            this.notBeforeMillis = notBeforeMillis;
            this.remainingAttempts = remainingAttempts;
        }

        public RetryAttemptIdentity identity() { return identity; }
        public long notBeforeMillis() { return notBeforeMillis; }
        public int remainingAttempts() { return remainingAttempts; }
    }
}
