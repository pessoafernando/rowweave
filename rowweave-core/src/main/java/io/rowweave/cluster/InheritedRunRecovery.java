package io.rowweave.cluster;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.run.AttemptSnapshot;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStatus;
import io.rowweave.run.RunSubmissionPolicyValidator;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/** Models only an inherited run whose authority provably has not been dispatched yet. */
final class InheritedRunRecovery {
    private static final String COMMAND_NAMESPACE = "run-recovery-v1-";
    private static final int MAX_DETAIL_BYTES = 1024;

    enum Eligibility {
        EXECUTE_READY,
        EXECUTE_CREATED,
        RECONCILE_IN_FLIGHT,
        RECONCILE_CANCELLATION,
        PENDING
    }

    enum ResultCode {
        RESOLVED,
        PENDING,
        RETRY
    }

    interface Handler {
        Result recover(Candidate candidate);
    }

    private InheritedRunRecovery() {
    }

    static Eligibility eligibility(RunSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        if (!RunSubmissionPolicyValidator.isSupported(snapshot)) {
            return Eligibility.PENDING;
        }
        if (snapshot.status() == RunStatus.READY
                && snapshot.attemptCount() < snapshot.maxAttempts()) {
            AttemptSnapshot attempt = snapshot.currentAttempt();
            if (attempt == null || attempt.status().isTerminal()) {
                return Eligibility.EXECUTE_READY;
            }
        }
        AttemptSnapshot attempt = snapshot.currentAttempt();
        if (snapshot.status() == RunStatus.RUNNING && attempt != null
                && attempt.status() == AttemptStatus.CREATED) {
            return Eligibility.EXECUTE_CREATED;
        }
        if (snapshot.status() == RunStatus.RUNNING && attempt != null
                && (attempt.status() == AttemptStatus.DISPATCHING
                || attempt.status() == AttemptStatus.RUNNING
                || attempt.status() == AttemptStatus.COMMITTING)) {
            return Eligibility.RECONCILE_IN_FLIGHT;
        }
        if (snapshot.status() == RunStatus.CANCELING && attempt != null
                && attempt.status() == AttemptStatus.CANCELING
                && (attempt.cancellationCause()
                == io.rowweave.run.CancellationCause.USER
                || attempt.cancellationCause()
                == io.rowweave.run.CancellationCause.TIMEOUT)) {
            return Eligibility.RECONCILE_CANCELLATION;
        }
        return Eligibility.PENDING;
    }

    static Identity identityFor(RunSnapshot snapshot) {
        Eligibility eligibility = eligibility(snapshot);
        if (eligibility == Eligibility.EXECUTE_READY) {
            int attemptNumber = snapshot.attemptCount() + 1;
            String suffix = sha256Hex(snapshot.runId() + '\0' + attemptNumber);
            String attemptId = "recovery-attempt-v1-" + suffix;
            return new Identity(snapshot.runId(), attemptId,
                    "recovery-publication-v1-" + suffix, attemptNumber,
                    snapshot.updatedAtMillis());
        }
        if (eligibility == Eligibility.EXECUTE_CREATED) {
            AttemptSnapshot attempt = snapshot.currentAttempt();
            return new Identity(snapshot.runId(), attempt.attemptId(),
                    attempt.publicationId(), attempt.fenceToken(),
                    attempt.createdAtMillis());
        }
        throw new IllegalArgumentException("snapshot is not safely recoverable");
    }

    static String inFlightCommandId(RunSnapshot snapshot, String phase) {
        AttemptSnapshot attempt = snapshot == null ? null : snapshot.currentAttempt();
        Eligibility eligibility = eligibility(snapshot);
        if (attempt == null || (eligibility != Eligibility.RECONCILE_IN_FLIGHT
                && eligibility != Eligibility.RECONCILE_CANCELLATION
                && attempt.status() != AttemptStatus.RECONCILING)) {
            throw new IllegalArgumentException("snapshot is not an in-flight reconciliation");
        }
        return COMMAND_NAMESPACE + "in-flight-" + requireText(phase, "phase") + '-'
                + sha256Hex(snapshot.runId() + '\0' + attempt.attemptId());
    }

    static final class Candidate {
        private final RunSnapshot snapshot;
        private final RecoveryToken token;
        private final PlanArtifact artifact;
        private final io.rowweave.cluster.rpc.ControlResidentLease residentLease;
        private io.rowweave.cluster.rpc.ControlResidentLease.OwnerToken residentOwner;

        Candidate(RunSnapshot snapshot, RecoveryToken token, PlanArtifact artifact) {
            this(snapshot, token, artifact, null, null);
        }

        Candidate(RunSnapshot snapshot, RecoveryToken token, PlanArtifact artifact,
                  io.rowweave.cluster.rpc.ControlResidentLease residentLease,
                  io.rowweave.cluster.rpc.ControlResidentLease.OwnerToken residentOwner) {
            this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
            this.token = Objects.requireNonNull(token, "token");
            this.artifact = Objects.requireNonNull(artifact, "artifact");
            if ((residentLease == null) != (residentOwner == null)) {
                throw new IllegalArgumentException("recovery resident lease/owner mismatch");
            }
            this.residentLease = residentLease;
            this.residentOwner = residentOwner;
            if (!snapshot.runId().equals(token.runId())) {
                throw new IllegalArgumentException("recovery token belongs to another run");
            }
            if (!snapshot.definitionId().equals(artifact.definitionId())
                    || !snapshot.artifactId().equals(artifact.artifactId())) {
                throw new IllegalArgumentException("artifact identity does not match run authority");
            }
        }

        RunSnapshot snapshot() { return snapshot; }
        RecoveryToken token() { return token; }
        PlanArtifact artifact() { return artifact; }
        io.rowweave.cluster.rpc.ControlResidentLease residentLease() { return residentLease; }

        void reserveResident(long bytes) {
            if (residentLease != null) residentLease.reserve(residentOwner, bytes);
        }

        io.rowweave.cluster.rpc.ControlResidentLease.OwnerToken transferResidentToRun() {
            if (residentLease == null) return null;
            residentOwner = residentLease.transfer(residentOwner,
                    io.rowweave.cluster.rpc.ControlResidentOwner.RUN_OWNED);
            return residentOwner;
        }
    }

    static final class Identity {
        private final String runId;
        private final String attemptId;
        private final String publicationId;
        private final long fenceToken;
        private final long eventTimeMillis;
        private final String commandSeed;

        private Identity(String runId, String attemptId, String publicationId,
                         long fenceToken, long eventTimeMillis) {
            this.runId = requireText(runId, "runId");
            this.attemptId = requireText(attemptId, "attemptId");
            this.publicationId = requireText(publicationId, "publicationId");
            if (fenceToken <= 0L) {
                throw new IllegalArgumentException("fenceToken must be positive");
            }
            this.fenceToken = fenceToken;
            this.eventTimeMillis = eventTimeMillis;
            this.commandSeed = sha256Hex(runId + '\0' + attemptId);
        }

        String runId() { return runId; }
        String attemptId() { return attemptId; }
        String publicationId() { return publicationId; }
        long fenceToken() { return fenceToken; }
        long eventTimeMillis() { return eventTimeMillis; }

        String commandId(String phase) {
            return COMMAND_NAMESPACE + requireText(phase, "phase") + '-' + commandSeed;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof Identity)) return false;
            Identity that = (Identity) other;
            return fenceToken == that.fenceToken
                    && eventTimeMillis == that.eventTimeMillis
                    && runId.equals(that.runId)
                    && attemptId.equals(that.attemptId)
                    && publicationId.equals(that.publicationId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(runId, attemptId, publicationId,
                    fenceToken, eventTimeMillis);
        }
    }

    static final class Result {
        private final ResultCode code;
        private final String detail;

        private Result(ResultCode code, String detail) {
            this.code = Objects.requireNonNull(code, "code");
            this.detail = detail;
        }

        static Result resolved() {
            return new Result(ResultCode.RESOLVED, null);
        }

        static Result pending(String detail) {
            return new Result(ResultCode.PENDING, requireDetail(detail));
        }

        static Result retry(String detail) {
            return new Result(ResultCode.RETRY, requireDetail(detail));
        }

        ResultCode code() { return code; }
        String detail() { return detail; }
    }

    private static String requireDetail(String value) {
        String result = requireText(value, "detail");
        if (result.getBytes(StandardCharsets.UTF_8).length > MAX_DETAIL_BYTES) {
            throw new IllegalArgumentException("detail exceeds UTF-8 byte limit");
        }
        return result;
    }

    private static String requireText(String value, String field) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    private static String sha256Hex(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            char[] result = new char[digest.length * 2];
            char[] alphabet = "0123456789abcdef".toCharArray();
            for (int i = 0; i < digest.length; i++) {
                int unsigned = digest[i] & 0xff;
                result[i * 2] = alphabet[unsigned >>> 4];
                result[i * 2 + 1] = alphabet[unsigned & 0x0f];
            }
            return new String(result);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
