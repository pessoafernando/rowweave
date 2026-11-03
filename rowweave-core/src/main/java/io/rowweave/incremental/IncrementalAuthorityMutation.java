package io.rowweave.incremental;

import io.rowweave.run.RecoveryToken;

import java.security.MessageDigest;

/** Internal envelope that prevents terminal mutations from bypassing their typed proof. */
final class IncrementalAuthorityMutation {
    private final IncrementalAuthorityCommand command;
    private final FrontierRecoveryToken recoveryToken;
    private final RecoveryToken runRecoveryToken;
    private final IncrementalInitializeRequest initializeRequest;
    private final IncrementalClaimRequest claimRequest;
    private final VerifiedIncrementalJointCommitProof jointCommitProof;
    private final VerifiedIncrementalAbortClosureProof abortClosureProof;
    private final VerifiedIncrementalConflictProof conflictProof;

    private IncrementalAuthorityMutation(
            IncrementalAuthorityCommand command, FrontierRecoveryToken recoveryToken,
            RecoveryToken runRecoveryToken,
            IncrementalInitializeRequest initializeRequest,
            IncrementalClaimRequest claimRequest,
            VerifiedIncrementalJointCommitProof jointCommitProof,
            VerifiedIncrementalAbortClosureProof abortClosureProof,
            VerifiedIncrementalConflictProof conflictProof) {
        if (command == null) throw new IllegalArgumentException("authority command is required");
        this.command = command;
        this.recoveryToken = recoveryToken;
        this.runRecoveryToken = runRecoveryToken;
        this.initializeRequest = initializeRequest;
        this.claimRequest = claimRequest;
        this.jointCommitProof = jointCommitProof;
        this.abortClosureProof = abortClosureProof;
        this.conflictProof = conflictProof;
        validateCommon();
    }

    static IncrementalAuthorityMutation control(
            IncrementalAuthorityCommand command, FrontierRecoveryToken token) {
        if (command == null || (command.kind() != IncrementalAuthorityCommandKind.INITIALIZE
                && command.kind() != IncrementalAuthorityCommandKind.CLAIM)) {
            throw new IllegalArgumentException("control envelope cannot carry terminal command");
        }
        if ((command.kind() == IncrementalAuthorityCommandKind.INITIALIZE) != (token == null)) {
            throw new IllegalArgumentException("only INITIALIZE is valid without recovery ownership");
        }
        return new IncrementalAuthorityMutation(
                command, token, null, null, null, null, null, null);
    }

    static IncrementalAuthorityMutation initialize(
            IncrementalAuthorityCommand command,
            IncrementalInitializeRequest request) {
        if (command == null || request == null
                || command.kind() != IncrementalAuthorityCommandKind.INITIALIZE
                || !command.feedId().equals(request.feedId())
                || command.feedIncarnation() != request.feedIncarnation()
                || !MessageDigest.isEqual(
                command.transitionDigest(), request.transitionDigest())) {
            throw new IllegalArgumentException(
                    "initialize command requires its exact transition request");
        }
        return new IncrementalAuthorityMutation(
                command, null, null, request, null, null, null, null);
    }

    static IncrementalAuthorityMutation claim(
            IncrementalAuthorityCommand command, FrontierRecoveryToken token,
            IncrementalClaimRequest request) {
        if (command == null || token == null || request == null
                || command.kind() != IncrementalAuthorityCommandKind.CLAIM
                || !command.feedId().equals(request.feedId())
                || !command.intervalId().equals(request.intervalId())
                || !command.fence().equals(request.firstAttemptFence())
                || !MessageDigest.isEqual(
                command.transitionDigest(), request.transitionDigest())) {
            throw new IllegalArgumentException(
                    "claim command requires its exact pending Run request");
        }
        return new IncrementalAuthorityMutation(
                command, token, null, null, request, null, null, null);
    }

    static IncrementalAuthorityMutation jointTerminal(
            IncrementalAuthorityCommand command, FrontierRecoveryToken token,
            RecoveryToken runToken,
            VerifiedIncrementalJointCommitProof proof) {
        if (command == null || (command.kind() != IncrementalAuthorityCommandKind.COMMIT
                && command.kind() != IncrementalAuthorityCommandKind.COMPLETE_BACKFILL)
                || proof == null
                || token == null || runToken == null
                || !token.owner().equals(runToken.owner())
                || !proof.fence().runId().equals(runToken.runId())
                || !proof.feedId().equals(token.feedId())
                || proof.feedIncarnation() != token.feedIncarnation()
                || !MessageDigest.isEqual(
                proof.frontierRecoveryTokenDigest(), token.tokenDigest())
                || !MessageDigest.isEqual(
                proof.runRecoveryTokenDigest(), runToken.tokenDigest())
                || (proof.mode() == IncrementalRunMode.BACKFILL_V1)
                != (command.kind() == IncrementalAuthorityCommandKind.COMPLETE_BACKFILL)
                || !MessageDigest.isEqual(command.transitionDigest(), proof.proofDigest())) {
            throw new IllegalArgumentException("joint terminal command requires its exact joint proof");
        }
        return new IncrementalAuthorityMutation(
                command, token, runToken, null, null, proof, null, null);
    }

    static IncrementalAuthorityMutation closeAborted(
            IncrementalAuthorityCommand command, FrontierRecoveryToken token,
            RecoveryToken runToken,
            VerifiedIncrementalAbortClosureProof proof) {
        if (command == null || command.kind() != IncrementalAuthorityCommandKind.CLOSE_ABORTED_RUN
                || proof == null
                || token == null || runToken == null
                || !token.owner().equals(runToken.owner())
                || !proof.fence().runId().equals(runToken.runId())
                || !proof.feedId().equals(token.feedId())
                || proof.feedIncarnation() != token.feedIncarnation()
                || !MessageDigest.isEqual(
                proof.frontierRecoveryTokenDigest(), token.tokenDigest())
                || !MessageDigest.isEqual(
                proof.runRecoveryTokenDigest(), runToken.tokenDigest())
                || !MessageDigest.isEqual(command.transitionDigest(), proof.proofDigest())) {
            throw new IllegalArgumentException("close command requires its exact abort proof");
        }
        return new IncrementalAuthorityMutation(
                command, token, runToken, null, null, null, proof, null);
    }

    static IncrementalAuthorityMutation markConflict(
            IncrementalAuthorityCommand command, FrontierRecoveryToken token,
            RecoveryToken runToken, VerifiedIncrementalConflictProof proof) {
        if (command == null
                || command.kind() != IncrementalAuthorityCommandKind.MARK_CONFLICT
                || token == null || runToken == null || proof == null
                || !token.owner().equals(runToken.owner())
                || !proof.feedId().equals(token.feedId())
                || proof.feedIncarnation() != token.feedIncarnation()
                || !proof.fence().runId().equals(runToken.runId())
                || command.intervalId() == null || command.fence() == null
                || !command.intervalId().equals(proof.intervalId())
                || !command.fence().equals(proof.fence())
                || !MessageDigest.isEqual(
                proof.frontierRecoveryTokenDigest(), token.tokenDigest())
                || !MessageDigest.isEqual(
                proof.runRecoveryTokenDigest(), runToken.tokenDigest())
                || !MessageDigest.isEqual(
                command.transitionDigest(), proof.proofDigest())) {
            throw new IllegalArgumentException(
                    "conflict command requires its exact typed proof");
        }
        return new IncrementalAuthorityMutation(
                command, token, runToken, null, null, null, null, proof);
    }

    private void validateCommon() {
        if (command.kind() != IncrementalAuthorityCommandKind.INITIALIZE) {
            if (recoveryToken == null
                    || !command.feedId().equals(recoveryToken.feedId())
                    || command.feedIncarnation() != recoveryToken.feedIncarnation()) {
                throw new IllegalArgumentException("command does not match recovery ownership token");
            }
        }
        if (jointCommitProof != null
                && (!command.intervalId().equals(jointCommitProof.intervalId())
                || !command.fence().equals(jointCommitProof.fence()))) {
            throw new IllegalArgumentException("joint proof does not match command interval/fence");
        }
        if (abortClosureProof != null
                && (!command.intervalId().equals(abortClosureProof.intervalId())
                || !command.fence().equals(abortClosureProof.fence()))) {
            throw new IllegalArgumentException("abort proof does not match command interval/fence");
        }
        if (conflictProof != null
                && (!command.intervalId().equals(conflictProof.intervalId())
                || !command.fence().equals(conflictProof.fence()))) {
            throw new IllegalArgumentException(
                    "conflict proof does not match command interval/fence");
        }
    }

    IncrementalAuthorityCommand command() { return command; }
    FrontierRecoveryToken recoveryToken() { return recoveryToken; }
    RecoveryToken runRecoveryToken() { return runRecoveryToken; }
    IncrementalInitializeRequest initializeRequest() { return initializeRequest; }
    IncrementalClaimRequest claimRequest() { return claimRequest; }
    VerifiedIncrementalJointCommitProof jointCommitProof() { return jointCommitProof; }
    VerifiedIncrementalAbortClosureProof abortClosureProof() { return abortClosureProof; }
    VerifiedIncrementalConflictProof conflictProof() { return conflictProof; }
}
