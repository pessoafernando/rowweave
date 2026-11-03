package io.rowweave.run;

import io.rowweave.QualityGateDecision;
import io.rowweave.QualityGateDecisionResult;

/** Recovery-only manifest publication guarded by the exact durable ownership token. */
public interface RecoveryFencedStageAssignmentAuthority
        extends DurableStageAssignmentAuthority {
    StageManifestPublication publishRecovered(
            RecoveryToken token, StageAssignmentManifest manifest);

    /** Fail-closed until a backend proves the exact recovery owner at receipt CAS. */
    default StageAcceptanceDecision acceptRecovered(
            RecoveryToken token, StageAssignmentAcceptance acceptance) {
        return StageAcceptanceDecision.rejected(StageAcceptanceDecision.Code.RECOVERY_OWNED);
    }

    /** Fail-closed until a backend proves the exact recovery owner at terminal CAS. */
    default StageTerminalDecision recordTerminalRecovered(
            RecoveryToken token, StageTerminalReceipt receipt) {
        if (token == null) throw new IllegalArgumentException("token is required");
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        return StageTerminalDecision.rejected(StageTerminalDecision.Code.RECOVERY_OWNED);
    }

    default PreparedSinkDecision recordPreparedSinkRecovered(
            RecoveryToken token, PreparedSinkReceipt receipt) {
        if (token == null) throw new IllegalArgumentException("token is required");
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        return PreparedSinkDecision.rejected(
                PreparedSinkDecision.Code.RECOVERY_OWNED);
    }

    default ManifestMemberDecision recordManifestMemberRecovered(
            RecoveryToken token, ManifestMemberReceipt receipt) {
        if (token == null) throw new IllegalArgumentException("token is required");
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        return ManifestMemberDecision.rejected(ManifestMemberDecision.Code.RECOVERY_OWNED);
    }

    default ManifestIntentDecision commitManifestIntentRecovered(
            RecoveryToken token, ManifestCommitIntent intent) {
        if (token == null) throw new IllegalArgumentException("token is required");
        if (intent == null) throw new IllegalArgumentException("intent is required");
        return ManifestIntentDecision.rejected(ManifestIntentDecision.Code.RECOVERY_OWNED);
    }

    default QualityGateDecisionResult recordQualityGateDecisionRecovered(
            RecoveryToken token, QualityGateDecision decision) {
        if (token == null) throw new IllegalArgumentException("token is required");
        if (decision == null) throw new IllegalArgumentException("decision is required");
        return QualityGateDecisionResult.rejected(
                QualityGateDecisionResult.Code.RECOVERY_OWNED);
    }

    default BatchSinkAbortDecision recordBatchSinkAbortRecovered(
            RecoveryToken token, BatchSinkAbortRecord record) {
        if (token == null) throw new IllegalArgumentException("token is required");
        if (record == null) throw new IllegalArgumentException("abort record is required");
        return BatchSinkAbortDecision.rejected(BatchSinkAbortDecision.Code.RECOVERY_OWNED);
    }
}
