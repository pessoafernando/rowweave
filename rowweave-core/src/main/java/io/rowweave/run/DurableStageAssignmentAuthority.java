package io.rowweave.run;

import io.rowweave.QualityGateDecision;
import io.rowweave.QualityGateDecisionResult;

/** Backend capability whose operations share the Run authority linearization point. */
public interface DurableStageAssignmentAuthority {
    StageManifestPublication publish(StageAssignmentManifest manifest);

    StageAssignmentSnapshot assignmentSnapshot(RunAttemptFence fence);

    StageAcceptanceDecision accept(StageAssignmentAcceptance acceptance);

    /**
     * Records the first terminal outcome. Backends not yet upgraded fail closed rather than
     * acknowledging a report without durable evidence.
     */
    default StageTerminalDecision recordTerminal(StageTerminalReceipt receipt) {
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        return StageTerminalDecision.rejected(StageTerminalDecision.Code.UNAVAILABLE);
    }

    default PreparedSinkDecision recordPreparedSink(PreparedSinkReceipt receipt) {
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        return PreparedSinkDecision.rejected(PreparedSinkDecision.Code.UNAVAILABLE);
    }

    default ManifestCommitJournalSnapshot manifestCommitJournal(RunAttemptFence fence) {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        return null;
    }

    /**
     * Reads and seals the exact durable intent observation at the authority boundary.
     * A missing journal or an undecided intent returns {@code null}.
     */
    default DurableManifestCommitIntentObservation manifestCommitIntentObservation(
            RunAttemptFence fence) {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        ManifestCommitJournalSnapshot snapshot = manifestCommitJournal(fence);
        return snapshot == null || snapshot.intent() == null
                ? null : new DurableManifestCommitIntentObservation(snapshot);
    }

    default QualityGateCommitJournalSnapshot qualityGateCommitJournal(
            RunAttemptFence fence) {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        return null;
    }

    default ManifestMemberDecision recordManifestMember(ManifestMemberReceipt receipt) {
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        return ManifestMemberDecision.rejected(ManifestMemberDecision.Code.UNAVAILABLE);
    }

    default ManifestIntentDecision commitManifestIntent(ManifestCommitIntent intent) {
        if (intent == null) throw new IllegalArgumentException("intent is required");
        return ManifestIntentDecision.rejected(ManifestIntentDecision.Code.UNAVAILABLE);
    }

    default QualityGateDecision qualityGateDecision(RunAttemptFence fence) {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        return null;
    }

    default QualityGateDecisionResult recordQualityGateDecision(
            QualityGateDecision decision) {
        if (decision == null) throw new IllegalArgumentException("decision is required");
        return QualityGateDecisionResult.rejected(
                QualityGateDecisionResult.Code.UNAVAILABLE);
    }

    /** Durable abort state outlives the assignment journal and remains queryable after Run terminal. */
    default BatchSinkAbortRecord batchSinkAbort(RunAttemptFence fence) {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        return null;
    }

    default BatchSinkAbortDecision recordBatchSinkAbort(BatchSinkAbortRecord record) {
        if (record == null) throw new IllegalArgumentException("abort record is required");
        return BatchSinkAbortDecision.rejected(BatchSinkAbortDecision.Code.UNAVAILABLE);
    }
}
