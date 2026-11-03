package io.rowweave.incremental;

import io.rowweave.run.DurableStageAssignmentAuthority;
import io.rowweave.run.BatchSinkAbortDecision;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.ManifestCommitIntent;
import io.rowweave.run.ManifestIntentDecision;
import io.rowweave.run.ManifestMemberDecision;
import io.rowweave.run.ManifestMemberReceipt;
import io.rowweave.run.PreparedSinkDecision;
import io.rowweave.run.PreparedSinkReceipt;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.StageAcceptanceDecision;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageManifestPublication;
import io.rowweave.run.StageTerminalDecision;
import io.rowweave.run.StageTerminalReceipt;

/** Internal Sink journal surface fenced by exact frontier and Run ownership. */
interface RecoveryFencedIncrementalSinkAuthority
        extends DurableStageAssignmentAuthority {
    StageManifestPublication publishRecovered(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageAssignmentManifest manifest);

    StageAcceptanceDecision acceptRecovered(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageAssignmentAcceptance acceptance);

    StageTerminalDecision recordTerminalRecovered(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            StageTerminalReceipt receipt);

    PreparedSinkDecision recordPreparedSinkRecovered(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            PreparedSinkReceipt receipt);

    ManifestMemberDecision recordManifestMemberRecovered(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            ManifestMemberReceipt receipt);

    ManifestIntentDecision commitManifestIntentRecovered(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            ManifestCommitIntent intent);

    BatchSinkAbortDecision recordBatchSinkAbortRecovered(
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            BatchSinkAbortRecord record);
}
