#!/usr/bin/env python3
"""Recovery and state-authority documentation parity gate."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs/product/architecture/recovery.md"

REQUIRED_SECTIONS = (
    "Authority map",
    "Batch run, attempt, and fencing",
    "Batch output, manifest, and whole-attempt retry",
    "Stream run and checkpoint authority",
    "Savepoints, restore, and Stream rescale",
    "Incremental and CDC frontier",
    "Persistent-store migration and rollback",
    "Status visibility and failure boundary",
)

REQUIRED_TERMS = (
    "RunStateStore", "RunSnapshot", "RunAttemptFence", "ownership",
    "whole attempt", "manifest", "checkpoint", "first-decision-wins",
    "COMPLETED", "savepoint", "restore", "rescale", "frontier", "CDC",
    "migration", "rollback", "acknowledgement", "fail closed",
)

SOURCES = {
    "RunStateStore.java": "rowweave-core/src/main/java/io/rowweave/run/RunStateStore.java",
    "RunSnapshot.java": "rowweave-core/src/main/java/io/rowweave/run/RunSnapshot.java",
    "RunAttemptFence.java": "rowweave-core/src/main/java/io/rowweave/run/RunAttemptFence.java",
    "RunRecoveryOwnership.java": "rowweave-core/src/main/java/io/rowweave/run/RunRecoveryOwnership.java",
    "WholeAttemptRetryAuthority.java": "rowweave-core/src/main/java/io/rowweave/run/WholeAttemptRetryAuthority.java",
    "InheritedRunRecovery.java": "rowweave-core/src/main/java/io/rowweave/cluster/InheritedRunRecovery.java",
    "ManifestCommitAuthority.java": "rowweave-core/src/main/java/io/rowweave/commit/ManifestCommitAuthority.java",
    "ManifestFileRecovery.java": "rowweave-core/src/main/java/io/rowweave/commit/ManifestFileRecovery.java",
    "MasterServer.java": "rowweave-core/src/main/java/io/rowweave/cluster/MasterServer.java",
    "FileStreamClusterRunAuthority.java": "rowweave-core/src/main/java/io/rowweave/stream/cluster/FileStreamClusterRunAuthority.java",
    "StreamClusterRunStateStore.java": "rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamClusterRunStateStore.java",
    "StreamCheckpointAuthority.java": "rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamCheckpointAuthority.java",
    "StreamCheckpointCoordinator.java": "rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamCheckpointCoordinator.java",
    "StreamSavepoint.java": "rowweave-core/src/main/java/io/rowweave/stream/contract/StreamSavepoint.java",
    "StreamSavepointRestoreValidator.java": "rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamSavepointRestoreValidator.java",
    "FileOfflineRescaleAuthority.java": "rowweave-core/src/main/java/io/rowweave/stream/runtime/FileOfflineRescaleAuthority.java",
    "IncrementalFrontierStore.java": "rowweave-core/src/main/java/io/rowweave/incremental/IncrementalFrontierStore.java",
    "IncrementalFrontierAuthorityState.java": "rowweave-core/src/main/java/io/rowweave/incremental/IncrementalFrontierAuthorityState.java",
    "UpgradeAuthorityKernel.java": "rowweave-core/src/main/java/io/rowweave/upgrade/UpgradeAuthorityKernel.java",
    "FileUpgradeAuthority.java": "rowweave-core/src/main/java/io/rowweave/upgrade/FileUpgradeAuthority.java",
    "RetryRecoveryMetrics.java": "rowweave-core/src/main/java/io/rowweave/cluster/RetryRecoveryMetrics.java",
}

REQUIRED_TEST_METHODS = {
    "RunStateMatrixTest": [
        "everyDeclaredAttemptEdgeIsAccepted",
        "genericRetryMutationIsRejectedWithoutConsumingRevision",
    ],
    "WholeAttemptRetryModelTest": [
        "retryPolicyIsFixedBoundedAndOverflowSafe",
        "nextAttemptIdentityIsStableAndSeparatesRunArtifactAndNumber",
        "cancelAndDeadlineCanTerminateReadyRetryWithoutDroppingPreviousAttempt",
    ],
    "RunRecoveryOwnershipContract": [
        "competingClaimsFromTheSameObservedEpochPublishOnlyOneOwner",
        "staleRevisionCannotPublishOrReplaceOwnership",
    ],
    "MasterRunAttemptFenceTest": [
        "wholeGraphAssignmentCarriesFenceAndWrongFenceCannotCompleteTracker",
    ],
    "WorkerReportRunAttemptFenceTest": [
        "portProgressAndCancelledTerminalReportsKeepAssignmentFence",
    ],
    "ManifestProcessCrashRecoveryTest": [
        "realProcessCrashMatrixAndRestartRecovery",
    ],
    "MasterInFlightRunRecoveryTest": [
        "replacementLeaderPersistsManifestAbortBeforeCanceledTerminal",
        "replacementLeaderFinishesAfterManifestPublishAckLoss",
        "livePreparedManifestIsInstalledSealedJournaledThenPublished",
    ],
    "FileStreamClusterRunAuthorityTest": [
        "liveRescaleAtomicallyAdvancesGenerationFromCompletedCut",
        "replacementRecoversCheckpointAndWorkerLossAtomicallyRestartsWholeGraph",
        "workerRejectsOldCoordinatorAttemptReplayAndWrongIncarnation",
        "decisionRecoveryIsIdempotentOutsideTheOrdinaryCommandSequence",
    ],
    "StreamCheckpointLifecycleContractTest": [
        "replacementMasterConvergesDurableRunningCheckpointWithoutAttach",
        "cancelConvergesCommitDecidedCheckpointBeforeTerminalTransition",
    ],
    "FileCheckpointAuthorityTest": [
        "commitAndCompletionSurviveReopenAndAckLoss",
        "firstAbortOrCommitDecisionCannotBeRewritten",
    ],
    "StreamCheckpointCoordinatorTest": [
        "replacementBeforeDecisionCanAbortButCannotCommitPartialClosure",
    ],
    "FileStreamSavepointStoreTest": [
        "completedCheckpointBecomesCanonicalReopenableSavepoint",
        "durablePublicationSurvivesAckLossAndCorruptionFailsClosed",
    ],
    "StreamSavepointRestoreValidatorTest": [
        "stateOrMaxParallelismDriftFailsClosed",
        "forgedPlanDigestFailsBeforeRestore",
    ],
    "StreamLiveRescaleAuthorityTest": [
        "intentAndCutoverSurviveAuthorityReplacement",
        "durableCutoverCannotRollBackToOldGeneration",
    ],
    "IncrementalFileAuthorityProcessCrashContractTest": [
        "processHaltAfterJointForceRecoversBothProjections",
    ],
    "FileUpgradeAuthorityTest": [
        "shadowCopyVerifyAndAckLossCutoverExposeCompleteAuthority",
        "corruptAuthorityFailsClosedBeforeMutation",
    ],
    "UpgradeRuntimeCoordinatorTest": [
        "freezeAndDrainShareOneLinearizationLockWithRuntimeAdmission",
        "quiesceIncludesDurableWorkRediscoveredAfterLeaderFailover",
    ],
    "MasterStoreUpgradeAdmissionTest": [
        "activeLeaderFreezesAllSubmitTypesBeforePayloadDecoding",
    ],
    "FileStreamStateMigrationAuthorityTest": [
        "corruptShadowChunkCannotMoveActivePointer",
    ],
    "StreamStateMigrationBindingContractTest": [
        "stateIdentityAndSemanticsCannotDriftAcrossMigration",
    ],
}


def find_test(test_name: str) -> Path:
    matches = list((ROOT / "rowweave-core/src/test/java").rglob(f"{test_name}.java"))
    if len(matches) != 1:
        raise ValueError(f"{test_name} has {len(matches)} test files")
    return matches[0]


def main() -> int:
    try:
        body = DOC.read_text(encoding="utf-8")
        failures = []
        if not re.search(r"^#\s+Recovery and state authority\s*$", body, re.M):
            failures.append("missing title")
        headings = re.findall(r"^##\s+(.+?)\s*$", body, re.M)
        for section in REQUIRED_SECTIONS:
            if section not in headings:
                failures.append(f"missing section: {section}")
        for term in REQUIRED_TERMS:
            if term.lower() not in body.lower():
                failures.append(f"missing recovery term: {term}")
        if "Authority map" not in body or "| Concern |" not in body:
            failures.append("missing authority-map table")
        if "manifest, first decision, completion" not in body:
            failures.append("missing Stream durable decision order")
        if "active pointer" not in body or "writer epoch" not in body:
            failures.append("missing store pointer/epoch authority")
        for filename, relative in SOURCES.items():
            path = ROOT / relative
            if not path.is_file():
                failures.append(f"source missing: {relative}")
            elif f"](../../../{relative})" not in body:
                failures.append(f"source link absent: {filename}")
        for test_name, methods in REQUIRED_TEST_METHODS.items():
            path = find_test(test_name)
            text = path.read_text(encoding="utf-8", errors="replace")
            for method in methods:
                if not re.search(rf"\bvoid\s+{re.escape(method)}\s*\(", text):
                    failures.append(f"{test_name}#{method}: method absent")
                elif f"`{test_name}#{method}`" not in body:
                    failures.append(f"test evidence absent: {test_name}#{method}")
        if failures:
            print(f"RECOVERY AUTHORITY PARITY: FAIL ({len(failures)} problem(s))")
            for failure in failures:
                print(f"- {failure}")
            return 1
        count = sum(len(methods) for methods in REQUIRED_TEST_METHODS.values())
        print(
            f"RECOVERY AUTHORITY PARITY: PASS ({len(REQUIRED_SECTIONS)} sections, "
            f"{len(SOURCES)} source files, {len(REQUIRED_TEST_METHODS)} test classes, "
            f"{count} cited methods)"
        )
        return 0
    except (OSError, ValueError) as failure:
        print(f"RECOVERY AUTHORITY PARITY: FAIL: {failure}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
