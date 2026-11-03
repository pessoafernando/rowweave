#!/usr/bin/env python3
"""Resource and backpressure documentation parity gate."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs/product/architecture/resources.md"

REQUIRED_SECTIONS = (
    "Resource layers and admission order",
    "Worker capacity and Batch placement",
    "Batch managed memory and spill",
    "Channels, backpressure, and failure propagation",
    "Control and data-plane admission",
    "Stream resource profile and placement",
    "Stream task bounds and channel evidence",
    "Cancellation, shutdown, and cleanup",
    "Public boundary and tuning model",
)

REQUIRED_TERMS = (
    "process envelope", "Worker", "slot", "managed memory", "spill",
    "quota", "channel", "256 records", "backpressure", "credit",
    "resident bytes", "frame", "StreamResourceProfile", "lease",
    "file descriptors", "checkpoint", "high-water", "cancellation",
    "typed failure",
)

SOURCES = {
    "WorkerConfig.java": "rowweave-core/src/main/java/io/rowweave/mem/WorkerConfig.java",
    "SlotLedger.java": "rowweave-core/src/main/java/io/rowweave/cluster/SlotLedger.java",
    "ProcessThreadCapacityModel.java": "rowweave-core/src/main/java/io/rowweave/cluster/ProcessThreadCapacityModel.java",
    "StageThreadCostModel.java": "rowweave-core/src/main/java/io/rowweave/cluster/StageThreadCostModel.java",
    "GlobalMemoryPool.java": "rowweave-core/src/main/java/io/rowweave/mem/GlobalMemoryPool.java",
    "ExecutionMemory.java": "rowweave-core/src/main/java/io/rowweave/mem/ExecutionMemory.java",
    "ManagedMemory.java": "rowweave-core/src/main/java/io/rowweave/mem/ManagedMemory.java",
    "SpillWorkspaceManager.java": "rowweave-core/src/main/java/io/rowweave/mem/spill/SpillWorkspaceManager.java",
    "LocalChannel.java": "rowweave-core/src/main/java/io/rowweave/sched/LocalChannel.java",
    "MultiProducerChannel.java": "rowweave-core/src/main/java/io/rowweave/sched/MultiProducerChannel.java",
    "MuxConnectionPool.java": "rowweave-core/src/main/java/io/rowweave/sched/net/MuxConnectionPool.java",
    "ControlResidentBudget.java": "rowweave-core/src/main/java/io/rowweave/cluster/rpc/ControlResidentBudget.java",
    "RpcFrameBudget.java": "rowweave-core/src/main/java/io/rowweave/cluster/rpc/RpcFrameBudget.java",
    "ShuffleIngressResidentBudget.java": "rowweave-core/src/main/java/io/rowweave/sched/net/ShuffleIngressResidentBudget.java",
    "StreamResourceProfile.java": "rowweave-core/src/main/java/io/rowweave/stream/api/StreamResourceProfile.java",
    "StreamTaskResourceRequest.java": "rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamTaskResourceRequest.java",
    "StreamNodeTaskContext.java": "rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamNodeTaskContext.java",
    "StreamCheckpointTransferBudget.java": "rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamCheckpointTransferBudget.java",
    "StreamTaskResourceUsage.java": "rowweave-core/src/main/java/io/rowweave/stream/cluster/StreamTaskResourceUsage.java",
    "StreamChannelMetrics.java": "rowweave-core/src/main/java/io/rowweave/stream/runtime/StreamChannelMetrics.java",
    "StreamMonitorBridge.java": "rowweave-core/src/main/java/io/rowweave/monitor/StreamMonitorBridge.java",
    "PipelineExecutor.java": "rowweave-core/src/main/java/io/rowweave/sched/PipelineExecutor.java",
    "WorkerServer.java": "rowweave-core/src/main/java/io/rowweave/cluster/WorkerServer.java",
}

REQUIRED_TEST_METHODS = {
    "SlotLedgerTest": [
        "demandExceedsCapacityFailsFast", "concurrentAcquireNoOversell",
    ],
    "ProcessThreadCapacityModelTest": [
        "masterWorkerAndCombinedFacilitiesAreAdditive",
    ],
    "WorkerStageThreadAdmissionTest": [
        "rejectsBeforeStageTaskPortOrProgressSideEffects",
    ],
    "SpillWiringTest": [
        "groupBySpillMatchesHeap", "orderBySpillOrdered",
        "sharedBudgetMultiBreakerPeakBounded",
        "concurrentMultiBreakerSpillDeterministic",
    ],
    "ExecutionMemoryTest": [
        "newOperatorMemoryAcquiresUpToMaxFromPool",
        "tinyPoolRejectsUnaccountedFallbackOwners",
    ],
    "SpillWorkspaceQuotaTest": [
        "jobQuotaRejectsBeforeExtendingRunByOneMoreFrame",
        "concurrentJobsLinearizeAtWorkerLimit",
    ],
    "SpillWorkspaceProcessCrashTest": [
        "realRuntimeHaltLeavesTrustedOrphanThatNextProcessReclaims",
        "liveChildLeaseCannotBeCrossedButBecomesReclaimableAfterDeath",
    ],
    "LocalChannelTest": [
        "parkCountIncrementsWhenFull", "pollReturnsNullAtEndOfStreamAndStaysNull",
    ],
    "ClusterPushTest": ["failFastNoDeadlock"],
    "RpcResidentAdmissionTest": [
        "decodedFieldReservationLivesThroughHandlerAndReleasesAfterReturn",
        "residentExhaustionReturnsStableFailureBeforeHandlerAndLeavesNoCharge",
        "clientReservesExternalSubmitPayloadBeforeWritingAnyRequestBytes",
    ],
    "StreamTopologyNeutralFoundationTest": [
        "placementExpansionUsesEveryNodeSubtaskAndNodeLocalResources",
        "placementRejectsAJobWhosePhysicalSocketsExceedItsFdCeiling",
        "placementPreflightRejectsInsufficientWorkerSlotsBeforeTaskExpansion",
        "jobCheckpointCeilingMustCoverEveryNodePayloadLease",
        "nodeTaskRuntimeConsumesTheSameResourceAuthorityAsPlacement",
    ],
    "PortableStreamPlanContractTest": [
        "nodeResidentLeaseMustCoverEveryIncidentEdgeFrame",
    ],
    "StreamCheckpointTransferBudgetTest": [
        "wholeGraphReceiptsAndPerTaskMembersHaveIndependentWireBudgets",
    ],
    "StreamKeyedProcessOperatorTaskTest": [
        "timerDeclarationMustFitJobResourceBeforeWorkerOpen",
    ],
    "StreamMonitorBridgeTest": [
        "progressUpdatesTypedViewAndMetricSet",
        "registryFailuresNeverEscapeExecutionPath",
    ],
    "InMemoryJobMonitorTest": ["boundedHistoryEvictsOldest"],
    "BoundedStageCancellationTest": [
        "uncooperativeRunningStageReturnsBoundedCancellationFailure",
        "rejectsNegativeCancelDrainBeforeOpeningWorker",
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
        if not re.search(r"^#\s+Resource and backpressure model\s*$", body, re.M):
            failures.append("missing title")
        headings = re.findall(r"^##\s+(.+?)\s*$", body, re.M)
        for section in REQUIRED_SECTIONS:
            if section not in headings:
                failures.append(f"missing section: {section}")
        for term in REQUIRED_TERMS:
            if term.lower() not in body.lower():
                failures.append(f"missing resource term: {term}")
        for forbidden in ("automatically scales", "unlimited memory", "raise a limit to avoid"):
            if forbidden.lower() in body.lower():
                failures.append(f"unsupported promise: {forbidden}")
        if "there is no public per-session Batch memory setter" not in body:
            failures.append("missing Batch tuning boundary")
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
            print(f"RESOURCE MODEL PARITY: FAIL ({len(failures)} problem(s))")
            for failure in failures:
                print(f"- {failure}")
            return 1
        count = sum(len(methods) for methods in REQUIRED_TEST_METHODS.values())
        print(
            f"RESOURCE MODEL PARITY: PASS ({len(REQUIRED_SECTIONS)} sections, "
            f"{len(SOURCES)} source files, {len(REQUIRED_TEST_METHODS)} test classes, "
            f"{count} cited methods)"
        )
        return 0
    except (OSError, ValueError) as failure:
        print(f"RESOURCE MODEL PARITY: FAIL: {failure}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
