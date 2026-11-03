#!/usr/bin/env python3
"""Cluster topology, scheduling, and transport documentation parity gate."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs/product/architecture/cluster.md"

REQUIRED_SECTIONS = (
    "Roles and process boundaries",
    "Startup, discovery, and readiness",
    "Artifact admission and deployment identity",
    "Compilation, capacity, and placement",
    "Assignment and Worker task lifecycle",
    "Transport and security boundaries",
    "Local pseudo-deployment",
    "Failure isolation and shutdown",
)

REQUIRED_TERMS = (
    "Client", "Master", "Worker", "registry", "artifact", "slot",
    "placement", "assignment", "incarnation", "bounded", "mutual TLS",
    "checkpoint", "Stream", "local", "cancellation",
)

SOURCES = {
    "ClusterMain.java": "rowweave-core/src/main/java/io/rowweave/cluster/ClusterMain.java",
    "StreamClusterMain.java": "rowweave-core/src/main/java/io/rowweave/StreamClusterMain.java",
    "ServiceRegistry.java": "rowweave-core/src/main/java/io/rowweave/cluster/registry/ServiceRegistry.java",
    "ControlSecurityConfig.java": "rowweave-core/src/main/java/io/rowweave/cluster/rpc/ControlSecurityConfig.java",
    "ClusterJobClient.java": "rowweave-core/src/main/java/io/rowweave/cluster/ClusterJobClient.java",
    "MasterServer.java": "rowweave-core/src/main/java/io/rowweave/cluster/MasterServer.java",
    "ExecutionGraphBuilder.java": "rowweave-core/src/main/java/io/rowweave/cluster/ExecutionGraphBuilder.java",
    "PhysicalPlan.java": "rowweave-core/src/main/java/io/rowweave/cluster/PhysicalPlan.java",
    "StageAssignment.java": "rowweave-core/src/main/java/io/rowweave/cluster/StageAssignment.java",
    "WorkerServer.java": "rowweave-core/src/main/java/io/rowweave/cluster/WorkerServer.java",
    "WorkerAssignmentLedger.java": "rowweave-core/src/main/java/io/rowweave/cluster/WorkerAssignmentLedger.java",
    "ProtocolNegotiator.java": "rowweave-core/src/main/java/io/rowweave/cluster/rpc/ProtocolNegotiator.java",
    "InProcessCluster.java": "rowweave-core/src/main/java/io/rowweave/cluster/InProcessCluster.java",
}

REQUIRED_TEST_METHODS = {
    "ClusterMainWorkerLocatorTest": [
        "zkCombinedWorkerLocatesRemoteLeaderNotLocal",
        "fixedMasterStartsRetentionButZkMasterWaitsForElection",
    ],
    "StreamClusterConfigTest": ["zkMasterRequiresSharedArtifactDirectory"],
    "DeploymentArtifactBindingTest": [
        "bindingAddsCanonicalRequiredFeatureAndChangesArtifactIdentity",
    ],
    "SlotLedgerTest": [
        "acquireWithinCapacityThenReject", "demandExceedsCapacityFailsFast",
        "bestFitPicksHighestFeasibleCandidate", "concurrentAcquireNoOversell",
    ],
    "RealProcessClusterTest": ["groupBySumRealThreeProcessCluster"],
    "ClusterCancelStageTest": [
        "duplicateAssignIsIdempotentAndConflictingAssignFails",
        "jobFailureMasterCancelsAndReclaims",
    ],
    "WorkerIncarnationFenceTest": [
        "wrongIncarnationAssignAndCancelHaveNoLedgerSideEffects",
        "currentGenerationRequiresExactAttemptFenceBeforeMutation",
    ],
    "MasterLeadershipFencingTest": [
        "registryWorkerFencesPreparedOutputControlsByLeaderTerm",
        "leaderTermLossRollsBackPreparedOutputs",
    ],
    "ProtocolHelloV1Test": [
        "negotiationComputesOnlyTheSafeIntersection",
        "requiredMismatchAndRevisionDriftFailBeforeUse",
    ],
    "ControlSecurityConfigTest": [
        "mtlsProfileRequiresCompleteMaterialDuringConfigLoad",
    ],
    "RpcMtlsAuthorizationTest": [
        "productionMtlsDataPortRejectsPlainFrameBeforeDemuxResourcesExist",
    ],
    "StreamRemoteWorkerWireTest": [
        "taskProgressUsesChecksummedBoundedV3AndCannotMasqueradeAsV2",
    ],
    "StreamRealProcessClusterTest": [
        "independentSourceAndSinkWorkerJvmsExchangeOnlyOverTcp",
    ],
    "ClusterConcurrentJobsTest": [
        "concurrentJobsOnClusterAvailabilityAndThroughput",
    ],
    "ClusterFailFastTest": ["leftSourceThrowsFailsFastNoHang"],
}


def find_test(test_name: str) -> Path:
    roots = ("rowweave-core/src/test/java",)
    matches = [path for root in roots for path in (ROOT / root).rglob(f"{test_name}.java")]
    if len(matches) != 1:
        raise ValueError(f"{test_name} has {len(matches)} test files")
    return matches[0]


def main() -> int:
    try:
        body = DOC.read_text(encoding="utf-8")
        failures = []
        if not re.search(r"^#\s+Cluster topology and scheduling\s*$", body, re.M):
            failures.append("missing title")
        headings = re.findall(r"^##\s+(.+?)\s*$", body, re.M)
        for section in REQUIRED_SECTIONS:
            if section not in headings:
                failures.append(f"missing section: {section}")
        for term in REQUIRED_TERMS:
            if term.lower() not in body.lower():
                failures.append(f"missing cluster term: {term}")
        if "```mermaid" not in body or "flowchart LR" not in body:
            failures.append("missing Mermaid topology diagram")
        if "```mermaid" in body and "Client" not in body.split("```mermaid", 1)[1].split("```", 1)[0]:
            failures.append("topology diagram omits Client")
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
            print(f"CLUSTER ARCHITECTURE PARITY: FAIL ({len(failures)} problem(s))")
            for failure in failures:
                print(f"- {failure}")
            return 1
        count = sum(len(methods) for methods in REQUIRED_TEST_METHODS.values())
        print(
            f"CLUSTER ARCHITECTURE PARITY: PASS ({len(REQUIRED_SECTIONS)} sections, "
            f"{len(SOURCES)} source files, {len(REQUIRED_TEST_METHODS)} test classes, "
            f"{count} cited methods)"
        )
        return 0
    except (OSError, ValueError) as failure:
        print(f"CLUSTER ARCHITECTURE PARITY: FAIL: {failure}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
