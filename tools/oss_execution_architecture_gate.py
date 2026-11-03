#!/usr/bin/env python3
"""Pure-push execution architecture parity gate."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs/product/architecture/pure-push-model.md"

REQUIRED_SECTIONS = (
    "End-to-end supported shape",
    "Whole-job compilation and stages",
    "Backpressure and failure propagation",
    "Shared subtrees and materialization boundaries",
    "Deterministic semantics",
    "Cancellation and bounded shutdown",
    "Public and internal boundaries",
)

REQUIRED_TERMS = (
    "one admitted DAG", "bounded channels", "pass-through", "breaker",
    "shared subtree", "materialization", "backpressure", "fail fast",
    "deterministic", "cancellation",
)

SOURCES = {
    "PushDagExecutor.java": "rowweave-core/src/main/java/io/rowweave/plan/PushDagExecutor.java",
    "PipelineExecutor.java": "rowweave-core/src/main/java/io/rowweave/sched/PipelineExecutor.java",
    "LocalChannel.java": "rowweave-core/src/main/java/io/rowweave/sched/LocalChannel.java",
    "ChannelPoisonedException.java": "rowweave-core/src/main/java/io/rowweave/sched/ChannelPoisonedException.java",
    "BlockingFanout.java": "rowweave-core/src/main/java/io/rowweave/sched/BlockingFanout.java",
    "MaterializationPlan.java": "rowweave-core/src/main/java/io/rowweave/plan/MaterializationPlan.java",
    "StageExecutionTelemetry.java": "rowweave-core/src/main/java/io/rowweave/plan/StageExecutionTelemetry.java",
}

REQUIRED_TEST_METHODS = {
    "ClusterPushTest": ["executeToOutput", "e2eCrossStageGroupBy", "failFastNoDeadlock", "ironRuleNoListRowDataCarrier"],
    "FanoutMultiOutputEquivalenceTest": ["fiveOutputFanoutCorrectAndComputedOnce"],
    "ComplexDagQuadrantTest": ["dag01_filterSelectGroupOrderTop"],
    "BlockingForkGraphTest": [],
    "MultiSinkExecutionGraphTest": [],
    "SpillWiringTest": ["groupBySpillMatchesHeap", "orderBySpillOrdered", "sharedBudgetMultiBreakerPeakBounded", "concurrentMultiBreakerSpillDeterministic"],
    "SingleMachineJoinShuffleTest": ["innerJoinParallelBitIdenticalToOracle"],
    "BoundedStageCancellationTest": ["uncooperativeRunningStageReturnsBoundedCancellationFailure", "rejectsNegativeCancelDrainBeforeOpeningWorker"],
    "MuxConnectionPoolFanoutTest": [],
}


def find_test(test_name: str) -> Path:
    roots = (
        "rowweave-core/src/test/java", "rowweave-pipeline/src/test/java",
        "rowweave-connector-s3/src/test/java",
        "rowweave-connector-postgres-cdc/src/test/java",
    )
    matches = [path for root in roots for path in (ROOT / root).rglob(f"{test_name}.java")]
    if len(matches) != 1:
        raise ValueError(f"{test_name} has {len(matches)} test files")
    return matches[0]


def main() -> int:
    try:
        body = DOC.read_text(encoding="utf-8")
        failures = []
        if not re.search(r"^#\s+Pure push execution model\s*$", body, re.M):
            failures.append("missing title")
        headings = re.findall(r"^##\s+(.+?)\s*$", body, re.M)
        for section in REQUIRED_SECTIONS:
            if section not in headings:
                failures.append(f"missing section: {section}")
        for term in REQUIRED_TERMS:
            if term.lower() not in body.lower():
                failures.append(f"missing architecture term: {term}")
        if "```mermaid" not in body or "flowchart LR" not in body:
            failures.append("missing Mermaid end-to-end diagram")
        if re.search(r"List<DataRow>|List<Row>", body):
            failures.append("forbidden intermediate list carrier in architecture prose")
        for filename, relative in SOURCES.items():
            path = ROOT / relative
            if not path.is_file():
                failures.append(f"source missing: {relative}")
            elif f"](../../../{relative})" not in body:
                failures.append(f"source link absent: {filename}")
        for test_name, methods in REQUIRED_TEST_METHODS.items():
            path = find_test(test_name)
            text = path.read_text(encoding="utf-8", errors="replace")
            cited = f"`{test_name}`" in body
            for method in methods:
                cited = cited or f"`{test_name}#{method}`" in body
            if not cited:
                failures.append(f"test evidence absent: {test_name}")
            for method in methods:
                if not re.search(rf"\bvoid\s+{re.escape(method)}\s*\(", text):
                    failures.append(f"{test_name}#{method}: method absent")
                elif f"`{test_name}#{method}`" not in body:
                    failures.append(f"test evidence absent: {test_name}#{method}")
        if failures:
            print(f"EXECUTION ARCHITECTURE PARITY: FAIL ({len(failures)} problem(s))")
            for failure in failures:
                print(f"- {failure}")
            return 1
        count = sum(len(methods) for methods in REQUIRED_TEST_METHODS.values())
        print(
            f"EXECUTION ARCHITECTURE PARITY: PASS ({len(REQUIRED_SECTIONS)} sections, "
            f"{len(SOURCES)} source files, {len(REQUIRED_TEST_METHODS)} test classes, "
            f"{count} cited methods)"
        )
        return 0
    except (OSError, ValueError) as failure:
        print(f"EXECUTION ARCHITECTURE PARITY: FAIL: {failure}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
