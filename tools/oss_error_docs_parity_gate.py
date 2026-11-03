#!/usr/bin/env python3
"""RowWeave error and refusal reference parity gate.

The gate has two exhaustive invariants:

* every public exception family in a product module appears in the reference;
* every member of each cataloged closed failure/decision enum appears under the
  family's own heading.

String-coded contract families are resolved from their production failure
helpers so literal code constants do not need to be duplicated in this tool.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs/product/reference/errors-and-refusals.md"
MODULES = (
    "rowweave-core", "rowweave-pipeline", "rowweave-connector-s3",
    "rowweave-connector-postgres-cdc", "rowweave-registry-zk",
    "rowweave-monitor",
)

# family -> module, enum type name (or None for string-coded family)
CODE_FAMILIES = {
    "CompatibilityDataLimitException": ("rowweave-core", "Kind"),
    "StreamProcessException": ("rowweave-core", "Code"),
    "QualityGateDecision": ("rowweave-core", "FailureCode"),
    "QualityGateDecisionResult": ("rowweave-core", "Code"),
    "ProcessCapacityException": ("rowweave-core", "Code"),
    "DurableRunSubmitException": ("rowweave-core", "Category"),
    "SpillFailureCode": ("rowweave-core", "SpillFailureCode"),
    "IncrementalAuthorityDecision": ("rowweave-core", "Code"),
    "UpgradeTransition": ("rowweave-core", "Code"),
    "StreamApiException": ("rowweave-core", "Code"),
    "StreamExpressionException": ("rowweave-core", None),
    "StreamControlException": ("rowweave-core", "Code"),
    "StreamContractException": ("rowweave-core", None),
    "StreamChangeException": ("rowweave-core", "Code"),
    "StreamUpsertException": ("rowweave-core", "Code"),
    "StreamClusterProtocolException": ("rowweave-core", "Code"),
    "StreamProductControlException": ("rowweave-core", "Code"),
    "StreamCdcException": ("rowweave-core", "Code"),
    "StreamCheckpointException": ("rowweave-core", "Code"),
    "StreamStateException": ("rowweave-core", "Code"),
    "StreamTemporalJoinException": ("rowweave-core", "Code"),
    "StreamTimeException": ("rowweave-core", "Code"),
    "StreamWindowException": ("rowweave-core", "Code"),
    "StreamConnectorConformanceException": ("rowweave-core", "Code"),
    "PlanArtifactValidationException": ("rowweave-core", None),
    "DeploymentValidationException": ("rowweave-core", None),
    "ProtocolNegotiationException": ("rowweave-core", None),
    "WriterRevisionRejectedException": ("rowweave-core", None),
}

FAMILY_TESTS = {
    "CompatibilityDataLimitException": ["CompatibilityDataLimitsTest"],
    "StreamProcessException": ["StreamKeyedProcessOperatorTaskTest"],
    "QualityGateDecision": ["QualityGateDecisionCodecTest"],
    "QualityGateDecisionResult": ["QualityGateInMemoryAuthorityTest"],
    "ProcessCapacityException": ["ClusterConfigCapacityTest"],
    "DurableRunSubmitException": ["DurableCommitBarrierFailureTest"],
    "SpillFailureCode": ["SpillDiskFailureTest"],
    "IncrementalAuthorityDecision": ["IncrementalInMemoryAuthorityClaimContractTest"],
    "UpgradeTransition": ["UpgradeAuthorityKernelTest"],
    "StreamApiException": ["StreamPublicPlanCompilerContractTest"],
    "StreamExpressionException": ["StreamExpressionContractTest"],
    "StreamControlException": ["StreamPublicJobClientContractTest"],
    "StreamContractException": ["PortableStreamPlanContractTest"],
    "StreamChangeException": ["StreamChangeRecordCodecTest"],
    "StreamUpsertException": ["StreamUpsertRecordCodecTest"],
    "StreamClusterProtocolException": ["StreamClusterProtocolContractTest"],
    "StreamProductControlException": ["StreamProductJobAuthorityContractTest"],
    "StreamCdcException": ["ResidentStreamCdcEnumeratorTest"],
    "StreamCheckpointException": ["StreamCheckpointCoordinatorTest"],
    "StreamStateException": ["ManagedKeyedValueStateTest"],
    "StreamTemporalJoinException": ["BoundedEventTimeTemporalJoinTest"],
    "StreamTimeException": ["StreamTimeServiceTest"],
    "StreamWindowException": ["FinalEventTimeDecimalWindowTest"],
    "StreamConnectorConformanceException": ["StreamBoundedSourceConformanceKitTest"],
    "PlanArtifactValidationException": ["PlanArtifactCodecTest"],
    "DeploymentValidationException": ["DeploymentArchiveScannerTest"],
    "ProtocolNegotiationException": ["ProtocolHelloV1Test"],
    "WriterRevisionRejectedException": ["WriterRevisionGateTest"],
}

CLASS_DECL = re.compile(
    r"\bpublic\s+(?:final\s+|abstract\s+)?class\s+(\w*Exception)\b"
)
ENUM_DECL = re.compile(r"\bpublic\s+enum\s+(\w+)\s*\{")


def production_paths(module: str):
    return sorted((ROOT / module / "src/main/java").rglob("*.java"))


def exception_inventory():
    found: dict[str, Path] = {}
    for module in MODULES:
        for path in production_paths(module):
            match = CLASS_DECL.search(path.read_text(encoding="utf-8", errors="replace"))
            if match:
                found[match.group(1)] = path
    return found


def owner_path(module: str, family: str) -> Path:
    module_path = ROOT / module / "src/main/java"
    if family == "StreamExpressionException":
        return next(module_path.rglob("stream/api/StreamExpression.java"))
    matches = []
    for path in module_path.rglob("*.java"):
        text = path.read_text(encoding="utf-8", errors="replace")
        type_pattern = rf"\bpublic\s+(?:final\s+|abstract\s+)?(?:class|enum)\s+{re.escape(family)}\b"
        enum_pattern = rf"\bpublic\s+enum\s+{re.escape(family)}\b"
        if re.search(type_pattern, text) or re.search(enum_pattern, text):
            matches.append(path)
    if len(matches) != 1:
        raise ValueError(f"{family} owner is not unique: {matches}")
    return matches[0]


def enum_values(path: Path, enum_name: str) -> list[str]:
    text = path.read_text(encoding="utf-8", errors="replace")
    match = re.search(rf"\bpublic\s+enum\s+{re.escape(enum_name)}\s*\{{", text)
    if not match:
        return []
    start = match.end() - 1
    depth = 0
    for index, char in enumerate(text[start:], start):
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                body = text[start:index + 1]
                return re.findall(r"\b([A-Z][A-Z0-9_]+)\s*(?:[,{};()]|$)", body)
    raise ValueError(f"unclosed enum {enum_name} in {path}")


def string_values(path: Path) -> list[str]:
    files = path.rglob("*.java") if path.is_dir() else [path]
    values = []
    for java_file in files:
        text = java_file.read_text(encoding="utf-8", errors="replace")
        values.extend(re.findall(
            r"\b(?:failure|fail|malformed)\s*\(\s*\"([A-Z][A-Z0-9_]+)\"",
            text, re.S,
        ))
    return list(dict.fromkeys(values))


def admission_values(family: str) -> list[str]:
    if family == "PlanArtifactValidationException":
        root = ROOT / "rowweave-core/src/main/java/io/rowweave/artifact"
        values = []
        for path in root.glob("*.java"):
            text = path.read_text(encoding="utf-8", errors="replace")
            values.extend(re.findall(r"\b(?:failure|invalid|integrity|format)\s*\(\s*\"([A-Z][A-Z0-9_]+)\"", text, re.S))
            values.extend(re.findall(r"PlanArtifactValidationException\(\s*\"([A-Z][A-Z0-9_]+)\"", text, re.S))
        return list(dict.fromkeys(values))
    if family == "DeploymentValidationException":
        excluded = {"RWXXDEP1", "RSA", "DSA", "EC", "MTLS", "LOOPBACK", "HTTPS", "PKCS12", "TERMINAL", "ABSENT", "COMMITTED", "STREAM_CDC_ENUMERATOR_V2"}
        values = []
        for path in (ROOT / "rowweave-core/src/main/java/io/rowweave/deployment").glob("*.java"):
            values.extend(v for v in re.findall(r"\"([A-Z][A-Z0-9_]{3,})\"", path.read_text(encoding="utf-8", errors="replace")) if v not in excluded)
        return list(dict.fromkeys(values))
    if family == "ProtocolNegotiationException":
        values = []
        for path in (ROOT / "rowweave-core/src/main/java/io/rowweave/cluster/rpc").glob("*.java"):
            values.extend(re.findall(r"ProtocolNegotiationException\(\s*\"([A-Z][A-Z0-9_]+)\"", path.read_text(encoding="utf-8", errors="replace"), re.S))
        return list(dict.fromkeys(values))
    if family == "WriterRevisionRejectedException":
        values = []
        for path in (ROOT / "rowweave-core/src/main/java/io/rowweave/upgrade").glob("*.java"):
            values.extend(re.findall(r"\brejected\(\s*\"([A-Z][A-Z0-9_]+)\"", path.read_text(encoding="utf-8", errors="replace"), re.S))
        return list(dict.fromkeys(values))
    return []

def code_inventory():
    result = {}
    for family, (module, enum_name) in CODE_FAMILIES.items():
        path = owner_path(module, family)
        if enum_name is None:
            special = admission_values(family)
            values = special or string_values(path if family == "StreamExpressionException" else path.parent)
        else:
            values = enum_values(path, enum_name)
        if not values:
            raise ValueError(f"{family} has no catalogable values")
        result[family] = (path, values)
    return result


def sections(body: str):
    result = {}
    current = None
    buffer: list[str] = []
    for line in body.splitlines():
        heading = re.match(r"^#{2,3}\s+(.+?)\s*$", line)
        if heading:
            if current is not None:
                result[current] = "\n".join(buffer)
            current = heading.group(1)
            buffer = []
        elif current is not None:
            buffer.append(line)
    if current is not None:
        result[current] = "\n".join(buffer)
    return result


def test_file(test_name: str) -> bool:
    return any((ROOT / module / "src/test/java").rglob(f"{test_name}.java")
               for module in MODULES)


def main() -> int:
    try:
        families = exception_inventory()
        codes = code_inventory()
        body = DOC.read_text(encoding="utf-8")
        if not re.search(r"^#\s+Error and refusal reference\s*$", body, re.M):
            raise ValueError("missing Error and refusal reference title")
        parts = sections(body)
        failures = []
        for family, path in sorted(families.items()):
            if f"[`{family}`](../../../{path.relative_to(ROOT)})" not in body:
                failures.append(f"exception family absent: {family}")
        for family, (path, values) in sorted(codes.items()):
            matches = [name for name in parts if f"`{family}`" in name]
            if len(matches) != 1:
                failures.append(f"{family}: expected one code section, found {len(matches)}")
                continue
            section = parts[matches[0]]
            if f"](../../../{path.relative_to(ROOT)})" not in section:
                failures.append(f"{family}: source path absent")
            for test_name in FAMILY_TESTS[family]:
                if f"`{test_name}`" not in section or not test_file(test_name):
                    failures.append(f"{family}: missing or invalid test {test_name}")
            for value in values:
                if not re.search(rf"^\|\s*`{re.escape(value)}`\s*\|", section, re.M):
                    failures.append(f"{family}.{value}: code row absent")
                else:
                    row = next(line for line in section.splitlines()
                               if line.startswith(f"| `{value}` |"))
                    if len(row.strip().strip("|").split("|")) != 3:
                        failures.append(f"{family}.{value}: row lacks meaning/action")
        for route in ("batch-behavior.md", "stream-behavior.md",
                      "pipeline-boundary.md", "format-constants.md",
                      "../operations/troubleshooting.md"):
            if route not in body:
                failures.append(f"route absent: {route}")
        total = sum(len(values) for _, values in codes.values())
        if failures:
            print(f"ERROR REFUSAL PARITY: FAIL ({len(failures)} problem(s))")
            for failure in failures:
                print(f"- {failure}")
            return 1
        print(
            f"ERROR REFUSAL PARITY: PASS ({len(families)} exception families, "
            f"{len(codes)} coded families, {total} stable codes)"
        )
        return 0
    except (OSError, ValueError) as failure:
        print(f"ERROR REFUSAL PARITY: FAIL: {failure}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
