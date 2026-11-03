#!/usr/bin/env python3
"""Generate and verify the exhaustive public Java API inventory."""
from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
PAGE = ROOT / "docs/product/reference/api-inventory.md"
FIXTURE = (
    ROOT / "rowweave-core/src/test/resources/api/public-api-vocabulary-0.2.0.json"
)
START = "<!-- GENERATED:PUBLIC-API-INVENTORY:START -->"
END = "<!-- GENERATED:PUBLIC-API-INVENTORY:END -->"
MODULE_ORDER = [
    "rowweave-core",
    "rowweave-pipeline",
    "rowweave-connector-s3",
    "rowweave-connector-postgres-cdc",
    "rowweave-registry-zk",
    "rowweave-monitor",
    "rowweave-examples",
    "rowweave-soak",
]
DECLARATION = re.compile(
    r"^\s*public\s+(?:sealed\s+|non-sealed\s+|final\s+|abstract\s+|"
    r"static\s+|strictfp\s+)*"
    r"(class|interface|enum|record|@interface)\s+"
    r"([A-Za-z_$][A-Za-z0-9_$]*)",
    re.MULTILINE,
)
PACKAGE = re.compile(r"^\s*package\s+([A-Za-z_$][A-Za-z0-9_.]*)\s*;", re.MULTILINE)


@dataclass(frozen=True)
class PublicType:
    module: str
    package: str
    name: str
    kind: str
    path: Path

    @property
    def qualified_name(self) -> str:
        return f"{self.package}.{self.name}"

    @property
    def source_link(self) -> str:
        return f"../../../{self.path.as_posix()}"


def discover_types() -> list[PublicType]:
    result: list[PublicType] = []
    modules = sorted(
        path.name
        for path in ROOT.iterdir()
        if path.is_dir() and (path / "src/main/java").is_dir()
    )
    unexpected = sorted(set(modules) - set(MODULE_ORDER))
    if unexpected:
        raise ValueError(f"module not present in inventory order: {unexpected}")
    for module in MODULE_ORDER:
        seen_in_file: set[Path] = set()
        for path in sorted((ROOT / module / "src/main/java").rglob("*.java")):
            if path in seen_in_file:
                raise ValueError(f"duplicate path traversal: {path}")
            seen_in_file.add(path)
            text = path.read_text(encoding="utf-8", errors="ignore")
            match = DECLARATION.search(text)
            if not match:
                continue
            package_match = PACKAGE.search(text)
            if not package_match:
                raise ValueError(f"public type without package: {path}")
            result.append(
                PublicType(
                    module=module,
                    package=package_match.group(1),
                    name=match.group(2),
                    kind=match.group(1),
                    path=Path(module) / "src/main/java" / path.relative_to(
                        ROOT / module / "src/main/java"
                    ),
                )
            )
    names = [item.qualified_name for item in result]
    duplicates = sorted(name for name in set(names) if names.count(name) > 1)
    if duplicates:
        raise ValueError(f"duplicate public top-level types: {duplicates}")
    return result


def short_name(qualified: str) -> str:
    return qualified.rsplit(".", 1)[-1]


def type_link(types: dict[str, PublicType], qualified: str) -> str:
    item = types.get(qualified)
    if item is None:
        raise ValueError(f"functional inventory references absent type: {qualified}")
    return f"[`{qualified}`]({item.source_link})"


def owner_link(types: dict[str, PublicType], owner: str) -> str:
    return type_link(types, owner)


def relative_type(value: str) -> str:
    if value.startswith("java.") or value == "void":
        return value
    return f"[`{value}`]({document_source(value)})"


def document_source(qualified: str) -> str:
    module = qualified.split(".")[2] if qualified.startswith("io.rowweave.") else None
    if module is None:
        raise ValueError(f"unsupported contract type: {qualified}")
    relative = qualified[len(f"io.rowweave.{module}."):].replace(".", "/")
    return f"../../../rowweave-{module}/src/main/java/io/rowweave/{relative}.java"


def signature_label(entry: dict) -> str:
    params = ", ".join(entry["parameters"])
    returns = entry["returns"]
    return f"`{entry['name']}({params}): {returns}`"


def render_functional_view(types: dict[str, PublicType]) -> list[str]:
    required = contract()["requiredClasses"]
    retained = contract()["retainedClasses"]
    approved = dict.fromkeys(required + retained)
    categories = [
        (
            "Batch DataSet",
            [
                "io.rowweave.RowWeave",
                "io.rowweave.ExecutionEnvironment",
                "io.rowweave.DataSet",
                "io.rowweave.JoinDataSet",
                "io.rowweave.CoGroupDataSet",
                "io.rowweave.Grouper",
            ],
            "Create, compose, validate, and execute a whole Batch graph.",
        ),
        (
            "StreamSession / DataStream",
            [
                "io.rowweave.RowWeave",
                "io.rowweave.StreamSession",
                "io.rowweave.DataStream",
                "io.rowweave.StreamJobClient",
                "io.rowweave.StreamJobHandle",
                "io.rowweave.StreamCommandHandle",
                "io.rowweave.StreamCheckpointHandle",
                "io.rowweave.StreamSavepointHandle",
                "io.rowweave.StreamRestoreRequest",
                "io.rowweave.StreamOfflineRescaleRequest",
            ],
            "Configure a Stream session, compose its closed operator set, and control job lifecycle.",
        ),
        (
            "Functions",
            [
                name
                for name in approved
                if any(
                    part in name
                    for part in (
                        "Function",
                        "Collector",
                        "ResultSchemaAware",
                    )
                )
            ],
            "Typed callbacks supplied by applications and connector authors.",
        ),
        (
            "Sources and sinks",
            [
                name
                for name in approved
                if any(
                    part in short_name(name)
                    for part in ("Source", "Sink", "Batch", "Split", "Executor")
                )
                and not name.startswith("io.rowweave.stream.api.")
            ],
            "Built-in boundaries and provider-facing Batch source/sink contracts.",
        ),
        (
            "Schema and data types",
            [
                "io.rowweave.DataRow",
                "io.rowweave.DataSchema",
                "io.rowweave.Field",
                "io.rowweave.DataType",
                "io.rowweave.PartitionDataRowCursor",
                "io.rowweave.DataRowClassification",
                "io.rowweave.StreamWindowDataRowCursor",
                "io.rowweave.stream.api.StreamDataRowCodec",
                "io.rowweave.stream.api.StreamOutputTag",
            ],
            "Positional cells, explicit schemas, cursors, codecs, and tagged outputs.",
        ),
        (
            "Execution clients and process entry points",
            [
                "io.rowweave.cluster.InProcessCluster",
                "io.rowweave.cluster.ClusterJobClient",
                "io.rowweave.cluster.ClusterMain",
                "io.rowweave.StreamClusterMain",
                "io.rowweave.StreamMasterRuntime",
                "io.rowweave.StreamWorkerRuntime",
            ],
            "Single-JVM and separate-process execution entry points.",
        ),
        (
            "Exceptions",
            [
                "io.rowweave.stream.api.StreamApiException",
                "io.rowweave.stream.api.StreamControlException",
                "io.rowweave.stream.api.StreamExpressionException",
                "io.rowweave.StreamProcessException",
                "io.rowweave.CompatibilityDataLimitException",
                "io.rowweave.stream.spi.conformance.StreamConnectorConformanceException",
            ],
            "Typed failure surfaces; detailed codes belong to the dedicated error reference.",
        ),
        (
            "Connector SPI",
            [
                "io.rowweave.connector.s3.S3ConnectorProvider",
                "io.rowweave.connector.s3.S3ConnectorContract",
                "io.rowweave.connector.s3.S3ConnectorConfiguration",
                "io.rowweave.connector.s3.S3DeploymentPackager",
                "io.rowweave.connector.postgres.PostgresConnectorProvider",
                "io.rowweave.connector.postgres.PostgresConnectorContract",
                "io.rowweave.connector.postgres.PostgresConnectorConfiguration",
                "io.rowweave.connector.postgres.PostgresDeploymentPackager",
            ],
            "Deployable connector boundaries; codecs and clients are source-visible implementation details unless separately documented.",
        ),
        (
            "Pipeline boundary",
            [
                "io.rowweave.pipeline.PipelineCompiler",
                "io.rowweave.pipeline.PipelineDefinition",
                "io.rowweave.pipeline.PipelineExecutionPlan",
                "io.rowweave.pipeline.PipelineValidator",
                "io.rowweave.pipeline.VersionedDatasetHandoffSdk",
                "io.rowweave.pipeline.VersionedDatasetHandoffAuthority",
            ],
            "Compose portable Batch/Stream artifacts without executing them inside Pipeline.",
        ),
        (
            "Monitor public contract",
            [
                "io.rowweave.monitor.http.MonitorHttpServer",
                "io.rowweave.monitor.http.NettyMonitorLauncher",
            ],
            "The optional read-only HTTP monitor; monitoring never gates execution.",
        ),
    ]

    lines = ["## Functional inventory", ""]
    for title, names, boundary in categories:
        unique_names = list(dict.fromkeys(names))
        absent = [name for name in unique_names if name not in types]
        if absent:
            raise ValueError(f"{title} references absent types: {absent}")
        lines.extend([f"### {title}", ""])
        lines.extend(type_link(types, name) for name in sorted(unique_names))
        lines.extend(["", f"Boundary: {boundary}", ""])
    return lines


def contract() -> dict:
    with FIXTURE.open(encoding="utf-8") as stream:
        return json.load(stream)


def render_approved_contract(types: dict[str, PublicType]) -> list[str]:
    data = contract()
    required = list(data["requiredClasses"])
    retained = [name for name in data["retainedClasses"] if name not in required]
    lines = [
        "## Frozen public API vocabulary",
        "",
        f"Version `{data['apiVersion']}` freezes **{len(required) + len(retained)}** unique public classes",
        f"and **{len(data['methods'])}** method signatures. Required symbols are the primary application and provider vocabulary; retained symbols preserve the public composition and function surface.",
        "",
        "### Classes",
        "",
    ]
    for tier, names in (("Required", required), ("Retained", retained)):
        lines.extend([f"#### {tier}", ""])
        lines.extend(
            f"- {type_link(types, name)} — {tier.lower()} in `{data['apiVersion']}`."
            for name in sorted(names)
        )
        lines.append("")
    lines.extend(["### Method signatures", ""])
    for entry in data["methods"]:
        owner = entry["owner"]
        lines.append(
            f"- {owner_link(types, owner)} — {signature_label(entry)}."
        )
    lines.append("")
    return lines


def render_source_inventory(types_arg: Iterable[PublicType]) -> list[str]:
    items = list(types_arg)
    by_module: dict[str, list[PublicType]] = defaultdict(list)
    for item in items:
        by_module[item.module].append(item)
    lines = [
        "## Source-visible public types",
        "",
        "This inventory is exhaustive for top-level Java declarations with the `public` modifier in production source. Nested public types are visible through their owner and are not separate inventory rows. A declaration below is not automatically a supported extension point.",
        "",
        "### Module and package counts",
        "",
        "| Module | Public packages | Public top-level types | Application dependency |",
        "|---|---:|---:|---|",
    ]
    dependency = {
        "rowweave-core": "Required",
        "rowweave-pipeline": "Optional",
        "rowweave-connector-s3": "Optional",
        "rowweave-connector-postgres-cdc": "Optional",
        "rowweave-registry-zk": "Optional registry",
        "rowweave-monitor": "Optional monitor",
        "rowweave-examples": "Not an application dependency",
        "rowweave-soak": "Not an application dependency",
    }
    for module in MODULE_ORDER:
        items = by_module[module]
        packages = {item.package for item in items}
        lines.append(
            f"| `{module}` | {len(packages)} | {len(items)} | {dependency[module]} |"
        )
    total = sum(len(by_module[module]) for module in MODULE_ORDER)
    lines.extend(
        [
            f"| **Total** | **{len({(item.module, item.package) for item in types_arg})}** | **{total}** | — |",
            "",
            "### Package map",
            "",
            "| Package | Public top-level types |",
            "|---|---:|",
        ]
    )
    package_counts: dict[tuple[str, str], int] = defaultdict(int)
    for item in items:
        package_counts[(item.module, item.package)] += 1
    for (module, package), count in sorted(package_counts.items()):
        lines.append(f"| `{module}` · `{package}` | {count} |")
    lines.extend(
        [
            "",
            "### Type inventory",
            "",
            "Each name links to its production source declaration.",
        ]
    )
    for module in MODULE_ORDER:
        lines.extend(["", f"#### `{module}`", ""])
        by_package: dict[str, list[PublicType]] = defaultdict(list)
        for item in by_module[module]:
            by_package[item.package].append(item)
        for package in sorted(by_package):
            lines.extend(["", f"##### `{package}`", ""])
            for item in sorted(by_package[package], key=lambda value: value.name):
                lines.append(
                    f"- [`{item.qualified_name}`]({item.source_link}) — public {item.kind}."
                )
    lines.append("")
    return lines


def expected_page(types: list[PublicType]) -> str:
    type_map = {item.qualified_name: item for item in types}
    lines = []
    lines.extend(render_functional_view(type_map))
    lines.extend(render_approved_contract(type_map))
    lines.extend(render_source_inventory(types))
    lines.extend(
        [
            "## Traceability and qualification",
            "",
            "The inventory generator checks source declarations, functional rows, the frozen class fixture, and method owners on every documentation run. The Java contract test additionally verifies that every approved class exists and every approved signature remains binary-present.",
            "",
            "Run both gates from the repository root:",
            "",
            "```bash",
            "python3 tools/oss_api_inventory_gate.py",
            "mvn -q -pl rowweave-core test -Dtest=PublicApiVocabularyContractTest",
            "```",
        ]
    )
    return (chr(10).join(lines) + chr(10))


def replace_generated(text: str, generated: str) -> str:
    prefix = f"{START}\n"
    suffix = f"{END}\n"
    start = text.find(prefix)
    end = text.find(suffix)
    if start < 0 or end < 0 or end < start:
        raise ValueError(f"inventory markers missing in {PAGE}")
    return text[:start] + prefix + generated + suffix + text[end + len(suffix):]


def check_documented_names(types: list[PublicType], text: str) -> None:
    type_map = {item.qualified_name: item for item in types}
    generated = text[text.find(START) + len(START) + 1:text.find(END)]
    if generated.rstrip(chr(10)) != expected_page(types).rstrip(chr(10)):
        raise ValueError("generated inventory is stale")
    # Every explicit io.rowweave.* type reference in the generated functional and
    # contract sections must resolve to a public production declaration.
    functional = generated.split("## Source-visible public types", 1)[0]
    referenced = set(re.findall(r"io\.rowweave(?:\.[A-Za-z0-9_]+)+", functional))
    package_names = {item.package for item in types}
    absent = sorted(
        name
        for name in referenced
        if name not in type_map and name not in package_names
    )
    if absent:
        raise ValueError(f"documented API names absent from source: {absent}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--write", action="store_true", help="rewrite the marker-managed inventory"
    )
    args = parser.parse_args()
    try:
        types = discover_types()
        generated = expected_page(types)
        current = PAGE.read_text(encoding="utf-8")
        if args.write:
            PAGE.write_text(
                replace_generated(current, generated), encoding="utf-8"
            )
            print(
                f"API INVENTORY: WROTE {len(types)} public top-level types "
                f"across {len({(item.module, item.package) for item in types})} module packages"
            )
        else:
            check_documented_names(types, current)
            data = contract()
            required = list(data["requiredClasses"])
            retained = [
                name for name in data["retainedClasses"] if name not in required
            ]
            print(
                "API INVENTORY: PASS "
                f"({len(types)} source-visible types, "
                f"{len(required) + len(retained)} frozen classes, "
                f"{len(data['methods'])} frozen methods, "
                f"{len({(item.module, item.package) for item in types})} module packages)"
            )
        return 0
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as failure:
        print(f"API INVENTORY: FAIL: {failure}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
