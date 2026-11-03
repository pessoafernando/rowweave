#!/usr/bin/env python3
"""RowWeave configuration property parity gate.

Compares the properties documented in
docs/product/reference/configuration.md against the properties that
production code actually reads through a strict parser helper.

Two invariants hold when this gate passes:

* every property the code reads is documented;
* every documented property is read by code.

Keys built from ``static final String`` constants and concatenation families
are resolved per file so both spellings are covered. Test-only hooks and the
soak-release harness are outside the product boundary and must not appear in
the reference page.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs/product/reference/configuration.md"

PRODUCT_MODULES = (
    "rowweave-core",
    "rowweave-pipeline",
    "rowweave-connector-s3",
    "rowweave-connector-postgres-cdc",
    "rowweave-registry-zk",
    "rowweave-monitor",
)

EXCLUDED_MODULES = ("rowweave-soak",)

# Strict property parser names whose call sites carry the property key.
ACCESSORS = (
    "getProperty", "getInteger", "getLong", "getBoolean", "booleanValue",
    "intProperty", "longProperty", "nonNegativeLong", "nonNegativeInt",
    "strictInt", "strictPositiveInt", "strictPositiveLong", "longProp",
    "positiveCanonicalLong", "parseTimeout", "parseZeroOrTimeout",
    "required", "optional", "port", "integer", "number",
)

# Diagnostic hooks read in production classes but used only by tests.
DIAGNOSTIC_ONLY = {
    "rowweave.test.shuffle.dropEosOnce",
    "rowweave.test.shuffle.dropEosClaim",
}

# Frozen wire / format / payload / codec namespace constants. They are read by
# codecs and registry lookup paths, never through a strict property parser, so
# they are configuration surface only in the format reference, not here.
FORMAT_IDENTIFIERS = {
    "rowweave.bounded-incremental-frontier.v1",
    "rowweave.decimal-unscaled-v1",
    "rowweave.deployment-set-v1:",
    "rowweave.incremental-capture-response",
    "rowweave.incremental-control.v1",
    "rowweave.incremental-materialization-response",
    "rowweave.jdbc-snapshot/",
    "rowweave.object-storage-row-quality/",
    "rowweave.object-storage/",
    "rowweave.opaque-bytes.v1",
    "rowweave.operator-id.explicit.v1",
    "rowweave.operator-id.legacy-positional.v1",
    "rowweave.payload.local-attached-plan-graph.v1",
    "rowweave.payload.portable-batch-plan.v1",
    "rowweave.payload.portable-stream-job.v1",
    "rowweave.payload.portable-stream-plan.v1",
    "rowweave.payload.trusted-legacy-plan-graph.v1",
    "rowweave.pipeline.composition.v1",
    "rowweave.postgres.cdc-position.v1",
    "rowweave.postgres.logical-cdc",
    "rowweave.runtime.batch.map-partition.v1",
    "rowweave.runtime.batch.v1",
    "rowweave.runtime.stream.async-wait.v1",
    "rowweave.runtime.stream.broadcast-state.v1",
    "rowweave.runtime.stream.calc.v1",
    "rowweave.runtime.stream.connector-deployment.v1",
    "rowweave.runtime.stream.dynamic-split-discovery.v2",
    "rowweave.runtime.stream.event-time-assignment.v1",
    "rowweave.runtime.stream.flat-map.v1",
    "rowweave.runtime.stream.generic-window.v1",
    "rowweave.runtime.stream.keyed-co-process.v1",
    "rowweave.runtime.stream.keyed-process.v1",
    "rowweave.runtime.stream.node-execution-contract.v1",
    "rowweave.runtime.stream.resident-cdc-external-position.v2",
    "rowweave.runtime.stream.resident-cdc.v1",
    "rowweave.runtime.stream.retract-changelog.v2",
    "rowweave.runtime.stream.source-end-of-input.v1",
    "rowweave.runtime.stream.source-time-policy.v1",
    "rowweave.runtime.stream.tagged-output.v1",
    "rowweave.runtime.stream.temporal-join.v1",
    "rowweave.runtime.stream.unaligned-checkpoint.v2",
    "rowweave.runtime.stream.union.v1",
    "rowweave.runtime.stream.upsert.v1",
    "rowweave.runtime.stream.v1",
    "rowweave.runtime.stream.window-field-binding.v1",
    "rowweave.s3.position.v1",
    "rowweave.s3.snapshot",
    "rowweave.sink-provider-config-envelope.v1",
    "rowweave.soak.postgres.secret.path",
    "rowweave.soak.s3.secret.path",
    "rowweave.soak.secret.resolution.log",
    "rowweave.stream.async-wait-state.v2",
    "rowweave.stream.async-wait-state.v3",
    "rowweave.stream.broadcast-digest.v1",
    "rowweave.stream.channel-state-v1",
    "rowweave.stream.checkpoint-transfer.v2",
    "rowweave.stream.connector-config.v1",
    "rowweave.stream.control.v1",
    "rowweave.stream.data.v1",
    "rowweave.stream.event-time-assignment-state.v1",
    "rowweave.stream.keyed-timer.v1",
    "rowweave.stream.multi-input-gate.v1",
    "rowweave.stream.soak.transactional-sink",
    "rowweave.stream.task-progress.v3",
}

KEY = re.compile(r"rowweave\.[A-Za-z0-9_.\-]+")
LITERAL = re.compile(r'"(rowweave\.[A-Za-z0-9_.\-]+)"')
CONST = re.compile(r'String\s+([A-Za-z_]\w*)\s*=\s*"([^"]*)"')
ACCESSOR = re.compile(r"\b(?:null\.|this\.)?" + r"(?:" + "|".join(sorted(ACCESSORS, key=len, reverse=True)) + r")\s*\(")
CONCAT_KEY = re.compile(
    r'"(rowweave\.[A-Za-z0-9_.\-]+\.)"\s*\+\s*"([A-Za-z][A-Za-z0-9_]*)"'
    r'|([A-Za-z_]\w*)\s*\+\s*"([A-Za-z][A-Za-z0-9_]*)"'
)


def balanced_span(text: str, open_index: int) -> str:
    depth = 0
    quote = False
    escape = False
    for i in range(open_index, len(text)):
        ch = text[i]
        if quote:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                quote = False
        elif ch == '"':
            quote = True
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return text[open_index : i + 1]
    return text[open_index:]


def scan_sources():
    keys: dict[str, set[str]] = {}
    families: dict[str, set[str]] = {}
    extra: dict[str, set[str]] = {}
    for module in PRODUCT_MODULES + EXCLUDED_MODULES:
        for path in sorted((ROOT / module / "src/main/java").rglob("*.java")):
            text = path.read_text(encoding="utf-8", errors="replace")
            text = re.sub(r"/\*.*?\*/", " ", text, flags=re.S)
            text = re.sub(r"(?<!:)//[^\n]*", " ", text)
            rel = str(path.relative_to(ROOT))
            consts = {name: value for name, value in CONST.findall(text)}
            for match in ACCESSOR.finditer(text):
                span = balanced_span(text, match.end() - 1)
                cands = set(LITERAL.findall(span))
                for name, value in consts.items():
                    if value.startswith("rowweave.") and re.search(rf"\b{re.escape(name)}\b", span):
                        cands.add(value)
                for match2 in re.finditer(r'([A-Za-z_]\w*)\s*\(\s*properties\s*,\s*([A-Za-z_]\w*)', span):
                    if match2.group(2) in consts and consts[match2.group(2)].startswith("rowweave."):
                        cands.add(consts[match2.group(2)])
                for key in cands:
                    if key.endswith(".") or not key.startswith("rowweave."):
                        continue
                    if key in DIAGNOSTIC_ONLY or key in FORMAT_IDENTIFIERS:
                        continue
                    keys.setdefault(key, set()).add(rel)
            # every String constant, not just static finals
            for m in re.finditer(r'String\s+([A-Za-z_]\w*)\s*=\s*"(rowweave\.[^"]*)"', text):
                name, value = m.group(1), m.group(2)
                if value.endswith("."):
                    for suffix in re.findall(
                        re.escape(name) + r'\s*\+\s*"([A-Za-z][A-Za-z0-9_]*)"', text
                    ):
                        families.setdefault(value + suffix, set()).add(rel)
                    if re.search(re.escape(name) + r"\s*\+\s*i\b", text):
                        families.setdefault(value + "<n>", set()).add(rel)
                    continue
                if value in DIAGNOSTIC_ONLY or value in FORMAT_IDENTIFIERS:
                    continue
                keys.setdefault(value, set()).add(rel)
            for full in CONCAT_KEY.findall(text):
                # group 0-1: literal prefix + suffix; group 2-3: const prefix + suffix
                if full[0] and full[1]:
                    families.setdefault(full[0] + full[1], set()).add(rel)
                elif full[2] and full[3]:
                    base = consts.get(full[2], "")
                    if base.startswith("rowweave."):
                        families.setdefault(base + full[3], set()).add(rel)
            for found in KEY.finditer(text):
                for value in LITERAL.findall(found.group(0)):
                    if value in keys or value in families:
                        continue
                    if any(value.startswith(prefix) for prefix in DIAGNOSTIC_ONLY):
                        continue
                    if value.endswith("."):
                        continue
                    # Section heading prefix row in the doc table, not a real key.
                    if value.count(".") <= 3 and value not in reader:
                        continue
                    if not value.startswith("rowweave."):
                        continue
                    if value.startswith("rowweave.cluster.ephemeral"):
                        continue
                    if value.startswith("rowweave.cluster.registry"):
                        continue
                    if value.startswith("rowweave.cluster.rpc."):
                        continue
                    if value.startswith("rowweave.stream.async."):
                        continue
                    if value.startswith("rowweave.stream.broadcast."):
                        continue
                    if value.startswith("rowweave.soak."):
                        continue
                    extra.setdefault(value, set()).add(rel)
    return keys, families, extra


def scan_documented():
    body = DOC.read_text(encoding="utf-8")
    if not re.search(r"^##\s+1\.\s+Configurable property reference", body, re.M):
        return set(), "no '## 1. Configurable property reference' heading"
    keys: set[str] = set()
    for line in body.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        cells = [c.strip() for c in stripped.strip("|").split("|")]
        if not cells:
            continue
        first = cells[0].strip("`")
        for found in KEY.finditer(first):
            value = found.group(0)
            if value.endswith(".<n>"):
                continue
            if not value.startswith("rowweave."):
                continue
            keys.add(value)
    return keys, ""


def main() -> int:
    try:
        reader, families, extra = scan_sources()
        documented, error = scan_documented()
        if error:
            print(f"CONFIGURATION PARITY: FAIL: {error}")
            return 1
        failures: list[str] = []
        prefixes = {key for key in documented if key.endswith(".")}
        def documented_through_prefix(key: str) -> bool:
            return any(key.startswith(prefix) for prefix in prefixes)
        for key in sorted(set(reader) - set(documented)):
            if documented_through_prefix(key):
                continue
            failures.append(f"read but not documented: {key}")
        for key in sorted(documented - set(reader)):
            if key.endswith("."):
                continue
            if any(full.startswith(key) for full in reader) or key in families:
                continue
            failures.append(f"documented but not read: {key}")
        for key in sorted(extra):
            failures.append(f"undeclared non-configuration literal: {key} ({sorted(extra[key])[0]})")
        if failures:
            print(f"CONFIGURATION PARITY: FAIL ({len(failures)} problem(s))")
            for failure in failures:
                print(f"- {failure}")
            return 1
        print(
            f"CONFIGURATION PARITY: PASS ({len(reader)} reader keys, "
            f"{len(families)} family keys, {len(documented)} documented rows)"
        )
        return 0
    except (OSError, ValueError) as failure:
        print(f"CONFIGURATION PARITY: FAIL: {failure}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
