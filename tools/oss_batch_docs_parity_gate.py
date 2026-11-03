#!/usr/bin/env python3
"""Check Batch capability rows against the Batch behavior reference and tests."""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CAPABILITY = ROOT / "docs/product/reference/capability-matrix.md"
REFERENCE = ROOT / "docs/product/reference/batch-behavior.md"
EXTRA_ROWS = (
    "Batch deployment modes",
    "Unbounded graph refusal",
)
REQUIRED_SECTIONS = (
    "## Operator semantics",
    "## Null and decimal policy",
    "## Aggregation",
    "## Joins and co-groups",
    "## Ordering, TopN, and distinct",
    "## Whole-job execution",
    "## Schema conversion",
    "## Row validation",
    "## Breakers and spill",
    "## Pipeline composition",
    "## Run-mode parity",
    "## Accepted and refused shapes",
)
EVIDENCE_HEADING = "| Capability matrix row | Reference | Tested demonstration |"


def table_rows(text: str, heading: str) -> list[list[str]]:
    lines = text.splitlines()
    try:
        start = lines.index(heading)
    except ValueError as failure:
        raise ValueError(f"missing heading: {heading}") from failure
    rows: list[list[str]] = []
    for line in lines[start + 1:]:
        if line.startswith("## "):
            break
        if not line.startswith("|"):
            if rows:
                break
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) in {2, 3} and set(cells[0]) <= {"-", ":"}:
            continue
        rows.append(cells)
    return rows


def capability_rows(text: str) -> list[str]:
    rows = table_rows(text, "## Batch and pipeline capability map")
    if rows and rows[0] == ["Need", "Supported contract"]:
        rows = rows[1:]
    if not rows:
        raise ValueError("Batch capability map has no rows")
    names = [row[0] for row in rows]
    expected = [
        "Map, filter, projection, calculation",
        "Grouping and aggregation",
        "Join",
        "Sort, TopN, distinct",
        "Multiple inputs/outputs",
        "Schema conversion",
        "Validation",
        "Pipeline composition",
    ]
    if names != expected:
        raise ValueError(f"unexpected Batch capability rows: {names}")
    return names + list(EXTRA_ROWS)


def evidence_rows(text: str) -> list[tuple[str, str, str]]:
    rows = table_rows(text, "## Contract index")
    heading = next((row for row in rows if row[0] == "Capability matrix row"), None)
    if heading is None:
        raise ValueError("Contract index has no header")
    evidence = [row for row in rows if row is not heading]
    if any(len(row) != 3 for row in evidence):
        raise ValueError("every Contract index row must have three columns")
    return [(row[0], row[1], row[2]) for row in evidence]


def find_java_file(class_name: str) -> Path:
    matches = list((ROOT / "rowweave-core/src/test/java").rglob(f"{class_name}.java"))
    matches += list((ROOT / "rowweave-examples/src/test/java").rglob(f"{class_name}.java"))
    matches += list((ROOT / "rowweave-examples/src/main/java").rglob(f"{class_name}.java"))
    unique = sorted(set(matches))
    if not unique:
        raise ValueError(f"test class not found: {class_name}")
    if len(unique) > 1:
        raise ValueError(f"ambiguous test class: {class_name}: {unique}")
    return unique[0]


def verify_test_reference(reference: str) -> tuple[str, str | None]:
    match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)(?:#([A-Za-z_][A-Za-z0-9_]*))?", reference)
    if not match:
        raise ValueError(f"invalid test reference: {reference}")
    class_name, method_name = match.groups()
    path = find_java_file(class_name)
    if method_name:
        source = path.read_text(encoding="utf-8")
        pattern = re.compile(
            r"(?:@Test\s+)?(?:public\s+|protected\s+|private\s+)?"
            r"void\s+" + re.escape(method_name) + r"\s*\("
        )
        if not pattern.search(source):
            raise ValueError(f"test method not found: {class_name}#{method_name}")
    return class_name, method_name


def main() -> int:
    try:
        capability_text = CAPABILITY.read_text(encoding="utf-8")
        reference_text = REFERENCE.read_text(encoding="utf-8")
        matrix = capability_rows(capability_text)
        evidence = evidence_rows(reference_text)
        documented = [row[0] for row in evidence]
        missing = [row for row in matrix if row not in documented]
        unknown = [row for row in documented if row not in matrix]
        if missing or unknown:
            raise ValueError(f"matrix parity mismatch: missing={missing}, unknown={unknown}")
        for section in REQUIRED_SECTIONS:
            if section not in reference_text:
                raise ValueError(f"missing reference section: {section}")
        checked: set[str] = set()
        demonstrations = 0
        for row, link, demo_cell in evidence:
            if not link.startswith("[") or "](" not in link:
                raise ValueError(f"row lacks a reference link: {row}")
            normalized_cell = demo_cell.replace("`", "")
            references = [item.strip() for item in normalized_cell.split(";") if item.strip()]
            if not references:
                raise ValueError(f"row lacks tested demonstration: {row}")
            for item in references:
                class_name, method_name = verify_test_reference(item)
                checked.add(class_name)
                demonstrations += 1
        print(
            "BATCH DOCS PARITY: PASS "
            f"({len(matrix)} capability rows, {len(REQUIRED_SECTIONS)} sections, "
            f"{demonstrations} test references, {len(checked)} test classes)"
        )
        return 0
    except (OSError, ValueError) as failure:
        print(f"BATCH DOCS PARITY: FAIL: {failure}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
