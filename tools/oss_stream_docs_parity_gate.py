#!/usr/bin/env python3
"""Check Stream lifecycle contracts against the Stream behavior reference."""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REFERENCE = ROOT / "docs/product/reference/stream-behavior.md"
EXPECTED_ROWS = (
    "Session and job lifecycle",
    "Closed operator set",
    "Changelog contracts",
    "Event time and watermarks",
    "Keyed state",
    "Broadcast state",
    "Timers",
    "Windows",
    "Temporal join",
    "Async lookup",
    "Sources and dynamic splits",
    "Checkpoints",
    "Savepoints and recovery",
    "Restart and rescale",
    "Deployment modes",
    "Stable refusals",
)
REQUIRED_SECTIONS = tuple(f"## {row}" for row in EXPECTED_ROWS)


def contract_rows(text: str) -> list[tuple[str, str, str]]:
    lines = text.splitlines()
    try:
        start = lines.index("## Contract index")
    except ValueError as failure:
        raise ValueError("Contract index is missing") from failure
    rows: list[list[str]] = []
    for line in lines[start + 1:]:
        if line.startswith("## "):
            break
        if not line.startswith("|"):
            if rows:
                break
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) == 3 and set(cells[0]) <= {"-", ":"}:
            continue
        rows.append(cells)
    header = next((row for row in rows if row[:1] == ["Lifecycle contract"]), None)
    if header is None:
        raise ValueError("Contract index has no header")
    evidence = [row for row in rows if row is not header]
    if any(len(row) != 3 for row in evidence):
        raise ValueError("every Contract index row must have three columns")
    return [(row[0], row[1], row[2]) for row in evidence]


def find_java_file(class_name: str) -> Path:
    matches: list[Path] = []
    for directory in (
        ROOT / "rowweave-core/src/test/java",
        ROOT / "rowweave-examples/src/test/java",
        ROOT / "rowweave-core/src/main/java",
        ROOT / "rowweave-examples/src/main/java",
    ):
        matches.extend(directory.rglob(f"{class_name}.java"))
    unique = sorted(set(matches))
    if not unique:
        raise ValueError(f"referenced class not found: {class_name}")
    if len(unique) > 1:
        raise ValueError(f"ambiguous referenced class: {class_name}: {unique}")
    return unique[0]


def verify_reference(reference: str) -> tuple[str, str | None]:
    match = re.fullmatch(
        r"([A-Za-z_][A-Za-z0-9_]*)(?:#([A-Za-z_][A-Za-z0-9_]*))?", reference
    )
    if not match:
        raise ValueError(f"invalid demonstration reference: {reference}")
    class_name, method_name = match.groups()
    path = find_java_file(class_name)
    if method_name:
        source = path.read_text(encoding="utf-8")
        pattern = re.compile(
            r"(?:@Test\s+)?(?:public\s+|protected\s+|private\s+)?"
            r"void\s+" + re.escape(method_name) + r"\s*\("
        )
        if not pattern.search(source):
            raise ValueError(f"method not found: {class_name}#{method_name}")
    return class_name, method_name


def main() -> int:
    try:
        text = REFERENCE.read_text(encoding="utf-8")
        rows = contract_rows(text)
        names = [row[0] for row in rows]
        if names != list(EXPECTED_ROWS):
            raise ValueError(f"contract parity mismatch: {names}")
        for section in REQUIRED_SECTIONS:
            if section not in text:
                raise ValueError(f"missing reference section: {section}")
        checked: set[str] = set()
        demonstrations = 0
        for name, link, demonstrations_cell in rows:
            if not link.startswith("[") or "](#" not in link:
                raise ValueError(f"row lacks a section link: {name}")
            normalized = demonstrations_cell.replace("`", "")
            references = [item.strip() for item in normalized.split(";") if item.strip()]
            if not references:
                raise ValueError(f"row lacks demonstrations: {name}")
            for item in references:
                class_name, _ = verify_reference(item)
                checked.add(class_name)
                demonstrations += 1
        print(
            "STREAM DOCS PARITY: PASS "
            f"({len(rows)} lifecycle rows, {len(REQUIRED_SECTIONS)} sections, "
            f"{demonstrations} test references, {len(checked)} test classes)"
        )
        return 0
    except (OSError, ValueError) as failure:
        print(f"STREAM DOCS PARITY: FAIL: {failure}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
