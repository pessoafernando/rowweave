#!/usr/bin/env python3
"""Architecture reading-map routing gate."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs/product/architecture/index.md"

REQUIRED_ROUTES = {
    "determinism": (
        "Why are results deterministic?",
        "pure-push-model.md#deterministic-semantics",
    ),
    "memory": (
        "Why does memory remain bounded?",
        "resources.md#batch-managed-memory-and-spill",
    ),
    "cluster": (
        "How are roles placed and scheduled?",
        "cluster.md#compilation-capacity-and-placement",
    ),
    "recovery": (
        "How does recovery find durable state?",
        "recovery.md#authority-map",
    ),
    "stream": (
        "How does Stream use one lifecycle?",
        "../reference/stream-behavior.md",
    ),
    "extension": (
        "Where is extension allowed?",
        "../reference/capability-matrix.md#capability-boundaries",
    ),
}

REQUIRED_TERMS = (
    "one submitted graph", "row by row", "bounded", "spill",
    "admitted", "identity-bound", "fenced", "advisory", "stable refusals",
)


def main() -> int:
    try:
        body = DOC.read_text(encoding="utf-8")
        failures = []
        if not re.search(r"^#\s+Architecture reading map\s*$", body, re.M):
            failures.append("missing title")
        if not re.search(r"^##\s+Route by question\s*$", body, re.M):
            failures.append("missing routing table section")
        for label, (question, route) in REQUIRED_ROUTES.items():
            row_pattern = rf"^\|\s*{re.escape(question)}\s*\|\s*\[[^]]+\]\({re.escape(route)}\)\s*\|"
            if not re.search(row_pattern, body, re.M):
                failures.append(f"missing route row: {label}")
        if len(re.findall(r"\| Why are results deterministic\?", body)) != 1:
            failures.append("determinism route is not unique")
        if len(re.findall(r"\| Why does memory remain bounded\?", body)) != 1:
            failures.append("memory route is not unique")
        if len(re.findall(r"\| How are roles placed and scheduled\?", body)) != 1:
            failures.append("cluster route is not unique")
        if len(re.findall(r"\| How does recovery find durable state\?", body)) != 1:
            failures.append("recovery route is not unique")
        if len(re.findall(r"\| How does Stream use one lifecycle\?", body)) != 1:
            failures.append("Stream route is not unique")
        if len(re.findall(r"\| Where is extension allowed\?", body)) != 1:
            failures.append("extension route is not unique")
        for term in REQUIRED_TERMS:
            if term.lower() not in body.lower():
                failures.append(f"missing invariant term: {term}")
        if failures:
            print(f"ARCHITECTURE READING MAP: FAIL ({len(failures)} problem(s))")
            for failure in failures:
                print(f"- {failure}")
            return 1
        print(f"ARCHITECTURE READING MAP: PASS ({len(REQUIRED_ROUTES)} unique routes)")
        return 0
    except (OSError, UnicodeError) as failure:
        print(f"ARCHITECTURE READING MAP: FAIL: {failure}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
