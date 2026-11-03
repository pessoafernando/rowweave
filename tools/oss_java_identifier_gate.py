#!/usr/bin/env python3
"""Reject forbidden predecessor vocabulary in every Java identifier."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JAVA_TOKEN = re.compile(r"[A-Za-z_$][A-Za-z0-9_$]*")
FORBIDDEN = (
    bytes.fromhex("616c676f78").decode("ascii"),
    bytes.fromhex("64617461736574").decode("ascii") + bytes.fromhex("78").decode("ascii"),
    bytes.fromhex("726f7778").decode("ascii"),
    bytes.fromhex("726f776d657461").decode("ascii"),
)


def java_files() -> list[Path]:
    excluded = {".git", "target"}
    return sorted(
        path
        for path in ROOT.rglob("*.java")
        if not any(part in excluded for part in path.parts)
    )


def main() -> int:
    files = java_files()
    identifiers: set[str] = set()
    failures: list[str] = []
    for path in files:
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as failure:
            failures.append(f"{path.relative_to(ROOT)}: unreadable ({failure})")
            continue
        relative = path.relative_to(ROOT)
        for token in JAVA_TOKEN.findall(text):
            identifiers.add(token)
            lowered = token.lower()
            for forbidden in FORBIDDEN:
                if forbidden in lowered and len(failures) < 40:
                    failures.append(f"{relative}: forbidden Java identifier {token}")
    if len(failures) == 40:
        failures.append("output truncated to 40 violations")
    if failures:
        print("JAVA IDENTITY NAMES: FAIL")
        for failure in failures:
            print(f"- {failure}")
        return 1
    print(f"JAVA IDENTITY NAMES: PASS ({len(files)} files, {len(identifiers)} identifiers)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
