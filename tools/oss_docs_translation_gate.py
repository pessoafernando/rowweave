#!/usr/bin/env python3
"""Validate locale manifests and canonical-content bindings for translations."""

from __future__ import annotations

import argparse
import hashlib
import html
import re
import sys
from pathlib import Path

SCRIPT_ROOT = Path(__file__).resolve().parents[1]
LOCALE_PATTERN = re.compile(r"^[a-z]{2,3}(?:-[A-Z][a-zA-Z]{1,3})?$")
HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
ALLOWED_KEYS = {
    "doc-contract",
    "doc-channel",
    "doc-status",
    "doc-source",
    "source-sha256",
    "translation-status",
}


def fail(problems: list[str], message: str) -> None:
    if message not in problems:
        problems.append(message)


def split_front_matter(path: Path) -> tuple[dict[str, str], str]:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        raise ValueError(f"{path.relative_to(SCRIPT_ROOT if path.is_relative_to(SCRIPT_ROOT) else path.anchor)}: missing front matter")
    end = text.find("\n---\n", 4)
    if end < 0:
        raise ValueError(f"{path}: unterminated front matter")
    metadata: dict[str, str] = {}
    for line in text[4:end].splitlines():
        key, separator, raw_value = line.partition(":")
        value = raw_value.strip()
        if not separator or not key.strip() or key != key.strip() or not value:
            raise ValueError(f"{path}: invalid front-matter line: {line}")
        if key not in ALLOWED_KEYS:
            raise ValueError(f"{path}: unknown front-matter key: {key}")
        if key in metadata:
            raise ValueError(f"{path}: duplicate front-matter key: {key}")
        metadata[key] = value
    body = text[end + 5 :]
    return metadata, body


def canonical_body(path: Path) -> str:
    _, body = split_front_matter(path)
    return body


def read_manifest(path: Path, problems: list[str]) -> dict[str, str]:
    entries: dict[str, str] = {}
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0].split("\t") != ["source", "status"]:
        fail(problems, f"{path.relative_to(path.parents[2])}: manifest header must be source<TAB>status")
        return entries
    for number, line in enumerate(lines[1:], 2):
        fields = line.split("\t")
        if len(fields) != 2 or not fields[0] or not fields[1]:
            fail(problems, f"{path}:{number}: manifest row must contain source and status")
            continue
        source, status = fields
        if source in entries:
            fail(problems, f"{path}:{number}: duplicate manifest source: {source}")
        entries[source] = status
    return entries


def forbidden_identity(text: str) -> str | None:
    haystack = text.encode("utf-8").lower()
    forbidden = [
        bytes.fromhex("616c676f78"),
        bytes.fromhex("6461746173657478"),
        bytes.fromhex("726f7778"),
        bytes.fromhex("6b642e626f73"),
        bytes.fromhex("6b696e67646565"),
        bytes.fromhex("e9879ee89ea8"),
    ]
    for value in forbidden:
        if value in haystack:
            return value.hex()
    if re.search(r"(^|[\s`])(/Users/|/home/|/tmp/|C:\\\\)", text):
        return "local machine path"
    return None


def validate_locale(root: Path, locale_dir: Path, problems: list[str]) -> tuple[int, int]:
    locale = locale_dir.name
    manifest_path = locale_dir / "manifest.tsv"
    if not manifest_path.is_file():
        fail(problems, f"{locale_dir.relative_to(root)}: missing manifest.tsv")
        return 0, 0
    manifest = read_manifest(manifest_path, problems)
    translations = {
        path.relative_to(locale_dir).as_posix()
        for path in locale_dir.rglob("*.md")
    }
    checked = 0
    for source_relative, status in sorted(manifest.items()):
        source = root / source_relative
        translation = locale_dir / source_relative
        if not source.is_file():
            fail(problems, f"{locale}/manifest.tsv: manifest source is missing: {source_relative}")
            continue
        if not translation.is_file():
            fail(problems, f"{locale}/manifest.tsv: manifest translation is missing: {source_relative}")
            continue
        translations.discard(source_relative)
        if status != "reviewed":
            fail(problems, f"{locale}/manifest.tsv: {source_relative}: only reviewed translations may ship")
            continue
        try:
            canonical_metadata, canonical_text = split_front_matter(source)
            translation_metadata, _ = split_front_matter(translation)
        except ValueError as error:
            fail(problems, str(error))
            continue
        expected = {
            "doc-source": source_relative,
            "source-sha256": hashlib.sha256(canonical_text.encode("utf-8")).hexdigest(),
            "translation-status": "reviewed",
        }
        for key, value in expected.items():
            if translation_metadata.get(key) != value:
                detail = "source revision changed" if key == "source-sha256" else "required binding is missing or incorrect"
                fail(problems, f"{translation.relative_to(root)}: {detail} for {key}")
        for key in ("doc-contract", "doc-channel", "doc-status"):
            if translation_metadata.get(key, "") != canonical_metadata.get(key, ""):
                fail(problems, f"{translation.relative_to(root)}: contract metadata differs from {source_relative} for {key}")
        identity = forbidden_identity(translation.read_text(encoding="utf-8"))
        if identity:
            fail(problems, f"{translation.relative_to(root)}: forbidden predecessor identity or local path: {identity}")
        checked += 1
    for relative in sorted(translations):
        fail(problems, f"{locale}/{relative}: translation is not listed in the locale manifest")
    return len(manifest), checked


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=SCRIPT_ROOT, help="repository root")
    arguments = parser.parse_args()
    root = arguments.root.resolve()
    problems: list[str] = []
    locale_root = root / "docs/i18n"
    locales = [path for path in locale_root.iterdir() if path.is_dir()] if locale_root.is_dir() else []
    declared = checked = 0
    for locale_dir in sorted(locales):
        if not LOCALE_PATTERN.fullmatch(locale_dir.name):
            fail(problems, f"docs/i18n/{locale_dir.name}: invalid locale directory name")
            continue
        locale_declared, locale_checked = validate_locale(root, locale_dir, problems)
        declared += locale_declared
        checked += locale_checked
    if problems:
        for problem in problems:
            print(f"TRANSLATION PARITY: FAIL {problem}", file=sys.stderr)
        print(f"TRANSLATION PARITY: FAIL ({len(problems)} problems)")
        return 1
    print(f"TRANSLATION PARITY: PASS ({len(locales)} locales, {declared} declared, {checked} synchronized)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
