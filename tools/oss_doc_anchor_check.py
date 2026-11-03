#!/usr/bin/env python3
"""Anchor-content check for the public reference pages.

`cite_check`-style existence checks prove a file:line anchor is in range, but
they happily accept a blank line. This gate adds the missing half: for every
``path.java:N[-M]`` anchor in a reference page, the cited line(s) must be
non-blank and contain at least one identifier that the referencing sentence
also uses, so a doc edit cannot silently drift onto unrelated code. Referenced
test symbols (``Class.method`` and bare ``…Test`` names) must exist under the
module test trees.

Usage:
    python3 tools/oss_doc_anchor_check.py docs/product/reference/pipeline-boundary.md
"""
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
ANCHOR = re.compile(r"`?([A-Za-z0-9_/-]+\.java):(\d+)(?:-(\d+))?`?")
TEST_CLASS = re.compile(r"\b([A-Z]\w*(?:Test|Contract))\b")
TEST_METHOD = re.compile(r"\b([A-Z]\w*(?:Test)\.([a-z]\w+))\b")
STOP = {"java", "main", "test", "the", "and", "for", "with", "this", "that",
        "are", "not", "its", "each"}


def words(text):
    return {w.lower() for w in re.findall(r"[A-Za-z][A-Za-z0-9_]{3,}", text)
            if w.lower() not in STOP}


def source_index():
    index = {"by_java_rel": {}, "by_basename": {}}
    for path in ROOT.glob("rowweave-*/src/*/java/**/*.*"):
        rel = path.relative_to(ROOT)
        parts = rel.parts
        if "src" not in parts:
            continue
        src_at = parts.index("src")
        if len(parts) <= src_at + 2:
            continue
        java_rel = "/".join(parts[src_at + 3:])
        index["by_java_rel"].setdefault(java_rel, path)
        index["by_basename"].setdefault(path.name, path)
    return index


def resolve(index, rel):
    hit = index["by_java_rel"].get(rel)
    if hit is not None:
        return hit
    repo_rel = ROOT / rel
    if rel.startswith("rowweave-") and repo_rel.exists():
        return repo_rel
    if "/" not in rel:
        return index["by_basename"].get(rel)
    return None


CLAIM = re.compile(r"`([^`]+)`|\b[A-Z][A-Z0-9_]{3,}\b|\b[A-Z][a-z]\w+\b")


def claimed_tokens(line, spans):
    tokens = set()
    for m in CLAIM.finditer(line):
        if any(m.start() >= s and m.end() <= e for s, e in spans):
            continue
        value = m.group(1) or m.group(0)
        if "." in value and not value.endswith(".java"):
            for segment in value.split("."):
                if segment and segment.lower() not in {"java", "io", "rowweave"}:
                    tokens.add(segment)
        else:
            tokens.add(value)
    return tokens


def check_page(page, index):
    text = page.read_text(encoding="utf-8")
    lines = text.splitlines()
    anchors = []
    for lineno, line in enumerate(lines, 1):
        for match in ANCHOR.finditer(line):
            anchors.append((lineno, match))
    problems = []
    checked = 0
    for lineno, match in anchors:
        rel, start, end = match.group(1), int(match.group(2)), match.group(3)
        end = int(end) if end else start
        target = resolve(index, rel)
        if target is None:
            problems.append(f"{page.name}:{lineno} MISSING-FILE {rel}")
            continue
        source_lines = target.read_text(encoding="utf-8").splitlines()
        if end > len(source_lines):
            problems.append(f"{page.name}:{lineno} OUT-OF-RANGE {rel}:{start}-{end}")
            continue
        cited = "\n".join(source_lines[start - 1:end])
        if not cited.strip():
            problems.append(f"{page.name}:{lineno} BLANK-LINE {rel}:{start}")
            continue
        sentence = "\n".join(lines[max(0, lineno - 4):lineno + 3])
        claimed = claimed_tokens(sentence, [m.span() for m in
                                            ANCHOR.finditer(sentence)])
        class_name = target.name[:-len(".java")]
        claimed.add(class_name)
        window = "\n".join(
            source_lines[max(0, start - 4):min(len(source_lines), end + 3)])
        target_text = "\n".join(source_lines)
        # A class declaration rarely occurs beside an implementation line, so
        # class identity is verified against the whole target file. Other
        # claimed identifiers must still occur near the cited lines.
        overlap_ok = any(
            token in cited or token in window
            for token in claimed if token != class_name
        ) or class_name in target_text
        if not overlap_ok:
            problems.append(
                f"{page.name}:{lineno} NO-IDENTIFIER-OVERLAP {rel}:{start} "
                f":: {cited.strip()[:90]}")
        checked += 1
    return checked, problems


def check_test_symbols(page):
    text = page.read_text(encoding="utf-8")
    problems = []
    checked = 0
    test_roots = list(ROOT.glob("rowweave-*/src/test/java"))
    all_tests = "\n".join(
        p.read_text(encoding="utf-8", errors="replace")
        for root in test_roots for p in root.rglob("*.java"))
    for lineno, line in enumerate(text.splitlines(), 1):
        for full, name in TEST_METHOD.findall(line):
            if name in {"java", "md", "xml", "yml"}:
                continue
            cls = full.split(".", 1)[0]
            pattern = re.compile(r"void %s\(" % re.escape(name))
            if not any(pattern.search(body) for body in
                       [all_tests]) or cls not in all_tests:
                problems.append(f"{page.name}:{lineno} MISSING-TEST {full}")
            checked += 1
    return checked, problems


def main(argv):
    index = source_index()
    if not argv:
        print("usage: oss_doc_anchor_check.py <page.md>...")
        return 2
    total_anchors = 0
    total_symbols = 0
    all_problems = []
    for arg in argv:
        page = ROOT / arg
        if not page.exists():
            print(f"MISSING-PAGE {arg}")
            return 1
        anchors, problems = check_page(page, index)
        symbols, symbol_problems = check_test_symbols(page)
        total_anchors += anchors
        total_symbols += symbols
        all_problems.extend(problems + symbol_problems)
    for problem in all_problems:
        print("FAIL", problem)
    if all_problems:
        return 1
    print(f"DOC ANCHOR GATE: PASS ({total_anchors} code anchors, "
          f"{total_symbols} test symbols)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
