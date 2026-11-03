#!/usr/bin/env python3
"""Structural quality checks for RowWeave documentation."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROBLEM_PREFIX = "DOCS QUALITY"


def fail(message: str) -> None:
    print(f"{PROBLEM_PREFIX}: {message}")


def markdown_files() -> list[Path]:
    files = [ROOT / "README.md"]
    files.extend(sorted((ROOT / "docs").rglob("*.md")))
    return [path.relative_to(ROOT) for path in files]


def heading_slug(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text, flags=re.UNICODE)
    return re.sub(r"\s+", "-", text)


def page_headings(path: Path) -> set[str]:
    headings: set[str] = set()
    inside_fence = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("```"):
            inside_fence = not inside_fence
            continue
        if inside_fence:
            continue
        match = re.match(r"^#{1,6}\s+(.+?)\s*#*$", line)
        if match:
            headings.add(heading_slug(match.group(1)))
    return headings


def check_markdown_structure(pages: list[Path], problems: list[str]) -> tuple[int, int]:
    code_blocks = 0
    for page in pages:
        text = page.read_text(encoding="utf-8")
        lines = text.splitlines()
        first_level = sum(1 for line in lines if re.match(r"^#\s+\S", line))
        if first_level != 1:
            problems.append(f"{page}: expected one first-level title, found {first_level}")
        inside = False
        for number, line in enumerate(lines, 1):
            if not line.startswith("```"):
                continue
            if not inside:
                code_blocks += 1
                tag = line[3:].strip()
                if not tag:
                    problems.append(f"{page}:{number}: opening code fence has no language tag")
            inside = not inside
        if inside:
            problems.append(f"{page}: unbalanced code fence")
    return len(pages), code_blocks


def check_relative_links(pages: list[Path], problems: list[str]) -> int:
    checked = 0
    for page in pages:
        base = (ROOT / page).parent
        text = page.read_text(encoding="utf-8")
        for target in re.findall(r"\[[^\]]+\]\(([^)\s]+)\)", text):
            if target.startswith(("http://", "https://", "mailto:")):
                continue
            checked += 1
            path_part, _, fragment = target.partition("#")
            resolved = (base / path_part).resolve()
            try:
                resolved.relative_to(ROOT)
            except ValueError:
                problems.append(f"{page}: link escapes repository: {target}")
                continue
            if not resolved.exists():
                problems.append(f"{page}: missing link target: {target}")
                continue
            if fragment and resolved.is_file() and resolved.suffix == ".md":
                if fragment not in page_headings(resolved):
                    problems.append(f"{page}: missing heading anchor in {target}")
    return checked


def check_product_orphans(pages: list[Path], problems: list[str]) -> int:
    home = ROOT / "docs/product/index.md"
    home_text = home.read_text(encoding="utf-8")
    linked: set[Path] = set()
    for target in re.findall(r"\[[^\]]+\]\(([^)\s]+)\)", home_text):
        if target.startswith(("http://", "https://", "mailto:")):
            continue
        linked.add(((home.parent / target).resolve()).relative_to(ROOT))
    product_pages = [
        page for page in pages
        if page.parts[:2] == ("docs", "product") and page != home.relative_to(ROOT)
    ]
    for page in product_pages:
        if page not in linked:
            problems.append(f"orphan product page: {page}")
    return len(product_pages)


def check_monitor_contract(problems: list[str]) -> tuple[int, int]:
    handler = ROOT / "rowweave-monitor/src/main/java/io/rowweave/monitor/http/MonitorHttpHandler.java"
    docs_text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in [ROOT / "docs/product/operations/monitoring.md",
                     ROOT / "docs/product/reference/monitor-contract.md"]
    )
    literals = set(re.findall(r'"(/[^"]*)"', handler.read_text(encoding="utf-8")))
    routes = {route for route in literals if route.startswith("/api/") or route in {"/", "/index.html"}}
    routes.add("/api/jobs/{jobId}")
    for route in sorted(routes):
        if route not in docs_text:
            problems.append(f"monitor route missing from documentation: {route}")
    production_java = "\n".join(
        path.read_text(errors="ignore")
        for path in (ROOT / "rowweave-core/src/main/java").rglob("*.java")
    )
    production_java += "\n".join(
        path.read_text(errors="ignore")
        for path in (ROOT / "rowweave-monitor/src/main/java").rglob("*.java")
    )
    string_literals = set(re.findall(r'"([^"\\]+)"', production_java))
    property_pattern = re.compile(
        r"rowweave\.monitor\.[A-Za-z0-9]+(?:\.[A-Za-z0-9]+)*"
    )
    properties = sorted(
        value for value in string_literals if property_pattern.fullmatch(value)
    )
    config_text = (ROOT / "docs/product/reference/configuration.md").read_text(encoding="utf-8")
    for prop in properties:
        if prop not in config_text:
            problems.append(f"monitor property missing from configuration reference: {prop}")
    return len(routes), len(properties)


def check_examples_and_governance(problems: list[str]) -> tuple[int, int]:
    documentation = "\n".join(path.read_text(encoding="utf-8") for path in markdown_files())
    names = sorted(set(re.findall(r"\b([A-Z][A-Za-z0-9]*Example)\b", documentation)))
    for name in names:
        matches = list((ROOT / "rowweave-examples/src/main/java").rglob(f"{name}.java"))
        if not matches:
            problems.append(f"documented example has no production source: {name}")
    governance = [
        "CHANGELOG.md", "CONTRIBUTING.md", "LICENSE", "NOTICE", "README.md",
        "ROADMAP.md", "SECURITY.md", "THIRD-PARTY-NOTICES.md",
    ]
    for name in governance:
        if not (ROOT / name).is_file():
            problems.append(f"governance file missing: {name}")
    return len(names), len(governance)


def check_prohibited_text(pages: list[Path], problems: list[str]) -> int:
    predecessor_names = [
        bytes.fromhex("616c676f78").decode("ascii"),
        "data" + "set" + "x",
        "row" + "x",
        bytes.fromhex("6b696e67646565").decode("ascii"),
        bytes.fromhex("e9879ee89ea8").decode("utf-8"),
        "kd" + "." + "bos",
    ]
    prohibited = re.compile(
        "|".join(re.escape(name) for name in predecessor_names)
        + r"|/Users/|/root/|[A-Za-z]:\\\\",
        re.IGNORECASE,
    )
    checked = 0
    for page in pages:
        checked += 1
        for number, line in enumerate(page.read_text(encoding="utf-8").splitlines(), 1):
            if prohibited.search(line):
                problems.append(f"{page}:{number}: prohibited identity or local path")
    return checked


def main() -> int:
    problems: list[str] = []
    pages = markdown_files()
    page_count, code_block_count = check_markdown_structure(pages, problems)
    link_count = check_relative_links(pages, problems)
    product_page_count = check_product_orphans(pages, problems)
    route_count, property_count = check_monitor_contract(problems)
    example_count, governance_count = check_examples_and_governance(problems)
    scanned_page_count = check_prohibited_text(pages, problems)
    if problems:
        for problem in problems:
            fail(problem)
        print(f"DOCS QUALITY GATE: FAIL ({len(problems)} problems)")
        return 1
    print(
        "DOCS QUALITY GATE: PASS "
        f"({page_count} pages, {code_block_count} code blocks, {link_count} links, "
        f"{product_page_count} product pages, {route_count} monitor routes, "
        f"{property_count} monitor properties, {example_count} documented examples, "
        f"{governance_count} governance files)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
