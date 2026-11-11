#!/usr/bin/env python3
"""Build the RowWeave documentation site with the Python standard library."""

from __future__ import annotations

import argparse
import html
import json
import os
import posixpath
import re
import shutil
import sys
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

SCRIPT_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_URL = "https://github.com/pessoafernando/rowweave"
PREVIEW_CONTRACT = "preview-0.2.0"
RELEASE_CONTRACT = "rowweave-1.0.0"
ROOT_PAGES = (
    "README.md",
    "CHANGELOG.md",
    "CODE_OF_CONDUCT.md",
    "CONTRIBUTING.md",
    "ROADMAP.md",
    "SECURITY.md",
    "SUPPORT.md",
    "THIRD-PARTY-NOTICES.md",
)


@dataclass
class Heading:
    level: int
    text: str
    slug: str


@dataclass
class Page:
    source_relative: str
    title: str
    metadata: dict[str, str] = field(default_factory=dict)
    headings: list[Heading] = field(default_factory=list)
    blocks: list[tuple] = field(default_factory=list)


def split_front_matter(text: str) -> tuple[dict[str, str], list[str]]:
    lines = text.splitlines()
    if not lines or lines[0] != "---":
        return {}, lines
    try:
        end = lines[1:].index("---") + 1
    except ValueError as error:
        raise ValueError("unterminated documentation front matter") from error
    metadata: dict[str, str] = {}
    allowed_keys = {"doc-contract", "doc-channel", "doc-status"}
    for line in lines[1:end]:
        if not line.strip():
            raise ValueError("empty documentation front-matter line")
        key, separator, raw_value = line.partition(":")
        value = raw_value.strip()
        if not separator or key != key.strip() or not key or not value:
            raise ValueError(f"invalid documentation front-matter line: {line}")
        if key not in allowed_keys:
            raise ValueError(f"unknown documentation front-matter key: {key}")
        if key in metadata:
            raise ValueError(f"duplicate documentation front-matter key: {key}")
        metadata[key] = value
    return metadata, lines[end + 1 :]


def validate_metadata(source_relative: str, metadata: dict[str, str]) -> None:
    required = ("doc-contract", "doc-channel")
    missing = [key for key in required if key not in metadata]
    if missing:
        raise ValueError(f"{source_relative}: missing contract metadata: {', '.join(missing)}")
    allowed_keys = {"doc-contract", "doc-channel", "doc-status"}
    unknown = sorted(set(metadata) - allowed_keys)
    if unknown:
        raise ValueError(f"{source_relative}: unknown contract metadata: {', '.join(unknown)}")
    contract = metadata["doc-contract"]
    channel = metadata["doc-channel"]
    if contract == PREVIEW_CONTRACT and channel == "default-branch":
        allowed_status = ("", "development-planning")
    elif contract == RELEASE_CONTRACT and channel == "frozen-release":
        allowed_status = ("",)
    else:
        raise ValueError(
            f"{source_relative}: invalid contract pair {contract} / {channel}"
        )
    status = metadata.get("doc-status", "")
    if status not in allowed_status:
        raise ValueError(f"{source_relative}: invalid contract status: {status}")


def contract_badge(metadata: dict[str, str]) -> str:
    if metadata["doc-contract"] == RELEASE_CONTRACT:
        label = "rowweave-1.0.0 release contract · immutable"
        css_class = "release"
    else:
        label = "0.2.0 source preview · default branch"
        css_class = "preview"
    if metadata.get("doc-status") == "development-planning":
        label += " · development planning · not support"
        css_class += " development"
    return css_class, label


def is_canonical(source: Path) -> bool:
    try:
        source.resolve().relative_to(SCRIPT_ROOT)
        return source.resolve() == SCRIPT_ROOT
    except ValueError:
        return False


def source_pages(source: Path) -> list[Path]:
    pages: list[Path] = []
    for relative in ROOT_PAGES:
        candidate = source / relative
        if candidate.exists():
            pages.append(candidate)
    pages.extend(sorted((source / "docs").rglob("*.md")))
    if not pages:
        raise SystemExit("documentation source contains no curated Markdown pages")
    return pages




def output_relative(source_relative: str) -> str:
    if source_relative == "README.md":
        return "index.html"
    if source_relative.lower().endswith(".md"):
        source_relative = source_relative[:-3] + ".html"
    if source_relative.endswith("/index.html"):
        source_relative = source_relative
    return source_relative.replace(os.sep, "/")


def relative_url(from_output: str, to_output: str) -> str:
    source_directory = posixpath.dirname(from_output)
    quoted = urllib.parse.quote(posixpath.relpath(to_output, source_directory or "."))
    return quoted


def normalize_target(target: str) -> str:
    return urllib.parse.unquote(target)


def slug_for(text: str) -> str:
    value = text.strip().lower()
    value = re.sub(r"[^\w\s-]", "", value, flags=re.UNICODE)
    return re.sub(r"\s+", "-", value)


def split_table_row(line: str) -> list[str]:
    value = line.strip()
    if value.startswith("|"):
        value = value[1:]
    if value.endswith("|"):
        value = value[:-1]
    return [part.strip() for part in value.split("|")]


def collect_fenced(lines: list[str], start: int) -> tuple[tuple, int]:
    first = lines[start]
    language = first[3:].strip()
    body: list[str] = []
    index = start + 1
    while index < len(lines) and not lines[index].startswith("```"):
        body.append(lines[index])
        index += 1
    if index >= len(lines):
        raise ValueError("unbalanced fenced code block")
    return ("code", language, "\n".join(body)), index + 1


def is_table_start(lines: list[str], index: int) -> bool:
    return (
        index + 1 < len(lines)
        and lines[index].lstrip().startswith("|")
        and re.fullmatch(r"\s*\|?(?:\s*:?-{3,}:?\s*\|)+\s*:?-{3,}:?\s*\|?\s*", lines[index + 1])
        is not None
    )


def collect_table(lines: list[str], start: int) -> tuple[tuple, int]:
    header = split_table_row(lines[start])
    alignment_text = split_table_row(lines[start + 1])
    alignment = []
    for value in alignment_text:
        left = value.startswith(":")
        right = value.endswith(":")
        alignment.append("center" if left and right else "right" if right else "left" if left else "start")
    body: list[list[str]] = []
    index = start + 2
    while index < len(lines) and lines[index].lstrip().startswith("|"):
        row = split_table_row(lines[index])
        row += [""] * (len(header) - len(row))
        body.append(row[: len(header)])
        index += 1
    return ("table", header, alignment, body), index


def indent_width(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def list_marker(line: str) -> tuple[str, str] | None:
    match = re.match(r"^(\s*)([-*+]|\d+[.)])\s+(.*)$", line)
    if not match:
        return None
    return match.group(2), match.group(3)


def collect_list(lines: list[str], start: int) -> tuple[tuple, int]:
    first = list_marker(lines[start])
    assert first is not None
    ordered = bool(re.fullmatch(r"\d+[.)]", first[0]))
    base_indent = indent_width(lines[start])
    items: list[str] = []
    index = start
    while index < len(lines):
        line = lines[index]
        if not line.strip():
            # A blank line belongs to the list when the next nonblank line remains indented.
            next_index = index + 1
            while next_index < len(lines) and not lines[next_index].strip():
                next_index += 1
            if next_index >= len(lines) or indent_width(lines[next_index]) <= base_indent:
                break
            items.append("")
            index += 1
            continue
        if indent_width(line) < base_indent:
            break
        marker = list_marker(line)
        if marker and indent_width(line) == base_indent:
            items.append(marker[1])
        elif items:
            items[-1] += "\n" + line[min(base_indent + 2, len(line)) :]
        else:
            break
        index += 1
    rendered = [("list_item", item) for item in items]
    return ("list", ordered, rendered), index


def parse_blocks(lines: list[str]) -> tuple[list[tuple], list[Heading]]:
    blocks: list[tuple] = []
    headings: list[Heading] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        stripped = line.strip()
        if not stripped:
            index += 1
            continue
        if line.startswith("```"):
            block, index = collect_fenced(lines, index)
            blocks.append(block)
            continue
        if re.match(r"^#{1,6}\s+\S", line):
            match = re.match(r"^(#{1,6})\s+(.+?)\s*#*$", line)
            assert match
            text = match.group(2).strip()
            slug = slug_for(text)
            suffix = 2
            occupied = {heading.slug for heading in headings}
            while slug in occupied:
                slug = f"{slug_for(text)}-{suffix}"
                suffix += 1
            headings.append(Heading(len(match.group(1)), text, slug))
            blocks.append(("heading", len(match.group(1)), text, slug))
            index += 1
            continue
        if re.fullmatch(r"(?:-{3,}|\*{3,}|_{3,})", stripped):
            blocks.append(("rule",))
            index += 1
            continue
        if is_table_start(lines, index):
            block, index = collect_table(lines, index)
            blocks.append(block)
            continue
        if list_marker(line) is not None:
            block, index = collect_list(lines, index)
            blocks.append(block)
            continue
        if stripped.startswith(">"):
            quoted: list[str] = []
            while index < len(lines) and lines[index].strip().startswith(">"):
                quoted.append(re.sub(r"^\s*>\s?", "", lines[index]))
                index += 1
            children, _ = parse_blocks(quoted)
            blocks.append(("quote", children))
            continue
        paragraph: list[str] = []
        while index < len(lines) and lines[index].strip():
            current = lines[index]
            if (
                current.startswith("```")
                or re.match(r"^#{1,6}\s+\S", current)
                or list_marker(current) is not None
                or current.strip().startswith(">")
                or is_table_start(lines, index)
            ):
                break
            paragraph.append(current.strip())
            index += 1
        if paragraph:
            blocks.append(("paragraph", " ".join(paragraph)))
    return blocks, headings


def inline_markdown(value: str, page: Page, source_root: Path) -> str:
    placeholders: list[str] = []

    def store_code(match: re.Match) -> str:
        token = f"\0{len(placeholders)}\0"
        placeholders.append(f"<code>{html.escape(match.group(1), quote=False)}</code>")
        return token

    value = re.sub(r"`([^`]+)`", store_code, value)
    value = html.escape(value, quote=False)
    image_pattern = re.compile(r"!\[([^\]]*)\]\(([^)\s]+)(?:\s+&quot;([^&]*)&quot;)?\)")
    value = image_pattern.sub(
        lambda match: f'<img src="{site_link(page, match.group(2), source_root)}" '
        f'alt="{match.group(1)}" loading="lazy">',
        value,
    )
    link_pattern = re.compile(r"\[([^\]]+)\]\(([^)\s]+)(?:\s+&quot;([^&]*)&quot;)?\)")
    value = link_pattern.sub(
        lambda match: f'<a href="{site_link(page, match.group(2), source_root)}">{match.group(1)}</a>',
        value,
    )
    value = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", value)
    value = re.sub(r"(?<!\*)\*([^*\n]+)\*(?!\*)", r"<em>\1</em>", value)
    value = re.sub(r"`([^`]+)`", r"<code>\1</code>", value)
    for number, replacement in enumerate(placeholders):
        value = value.replace(f"\0{number}\0", replacement)
    return value


def site_link(page: Page, target: str, source_root: Path) -> str:
    if target.startswith(("http://", "https://", "mailto:", "data:")):
        return html.escape(target, quote=True)
    path, separator, fragment = target.partition("#")
    output_target = output_relative(page.source_relative)
    if path:
        decoded = normalize_target(path)
        if decoded.startswith("/"):
            raise ValueError(f"absolute documentation links are unsupported: {target}")
        resolved = (source_root / page.source_relative).parent.joinpath(decoded).resolve()
        try:
            relative_source = resolved.relative_to(source_root.resolve()).as_posix()
        except ValueError as error:
            raise ValueError(f"documentation link escapes source: {target}") from error
        if resolved.is_dir():
            output_target = output_relative(relative_source)
        elif resolved.suffix == ".md":
            output_target = output_relative(relative_source)
        else:
            source_path = urllib.parse.quote(relative_source)
            return f"{REPOSITORY_URL}/blob/main/{source_path}"
    url = relative_url(output_relative(page.source_relative), output_target)
    if separator or fragment:
        url += "#" + fragment
    return html.escape(url, quote=True)


def render_blocks(blocks: Iterable[tuple], page: Page, source_root: Path) -> list[str]:
    result: list[str] = []
    for block in blocks:
        kind = block[0]
        if kind == "heading":
            _, level, text, slug = block
            result.append(f'<h{level} id="{slug}">{inline_markdown(text, page, source_root)}</h{level}>')
        elif kind == "paragraph":
            result.append(f"<p>{inline_markdown(block[1], page, source_root)}</p>")
        elif kind == "code":
            _, language, body = block
            result.append(
                '<figure class="code-block"><pre tabindex="0"><code class="language-'
                + html.escape(language, quote=True)
                + '">'
                + html.escape(body)
                + "</code></pre></figure>"
            )
        elif kind == "table":
            _, header, alignment, rows = block
            result.append('<div class="table-wrap" tabindex="0" role="region" aria-label="Scrollable table"><table><thead><tr>')
            for cell, align in zip(header, alignment):
                result.append(f'<th scope="col" style="text-align:{align}">{inline_markdown(cell, page, source_root)}</th>')
            result.append("</tr></thead><tbody>")
            for row in rows:
                result.append("<tr>")
                for cell, align in zip(row, alignment):
                    result.append(f'<td style="text-align:{align}">{inline_markdown(cell, page, source_root)}</td>')
                result.append("</tr>")
            result.append("</tbody></table></div>")
        elif kind == "list":
            _, ordered, items = block
            tag = "ol" if ordered else "ul"
            result.append(f"<{tag}>")
            for item in items:
                if item[0] == "list_item":
                    children, _ = parse_blocks(item[1].splitlines())
                    if len(children) == 1 and children[0][0] == "paragraph":
                        rendered = inline_markdown(children[0][1], page, source_root)
                    else:
                        rendered = "".join(render_blocks(children, page, source_root))
                    result.append(f"<li>{rendered}</li>")
            result.append(f"</{tag}>")
        elif kind == "quote":
            result.append("<blockquote>" + "".join(render_blocks(block[1], page, source_root)) + "</blockquote>")
        elif kind == "rule":
            result.append("<hr>")
    return result




def page_title(lines: list[str], fallback: str) -> str:
    for line in lines:
        match = re.match(r"^#\s+(.+?)\s*#*$", line)
        if match:
            return html.unescape(re.sub(r"`", "", match.group(1))).strip()
    return fallback


def navigation_sections(pages: dict[str, Page]) -> list[tuple[str, list[str]]]:
    specifications = (
        ("Getting started", ("README.md", "docs/product/index.md", "docs/product/quickstart/*.md")),
        ("Guides", ("docs/product/guides/*.md",)),
        ("Reference", ("docs/product/reference/*.md",)),
        ("Architecture", ("docs/product/architecture/*.md",)),
        ("Operations", ("docs/product/operations/*.md",)),
        ("Operator runbooks", ("docs/runbooks/*.md",)),
        ("Project", ("CHANGELOG.md", "CODE_OF_CONDUCT.md", "CONTRIBUTING.md", "ROADMAP.md", "SECURITY.md", "SUPPORT.md", "THIRD-PARTY-NOTICES.md")),
    )
    sections: list[tuple[str, list[str]]] = []
    included: set[str] = set()
    for title, patterns in specifications:
        entries: list[str] = []
        for pattern in patterns:
            if any(char in pattern for char in "*?"):
                candidates = [path.relative_to("/") for path in Path("/").glob("/nonexistent")] if False else []
                candidates = [relative for relative in pages if Path(relative).match(pattern)]
            else:
                candidates = [pattern] if pattern in pages else []
            for candidate in sorted(candidates):
                if candidate not in included:
                    entries.append(candidate)
                    included.add(candidate)
        if entries:
            sections.append((title, entries))
    remaining = [
        relative for relative in pages
        if relative not in included
    ]
    if remaining:
        sections.insert(0, ("More", sorted(remaining)))
    return sections


def render_navigation(sections: list[tuple[str, list[str]]], pages: dict[str, Page], page: Page, base: str) -> str:
    output: list[str] = ['<nav id="sections" class="sections" aria-label="Documentation sections">']
    for section, relatives in sections:
        output.append("<details open><summary>" + html.escape(section) + "</summary><ul>")
        for relative in relatives:
            current = " aria-current=\"page\"" if relative == page.source_relative else ""
            title = html.escape(re.sub(r"<[^>]+>", "", pages[relative].title), quote=True)
            href = base + output_relative(relative)
            output.append(f'<li><a href="{href}"{current}>{title}</a></li>')
        output.append("</ul></details>")
    output.append("</nav>")
    return "".join(output)


def search_entry(page: Page, source_root: Path) -> dict[str, object]:
    headings = " ".join(heading.text for heading in page.headings)
    text_parts: list[str] = []

    def walk(blocks: Iterable[tuple]) -> None:
        for block in blocks:
            kind = block[0]
            if kind == "heading":
                text_parts.append(block[2])
            elif kind in ("paragraph", "code"):
                text_parts.append(str(block[-1]))
            elif kind == "table":
                text_parts.extend(block[2])
                for row in block[3]:
                    text_parts.extend(row)
            elif kind == "list":
                for item in block[2]:
                    children, _ = parse_blocks(item[1].splitlines())
                    walk(children)
            elif kind == "quote":
                walk(block[1])

    walk(page.blocks)
    return {
        "url": output_relative(page.source_relative),
        "title": re.sub(r"<[^>]+>", "", page.title),
        "headings": headings,
        "contract": contract_badge(page.metadata)[1],
        "text": " ".join(text_parts)[:24000],
    }


CSS = """:root{color-scheme:light dark;--background:#fff;--surface:#f6f8fa;--text:#182230;--muted:#5a6675;--border:#d8dee7;--brand:#0757ba;--brand-dark:#034183;--code:#f3f5f8;--shadow:0 1px 2px rgba(20,29,45,.08)}@media (prefers-color-scheme:dark){:root{--background:#10151c;--surface:#161e28;--text:#e8edf4;--muted:#a2adba;--border:#303a48;--brand:#78b5ff;--brand-dark:#9ec9ff;--code:#1b242f;--shadow:0 1px 2px rgba(0,0,0,.35)}}*{box-sizing:border-box}@media(prefers-reduced-motion:no-preference){html{scroll-behavior:smooth}}body{margin:0;background:var(--background);color:var(--text);font:16px/1.62 system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;min-height:100vh;display:flex;flex-direction:column}.skip-link{position:absolute;left:-9999px}:focus-visible{outline:3px solid var(--brand);outline-offset:2px}.skip-link:focus{left:1rem;top:1rem;z-index:20;background:var(--background);padding:.6rem 1rem;border:1px solid var(--brand)}.site-header{position:sticky;top:0;z-index:10;display:flex;align-items:center;gap:1rem;flex-wrap:wrap;padding:.8rem max(2rem,calc((100vw - 92rem)/2));border-bottom:1px solid var(--border);background:color-mix(in srgb,var(--background) 92%,transparent);backdrop-filter:blur(8px)}.brand{font-weight:750;font-size:1.12rem;white-space:nowrap;color:var(--text);text-decoration:none}.brand span{color:var(--brand)}.menu-button{display:none;margin-left:auto;border:1px solid var(--border);background:var(--surface);color:var(--text);border-radius:8px;padding:.5rem .65rem}.search{margin-left:auto;position:relative;min-width:min(21rem,38vw)}.search input{width:100%;padding:.55rem .65rem;border:1px solid var(--border);border-radius:8px;background:var(--surface);color:var(--text);font:inherit}.search-results{position:absolute;right:0;top:calc(100% + .4rem);width:min(34rem,90vw);max-height:65vh;overflow:auto;background:var(--background);border:1px solid var(--border);border-radius:10px;box-shadow:0 12px 35px rgba(10,18,30,.16);padding:.35rem;display:none;z-index:20}.search-results.open{display:block}.search-result{display:block;padding:.6rem;border-radius:7px;color:var(--text);text-decoration:none}.search-result:hover,.search-result:focus{background:var(--surface)}.search-result strong{display:block;font-size:.95rem}.search-result small{color:var(--muted)}.search-empty{padding:.7rem;color:var(--muted)}.layout{width:min(92rem,100%);margin:auto;display:grid;grid-template-columns:18rem minmax(0,1fr);gap:3rem;padding:2rem max(1.5rem,calc((100% - 92rem)/2))}.sections{position:sticky;top:5rem;max-height:calc(100vh - 6rem);overflow:auto;font-size:.94rem}.sections summary{cursor:pointer;font-size:.82rem;text-transform:uppercase;letter-spacing:.055em;color:var(--muted);margin:.8rem 0 .3rem}.sections ul{list-style:none;margin:0;padding:0}.sections a{display:block;padding:.32rem .48rem;border-radius:6px;color:var(--text);text-decoration:none}.sections a:hover{background:var(--surface);color:var(--brand)}.sections a[aria-current]{background:var(--surface);color:var(--brand);font-weight:650}.content{max-width:52rem;min-width:0;padding-bottom:4rem}.doc-contract{display:inline-block;margin:.4rem 0 .8rem;padding:.38rem .6rem;border:1px solid var(--border);border-radius:999px;background:var(--surface);color:var(--text);font-size:.84rem;line-height:1.3}.doc-contract strong{font-weight:650}.doc-contract.release{border-color:#2f6f4f}.doc-contract.development{border-style:dashed}.breadcrumbs{color:var(--muted);font-size:.88rem}.breadcrumbs a{color:inherit}.content>:first-child{margin-top:.4rem}h1,h2,h3,h4{line-height:1.22;overflow-wrap:anywhere}h1{font-size:2.15rem;margin:1rem 0 1rem}h2{font-size:1.45rem;margin-top:2.2rem;padding-bottom:.3rem;border-bottom:1px solid var(--border)}h3{font-size:1.18rem;margin-top:1.7rem}a{color:var(--brand)}a:hover{color:var(--brand-dark)}p,li{overflow-wrap:break-word}.code-block{margin:1.2rem 0}.code-block pre{margin:0;padding:1rem;overflow-x:auto;background:var(--code);border:1px solid var(--border);border-radius:9px}.code-block code{font:13.4px/1.55 ui-monospace,SFMono-Regular,Consolas,monospace;white-space:pre}.language-java .keyword,.language-bash .keyword{color:#b3439a;font-weight:650}.language-java .string,.language-bash .string{color:#177245}.language-java .comment,.language-bash .comment{color:#68737f}.language-java .number,.language-bash .number{color:#a15800}code:not([class]){background:var(--code);border:1px solid var(--border);border-radius:5px;padding:.08rem .28rem;font-size:.9em}.table-wrap{overflow-x:auto;margin:1.2rem 0;
border:1px solid var(--border);border-radius:9px}table{width:100%;border-collapse:collapse;font-size:.94rem}th,td{padding:.6rem .7rem;border-bottom:1px solid var(--border);vertical-align:top}thead th{background:var(--surface)}tbody tr:last-child td{border-bottom:0}blockquote{margin:1.2rem 0;padding:.2rem 1.2rem;border-left:4px solid var(--brand);background:var(--surface);border-radius:0 8px 8px 0}hr{border:0;border-top:1px solid var(--border);margin:2rem 0}.site-footer{margin-top:auto;border-top:1px solid var(--border);padding:1.2rem max(1.5rem,calc((100vw - 92rem)/2));color:var(--muted);font-size:.88rem}@media(max-width:54rem){.layout{display:block;padding:1.2rem;gap:0}.menu-button{display:inline-flex}.search{order:3;flex-basis:100%;min-width:0;margin-top:.5rem}.sections{position:fixed;inset:4.9rem 0 auto 0;max-height:70vh;margin:0 1rem;background:var(--background);border:1px solid var(--border);border-radius:10px;padding:.5rem .7rem;box-shadow:var(--shadow);display:none;z-index:15}.sections.open{display:block}.search{min-width:9rem}.site-header{padding:.7rem 1rem}}"""

JS = """(() => {
  'use strict';
  const button = document.querySelector('.menu-button');
  const sections = document.getElementById('sections');
  button?.addEventListener('click', () => {
    const open = sections.classList.toggle('open');
    button.setAttribute('aria-expanded', String(open));
  });
  document.addEventListener('click', (event) => {
    if (sections?.classList.contains('open') && !sections.contains(event.target) && event.target !== button) {
      sections.classList.remove('open');
      button?.setAttribute('aria-expanded', 'false');
    }
  });
  const escapeHtml = (value) => value.replace(/[&<>"']/g, (letter) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[letter]));
  const keywords = ['abstract','assert','boolean','break','byte','case','catch','char','class','const','continue','default','do','double','else','enum','extends','final','finally','float','for','if','implements','import','instanceof','int','interface','long','native','new','package','private','protected','public','return','short','static','strictfp','super','switch','synchronized','this','throw','throws','transient','try','void','volatile','while','true','false','null'];
  const tokenize = (source, language) => {
    const escaped = escapeHtml(source);
    if (!['java','bash','shell','xml','yaml','properties'].includes(language)) return escaped;
    const pattern = new RegExp('(\\/\\*[\\s\\S]*?\\*\\/|\\/\\/[^\\n]*|#[^\\n]*|&quot;.*?&quot;|&#39;.*?&#39;|@[A-Za-z][\\w.]*|\\b\\d[\\w.]*\\b|\\b(?:' + keywords.join('|') + ')\\b)', 'g');
    return escaped.replace(pattern, (match) => {
      if (match.startsWith('/*') || match.startsWith('//') || match.startsWith('#')) return '<span class="comment">' + match + '</span>';
      if (match.startsWith('&quot;') || match.startsWith('&#39;')) return '<span class="string">' + match + '</span>';
      if (/^\\d/.test(match)) return '<span class="number">' + match + '</span>';
      if (keywords.includes(match)) return '<span class="keyword">' + match + '</span>';
      return '<span class="keyword">' + match + '</span>';
    });
  };
  document.querySelectorAll('.language-java,.language-bash,.language-shell,.language-xml,.language-yaml,.language-properties').forEach((element) => {
    const language = [...element.classList].find((name) => name.startsWith('language-'))?.slice(9) || '';
    element.innerHTML = tokenize(element.textContent || '', language);
  });
  const input = document.querySelector('.search input');
  const results = document.querySelector('.search-results');
  const base = document.querySelector('meta[name="doc-base"]')?.content || '.';
  const entries = window.DocumentSearchIndex || [];
  const close = () => results?.classList.remove('open');
  const open = () => results?.classList.add('open');
  document.addEventListener('click', (event) => { if (!results?.contains(event.target) && event.target !== input) close(); });
  input?.addEventListener('input', () => {
    const query = input.value.trim().toLowerCase();
    if (!results) return;
    if (query.length < 2) { close(); results.innerHTML = ''; return; }
    const scored = entries.map((entry) => {
      const title = entry.title.toLowerCase();
      const headings = entry.headings.toLowerCase();
      const text = entry.text.toLowerCase();
      let score = 0;
      if (title.includes(query)) score += 100 - title.indexOf(query) * .2;
      if (headings.includes(query)) score += 45;
      const occurrences = text.split(query).length - 1;
      if (occurrences) score += Math.min(30, occurrences * 3);
      const position = text.indexOf(query);
      return {entry, score, position};
    }).filter((item) => item.score > 0).sort((left, right) => right.score - left.score).slice(0, 10);
    results.innerHTML = scored.length ? scored.map(({entry, position}) => {
      const start = Math.max(0, (position < 0 ? 0 : position) - 55);
      const excerpt = (start ? '…' : '') + entry.text.slice(start, start + 170) + (entry.text.length > start + 170 ? '…' : '');
      return '<a class="search-result" href="' + base + encodeURI(entry.url) + '"><strong>' + escapeHtml(entry.title) + '</strong><small>' + escapeHtml(excerpt) + '</small></a>';
    }).join('') : '<div class="search-empty">No matching page.</div>';
    open();
  });
})();
"""


def asset_url(page: Page, name: str) -> str:
    return relative_url(output_relative(page.source_relative), "assets/" + name)


def render_page(
    page: Page,
    sections: list[tuple[str, list[str]]],
    pages: dict[str, Page],
    source_root: Path,
) -> str:
    base = relative_url(output_relative(page.source_relative), "index.html")
    base = posixpath.dirname(base) + "/" if posixpath.dirname(base) else ""
    title = re.sub(r"<[^>]+>", "", page.title)
    document_title = "RowWeave Docs" if title == "RowWeave" else title
    description = "RowWeave documentation" if title == "RowWeave" else f"{title} — RowWeave documentation"
    page_title_attribute = document_title if document_title == "RowWeave Docs" else f"{document_title} | RowWeave"
    section_name = next((name for name, values in sections if page.source_relative in values), "Documentation")
    badge_class, badge_label = contract_badge(page.metadata)
    contract = (
        f'<p class="doc-contract {badge_class}" role="note" '
        f'aria-label="Documentation contract"><strong>Documentation contract:</strong> '
        f'{html.escape(badge_label)}</p>'
    )
    breadcrumb = f'<nav class="breadcrumbs" aria-label="Breadcrumb"><a href="{base}index.html">Docs</a> / {html.escape(section_name)} / {html.escape(title)}</nav>'
    body = "".join(render_blocks(page.blocks, page, source_root))
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="description" content="{html.escape(description)}">
<meta name="doc-base" content="{base}">
<title>{html.escape(page_title_attribute)}</title>
<link rel="stylesheet" href="{asset_url(page, "site.css")}">
</head>
<body>
<a class="skip-link" href="#main">Skip to content</a>
<header class="site-header">
  <a class="brand" href="{base}index.html">Row<span>Weave</span> Docs</a>
  <button class="menu-button" type="button" aria-expanded="false" aria-controls="sections">Sections</button>
  <div class="search">
    <label class="skip-link" for="doc-search">Search documentation</label>
    <input id="doc-search" type="search" placeholder="Search documentation" autocomplete="off">
    <div class="search-results" role="region" aria-live="polite" aria-label="Search results"></div>
  </div>
</header>
<div class="layout">
{render_navigation(sections, pages, page, base)}
<main class="content" id="main">
{contract}
{breadcrumb}
{body}
</main>
</div>
<footer class="site-footer">Apache-2.0 licensed documentation.</footer>
<script src="{asset_url(page, "search-index.js")}" defer></script>
<script src="{asset_url(page, "site.js")}" defer></script>
</body>
</html>
"""


def validate_build(source_root: Path, pages: dict[str, Page]) -> None:
    problems: list[str] = []
    for relative, page in pages.items():
        source = source_root / relative
        text = source.read_text(encoding="utf-8")
        for match in re.finditer(r"\[[^\]]+\]\(([^)\s]+)(?:\s+\"[^\"]+\")?\)", text):
            target = match.group(1)
            if target.startswith(("http://", "https://", "mailto:")):
                continue
            path, _, fragment = target.partition("#")
            if not path:
                continue
            linked = (source.parent / normalize_target(path)).resolve()
            if not linked.exists():
                problems.append(f"{relative}: missing source link {target}")
                continue
            if fragment and linked.suffix == ".md":
                linked_relative = linked.relative_to(source_root).as_posix()
                slugs = {heading.slug for heading in pages[linked_relative].headings}
                if fragment not in slugs:
                    problems.append(f"{relative}: missing heading {fragment}")
    if problems:
        raise ValueError("\n".join(problems))


def build(source_root: Path, output_root: Path) -> int:
    source_root = source_root.resolve()
    output_root = output_root.resolve()
    if output_root == source_root or output_root in source_root.parents:
        raise ValueError("output directory must be outside the documentation source")
    paths = source_pages(source_root)
    pages: dict[str, Page] = {}
    for path in paths:
        relative = path.relative_to(source_root).as_posix()
        text = path.read_text(encoding="utf-8")
        metadata, lines = split_front_matter(text)
        validate_metadata(relative, metadata)
        blocks, headings = parse_blocks(lines)
        page = Page(relative, page_title(lines, relative), metadata, headings, blocks)
        pages[relative] = page
    validate_build(source_root, pages)
    staging = output_root.parent / f".{output_root.name}.staging"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    sections = navigation_sections(pages)
    (staging / "assets").mkdir()

    for relative, page in pages.items():
        target = staging / output_relative(relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(render_page(page, sections, pages, source_root), encoding="utf-8")
    index = [
        search_entry(page, source_root)
        for _, page in sorted(pages.items())
    ]
    (staging / "assets/search-index.js").write_text(
        "window.DocumentSearchIndex = " + json.dumps(index, ensure_ascii=False) + ";\n",
        encoding="utf-8",
    )
    (staging / "assets/site.css").write_text(CSS, encoding="utf-8")
    (staging / "assets/site.js").write_text(JS, encoding="utf-8")
    if output_root.exists():
        shutil.rmtree(output_root)
    staging.replace(output_root)
    return len(pages)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=SCRIPT_ROOT, help="documentation source root")
    parser.add_argument("--out", type=Path, default=SCRIPT_ROOT / "target/docs-site", help="output directory")
    arguments = parser.parse_args()
    try:
        count = build(arguments.source, arguments.out)
    except (OSError, ValueError) as error:
        print(f"DOCS SITE: FAIL {error}", file=sys.stderr)
        return 1
    print(f"DOCS SITE: PASS ({count} pages)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
