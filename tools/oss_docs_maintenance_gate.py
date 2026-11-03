#!/usr/bin/env python3
"""Verify documentation maintenance wiring and stale-translation prevention."""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TRANSLATION_GATE = ROOT / "tools/oss_docs_translation_gate.py"


def read(relative: str) -> str:
    path = ROOT / relative
    if not path.is_file():
        raise SystemExit(f"MAINTENANCE WORKFLOW: FAIL missing {relative}")
    return path.read_text(encoding="utf-8")


def require(text: str, expected: str, source: str) -> None:
    if expected not in text:
        raise SystemExit(f"MAINTENANCE WORKFLOW: FAIL {source} lacks {expected!r}")


def check_configuration() -> None:
    ci = read(".github/workflows/ci.yml")
    for expected in (
        "pull_request:",
        "branches: [main]",
        "name: Documentation maintenance",
        "bash tools/oss_docs_quality_gate.sh",
    ):
        require(ci, expected, ".github/workflows/ci.yml")

    suite = read("tools/oss_docs_quality_gate.sh")
    require(suite, "python3 tools/oss_docs_maintenance_gate.py", "tools/oss_docs_quality_gate.sh")
    require(suite, "bash tools/oss_repo_url_check.sh", "tools/oss_docs_quality_gate.sh")

    template = read(".github/pull_request_template.md")
    for expected in (
        "bash tools/oss_docs_quality_gate.sh",
        "python3 tools/oss_docs_translation_gate.py",
        "Public API",
        "Configuration",
        "Runtime behavior",
        "Monitoring",
        "Release or support",
        "Translation",
    ):
        require(template, expected, ".github/pull_request_template.md")

    contributor = read("CONTRIBUTING.md")
    require(
        contributor,
        "[documentation maintenance](docs/product/reference/documentation-maintenance.md)",
        "CONTRIBUTING.md",
    )
    require(contributor, "bash tools/oss_docs_quality_gate.sh", "CONTRIBUTING.md")

    page = read("docs/product/reference/documentation-maintenance.md")
    flat_page = " ".join(page.split())
    for expected in (
        "# Documentation maintenance",
        "## Ownership",
        "| Public API |",
        "| Configuration |",
        "| Runtime behavior |",
        "| Monitoring |",
        "| Release or support |",
        "| Runnable example |",
        "| Translation |",
        "bash tools/oss_docs_quality_gate.sh",
        "python3 tools/oss_docs_translation_gate.py",
        "[capability matrix](capability-matrix.md)",
        "[editorial standards](documentation-style.md)",
        "[release visibility](release-visibility.md)",
        "[documentation audit](documentation-audit.md)",
        "blocks merge",
    ):
        source = flat_page if expected.startswith("[") else page
        require(source, expected, "documentation-maintenance.md")


def stale_translation_fixture() -> None:
    with tempfile.TemporaryDirectory(prefix="rowweave-docs-maintenance-") as directory:
        root = Path(directory)
        source_body = "# Documentation home\n\nUse the capability matrix for support.\n"
        canonical = (
            "---\ndoc-contract: preview-0.2.0\ndoc-channel: default-branch\n---\n"
            + source_body
        )
        translation_binding = (
            "---\ndoc-contract: preview-0.2.0\ndoc-channel: default-branch\n"
            "doc-source: README.md\n"
            "source-sha256: 0000000000000000000000000000000000000000000000000000000000000000\n"
            "translation-status: reviewed\n---\n"
        )
        for relative, value in {
            "README.md": canonical,
            "docs/i18n/zh-Hans/README.md": translation_binding
            + "# \u6587\u6863\u9996\u9875\n\n\u4ee5\u80fd\u529b\u77e9\u9635\u4e3a\u51c6\u3002\n",
            "docs/i18n/zh-Hans/manifest.tsv": "source\tstatus\nREADME.md\treviewed\n",
        }.items():
            target = root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(value, encoding="utf-8")

        # Change the canonical body after binding the translation. A zero digest
        # is intentionally invalid, but the expected stale-source diagnostic must
        # be the gate's binding failure, not a weaker missing-file failure.
        stale_body = "# Documentation home\n\nUse the revised capability matrix for support.\n"
        (root / "README.md").write_text(
            "---\ndoc-contract: preview-0.2.0\ndoc-channel: default-branch\n---\n"
            + stale_body,
            encoding="utf-8",
        )
        process = subprocess.run(
            [sys.executable, str(TRANSLATION_GATE), "--root", str(root)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        if process.returncode == 0 or "source revision changed" not in process.stdout:
            raise SystemExit(
                "TRANSLATION STALE FIXTURE: FAIL\n" + process.stdout
            )
        print("TRANSLATION STALE FIXTURE: PASS (source revision changed)")


def main() -> int:
    check_configuration()
    print("MAINTENANCE WORKFLOW: PASS")
    stale_translation_fixture()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
