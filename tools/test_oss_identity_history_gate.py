#!/usr/bin/env python3
"""Exercise full-history validation and shallow-clone rejection in real Git repositories."""
import pathlib
import shutil
import subprocess
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]


def run(*args, cwd):
    return subprocess.run(args, cwd=cwd, text=True, capture_output=True, check=True)


class IdentityHistoryGateTest(unittest.TestCase):
    def test_complete_history_detects_deleted_content_and_shallow_clone_fails(self):
        with tempfile.TemporaryDirectory(prefix="rowweave-history-") as temporary:
            root = pathlib.Path(temporary)
            repo = root / "repository"
            repo.mkdir()
            run("git", "init", "-q", cwd=repo)
            run("git", "config", "user.name", "Fernando Pessoa", cwd=repo)
            run("git", "config", "user.email",
                "326308057+pessoafernando@users.noreply.github.com", cwd=repo)
            tools = repo / "tools"
            tools.mkdir()
            shutil.copy2(ROOT / "tools/oss_identity_residue_gate.sh", tools)
            doc = repo / "docs/product/reference/format-constants.md"
            doc.parent.mkdir(parents=True)
            doc.write_text("# Format constants\n", encoding="utf-8")
            (repo / "README.md").write_text("[Formats](docs/product/reference/format-constants.md)\n")
            source = repo / "rowweave-core/src/main/java/io/rowweave"
            (source / "ops").mkdir(parents=True)
            for name in ("QualityReasonKey", "ObjectStorageQualityRecordDecoder",
                         "QualityPreparedEvidenceReader", "ops/ObjectStorageQualityRoute"):
                (source / (name + ".java")).write_text("// Placeholder.\n")
            files = repo / "fixtures"
            files.mkdir()
            for i in range(1000):
                (files / str(i)).write_text("fixture\n")
            run("git", "add", ".", cwd=repo)
            run("git", "commit", "-qm", "Initial source", cwd=repo)
            clean = subprocess.run(["bash", "tools/oss_identity_residue_gate.sh"],
                                   cwd=repo, capture_output=True, text=True)
            self.assertEqual(0, clean.returncode, clean.stdout + clean.stderr)
            leaked = repo / "removed.txt"
            leaked.write_text("".join(map(chr, (97, 108, 103, 111, 120))) + "\n")
            run("git", "add", ".", cwd=repo)
            run("git", "commit", "-qm", "Add fixture", cwd=repo)
            leaked.unlink()
            run("git", "add", ".", cwd=repo)
            run("git", "commit", "-qm", "Remove fixture", cwd=repo)
            historical = subprocess.run(["bash", "tools/oss_identity_residue_gate.sh"],
                                        cwd=repo, capture_output=True, text=True)
            self.assertNotEqual(0, historical.returncode)
            self.assertIn("historical content token", historical.stdout)
            shallow = root / "shallow"
            run("git", "clone", "-q", "--depth=1", repo.as_uri(), str(shallow), cwd=root)
            rejected = subprocess.run(["bash", "tools/oss_identity_residue_gate.sh"],
                                      cwd=shallow, capture_output=True, text=True)
            self.assertNotEqual(0, rejected.returncode)
            self.assertIn("full Git history is required", rejected.stdout)
            self.assertNotIn("PASS 5", rejected.stdout)


if __name__ == "__main__":
    unittest.main()
