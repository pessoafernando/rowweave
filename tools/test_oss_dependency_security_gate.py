#!/usr/bin/env python3
"""Security gate regression tests without network access."""
import unittest

from oss_dependency_security_gate import ROOT, check, coordinates
from oss_third_party_notices import parse_tree


class DependencySecurityGateTest(unittest.TestCase):
    def test_classified_native_artifact_is_in_both_inventories(self):
        lines = ["[INFO] io.github.rowweave:rowweave-parent:pom:0.2.0"]
        lines.extend("[INFO] io.github.rowweave:" + pom.parent.name + ":jar:0.2.0"
                     for pom in ROOT.glob("rowweave-*/pom.xml"))
        lines.append("[INFO] +- example:native-library:jar:linux-aarch_64:1.0:runtime")
        self.assertIn(("example:native-library", "1.0"), coordinates("\n".join(lines)))
        dependencies, _, _ = parse_tree(lines)
        self.assertEqual({"1.0"}, dependencies[("example", "native-library")])

    def test_partial_tree_cannot_pass_as_complete_reactor(self):
        with self.assertRaisesRegex(ValueError, "incomplete dependency tree"):
            coordinates("io.github.rowweave:rowweave-core:jar:0.2.0\n"
                        "[INFO] +- org.slf4j:slf4j-api:jar:2.0.20:compile\n")

    def test_clean_response_and_known_advisory_have_distinct_results(self):
        resolved = [("example:library", "1.0")]
        self.assertEqual([], check(resolved, lambda payload: {"results": [{}]}))
        self.assertEqual([("example:library", "1.0", "TEST-ADVISORY")],
                         check(resolved, lambda payload: {"results": [
                             {"vulns": [{"id": "TEST-ADVISORY"}]}]}))

    def test_missing_or_invalid_response_fails_closed(self):
        for response in ({}, {"results": []}, {"results": [None]},
                         {"results": [{"error": "unavailable"}]},
                         {"results": [{"next_page_token": "more"}]},
                         {"results": [{"vulns": [{}]}]}):
            with self.subTest(response=response), self.assertRaises(ValueError):
                check([("example:library", "1.0")], lambda payload: response)


if __name__ == "__main__":
    unittest.main()
