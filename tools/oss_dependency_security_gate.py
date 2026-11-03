#!/usr/bin/env python3
"""Check every resolved reactor dependency against OSV; network failures fail closed."""
import argparse
import json
import pathlib
import re
import sys
import urllib.request
import xml.etree.ElementTree as ET

ROOT = pathlib.Path(__file__).resolve().parents[1]
DEPENDENCY = re.compile(
    r"^[\s+\\|-]*([\w.\-]+):([\w.\-]+):(?:jar|pom):"
    r"(?:[\w.\-]+:)?([\w.\-]+):(compile|runtime|test|provided|system)$")
MODULE = re.compile(r"^io\.github\.rowweave:(rowweave-[\w.-]+):(?:jar|pom):\S+$")


def coordinates(text):
    modules, dependencies = set(), set()
    for line in text.splitlines():
        line = re.sub(r"^\[INFO\]\s?", "", line).strip()
        module = MODULE.fullmatch(line)
        if module:
            modules.add(module[1])
        match = DEPENDENCY.fullmatch(line)
        if match and match[1] != "io.github.rowweave":
            dependencies.add((match[1] + ":" + match[2], match[3]))
    pom = ET.parse(ROOT / "pom.xml").getroot()
    expected = {pom.findtext("{http://maven.apache.org/POM/4.0.0}artifactId")}
    expected.update(item.text for item in pom.findall(
        "{http://maven.apache.org/POM/4.0.0}modules/{http://maven.apache.org/POM/4.0.0}module"))
    if expected - modules or not dependencies:
        raise ValueError("incomplete dependency tree; missing modules: "
                         + ", ".join(sorted(expected - modules)))
    return sorted(dependencies)


def check(resolved, query):
    findings = []
    for start in range(0, len(resolved), 100):
        batch = resolved[start:start + 100]
        response = query({"queries": [{"package": {"ecosystem": "Maven", "name": name},
                                   "version": version} for name, version in batch]})
        results = response.get("results")
        if not isinstance(results, list) or len(results) != len(batch):
            raise ValueError("OSV returned an incomplete response")
        for (name, version), result in zip(batch, results):
            if not isinstance(result, dict):
                raise ValueError("OSV returned an invalid result")
            if "error" in result or result.get("next_page_token"):
                raise ValueError("OSV returned an error or incomplete advisory page")
            if not isinstance(result.get("vulns", []), list):
                raise ValueError("OSV returned an invalid advisory list")
            for vulnerability in result.get("vulns", []):
                identity = vulnerability.get("id")
                if not identity:
                    raise ValueError("OSV returned a finding without an identity")
                findings.append((name, version, identity))
    return findings


def osv_query(payload):
    request = urllib.request.Request("https://api.osv.dev/v1/querybatch",
                                    data=json.dumps(payload).encode("utf-8"),
                                    headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=45) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dependency_tree", type=pathlib.Path)
    args = parser.parse_args()
    try:
        resolved = coordinates(args.dependency_tree.read_text(encoding="utf-8"))
        findings = check(resolved, osv_query)
    except Exception as failure:
        print("DEPENDENCY SECURITY GATE: FAIL (" + str(failure) + ")", file=sys.stderr)
        return 1
    for name, version, identity in findings:
        print(f"VULNERABLE {name}:{version} https://osv.dev/vulnerability/{identity}")
    print(f"DEPENDENCY SECURITY GATE: {'FAIL' if findings else 'PASS'} "
          f"({len(resolved)} resolved coordinates, {len(findings)} advisories)")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
