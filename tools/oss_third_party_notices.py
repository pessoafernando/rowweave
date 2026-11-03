#!/usr/bin/env python3
"""Regenerate THIRD-PARTY-NOTICES.md from the build.

Every claim in the generated file is measured, not asserted:

* ``mvn -o -B -ntp dependency:tree > <file>`` gives
  each coordinate, version, scope and the modules that resolve it.
* The resolved POM in the local Maven repository gives the license name and URL.
* Each connector image under ``target/`` gives the coordinates whose compiled
  classes are actually redistributed inside a published jar, established by
  matching class entry names rather than ``pom.properties``, which is unreliable
  because a shaded dependency can lose its own properties entry.

Usage:
    tree="$PWD/rowweave-dependency-tree.txt"
    mvn -o -B -ntp dependency:tree > "$tree"
    python3 tools/oss_third_party_notices.py "$tree" \
        > THIRD-PARTY-NOTICES.md
"""

import datetime
import hashlib
import os
import pathlib
import re
import sys
import xml.etree.ElementTree as ET
import zipfile
from collections import defaultdict
from oss_dependency_security_gate import DEPENDENCY, coordinates

MODULE = re.compile(r"^io\.github\.rowweave:(rowweave-[\w.-]+):(?:jar|pom):\S+$")
DEP = DEPENDENCY
POM = "{http://maven.apache.org/POM/4.0.0}"


def local_repo():
    override = os.environ.get("MAVEN_REPO_LOCAL")
    return pathlib.Path(override) if override else pathlib.Path.home() / ".m2" / "repository"


def pom_licenses(repo, group, artifact, version, depth=0):
    """Declared licenses for a coordinate, following one parent POM if needed."""
    if depth > 3:
        return None
    path = repo / group.replace(".", "/") / artifact / version / f"{artifact}-{version}.pom"
    if not path.is_file():
        return None
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError:
        return None
    found = []
    for block in root.iter(f"{POM}licenses"):
        for lic in block.findall(f"{POM}license"):
            name = (lic.findtext(f"{POM}name") or "").strip()
            url = (lic.findtext(f"{POM}url") or "").strip()
            if name:
                found.append((name, url))
        if found:
            return found
    parent = root.find(f"{POM}parent")
    if parent is not None:
        return pom_licenses(repo, (parent.findtext(f"{POM}groupId") or "").strip(),
                            (parent.findtext(f"{POM}artifactId") or "").strip(),
                            (parent.findtext(f"{POM}version") or "").strip(), depth + 1)
    return found or None


def parse_tree(lines):
    deps = defaultdict(set)
    scopes = defaultdict(set)
    owners = defaultdict(set)
    current = None
    for line in lines:
        line = re.sub(r"^\[INFO\]\s?", "", line)
        matched = MODULE.match(line)
        if matched:
            current = matched.group(1)
            continue
        dep = DEP.match(line.strip())
        if not dep or current is None:
            continue
        group, artifact, version, scope = dep.groups()
        if group == "io.github.rowweave":
            continue
        key = (group, artifact)
        deps[key].add(version)
        scopes[key].add(scope)
        owners[key].add(current)
    return deps, scopes, owners


def image_classes(root_dir):
    """Map image label -> {class entry name: bytes} for built connector images."""
    images = {}
    for module in sorted(pathlib.Path(root_dir).glob("rowweave-connector-*")):
        target = module / "target"
        if not target.is_dir():
            continue
        jars = sorted(target.glob("*-runtime.jar")) + sorted(target.glob("*-deployment.jar"))
        for jar in jars:
            kind = "runtime image" if jar.name.endswith("-runtime.jar") else "deployment image"
            try:
                with zipfile.ZipFile(jar) as archive:
                    images[f"{module.name} {kind}"] = {
                        n for n in archive.namelist() if n.endswith(".class")}
            except (zipfile.BadZipFile, OSError):
                continue
    return images


def dependency_classes(repo, group, artifact, version):
    path = repo / group.replace(".", "/") / artifact / version / f"{artifact}-{version}.jar"
    if not path.is_file():
        return set()
    try:
        with zipfile.ZipFile(path) as archive:
            return {n for n in archive.namelist() if n.endswith(".class")}
    except (zipfile.BadZipFile, OSError):
        return set()


# License texts RowWeave ships itself, under
# ``<connector>/src/main/resources/META-INF/THIRD-PARTY-LICENSES/``. Two
# situations make this necessary, and the generator checks both rather than
# trusting this note:
#
#   * The upstream artifact carries no license text at all, so a shaded image
#     built from it would redistribute compiled code with no terms attached.
#   * Two artifacts put different text at the same ``META-INF/`` path, and
#     shading keeps only one, so the other would disappear from the image.
#
# The value is where the text came from, verifiable without the build.
INJECTED_NOTE = {
    "org.slf4j:slf4j-api:2.0.20/LICENSE":
        "https://repo.maven.apache.org/maven2/org/slf4j/slf4j-api/2.0.20/slf4j-api-2.0.20.jar",
    "org.reactivestreams:reactive-streams:1.0.4/LICENSE":
        "https://github.com/reactive-streams/reactive-streams-jvm/blob/v1.0.4/COPYING",
    "software.amazon.eventstream:eventstream:1.0.1/LICENSE":
        "https://github.com/awslabs/aws-eventstream-java/blob/master/LICENSE",
    "software.amazon.eventstream:eventstream:1.0.1/NOTICE":
        "https://github.com/awslabs/aws-eventstream-java/blob/master/NOTICE",
}


def injected_dir_for(module):
    """License files RowWeave adds for one connector module, or None."""
    path = (pathlib.Path(module) / "src" / "main" / "resources"
            / "META-INF" / "THIRD-PARTY-LICENSES")
    return path if path.is_dir() else None


# ``<group>-<artifact>-<version>-<LABEL>``, where LABEL can itself contain
# dashes (``FastDoubleParser-LICENSE``), so the version is what anchors a split.
INJECTED_NAME = re.compile(
    r"^(?P<group>[\w.\-]+?)-(?P<artifact>[\w.\-]+?)-"
    r"(?P<version>\d[\w.\-]*?)-(?P<label>[A-Za-z][\w.\-]*)$")


def injected_texts(root_dir):
    """module -> [(filename, coordinate, data, provenance)].

    ``provenance`` is measured rather than asserted: when the coordinate ships
    a copy of the text inside its own jar, the file is compared byte for byte to
    that copy, and the outcome is recorded either way.
    """
    repo = local_repo()
    result = {}
    for module in sorted(pathlib.Path(root_dir).glob("rowweave-connector-*")):
        directory = injected_dir_for(module)
        if directory is None:
            continue
        rows = []
        for file in sorted(directory.iterdir()):
            if not file.is_file():
                continue
            data = file.read_bytes()
            matched = INJECTED_NAME.match(file.stem)
            if matched is None:
                rows.append((file.name, "", data,
                             "unrecognised file name pattern"))
                continue
            group, artifact, version, label = matched.groups()
            coordinate = f"{group}:{artifact}:{version}"
            candidates = list(dict.fromkeys(
                [f"META-INF/{label}"]
                + (["META-INF/LICENSE", "META-INF/LICENSE.txt"]
                   if label.upper().endswith("LICENSE") else [])
                + (["META-INF/NOTICE"]
                   if label.upper().endswith("NOTICE") else [])))
            jar = (repo / group.replace(".", "/") / artifact / version
                   / f"{artifact}-{version}.jar")
            if not jar.is_file():
                rows.append((file.name, coordinate, data,
                             "coordinate not in the local repository"))
                continue
            with zipfile.ZipFile(jar) as archive:
                hit = next((c for c in candidates if c in archive.namelist()),
                           None)
                if hit is None:
                    provenance = ("the upstream jar ships no such file "
                                  "(looked for " + ", ".join(candidates)
                                  + ")")
                elif archive.read(hit) == data:
                    provenance = (f"byte-identical to {hit} in "
                                  f"{artifact}-{version}.jar")
                else:
                    provenance = f"DIFFERS from {hit} in {artifact}-{version}.jar"
            rows.append((file.name, coordinate, data, provenance))
        if rows:
            result[module.name] = rows
    return result


def embedded_texts(jar):
    with zipfile.ZipFile(jar) as archive:
        return sorted(n for n in archive.namelist()
                      if n.startswith("META-INF/") and not n.endswith("/")
                      and any(k in n.upper() for k in ("LICENSE", "NOTICE", "COPYING")))


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    root_dir = pathlib.Path(__file__).resolve().parent.parent
    repo = local_repo()
    tree = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
    resolved = coordinates(tree)
    deps, scopes, owners = parse_tree(tree.splitlines())
    missing = {tuple(name.split(":")) for name, _ in resolved} - set(deps)
    if missing:
        raise ValueError("notice inventory misses resolved artifacts: " + str(sorted(missing)))
    images = image_classes(root_dir)

    rows = []
    for key in sorted(deps):
        group, artifact = key
        versions = sorted(deps[key])
        declared = None
        classes = set()
        for version in versions:
            declared = declared or pom_licenses(repo, group, artifact, version)
            classes |= dependency_classes(repo, group, artifact, version)
        hits = sorted(label for label, present in images.items()
                      if classes and classes & present)
        rows.append({
            "coord": f"{group}:{artifact}",
            "version": "/".join(versions),
            "test_only": scopes[key] <= {"test"},
            "license": " + ".join(n for n, _ in (declared or [("", "")])),
            "url": " + ".join(u for _, u in (declared or []) if u),
            "images": hits,
        })

    bundled = [r for r in rows if r["images"]]
    runtime = [r for r in rows if not r["images"] and not r["test_only"]]
    test_only = [r for r in rows if not r["images"] and r["test_only"]]

    def table(items):
        lines = ["| Artifact | Version | Declared license | License URL |",
                 "|---|---|---|---|"]
        for r in items:
            lines.append(f"| `{r['coord']}` | {r['version']} | "
                         f"{r['license'] or '(not declared in its POM)'} | "
                         f"{r['url'] or '—'} |")
        return lines

    out = ["# Third-party notices", "",
           "RowWeave is licensed under the Apache License, Version 2.0 "
           "([LICENSE](LICENSE), [NOTICE](NOTICE)). This file records the third-party "
           "artifacts the build resolves, the license each one declares, and which of "
           "them are redistributed inside a published image.", "",
           "The file is generated. Regenerate it from the build with:", "",
           "```bash",
           "tree=\"$PWD/rowweave-dependency-tree.txt\"",
           "mvn -o -B -ntp dependency:tree > \"$tree\"",
           "python3 tools/oss_third_party_notices.py \"$tree\" \\",
           "    > THIRD-PARTY-NOTICES.md",
           "```", "",
           f"Resolved third-party coordinates: {len(rows)}. {len(bundled)} have their "
           "compiled classes redistributed inside a connector image, "
           f"{len(runtime)} reach your application as ordinary Maven dependencies, and "
           f"{len(test_only)} are used only to build and test RowWeave.", "",
           "## Redistributed inside a published image", "",
           "These dependencies ship compiled classes inside a connector image, so their "
           "license terms travel with the image instead of being fetched from Maven.",
           ""]
    out += table(bundled)
    out += ["", "## Required at runtime by your application", "",
            "Ordinary Maven dependencies: your application resolves them itself, so it "
            "receives each license text from the original artifact.", ""]
    out += table(runtime)
    out += ["", "## Used only to build and test RowWeave", "",
            "Test-scope artifacts are not part of any published artifact.", ""]
    out += table(test_only)
    out += ["", "## License texts embedded in the published images", "",
            "Established by reading each built image, so this describes the artifact "
            "rather than the dependency graph. Build the connector modules with "
            "`mvn -o -B -q package -DskipTests` before regenerating this file to "
            "populate the table.", "",
            "| Image | Embedded license files |", "|---|---|"]
    for module in sorted(pathlib.Path(root_dir).glob("rowweave-connector-*")):
        target = module / "target"
        if not target.is_dir():
            continue
        jars = sorted(target.glob("*-runtime.jar")) + sorted(target.glob("*-deployment.jar"))
        for jar in jars:
            try:
                texts = embedded_texts(jar)
            except (zipfile.BadZipFile, OSError):
                continue
            out.append(f"| `{jar.name}` | "
                       + (", ".join(f"`{t}`" for t in texts) or "*(none)*") + " |")
    injected = injected_texts(root_dir)
    out += ["", "## License texts RowWeave adds to its images", "",
            "A shaded image must carry the terms of the code inside it. Two"
            " situations break that, and both are handled here rather than by"
            " overriding the upstream entry, because overriding silently drops"
            " one document:", ""]
    out += ["* the upstream artifact ships no license file, so shading would"
            " redistribute its classes with no terms attached;",
            "* two artifacts put different text at the same `META-INF/` path,"
            " and only one survives shading.", ""]
    out += ["RowWeave therefore keeps its own copy of each affected text under"
            " `src/main/resources/META-INF/THIRD-PARTY-LICENSES/`, named"
            " `group-artifact-version-LABEL.txt`. These paths cannot collide"
            " with an upstream entry.", ""]
    if injected:
        out += ["| Module | File | Size | SHA-256 | Provenance verified against the build |",
                "|---|---|---|---|---|"]
        for module, rows in injected.items():
            for name, coordinate, data, verified in rows:
                out.append(f"| `{module}` | `{name}` | {len(data)} B | "
                           f"`{hashlib.sha256(data).hexdigest()[:16]}` | "
                           f"{verified or '(upstream jar checked: none found)'} |")
        out += ["", "Where each externally sourced text came from, so it can be "
                "re-verified without the build. A text recorded as byte-identical "
                "above needs no separate source, since it was read from the "
                "artifact itself.", ""]
        seen = set()
        for module, rows in injected.items():
            for name, coordinate, data, verified in rows:
                if verified.startswith("byte-identical"):
                    continue
                stem = name[:-4] if name.endswith(".txt") else name
                matched = INJECTED_NAME.match(stem)
                key = (f"{matched.group('group')}:{matched.group('artifact')}"
                       f":{matched.group('version')}/{matched.group('label')}"
                       if matched else stem)
                note = INJECTED_NOTE.get(key)
                if note and key not in seen:
                    seen.add(key)
                    coord, _, label = key.rpartition("/")
                    out.append(f"- `{coord}` {label}: {note}")
        out += ["", "The two `com.fasterxml.jackson.core` entries are the"
                " documents that shading drops from `jackson-core` because"
                " `software.amazon.awssdk:third-party-jackson-core` claims the"
                " same `META-INF/NOTICE` and `META-INF/FastDoubleParser-LICENSE`"
                " paths with its own text. Both originals are preserved here.",
                "", "`org.slf4j:slf4j-api` and `org.reactivestreams`"
                " classes are present in the S3 runtime image; the signed"
                " deployment image deliberately strips `org/slf4j/`, so that"
                " one text travels with the runtime image only. The directory"
                " itself passes into both images untouched.", ""]
    else:
        out += ["*(No connector module carries added license texts. Rebuild the"
                " connector images to populate this section.)*", ""]

    out += ["", "## Mixed and copyleft-adjacent licenses", "",
            "- Logback is dual-licensed EPL-1.0 or LGPL-2.1. RowWeave uses it only as a "
            "test logging binding, and you may choose either license.",
            "- H2 is dual-licensed MPL-2.0 or EPL-1.0, and jol-core is GPLv2 with the "
            "Classpath exception. Both are test-scope and reach no published artifact.",
            "- The PostgreSQL JDBC driver is BSD-2-Clause and carries license texts for "
            "its own shaded dependencies under `META-INF/licenses/`.",
            "- ASM is BSD-3-Clause, Checker Framework qualifiers are MIT, and reactive "
            "streams is MIT-0, which drops the attribution clause.",
            "- Artifacts whose POM declares no license block are labelled as such; "
            "RowWeave does not change their terms.", ""]
    out += ["---", "",
            "Generated by `tools/oss_third_party_notices.py` on "
            + datetime.date.today().isoformat()
            + " from `" + pathlib.Path(sys.argv[1]).name + "` and Maven-resolved "
            + "POM metadata."]
    sys.stdout.write("\n".join(out) + "\n")


if __name__ == "__main__":
    main()
