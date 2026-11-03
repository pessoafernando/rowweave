#!/usr/bin/env bash
# Release-candidate packaging gate. This is the local equivalent of the CI
# `release` job's packaging steps: same commands, same assertions, no skips.
#
#   - the worktree must be committed (a release candidate is a commit, not a
#     dirty tree)
#   - a real JDK 8 must resolve through tools/java8-home.sh, and the named
#     environment must satisfy it; the consumer ITs are then passed
#     -Drowweave.java8.home, so a missing or incomplete home is an explicit
#     failure, never a silent skip
#   - identity/URL/anchor surface gates
#   - `mvn -Prelease -DskipTests clean package` produces, for every module:
#     jar + sources + javadoc + CycloneDX bom.json/bom.xml, each with a
#     `.SHA-256` sidecar whose digest matches the file
#
# The test/integration half of the release job (`mvn -Prelease verify` with
# -Drowweave.java8.home) runs the same Maven phases locally on demand; this
# script keeps to packaging so it stays fast enough to run before every push.
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -n "$(git status --porcelain)" ]]; then
    echo "RELEASE GATE: dirty worktree — commit the release candidate first" >&2
    git status --porcelain >&2
    exit 1
fi

# shellcheck source=tools/java8-home.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/java8-home.sh"
if [[ -z "${ROWWEAVE_JAVA8_HOME:-}${JAVA8_HOME:-}" ]]; then
    echo "RELEASE GATE: neither ROWWEAVE_JAVA8_HOME nor JAVA8_HOME is set —" \
         "the Java 8 consumer gate refuses to run silently" >&2
    exit 1
fi
rowweave_require_java8_home "RELEASE GATE"

bash tools/oss_repo_url_check.sh
bash tools/oss_docs_language_gate.sh
python3 tools/oss_java_identifier_gate.py
bash tools/oss_identity_residue_gate.sh
python3 tools/oss_java_identifier_gate.py
bash tools/oss_content_boundary_gate.sh
python3 tools/oss_source_language_gate.py
python3 tools/oss_doc_anchor_check.py docs/product/reference/pipeline-boundary.md

mvn -Prelease -B -ntp -DskipTests clean package > /tmp/rowweave-release-package.log

modules=(rowweave-core rowweave-connector-s3 rowweave-connector-postgres-cdc \
         rowweave-soak rowweave-pipeline rowweave-registry-zk \
         rowweave-monitor rowweave-examples)
version=$(sed -n 's#.*<version>\(.*\)</version>.*#\1#p' pom.xml | head -1)
artifacts=0
for module in "${modules[@]}"; do
    dir="$module/target"
    for suffix in "" "-sources" "-javadoc"; do
        jar="$dir/${module}-${version}${suffix}.jar"
        [[ -f "$jar" ]] || { echo "RELEASE GATE: missing $jar" >&2; exit 1; }
        artifacts=$((artifacts + 1))
    done
    for bom in bom.json bom.xml; do
        [[ -f "$dir/$bom" ]] || { echo "RELEASE GATE: missing $dir/$bom" >&2; exit 1; }
        artifacts=$((artifacts + 1))
    done
done

checked=0
while IFS= read -r artifact; do
    sidecar="${artifact}.SHA-256"
    [[ -f "$sidecar" ]] || { echo "RELEASE GATE: missing sidecar for $artifact" >&2; exit 1; }
    expected=$(command shasum -a 256 "$artifact" | awk '{print $1}')
    recorded=$(awk '{print $1}' "$sidecar")
    [[ "$expected" == "$recorded" ]] || {
        echo "RELEASE GATE: sidecar mismatch for $artifact" >&2
        exit 1
    }
    checked=$((checked + 1))
# The release set is the eight library modules; the parent pom's own target/
# directory is a build by-product, not a published artifact.
done < <(find . \( -path './target' -prune \) -o \
              \( -path '*/target/*' -not -path '*/surefire-reports/*' -type f \
                 \( -name '*.jar' -o -name 'bom.json' -o -name 'bom.xml' \) -print \) | sort)

echo "RELEASE GATE: PASS ($artifacts named artifacts, $checked checksummed files)"
