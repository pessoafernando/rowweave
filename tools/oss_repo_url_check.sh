#!/usr/bin/env bash
# Canonical repository URL single-source check.
#
# The GitHub coordinates of this project are defined exactly once, in the
# rowweave.repo.url property of the parent POM. Every tracked github.com URL
# that points at this project must start with that canonical value; third-party
# github.com URLs (dependency notices, vendored license texts) are ignored.
# The issue/PR templates and SECURITY.md cannot read Maven properties, so this
# gate is the registration point that keeps them in sync with the POM.
set -euo pipefail
cd "$(dirname "$0")/.."

canonical=$(sed -n 's#.*<rowweave.repo.url>\(.*\)</rowweave.repo.url>.*#\1#p' pom.xml | head -1)
if [[ -z "$canonical" ]]; then
    echo "rowweave.repo.url missing from pom.xml" >&2
    exit 1
fi
prefix="${canonical%/}"

bad=0
checked=0
while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    file=${line%%:*}
    rest=${line#*:}
    while IFS= read -r url; do
        [[ -n "$url" ]] || continue
        checked=$((checked + 1))
        case "$url" in
            "$prefix" | "$prefix".git | "$prefix"/*) ;;
            *)
                echo "NON-CANONICAL $file: $url"
                bad=1
                ;;
        esac
    done < <(printf '%s\n' "$rest" | grep -oE 'https?://github\.com/[^ "'"'"'<>)]+' || true)
done < <(git grep -n 'github.com/pessoafernando/rowweave' -- ':!tools/oss_repo_url_check.sh' || true)

if (( bad )); then
    exit 1
fi
echo "REPO URL GATE: PASS ($checked rowweave URL(s) match $prefix)"
