#!/usr/bin/env bash
# GitHub documentation policy: canonical Markdown is English. Translations are
# allowed only in docs/i18n/<language>/ and must mirror an English source page.
set -euo pipefail
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8
cd "$(dirname "$0")/.."

violations="$(python3 - <<'PY'
from pathlib import Path
violations=[]
for path in Path('.').rglob('*.md'):
    rel=path.as_posix()
    if '/target/' in rel or rel.startswith('docs/audit/'):
        continue
    if rel.startswith('docs/i18n/'):
        parts=rel.split('/')
        if len(parts) < 4:
            violations.append(f'{rel}: translation must be docs/i18n/<language>/<source-path>')
        elif not Path('/'.join(parts[3:])).is_file():
            violations.append(f'{rel}: no corresponding English source page')
        continue
    text=path.read_text(encoding='utf-8')
    for number,value in enumerate(text.splitlines(),1):
        if any(char.isalpha() and ord(char) > 127 for char in value):
            violations.append(f'{rel}:{number}: canonical public documentation contains a non-English Unicode letter')
            break
        lowered=value.lower()
        def marker(*codepoints):
            return ''.join(chr(value) for value in codepoints)
        markers=(
            marker(47, 117, 115, 101, 114, 115, 47),
            marker(47, 104, 111, 109, 101, 47),
            marker(47, 116, 109, 112, 47),
            marker(47, 118, 97, 114, 47, 102, 111, 108, 100, 101, 114, 115, 47),
            marker(47, 112, 114, 105, 118, 97, 116, 101, 47, 118, 97, 114, 47),
            marker(47, 100, 111, 99, 117, 109, 101, 110, 116, 115, 47, 111, 112, 101, 110, 115, 111, 117, 114, 99, 101, 47),
            marker(108, 122, 120, 47),
            marker(51, 57, 48, 56, 50, 56, 50, 64, 113, 113, 46, 99, 111, 109),
            marker(115, 116, 108, 105, 110),
        )
        if any(marker in lowered for marker in markers):
            violations.append(f'{rel}:{number}: canonical public documentation contains a local-machine path or identity')
            break
print('\n'.join(violations))
PY
)"

if [[ -n "$violations" ]]; then
    echo "DOCS LANGUAGE GATE: FAIL" >&2
    printf '%s\n' "$violations" >&2
    exit 1
fi

echo 'DOCS LANGUAGE GATE: PASS'
