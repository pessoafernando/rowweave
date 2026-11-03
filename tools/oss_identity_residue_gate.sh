#!/usr/bin/env bash
# Zero-residue gate for the open-source tree.
#
# Six reporting groups, all of which must pass.
#
#   1. No forbidden identity in the tree or reachable commit metadata. The
#      patterns have no whitelist: any match at all is undeclared residue.
#   5. The same sweep across every object reachable from any reference. A
#      tree-only check cannot see a name that an earlier commit carried and a
#      later commit renamed away, so the historical object set is scanned
#      directly.
#   6. Every author and committer in reachable history is a declared publishing
#      identity rather than a private machine account.
#   2. Fixed-width format magics (RWXX??????, RWX??????, 0x5257xxxx) do not
#      match the identity pattern, so check 1 cannot see them. They are frozen
#      format constants and are explained publicly in
#      docs/product/reference/format-constants.md. That page must exist, must
#      stay linked from README.md, and must be the only place a reader learns
#      why they are frozen.
#   3. Customer-visible and operator-visible strings must not carry an
#      ALG-shaped token or an internal platform codename. The rejected-row detail
#      tokens a run can write into a customer table are a closed set validated by
#      QualityReasonKey, and the manifest file name is an operator-typed
#      convention, so both are pinned here rather than left to review.
set -uo pipefail
cd "$(dirname "$0")/.."

if [ "$(git rev-parse --is-shallow-repository 2>/dev/null)" != "false" ]; then
  echo "ZERO-RESIDUE GATE: FAIL (full Git history is required; shallow or missing repository)"
  echo "Fetch complete history before running the historical identity checks."
  exit 1
fi

legacy_engine_token="$(printf '\x61\x6c\x67\x6f\x78')"
kd_token="$(printf '\x6b\x64')"
king_token="$(printf '\x6b\x69\x6e\x67')"; dee_token="$(printf '\x64\x65\x65')"
cos_token="$(printf '\x63\x6f\x73')"; mic_token="$(printf '\x6d\x69\x63')"
kon_token="$(printf '\x6b\x6f\x6e')"; one_token="$(printf '\x6f\x6e\x65')"
cang_token="$(printf '\u82cd')"; qiong_token="$(printf '\u7a79')"
cjk_vendor_token="$(printf '\u91d1\u8776')"
legacy_wire_token="$(printf '\x41\x4c\x47\x58')"
legacy_short_token="$(printf '\x41\x4c\x58')"
legacy_int_token="$(printf '\x30\x78\x34\x31\x35\x38')"
legacy_api_pattern="$(printf '\x52\x6f\x77\x58')|$(printf '\x44\x61\x74\x61\x53\x65\x74\x58')|$(printf '\x44\x61\x74\x61\x53\x74\x72\x65\x61\x6d\x58')|$(printf '\x4a\x6f\x69\x6e\x44\x61\x74\x61\x53\x65\x74\x58')|$(printf '\x43\x6f\x47\x72\x6f\x75\x70\x44\x61\x74\x61\x53\x65\x74\x58')"
legacy_shape_guard_token="$(printf '\x41\x4c\x47')"
legacy_brand_guard_token="$(printf '\x4b\x6f\x6e\x65')"
legacy_brand_upper_guard_token="$(printf '\x4b\x4f\x4e\x45')"
legacy_customer_tail_token="$(printf '\x61\x6c\x67\x78')"
PATTERN="${legacy_wire_token}|${legacy_short_token}|${legacy_int_token}|(?i)${legacy_engine_token}|${kd_token}\.bos|${kd_token}/bos|${cjk_vendor_token}|${cang_token}${qiong_token}|(?i)${king_token}${dee_token}|(?i)${cos_token}${mic_token}|(?i)${kon_token}${one_token}"
legacy_data_suffix_token="$(printf '\x64\x61\x74\x61\x73\x65\x74\x78')"
legacy_row_suffix_token="$(printf '\x72\x6f\x77\x78')"
machine_user_token="$(printf '\x6c\x7a\x78')"
OBJECT_PATTERN="${PATTERN}|(?i)${legacy_data_suffix_token}|(?i)${legacy_row_suffix_token}|\\b${machine_user_token}\\b"
PUBLISH_IDENTITY_ALLOW='^[^<]*<[0-9]+\+[^< ]*@users\.noreply\.github\.com>$'
STREAM_PATTERN='(?i)StreamV[0-9]|Stream[ ](?:Runtime[ ])?v[0-9]|stream-runtime-v[0-9]'
DOC='docs/product/reference/format-constants.md'

status=0

# The search path "." is explicit. Without one, ripgrep reads standard input
# whenever that input is not a terminal, so a scripted or CI invocation would
# scan an empty stream and report a clean tree.
HITS=$(
  rg -n --hidden --no-ignore -a -e "$PATTERN" \
    --glob '!target/' --glob '!.git/' --glob '!**/target/' . \
  || true
)
if [ -n "$HITS" ]; then
  echo "FAIL: undeclared predecessor identity"
  printf '%s\n' "$HITS" | head -40
  echo "--- total $(printf '%s\n' "$HITS" | wc -l) lines ---"
  status=1
fi
HISTORY_HITS=$(
  git log --format='commit %H%nAuthor: %an <%ae>%nCommitter: %cn <%ce>%nSubject: %s%n%n%b' \
    | rg -a -e "$PATTERN|$legacy_api_pattern" \
    || true
)
if [ -n "$HISTORY_HITS" ]; then
  echo "FAIL: forbidden identity in reachable commit metadata"
  echo "$HISTORY_HITS" | head -40
  echo "--- total $(printf '%s\n' "$HISTORY_HITS" | wc -l) lines ---"
  status=1
fi
[ "$status" -eq 0 ] && echo "PASS 1: no forbidden identity in the tree or commit metadata"


OBJECTS_FILE="$(mktemp)"
SCANNED="$(git rev-list --objects --all > "$OBJECTS_FILE" 2>/dev/null; wc -l < "$OBJECTS_FILE" | tr -d ' ')"
PATH_HITS=$(awk '{ print $2 }' "$OBJECTS_FILE" | rg -a -o -N -e "$OBJECT_PATTERN" | sort -u || true)
ALL_OIDS="$(mktemp)"
awk '{ print $1 }' "$OBJECTS_FILE" | sort -u > "$ALL_OIDS"
BLOB_HITS=$(git cat-file --batch < "$ALL_OIDS" 2>/dev/null \
  | rg -a -o -N -e "$OBJECT_PATTERN|$legacy_api_pattern" | sort | uniq -c || true)
IDENTITY_COUNT=$(git log --all --format='%an <%ae>%n%cn <%ce>' | sort -u \
  | rg -v -e "$PUBLISH_IDENTITY_ALLOW" | grep -c . || true)
if [ -n "$PATH_HITS" ] || [ -n "$BLOB_HITS" ] || [ "$IDENTITY_COUNT" != "0" ]; then
  echo "FAIL: forbidden identity in an object that only reachable history carries"
  [ -n "$PATH_HITS" ] && printf 'historical path token: %s\n' "$PATH_HITS" | head -20
  [ -n "$BLOB_HITS" ] && printf 'historical content token (count, token): %s\n' "$BLOB_HITS" | head -20
  [ "$IDENTITY_COUNT" != "0" ] && echo "undeclared author or committer identities: $IDENTITY_COUNT"
  echo "--- reachable objects scanned: $SCANNED ---"
  echo "every author and committer must match the declared GitHub noreply form"
  status=1
fi
rm -f "$OBJECTS_FILE" "$ALL_OIDS"
[ "$status" -eq 0 ] && echo "PASS 5: every historical object is clean ($SCANNED objects)"

[ "$status" -eq 0 ] && echo "PASS 6: reachable history carries only declared publishing identities"

if [ ! -f "$DOC" ]; then
  echo "FAIL 2: $DOC is missing; frozen format magics would be undocumented"
  status=1
elif ! grep -q 'format-constants.md' README.md; then
  echo "FAIL 2: README.md no longer links $DOC"
  status=1
else
  n=$(rg -c -a '"RWXX|"RWX|0x525' --glob '*/src/main/**' --glob '*.java' . \
      | awk -F: '{s+=$2} END {print s+0}')
  echo "PASS 2: $DOC present, linked from README, covers $n frozen magic declarations"
fi

REASON_FILES='
rowweave-core/src/main/java/io/rowweave/QualityReasonKey.java
rowweave-core/src/main/java/io/rowweave/ObjectStorageQualityRecordDecoder.java
rowweave-core/src/main/java/io/rowweave/QualityPreparedEvidenceReader.java
rowweave-core/src/main/java/io/rowweave/ops/ObjectStorageQualityRoute.java'
for reason_file in $REASON_FILES; do
  if [ ! -f "$reason_file" ]; then
    echo "FAIL 3: $reason_file not found; the check did not run"
    status=1
  fi
done
LEAK=$(
  rg -n -a -e "$legacy_shape_guard_token|$legacy_brand_guard_token|$legacy_brand_upper_guard_token" $REASON_FILES \
    || true
  rg -n -a -e "\.$legacy_customer_tail_token[a-z]" --glob 'docs/**' --glob '*/src/**' . || true
)
if [ -n "$LEAK" ]; then
  echo "FAIL 3: ALG-shaped token reached a customer or operator surface"
  echo "$LEAK" | head -20
  status=1
else
  echo "PASS 3: rejected-row detail tokens and manifest file names are clean"
fi

STREAM_HITS=$(
  rg -n --hidden --no-ignore -a -e "$STREAM_PATTERN" \
    --glob '*.md' --glob 'pom.xml' --glob '*/pom.xml' \
    --glob 'rowweave-examples/src/main/**/*.java' \
    --glob '!target/' --glob '!.git/' --glob '!**/target/' . \
  || true
)
if [ -n "$STREAM_HITS" ]; then
  echo "FAIL: undeclared Stream generation naming"
  printf '%s\n' "$STREAM_HITS" | head -40
  echo "--- total $(printf '%s\n' "$STREAM_HITS" | wc -l) lines ---"
  status=1
fi
[ "$status" -eq 0 ] && echo "PASS 4: canonical public content has no Stream generation naming"

# --- positive control -------------------------------------------------------
# A scan that sees nothing looks exactly like a scan that passes. The identity
# patterns are therefore exercised here against a known token planted in a
# temporary file outside the tree, and the tree scan must report a plausible
# file count. This guards the failure mode found once already: a ripgrep call
# with no search path reads standard input when that input is not a terminal,
# which is how CI and every scripted run invokes this file.
CTRL_DIR="$(mktemp -d)"
printf 'package io.rowweave;\n\nfinal class ControlProbe { private static final String %s = "x"; }\n' "$legacy_engine_token" > "$CTRL_DIR/ControlProbe.java"
CTRL_TREE=$(rg -c -a -e "$PATTERN" "$CTRL_DIR/ControlProbe.java" || true)
CTRL_FILES=$(rg --files --hidden --no-ignore --glob '!target/' --glob '!.git/' --glob '!**/target/' . | wc -l | tr -d ' ')
rm -rf "$CTRL_DIR"
if [ -z "$CTRL_TREE" ]; then
  echo "FAIL: positive control matched nothing - the identity pattern set or the engine is broken"
  status=1
elif [ "$CTRL_FILES" -lt 1000 ]; then
  echo "FAIL: the tree scan saw only $CTRL_FILES files - it is not scanning the repository"
  status=1
else
  echo "PASS control: identity token rejected in the control file, tree has $CTRL_FILES files"
fi

if [ "$status" -eq 0 ]; then
  echo "ZERO-RESIDUE GATE: PASS"
else
  echo "ZERO-RESIDUE GATE: FAIL"
fi
exit "$status"
