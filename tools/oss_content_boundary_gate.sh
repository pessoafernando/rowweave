#!/usr/bin/env bash
# Public-content boundary gate.
#
# Three rule families, all textual by design:
#
#   1. Private planning narrative - internal milestone identifiers, defect and
#      rework history - must not enter source, documentation, templates or
#      release tooling. Non-ASCII fragments are written as \x{...} escapes:
#      ripgrep's default engine does not interpret \u, and PCRE2 rejects it.
#   2. Financial-domain vocabulary. The published product is a general data
#      engine, not a bookkeeping system, so no identifier, comment or string
#      literal in a java source may encode that retired domain. These rules are
#      word-boundary and case-aware on purpose: a naive substring scan would hit
#      legitimate names (fingerprint, masterPort, MAX_CONCURRENCY, concurrency,
#      syntax, taxonomy, balanced, accounting, runbook, accompanying, the test
#      variable gbP), and a case-insensitive scan with a lowercase-only tail
#      would hit every SCREAMING identifier that merely begins with a noun.
#      English nouns are gated on java files only: Markdown and shell prose may
#      legitimately write "account for" or "GitHub account".
#   3. Financial-domain words in CJK, scanned across the whole tree. Each word is
#      assembled at run time from codepoints so the gate detects them without
#      containing them, then handed to the default engine as literal text.
#   4. Development-process narrative in java sources: the marker set that
#      oss_narrative_scan.py measures, kept as a permanent gate so a reworded
#      comment cannot reintroduce a provenance claim. Every pattern carries an
#      explicit left boundary, because an unbounded alternation reaches into
#      unrelated words: cross-check matched Across|Checkpoint inside
#      completedRunHistoryRemainsQueryableAcrossCheckpointRotation. The English
#      predecessor marker demands an accompanying lineage noun, because the tree
#      publishes its own anti-lineage policy and that text is honest: the
#      translation gate message "forbidden predecessor identity" and the stream
#      shape guard vocabulary producer|predecessor|upstream|parent state a rule
#      being enforced, not a history being recorded. The untranslated CJK terms
#      stay bare matches; in a Chinese comment the word itself is the claim.
#
# The five CJK words that still occur in not-yet-translated comments - the terms
# for budget, cost, benefit, expense and audit - join family 3 in the step that
# removes those comments; adding them now would fail a clean tree.
set -uo pipefail
cd "$(dirname "$0")/.."

PATTERN='(^|[^A-Za-z0-9_-])(s1[4-9]|t2[0-7])\b|\x{5386}\x{53f2}\x{76f2}\x{533a}|\x{7f3a}\x{9677}[:\x{ff1a}]|\x{4fee}\x{590d}[:\x{ff1a}]|bug \x{4fee}\x{590d}|\x{672a}\x{4fee}\x{524d}|R[0-9]+-K[0-9]+|ARL[0-9]+|RED \x{8bc1}\x{636e}|\x{2192} RED|deterministic RED|consumer RED|\x{6700}\x{5c0f} RED|GREEN \x{540e}|\x{5f53}\x{524d}\x{5b9e}\x{73b0}\x{9884}\x{671f}|dagF04/F05'
SELF='^(\./)?tools/oss_content_boundary_gate\.sh$'

# --- family 2: financial vocabulary in java sources (PCRE2) ---
VOCAB_LOW='bookkeeping|receivables|currencies|receivable|companies|accounts|balances|currency|invoices|payables|revenues|taxation|account|balance|company|invoice|payable|payroll|profits|revenue|fiscal|profit|taxes|erp|tax'
VOCAB_CAPS='Receivable|Currency|Account|Balance|Company|Invoice|Payable|Payroll|Revenue|Profit|CNY|EUR|GBP|JPY|RMB|Tax|USD'
VOCAB_SCREAM='BOOKKEEPING|RECEIVABLES|CURRENCIES|RECEIVABLE|COMPANIES|ACCOUNTS|BALANCES|CURRENCY|INVOICES|PAYABLES|REVENUES|TAXATION|ACCOUNT|BALANCE|COMPANY|INVOICE|PAYABLE|PAYROLL|PROFITS|REVENUE|FISCAL|PROFIT|TAXES|CNY|ERP|EUR|GBP|JPY|RMB|TAX|USD'
VOCAB_CODES='CNY|USD|RMB|EUR|GBP|JPY'

# W: the whole identifier or string token is the noun, in any casing.
VOCAB_W="(?<![A-Za-z0-9_])(?i:${VOCAB_LOW})(?![A-Za-z0-9_])"
# L: a lowercase noun forming a complete segment: nothing alphanumeric before
# (so an underscore separator counts) and no lowercase letter or digit after.
# Matches "balance" in opening_balance and "account.required"; not "balanced".
VOCAB_L="(?<![A-Za-z0-9])(?:${VOCAB_LOW})(?![a-z0-9])"
# C: a Capitalized noun or currency code closing a camel-hump segment, with a
# lowercase letter before it and no lowercase letter or digit after it
# (accountId, openingBalance, DecimalTaxAggregate, stateCnyTotal).
VOCAB_C="(?<=[a-z0-9])(?:${VOCAB_CAPS})(?![a-z0-9])"
# S: a SCREAMING_SNAKE segment that is exactly the noun or currency code
# (ACCOUNT_ID, OPENING_BALANCE, MAX_CURRENCY). A longer all-caps word is not a
# segment, so CONCURRENCY does not yield CURRENCY.
VOCAB_S="(?<![A-Z0-9])(?:${VOCAB_SCREAM})(?![A-Z0-9])"
# U: an uppercase ISO-style currency code as its own token.
VOCAB_U="(?<![A-Za-z0-9_])(?:${VOCAB_CODES})(?![A-Za-z0-9_])"

# --- family 3: financial vocabulary in CJK, one word per line, codepoints only ---
cjk_word() {
  local out='' cp
  for cp in $1; do out="$out$(printf "\u$cp")"; done
  printf '%s' "$out"
}
CJK_PATTERN=''
CJK_COUNT=0
while read -r line; do
  [ -z "$line" ] && continue
  word="$(cjk_word "${line%%#*}")"
  if [ -z "$word" ]; then
    echo "CONTENT BOUNDARY GATE: FAIL"
    echo "cannot decode CJK word from: $line"
    exit 1
  fi
  CJK_PATTERN="${CJK_PATTERN:+${CJK_PATTERN}|}${word}"
  CJK_COUNT=$((CJK_COUNT+1))
done <<'CJK_WORDS'
4e0d 542b 7a0e    # word 1
660e 7ec6 8d26    # word 2
4ed8 6b3e    # word 3
4f1a 8ba1    # word 4
4f59 989d    # word 5
501f 65b9    # word 6
501f 8d37    # word 7
51ed 8bc1    # word 8
51fa 7eb3    # word 9
5206 5f55    # word 10
5229 6da6    # word 11
53d1 7968    # word 12
542b 7a0e    # word 13
5916 5e01    # word 14
5bf9 8d26    # word 15
5e01 79cd    # word 16
5e94 4ed8    # word 17
5e94 6536    # word 18
5f00 6237    # word 19
5f00 7968    # word 20
5f80 6765    # word 21
603b 8d26    # word 22
62a5 9500    # word 23
6536 5165    # word 24
6536 6b3e    # word 25
671f 521d    # word 26
671f 672b    # word 27
672c 5e01    # word 28
6838 7b97    # word 29
73b0 91d1    # word 30
76d8 70b9    # word 31
79d1 76ee    # word 32
7a0e 7387    # word 33
7a0e 91d1    # word 34
7ea2 5229    # word 35
7ed3 7b97    # word 36
7ed3 8d26    # word 37
80a1 606f    # word 38
8bb0 8d26    # word 39
8d1f 503a    # word 40
8d22 52a1    # word 41
8d26 5957    # word 42
8d26 6237    # word 43
8d26 6b3e    # word 44
8d26 7c3f    # word 45
8d37 65b9    # word 46
8d39 7528    # word 47
8d44 4ea7    # word 48
8d4a 9500    # word 49
91d1 8776    # word 50
91d1 989d    # word 51
94f6 884c    # word 52
CJK_WORDS
if [ "$CJK_COUNT" -ne 52 ]; then
  echo "CONTENT BOUNDARY GATE: FAIL"
  echo "expected 52 CJK words, decoded $CJK_COUNT"
  exit 1
fi

# --- family 4: development-process narrative in java sources (PCRE2) ---
# CJK terms appear as \x{...} escapes so this file stays pure ASCII and cannot
# match itself. Case handling is scoped, not global: the narrative phrases match
# in any casing, while the internal identifier families stay uppercase-only. They
# are uppercase by convention, and a global flag applied to the whole alternation
# flagged the honest thread name arl6-master-a in a cluster test. Python rejects a
# mid-pattern (?i) and PCRE2 needs the scope anyway, so the case-insensitive
# families are wrapped in one inline (?i:...) group.
NARR_IRON='(?<![A-Za-z0-9_])iron[\s_-]?rules?(?![A-Za-z0-9_])'
NARR_PORT='\b(?:port|ported|porting)\s+(?:from|over)\s+(?:the\s+)?(?:predecessor|original|old|prior)\b'
NARR_PRED='\bpredecessor\s+(?:project|codebase|repository|implementation|engine|version|product|system)\b|\b(?:ported|copied|derived|adapted|rewritten|replaced|renamed)\s+from\s+(?:the|our|a)\s+predecessor\b'
NARR_REWRITE='\brewrite\s+(?:version|edition)\b|\b(?:re-?written?)\s+version\b'
NARR_CORRECT='\b(?:design|architecture)\s+corrections?\b|\(correction\)'
NARR_XCHECK='(?<![A-Za-z0-9])cross[-_]?check\w*|(?<![A-Za-z0-9])cross\s+checked\b'
NARR_MUSTFIX='\bmust[\s-]?fixes?\b'
NARR_MILESTONE='\bmilestones?\b'
NARR_REVIEW='\breview\s*(?:round|notes|find?ing|result)s?\b|\breviewed by\b'
NARR_INTERNAL='\b(?:MEDIUM|LOW|HIGH)-[0-9]+\b|\bARL[0-9]+\b|\bR[0-9]+-K[0-9]+\b'
NARR_FIXTURE='\bdag[TF][0-9]+\b'
NARR_CJK='\x{94C1}\x{5F8B}|\x{642C}\x{8FD0}|\x{7EA0}\x{6B63}|\x{5BF9}\x{62CD}|\x{5FC5}\x{987B}\x{4FEE}|\x{91CC}\x{7A0B}\x{7891}|\x{8BC4}\x{5BA1}|\x{524D}\x{8EAB}|\x{91CD}\x{5199}\x{7248}|\x{7167}\x{642C}'
NARR_PHRASES="${NARR_IRON}|${NARR_PORT}|${NARR_PRED}|${NARR_REWRITE}|${NARR_CORRECT}|${NARR_XCHECK}|${NARR_MUSTFIX}|${NARR_MILESTONE}|${NARR_REVIEW}"
NARRATIVE="(?i:${NARR_PHRASES})|${NARR_INTERNAL}|${NARR_FIXTURE}|${NARR_CJK}"
NARRATIVE_RULES=12

status=0
# Every scan names "." explicitly. Without a search path, ripgrep reads its
# standard input whenever that input is not a terminal, so a scripted or CI
# invocation would scan an empty stream and report a clean tree.
HITS=$(
  {
    rg -n --hidden --no-ignore -a -e "$PATTERN" \
      --glob '!target/' --glob '!.git/' --glob '!**/target/' . || true
    rg -n --hidden --no-ignore -a -P \
      -e "$VOCAB_W" -e "$VOCAB_L" -e "$VOCAB_C" -e "$VOCAB_S" -e "$VOCAB_U" \
      --glob '*.java' --glob '!target/' --glob '!.git/' --glob '!**/target/' . || true
    rg -n --hidden --no-ignore -a -e "$CJK_PATTERN" \
      --glob '!target/' --glob '!.git/' --glob '!**/target/' . || true
    rg -n --hidden --no-ignore -a -P -e "$NARRATIVE" \
      --glob '*.java' --glob '!target/' --glob '!.git/' --glob '!**/target/' . || true
  }
)
if [ -n "$HITS" ]; then
  printf '%s\n' "$HITS" | awk -F: -v self="$SELF" '
    BEGIN { m=split(self, s, "|"); for (i=1;i<=m;i++) selfre[i]=s[i]; }
    { file=$1; ok=0
      for (i=1;i<=m;i++) if (file ~ selfre[i]) ok=1
      if (!ok) print $0 }' > /tmp/content-boundary.out
  if [ -s /tmp/content-boundary.out ]; then
    echo "FAIL: private process narrative or gated financial vocabulary in public content"
    head -40 /tmp/content-boundary.out
    echo "--- total $(wc -l < /tmp/content-boundary.out) lines ---"
    status=1
  fi
fi

# --- positive control -------------------------------------------------------
# A scan that sees nothing is indistinguishable from a scan that passes, so the
# patterns are exercised here against a known marker planted in a temporary file
# outside the tree. This also guards the failure mode found once already: a
# ripgrep call with no search path reads standard input when that input is not a
# terminal, which is how CI and every scripted run invokes this file.
CTRL_DIR="$(mktemp -d)"
printf 'package io.rowweave;\n\n/**\n * Iron rule: the control file carries a marker.\n */\nfinal class ControlProbe { private static final int balance = 1; }\n' > "$CTRL_DIR/ControlProbe.java"
CTRL_NARR=$(rg -c -a -P -e "$NARRATIVE" "$CTRL_DIR/ControlProbe.java" || true)
CTRL_VOCAB=$(rg -c -a -P -e "$VOCAB_W" -e "$VOCAB_L" -e "$VOCAB_C" -e "$VOCAB_S" -e "$VOCAB_U" "$CTRL_DIR/ControlProbe.java" || true)
CTRL_FILES=$(rg --files --hidden --no-ignore --glob '!target/' --glob '!.git/' --glob '!**/target/' . | wc -l | tr -d ' ')
rm -rf "$CTRL_DIR"
if [ -z "$CTRL_NARR" ] || [ -z "$CTRL_VOCAB" ]; then
  echo "FAIL: positive control matched nothing - the pattern set or the engine is broken"
  status=1
elif [ "$CTRL_FILES" -lt 1000 ]; then
  echo "FAIL: the tree scan saw only $CTRL_FILES files - it is not scanning the repository"
  status=1
else
  echo "PASS control: marker rejected (narrative $CTRL_NARR hits, vocabulary $CTRL_VOCAB hits), tree has $CTRL_FILES files"
fi

if [ "$status" -eq 0 ]; then
  echo "CONTENT BOUNDARY GATE: PASS ($CJK_COUNT gated CJK words, 5 java vocabulary rules, $NARRATIVE_RULES java narrative rules)"
else
  echo "CONTENT BOUNDARY GATE: FAIL"
fi
exit "$status"
