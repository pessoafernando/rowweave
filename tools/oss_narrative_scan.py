#!/usr/bin/env python3
"""Scan tracked java sources for development-process narrative.

Published comments state what the code guarantees and why. They never record how
the code was derived: which private architecture law demanded it, which review
round flagged it, which predecessor implementation it was ported from or
differential-tested against, and which internal milestone id tracked the work.

The CJK vocabulary is assembled from code points, so this file stays pure ASCII
and cannot match itself.

Not narrative, on purpose, and therefore not scanned:
  * "forbidden" and its CJK equivalent - ordinary normative English. "A null key
    is forbidden from matching" states a contract, not a process. A prohibition
    that merely records a private law is still caught by the iron-rule marker.
  * "reconcile" / "reconciliation" - state reconciliation is what a runtime
    does, not how it was written.
  * bare "fix", "review", "baseline", "oracle" - general engineering and testing
    English. An "oracle" that is a locally computed expectation is the standard
    name for a test's source of truth; the process claim is that the oracle is a
    hidden previous implementation, which the cross-check and predecessor
    markers catch.
  * the enforcement text that guards against lineage: the translation gate
    message "forbidden predecessor identity" and the stream shape guard's
    producer|predecessor|upstream|parent vocabulary. Those state a policy the
    published tree keeps, so the English marker requires an explicit lineage
    claim (predecessor project, ported from the predecessor) rather than the
    bare word.

  * version suffixes on durable identifiers - "...-v1" inside a wire token,
    frame id or contract name is a format identity, not a generation claim.
    Generation narrative is caught by words: predecessor, rewrite, ported from.

usage: oss_narrative_scan.py [SCOPE ...]
  default: every tracked java file
"""
import re
import subprocess
import sys


def w(*cps):
    return "".join(chr(c) for c in cps)


IRON_RULE = w(0x94C1, 0x5F8B)          # private architecture law
PORTING = w(0x642C, 0x8FD0)            # carried over by hand
CORRECTION = w(0x7EA0, 0x6B63)         # design correction
CROSS_CHECK = w(0x5BF9, 0x62CD)        # differential cross-check
MUST_FIX = w(0x5FC5, 0x987B, 0x4FEE)   # must fix
MILESTONE = w(0x91CC, 0x7A0B, 0x7891)  # milestone
REVIEW = w(0x8BC4, 0x5BA1)             # review round
PREDECESSOR = w(0x524D, 0x8EAB)        # predecessor
REWRITE = w(0x91CD, 0x5199, 0x7248)    # rewritten version

# name, english pattern, CJK literal (None when the idea is English-only).
# A CJK literal is matched as the bare word: in a comment that is not yet
# translated, the word itself is already the provenance claim.
MARKERS = [
    ("iron-rule", r"\biron[\s-]?rules?\b", IRON_RULE),
    ("porting",
     r"\b(?:port|ported|porting)\s+(?:from|over)\s+(?:the\s+)?(?:predecessor|original|old|prior)\b",
     PORTING),
    # The bare English word is not enough: the tree enforces its own lineage
    # policy, and that enforcement is honest published text. Two mechanisms
    # collide with it: the docs translation gate emits the literal message
    # "forbidden predecessor identity" and its test asserts that string, and
    # the stream shape guard lists producer|predecessor|upstream|parent as the
    # graph vocabulary it rejects, with a finding named
    # "predecessor-opcode-classifier". Require an explicit lineage claim in
    # English; the untranslated CJK word stays a bare match because in a
    # Chinese comment it is already the provenance claim.
    ("predecessor",
     r"\bpredecessor\s+(?:project|codebase|repository|implementation|engine|"
     r"version|product|system)\b"
     r"|\b(?:ported|copied|derived|adapted|rewritten|replaced|renamed)\s+from"
     r"\s+(?:the|our|a)\s+predecessor\b",
     PREDECESSOR),
    ("rewrite", r"\brewrite\s+(?:version|edition)\b|\b(?:re-?written?)\s+version\b",
     REWRITE),
    ("correction", r"\b(?:design|architecture)\s+corrections?\b|\(correction\)",
     CORRECTION),
    # A left boundary is required, not just \b: without it the pattern reaches
    # into the middle of an unrelated word and flags the honest test method
    # completedRunHistoryRemainsQueryableAcrossCheckpointRotation, because
    # "Across|Checkpoint" contains a lowercase "crossCheck". Case-insensitive
    # for the same reason: "Cross-checks the three implementations" is narrative.
    ("cross-check-oracle",
     r"(?i)(?<![A-Za-z0-9])cross[-_]?check\w*|(?<![A-Za-z0-9])cross\s+checked\b",
     CROSS_CHECK),
    ("must-fix", r"\bmust[\s-]?fixes?\b", MUST_FIX),
    ("milestone", r"\bmilestones?\b", MILESTONE),
    ("review", r"\breview\s*(?:round|notes|find?ing|result)s?\b|\breviewed by\b", REVIEW),
    ("internal-id", r"\b(?:MEDIUM|LOW|HIGH)-[0-9]+\b|\bARL[0-9]+\b|\bR[0-9]+-K[0-9]+\b",
     None),
    ("fixture-id", r"\bdag[TF][0-9]+\b", None),
    ("milestone-ref", r"(?<![A-Za-z0-9_.-])(?:s1[4-9]|t2[0-7])(?![A-Za-z0-9_.-])", None),
]

COMPILED = []
for _name, _en, _cjk in MARKERS:
    COMPILED.append((_name, re.compile(_en)))
    if _cjk:
        COMPILED.append((_name, re.compile(re.escape(_cjk))))


def matches(path, scope):
    """True when SCOPE is a directory sequence of PATH.

    A bare ``src/main`` means that segment anywhere in the path, so it covers
    every module at once; a module-qualified scope such as
    ``rowweave-core/src/main`` selects only that module. The step that verifies
    this tool passes ``src/main`` and ``src/test`` without naming each module,
    and git pathspec ``*`` does not cross a directory separator, so the
    filtering happens here instead of in ``git ls-files``.
    """
    scope = scope.strip("/")
    return path.startswith(scope + "/") or ("/" + scope + "/") in ("/" + path)


def java_files(scopes):
    listing = subprocess.run(["git", "ls-files", "-z", "--", "*.java"],
                             capture_output=True, text=True).stdout
    paths = [p for p in listing.split("\0") if p]
    if not scopes:
        return sorted(paths)
    return sorted(p for p in paths if any(matches(p, s) for s in scopes))


def main():
    scopes = sys.argv[1:]
    files = java_files(scopes)
    if not files:
        print("narrative scan: no tracked java files under", scopes, file=sys.stderr)
        return 2
    per_marker = {}
    per_file = {}
    hits = []
    unreadable = 0
    for path in files:
        try:
            with open(path, encoding="utf-8") as fh:
                lines = fh.read().splitlines()
        except (OSError, UnicodeDecodeError) as exc:
            print("cannot read %s: %s" % (path, exc), file=sys.stderr)
            unreadable += 1
            continue
        for no, text in enumerate(lines, 1):
            seen = set()
            for name, rx in COMPILED:
                if name in seen:
                    continue
                if rx.search(text):
                    seen.add(name)
                    hits.append((path, no, name, text.strip()))
                    per_marker[name] = per_marker.get(name, 0) + 1
                    per_file[path] = per_file.get(path, 0) + 1
    print("scanned %d tracked java files (%d scope label(s))"
          % (len(files), len(scopes) or 1))
    if unreadable:
        print("unreadable files: %d" % unreadable, file=sys.stderr)
    if hits:
        for name in sorted(per_marker, key=lambda k: -per_marker[k]):
            print("  %-22s %4d" % (name, per_marker[name]))
        print("  %-22s %4d" % ("files affected", len(per_file)))
        for path, no, name, text in hits[:20]:
            print("  %s:%d [%s] %s" % (path, no, name, text[:100]))
        if len(hits) > 20:
            print("  ... %d more" % (len(hits) - 20))
        print("NARRATIVE SCAN: FAIL (%d marker lines)" % len(hits))
        return 1
    print("NARRATIVE SCAN: PASS (0 marker lines)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
