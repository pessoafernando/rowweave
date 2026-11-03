#!/usr/bin/env python3
"""Fail when any tracked file carries a Chinese comment or Chinese prose.

A first-time contributor must be able to read this repository without Chinese.
The canonical language is English everywhere: in comments, in javadoc, in
resource text, in test literals, in documentation and in the gate scripts
themselves. So the default scope is every tracked file, with no exception list,
because an exception list is where a Chinese comment goes to hide.

The scan is deliberately narrower than "every non-ASCII byte". Non-CJK
characters are legitimate content and are left alone: an em dash, a set of
Greek grouping keys, an accented place name, or an emoji carried by a
round-trip fixture. Only Han ideographs and CJK punctuation are rejected,
which is what a Chinese comment or a Chinese string is made of.

Test data that exists to prove multi-byte width is not exempt either, but it
does not have to be weakened: the same character written as an escape sequence
is byte-identical at runtime, so the assertion still proves the width while the
source carries no CJK code point.

usage: oss_source_language_gate.py [SCOPE ...]
  without SCOPE: every tracked file in the repository
  with SCOPE: only paths under the named directories or the named files,
              which is how one large file is checked while it is translated
"""
import os
import subprocess
import sys
import tempfile

CJK_RANGES = (
    (0x3000, 0x303F),     # CJK punctuation: ideographic comma, full stop, brackets
    (0x3400, 0x4DBF),     # extension A
    (0x4E00, 0x9FFF),     # unified ideographs
    (0xF900, 0xFAFF),     # compatibility ideographs
    (0xFE30, 0xFE4F),     # CJK compatibility forms
    (0xFF00, 0xFFEF),     # halfwidth and fullwidth forms
    (0x20000, 0x2A6DF),   # extension B
)


def cjk_positions(text):
    """Return the offsets of CJK code points in TEXT, as a list."""
    found = []
    for index, char in enumerate(text):
        code = ord(char)
        if code < 0x3000:
            continue
        for low, high in CJK_RANGES:
            if low <= code <= high:
                found.append(index)
                break
    return found


def tracked_files():
    listing = subprocess.run(["git", "ls-files", "-z"],
                             capture_output=True, text=True).stdout
    return [p for p in listing.split("\0") if p]


def in_scope(path, scopes):
    """True when PATH lies under one of SCOPES, or when no scope filter was given.

    A scope may name a file as well as a directory, which is how a single large
    class is checked while it is being translated. With no scope given every
    tracked file is in scope, which is the whole point of the gate.
    """
    if not scopes:
        return True
    normalised = path.replace("\\", "/")
    return any(normalised.startswith(s.strip("/") + "/") or normalised == s.strip("/")
               for s in scopes)


def scan(files):
    """Return (offending rows, files read, files unreadable) for the given paths."""
    offending = []
    scanned = 0
    unreadable = 0
    for path in files:
        try:
            text = open(path, encoding="utf-8").read()
        except (OSError, UnicodeDecodeError):
            unreadable += 1
            continue
        scanned += 1
        for number, line in enumerate(text.splitlines(), 1):
            positions = cjk_positions(line)
            if positions:
                offending.append("%s:%d: CJK at column %d: %s"
                                 % (path, number, positions[0] + 1,
                                    line.strip()[:120]))
    return offending, scanned, unreadable


def positive_control():
    """Prove the detector fires before a clean result is trusted.

    A scan that reads nothing looks exactly like a scan that passes, and this
    repository has already been bitten once by a gate whose search silently read
    standard input instead of files. So the pattern set is exercised here against
    a Chinese comment planted in a temporary file outside the tree. The comment is
    written as escape sequences so that this file carries no CJK byte itself.
    """
    probe = ("/**\n"
             " * \u94c1\u5f8b\uff1a\u7981\u6b62\u7269\u5316\u884c\u96c6\u3002\n"
             " */\n"
             "final class ControlProbe {\n"
             "    private static final int probes = 1;\n"
             "}\n")
    with tempfile.TemporaryDirectory(prefix="rowweave-source-language-") as directory:
        path = os.path.join(directory, "ControlProbe.java")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(probe)
        offending, scanned, unreadable = scan([path])
    if scanned != 1 or unreadable or not offending:
        print("SOURCE LANGUAGE GATE: FAIL (the control file, which carries a known "
              "Chinese comment, produced %d CJK findings from %d file(s) read - the "
              "pattern set or the reader is broken)" % (len(offending), scanned),
              file=sys.stderr)
        return False
    return True


def main():
    scopes = list(sys.argv[1:])

    files = [p for p in tracked_files() if in_scope(p, scopes)]
    files = [p for p in files if "/target/" not in p and not p.startswith("target/")]
    if not files:
        print("SOURCE LANGUAGE GATE: FAIL (matched no tracked file - the scope "
              "is wrong, not the tree)", file=sys.stderr)
        return 2
    # The unscoped run is the one CI and the release gate call, and it covers the
    # whole repository. A file list this small means the git listing broke, and a
    # PASS built on it would be a false guarantee.
    if not scopes and len(files) < 2000:
        print("SOURCE LANGUAGE GATE: FAIL (the default scope saw only %d files - "
              "the repository tracks far more than that)" % len(files), file=sys.stderr)
        return 2

    if not positive_control():
        return 2

    offending, scanned, unreadable = scan(files)
    if unreadable:
        print("unreadable files: %d" % unreadable, file=sys.stderr)
    print("scanned %d tracked files (%s)"
          % (scanned, ", ".join(scopes) if scopes else "every tracked file"))
    if offending:
        for row in offending[:25]:
            print("  " + row)
        if len(offending) > 25:
            print("  ... %d more offending lines" % (len(offending) - 25))
        print("SOURCE LANGUAGE GATE: FAIL (%d CJK lines in %d file(s))"
              % (len(offending), len({row.split(":")[0] for row in offending})))
        return 1
    print("SOURCE LANGUAGE GATE: PASS (0 CJK lines, control file rejected)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
