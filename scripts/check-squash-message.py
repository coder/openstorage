#!/usr/bin/env python3
"""Reject squash messages whose body release-please would misread (#405).

Usage: check-squash-message.py [FILE ...]  (default: stdin). Each input is one
squash message (subject, blank line, body). Exit 1 means a finding. The rules
mirror release-please 17.6.0 (release-please-action v5.0.0) and its
@conventional-commits/parser 0.4.1: recheck them when that action is bumped.
"""
import os
import re
import sys

# The parser's whitespace set: TAB VT FF SP NBSP ZWNBSP.
WS = "\t\v\f \u00a0\ufeff"
# The parser's <type>: any characters except parens, whitespace, CR, LF, "!" and ":".
TYPE = rf"[^(){WS}\r\n!:]+"
# Types that release-please shows in the CHANGELOG or that bump the version.
VISIBLE = {"feat", "feature", "fix", "perf", "revert"}
# At every body line the parser tries a footer token `type(scope)`. A "(" with
# no ")" before the next "(" or the line end makes it throw, and release-please
# then drops the whole commit (#323, #362).
UNPARSEABLE = re.compile(rf"^{TYPE}\([^()]*(?:\(|$)")
# A footer is `type(scope)!: value` or `type #value`. release-please glues the
# parts back together (the " #" separator is dropped) and, when that parses as
# a commit subject, adds it as another commit (#270 listed twice).
FOOTER = re.compile(rf"^({TYPE})(\([^()]*\))?(!)?(:| #)[{WS}]*(.*)$")
SUBJECT = re.compile(rf"^({TYPE})(?:\([^()]*\))?(!)?(?::| #)")
# release-please first splits a message before any paragraph that starts like
# a subject of these types, and parses each part as its own commit.
KINDS = "(?:feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert)"
SPLIT = re.compile(rf"^{KINDS}(?:\(.*?\))?: ")
BREAKING = re.compile(r"BREAKING[ -]CHANGE(?::| #)")
# An empty breaking-change footer clears the breaking mark, even from a "!" title.
EMPTY_BREAKING = re.compile(rf"^BREAKING[ -]CHANGE(?::| #)[{WS}]*$")
BANG_SUBJECT = re.compile(r"^[^(:]+(?:\([^)]*\))?!:")
BLOCK_SUBJECT = re.compile(rf"^{KINDS}(?:\([^()]*\))?!?: \S")
JS_TRIM = re.compile(r"^[\s\ufeff]+")  # JS trim() strips every Unicode space.
# release-please reads its markers anywhere, even mid-line, and an unclosed
# block runs to the end of the message.
MARKERS = {"BEGIN_COMMIT_OVERRIDE": "END_COMMIT_OVERRIDE",
           "BEGIN_NESTED_COMMIT": "END_NESTED_COMMIT"}


def footer_finding(line):
    """Return why release-please turns a footer-shaped line into a commit, or None."""
    footer = FOOTER.match(line)
    if not footer:
        return None
    kind, scope, bang, separator, value = footer.groups()
    if kind.lower() == "release-as":
        return "this footer sets the next release version. Remove it."
    glued = kind + (scope or "") + (bang or "") + (": " if separator == ":" else "") + value
    # The parser trims each footer commit, so an EM SPACE before "fix:" vanishes.
    # Check the line as written too: a subject's separator may also be " #".
    for text in (glued, line):
        commit = SUBJECT.match(JS_TRIM.sub("", text))
        if commit and (commit.group(1) in VISIBLE or commit.group(2)):
            return "release-please reads this line as another commit. Reword or rewrap it."
    return None


def findings(message):
    """Return (body line number, reason) pairs for one squash message."""
    assert isinstance(message, str), type(message)
    # The parser splits lines on CR as well as LF.
    lines = re.split(r"\r\n|\r|\n", message.strip())
    subject, body = lines[0], lines[1:]
    if body and body[0] == "":
        body = body[1:]  # The separator line: body line 1 is the PR body's line 1.
    out = []
    closing = None  # Inside an explicit commit block: its END marker.
    block_start = False  # The block's first line must be a commit subject.
    after_footer = False
    after_blank = True  # Body line 1 follows the subject's blank line.
    for number, line in enumerate(body, start=1):
        stripped = line.lstrip(WS)
        # An indented line after a footer-shaped line continues that footer, and
        # release-please parses the footer as a commit with the text unindented.
        continuation = after_footer and stripped != line
        if line and not continuation:  # Blank lines keep the footer context.
            after_footer = bool(FOOTER.match(line)) and not BREAKING.match(line)
        paragraph_start, after_blank = after_blank, line == ""
        reason = None
        marker = next((m for m in [*MARKERS, *MARKERS.values()] if m in line), None)
        if marker:
            if line.strip() != marker:
                reason = f"release-please reads {marker} even mid-line. Put it on its own line."
            if closing is None:
                closing = MARKERS.get(marker)  # A stray END marker opens nothing.
                block_start = closing is not None
            elif marker == closing:
                closing, block_start = None, False
        elif block_start and line.strip():
            block_start = False
            if not BLOCK_SUBJECT.match(line):
                reason = "release-please drops a commit block that does not start with a subject."
        elif not continuation and UNPARSEABLE.search(line):
            reason = ("release-please cannot parse a line that starts with a word and an "
                      "unclosed '(', and drops the whole commit. Reword or rewrap it.")
        elif closing:
            pass  # Inside an explicit commit block, only the parse failure matters.
        elif paragraph_start and SPLIT.match(line):
            reason = "release-please splits the message here into another commit. Reword it."
        elif EMPTY_BREAKING.match(line):
            reason = "an empty breaking-change footer clears the breaking mark. Add its text."
        elif continuation and BREAKING.search(line):
            reason = "release-please reads this line as a separate breaking change. Reword it."
        else:
            reason = footer_finding(stripped if continuation else line)
            if not reason and BREAKING.search(line) and not BANG_SUBJECT.match(subject):
                reason = "this line marks the release breaking, but the title has no '!'."
        if reason:
            out.append((number, reason))
    if closing:
        out.append((len(body), f"the explicit commit block has no {closing} line."))
    return out


def main(argv):
    actions = os.environ.get("GITHUB_ACTIONS") == "true"
    failed = False
    for source in argv[1:] or ["-"]:
        with open(0 if source == "-" else source, encoding="utf-8") as handle:
            message = handle.read()
        for number, reason in findings(message):
            failed = True
            # Never echo body text into a workflow command: it is untrusted.
            print(f"::error title=Squash body line {number}::{reason}" if actions
                  else f"{source}: body line {number}: {reason}")
    if not failed:
        print("OK: release-please reads the squash message(s) as intended.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
