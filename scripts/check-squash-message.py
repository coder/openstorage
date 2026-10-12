#!/usr/bin/env python3
"""Reject squash messages whose body release-please would misread (#405).

Usage: check-squash-message.py [FILE ...]   (default: stdin)

Each input is one squash message: the subject, a blank line, then the body.
Exit status 1 means a finding; each finding names the body line. The rules
mirror release-please 17.6.0 (release-please-action v5.0.0) and
@conventional-commits/parser 0.4.1: recheck them when that action is bumped.
docs/how-to/release.md lists them.
"""
import os
import re
import sys

# The parser's whitespace set: TAB VT FF SP NBSP ZWNBSP.
WS = "\t\v\f \u00a0\ufeff"
# The parser's <type>: any characters except parens, whitespace, CR, LF, "!" and ":".
TYPE = rf"[^(){WS}\r\n!:]+"
VISIBLE = {"feat", "fix", "perf", "revert"}  # release-please's shown CHANGELOG sections
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
SPLIT = re.compile(r"^(?:feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert)(?:\(.*?\))?: ")
BREAKING = re.compile(r"BREAKING[ -]CHANGE(?::| #)")
# An empty breaking-change footer clears the breaking mark, even from a "!" title.
EMPTY_BREAKING = re.compile(rf"^BREAKING[ -]CHANGE(?::| #)[{WS}]*$")
BANG_SUBJECT = re.compile(r"^[^(:]+(?:\([^)]*\))?!:")
EXPLICIT_COMMITS = ("BEGIN_COMMIT_OVERRIDE", "BEGIN_NESTED_COMMIT")


def footer_finding(line):
    """Return why release-please turns a footer-shaped line into a commit, or None."""
    footer = FOOTER.match(line)
    if not footer:
        return None
    kind, scope, bang, separator, value = footer.groups()
    if kind.lower() == "release-as":
        return "this footer sets the next release version. Remove it."
    glued = kind + (scope or "") + (bang or "") + (": " if separator == ":" else "") + value
    # Check the line as written too: a subject's separator may also be " #".
    for text in (glued, line):
        commit = SUBJECT.match(text)
        if commit and (commit.group(1) in VISIBLE or commit.group(2)):
            return ("release-please reads this line as another commit and adds a CHANGELOG "
                    "entry for it. Reword or rewrap it.")
    return None


def findings(message):
    """Return (body line number, reason) pairs for one squash message."""
    assert isinstance(message, str), type(message)
    # The parser splits lines on CR as well as LF.
    lines = re.split(r"\r\n|\r|\n", message.strip())
    subject, body = lines[0], lines[1:]
    if body and body[0] == "":
        body = body[1:]  # The separator line: body line 1 is the PR body's line 1.
    # Explicit release-please commit blocks are deliberate: only the
    # parse-failure rule applies.
    explicit = any(marker in message for marker in EXPLICIT_COMMITS)
    out = []
    after_footer = False
    after_blank = True  # Body line 1 follows the subject's blank line.
    for number, line in enumerate(body, start=1):
        stripped = line.lstrip(WS)
        # An indented line after a footer-shaped line is that footer's
        # continuation. release-please parses the footer as a commit of its
        # own, with the continuation text unindented as its body.
        continuation = after_footer and stripped != line
        if line == "" or continuation:
            pass  # Blank lines and continuations keep the footer context.
        else:
            after_footer = bool(FOOTER.match(line)) and not BREAKING.match(line)
        paragraph_start, after_blank = after_blank, line == ""
        reason = None
        if not continuation and UNPARSEABLE.search(line):
            reason = ("release-please cannot parse this line and drops the whole commit. "
                      "Reword or rewrap it so it does not start with a word followed by an "
                      "unclosed '('.")
        elif explicit:
            pass
        elif paragraph_start and SPLIT.match(line):
            reason = ("release-please splits the message here and reads the rest as another "
                      "commit. Reword or rewrap the line.")
        elif EMPTY_BREAKING.match(line):
            reason = ("release-please ignores an empty breaking-change footer and drops the "
                      "breaking mark. Put the description on the same line.")
        elif continuation and BREAKING.search(line):
            reason = ("release-please reads this line as a separate breaking change. "
                      "Reword it or move it out of the indented block.")
        else:
            reason = footer_finding(stripped if continuation else line)
            if not reason and BREAKING.search(line) and not BANG_SUBJECT.match(subject):
                reason = ("release-please marks the release breaking because of this line, "
                          "but the title has no '!'. Add '!' to the title or reword the line.")
        if reason:
            out.append((number, reason))
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
            if actions:
                print(f"::error title=Squash body line {number}::{reason}")
            else:
                print(f"{source}: body line {number}: {reason}")
    if not failed:
        print("OK: release-please reads the squash message(s) as intended.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
