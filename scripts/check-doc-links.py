#!/usr/bin/env python3
"""Offline docs link checker: validates every relative INLINE markdown link
(file + heading anchor) under README.md, CONTRIBUTING.md, CHANGELOG.md, and docs/.

Unsupported-but-valid Markdown fails closed rather than silently passing:
reference-style links ([text][ref] with a [ref]: destination line) and
angle-bracket destinations ([text](<file.md>)) are not resolved — this tree
uses neither, and any occurrence outside a code fence is itself reported as a
failure so it cannot bypass validation.

Zero dependencies. Not wired into CI on purpose — run it when touching docs:

    python3 scripts/check-doc-links.py

Exits non-zero and prints one line per broken link. Anchor checking follows
GitHub's algorithm (lowercase, strip backticks/asterisks, spaces to hyphens,
dedupe with -1/-2 suffixes) and skips docs/upstream/ anchors (vendor copies).

Deliberately minimal: it parses the Markdown this tree actually writes.
Destinations may contain balanced (single-level) parentheses and an optional
quoted title; fences track their opening character and length per CommonMark.
"""
import os, re, sys, posixpath

os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

# Destination: runs of non-space/non-paren/non-# chars, allowing one level of
# balanced parens; then an optional #fragment and an optional quoted title.
LINK = re.compile(
    r"\]\(((?:[^()\s#]|\([^()\s]*\))*)(#[^)\s]*)?(?:\s+[\"\x27][^)]*[\"\x27])?\)")
REFDEF = re.compile(r"^ {0,3}\[[^\]^]+\]:\s")
ANGLE = re.compile(r"\]\(\s*<")
FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})")
HEADING = re.compile(r"^ {0,3}(#+) +(.*)$")


def gh_anchor(h):
    h = re.sub(r"[`*]", "", h.strip().lower())
    h = re.sub(r"[^\w\- ]", "", h, flags=re.UNICODE)
    return h.replace(" ", "-")


def scan_lines(path):
    """Yield (lineno, line) for lines outside fenced code blocks, tracking the
    opening fence's character and length so only a compatible closer ends it."""
    fence = None  # (char, length) of the open fence
    for lineno, ln in enumerate(open(path), 1):
        m = FENCE.match(ln)
        if m:
            marker = m.group(1)
            if fence is None:
                fence = (marker[0], len(marker))
                continue
            if marker[0] == fence[0] and len(marker) >= fence[1]:
                fence = None
                continue
            # a shorter/other-char fence inside an open block is content
        if fence is None:
            yield lineno, ln


def check():
    md = ["README.md", "CONTRIBUTING.md", "CHANGELOG.md"]
    for dirpath, dirs, files in os.walk("docs"):
        for f in files:
            if f.endswith(".md"):
                md.append(os.path.join(dirpath, f))
    anchors = {}
    for path in md:
        aset = set()
        for _, ln in scan_lines(path):
            m = HEADING.match(ln)
            if m:
                base = gh_anchor(m.group(2))
                a, i = base, 1
                while a in aset:
                    a = f"{base}-{i}"; i += 1
                aset.add(a)
        anchors[path] = aset
    bad = 0
    for path in md:
        d = posixpath.dirname(path) or "."
        for lineno, ln in scan_lines(path):
            if REFDEF.match(ln):
                print(f"UNSUPPORTED REF-STYLE LINK DEF {path}:{lineno} -> {ln.strip()[:60]}")
                bad += 1
                continue
            if ANGLE.search(ln):
                print(f"UNSUPPORTED ANGLE-BRACKET DEST {path}:{lineno} -> {ln.strip()[:60]}")
                bad += 1
                continue
            for m in LINK.finditer(ln):
                target, anchor = m.group(1), m.group(2)
                if re.match(r"^[a-z]+:", target) or target.startswith("/"):
                    continue
                if not target and not anchor:
                    continue
                # fragment-only links resolve to the current file
                resolved = path if not target else posixpath.normpath(posixpath.join(d, target))
                if not os.path.exists(resolved):
                    print(f"BROKEN FILE {path}:{lineno} -> {target}")
                    bad += 1
                elif anchor and anchor != "#" and resolved.endswith(".md") and not path.startswith("docs/upstream"):
                    if anchor[1:] not in anchors.get(resolved, set()):
                        print(f"BROKEN ANCHOR {path}:{lineno} -> {target}{anchor}")
                        bad += 1
    print("broken links:", bad)
    return bad


if __name__ == "__main__":
    sys.exit(1 if check() else 0)
