#!/usr/bin/env python3
"""Offline docs link checker: validates every relative INLINE markdown link
(file + heading or explicit HTML anchor) under README.md, CONTRIBUTING.md,
CHANGELOG.md, and docs/.

Unsupported-but-valid Markdown fails closed rather than silently passing:
reference-style links ([text][ref] with a [ref]: destination line) and
angle-bracket destinations ([text](<file.md>)) are not resolved — this tree
uses neither, and any occurrence outside code is itself reported as a
failure so it cannot bypass validation.

Zero dependencies. Not wired into CI on purpose — run it when touching docs:

    python3 scripts/check-doc-links.py

Exits non-zero and prints one line per broken link. Anchor checking follows
GitHub's algorithm (lowercase, strip backticks/asterisks, spaces to hyphens,
dedupe with -1/-2 suffixes) and skips docs/upstream/ anchors (vendor copies).
Explicit <a name="..."> and <a id="..."> anchors retain their exact values;
anchors demonstrated in fenced, indented, or inline code do not create destinations.

Deliberately minimal: it parses the Markdown this tree actually writes.
Destinations may contain balanced (single-level) parentheses and an optional
quoted title; fences track their opening character and length per CommonMark.
"""
import os, re, sys, posixpath
from html.parser import HTMLParser

os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

# Destination: runs of non-space/non-paren/non-# chars, allowing one level of
# balanced parens; then an optional #fragment and an optional quoted title.
LINK = re.compile(
    r"\]\(((?:[^()\s#]|\([^()\s]*\))*)(#[^)\s]*)?(?:\s+[\"\x27][^)]*[\"\x27])?\)")
REFDEF = re.compile(r"^ {0,3}\[[^\]^]+\]:")
ANGLE = re.compile(r"\]\(\s*<")
FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})")
HEADING = re.compile(r"^ {0,3}(#{1,6}) +(.*)$")
BACKTICKS = re.compile(r"`+")
HTML = re.compile(
    r"<!--.*?-->|</?[A-Za-z][\w:-]*(?:\s+[^\s\"'<>/=]+"
    r"(?:\s*=\s*(?:'[^']*'|\"[^\"]*\"|[^\s\"'=<>`]+))?)*\s*/?>", re.S)
THEMATIC = re.compile(r"^ {0,3}(?:(?:\*[ \t]*){3,}|(?:-[ \t]*){3,}|(?:_[ \t]*){3,})$")
SETEXT = re.compile(r"^ {0,3}(?:=+|-+)[ \t]*$")
QUOTE = re.compile(r"^ {0,3}> ?")
ITEM = re.compile(r"^ {0,3}([-+*]|[0-9]{1,9}[.)])( +|$)")


def without_inline_code(text):
    """Mask code spans within one block, preserving line/column positions.

    Only a matching backtick run closes a span; an unmatched opener is literal.
    Raw HTML tags/comments take precedence over backticks inside their attributes.
    """
    masked = list(text)
    pos = 0
    while pos < len(text):
        if text[pos] == "\\" and text[pos:pos + 2] in ("\\\\", "\\`", "\\<"):
            if text[pos + 1] == "<":
                masked[pos + 1] = " "  # escaped markup is text, not an anchor
            pos += 2
            continue
        html = HTML.match(text, pos)
        if html:
            pos = html.end()
            continue
        opening = BACKTICKS.match(text, pos)
        if not opening:
            pos += 1
            continue
        closing = next((run for run in BACKTICKS.finditer(text, opening.end())
                        if run.group() == opening.group()), None)
        if closing is None:
            pos = opening.end()
            continue
        for i in range(pos, closing.end()):
            if masked[i] != "\n":
                masked[i] = " "
        pos = closing.end()
    return "".join(masked)


class ExplicitAnchors(HTMLParser):
    """Collect anchor attributes while letting HTMLParser skip HTML comments."""

    def __init__(self):
        super().__init__()
        self.anchors = set()

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for name, value in attrs:
                if name in ("name", "id") and value:
                    self.anchors.add(value)


def gh_anchor(h):
    h = re.sub(r"[`*]", "", h.strip().lower())
    h = re.sub(r"[^\w\- ]", "", h, flags=re.UNICODE)
    return h.replace(" ", "-")


def scan_blocks(path):
    """Yield numbered lines of each non-code block before parsing inline spans.

    Indented code cannot interrupt a paragraph. Tabs use four-column stops;
    list and blockquote prefixes are removed before classifying their contents.
    Fences and blank lines end paragraphs, so inline spans cannot cross them.
    Keep multiline HTML comments together even when they contain blank lines.
    """
    with open(path) as source:
        yield from markdown_blocks(list(enumerate(source, 1)))


def drop_columns(line, count):
    """Remove container indentation without changing tabs inside HTML values."""
    pos = column = 0
    while pos < len(line) and (column < count or line[pos] in " \t"):
        column += 4 - column % 4 if line[pos] == "\t" else 1
        pos += 1
    return " " * max(0, column - count) + line[pos:]


def starts_block(line):
    return (FENCE.match(line) or HEADING.match(line) or QUOTE.match(line)
            or ITEM.match(line) or THEMATIC.match(line.rstrip("\n"))
            or re.match(r"^ {0,3}<!--", line))


def markdown_blocks(lines):
    fence = None  # (char, length) of the open fence
    block = []
    comment = False
    index = 0
    while index < len(lines):
        lineno, ln = lines[index]
        index += 1
        expanded = ln.expandtabs(4)
        if comment:
            block.append((lineno, ln))
            if "-->" in ln:
                comment = False
                yield block
                block = []
            continue
        m = FENCE.match(expanded)
        if fence:
            if m and m.group(1)[0] == fence[0] and len(m.group(1)) >= fence[1] and not expanded[m.end():].strip():
                fence = None
            continue
        if not ln.strip():
            if block:
                yield block
                block = []
            continue
        # Four-space indentation starts a code block only outside a paragraph.
        if expanded.startswith("    ") and not block:
            continue
        if re.match(r"^ {0,3}</?[A-Za-z]", expanded):
            # A multiline HTML tag can end on a line beginning with '>' or
            # contain indented attributes. Those lines are not new containers.
            tail = "".join(text for _, text in lines[index - 1:])
            html = HTML.match(tail, len(ln) - len(ln.lstrip(" \t")))
            extra_lines = html.group().count("\n") if html else 0
            if extra_lines:
                block.extend(lines[index - 1:index + extra_lines])
                index += extra_lines
                continue
        if (HEADING.match(expanded) or THEMATIC.match(expanded.rstrip("\n"))
                or (block and SETEXT.match(expanded.rstrip("\n")))):
            if block:
                yield block
                block = []
            yield [(lineno, ln)]
            continue
        quote, item = QUOTE.match(expanded), ITEM.match(expanded)
        # Empty items and non-1 ordered items cannot interrupt a paragraph.
        if item and block and (not expanded[item.end():].strip()
                               or (item.group(1)[0].isdigit() and item.group(1)[:-1] != "1")):
            item = None
        if quote or item:
            if block:
                yield block
                block = []
            prefix = quote.end() if quote else item.end()
            if item and len(item.group(2)) > 4:
                prefix -= len(item.group(2)) - 1
            body = [(lineno, drop_columns(ln, prefix))]
            while index < len(lines):
                number, continuation = lines[index]
                expanded_next = continuation.expandtabs(4)
                marker = QUOTE.match(expanded_next) if quote else None
                if marker:
                    body.append((number, drop_columns(continuation, marker.end())))
                elif not quote and not continuation.strip():
                    body.append((number, continuation))
                elif not quote and expanded_next.startswith(" " * prefix):
                    body.append((number, drop_columns(continuation, prefix)))
                elif (body[-1][1].strip() and continuation.strip()
                      and not starts_block(expanded_next)
                      and any(part[-1][0] == body[-1][0]
                              for part in markdown_blocks(body))):
                    # Lazy paragraph continuations retain the container's context.
                    # A code block cannot have a lazy, unmarked continuation.
                    body.append((number, continuation))
                else:
                    break
                index += 1
            yield from markdown_blocks(body)
            continue
        if m and not (m.group(1)[0] == "`" and "`" in expanded[m.end():]):
            if block:
                yield block
                block = []
            marker = m.group(1)
            fence = (marker[0], len(marker))
            continue
        if re.match(r"^ {0,3}<!--", expanded):
            if block:
                yield block
            block = [(lineno, ln)]
            comment = "-->" not in ln
            if not comment:
                yield block
                block = []
            continue
        block.append((lineno, ln))
    if block:
        yield block


def scan_lines(path):
    for block in scan_blocks(path):
        masked = without_inline_code("".join(ln for _, ln in block)).splitlines(keepends=True)
        for (lineno, raw), visible in zip(block, masked):
            yield lineno, raw, visible


def check():
    md = ["README.md", "CONTRIBUTING.md", "CHANGELOG.md"]
    for dirpath, dirs, files in os.walk("docs"):
        for f in files:
            if f.endswith(".md"):
                md.append(os.path.join(dirpath, f))
    anchors = {}
    for path in md:
        aset = set()
        for block in scan_blocks(path):
            explicit = ExplicitAnchors()
            explicit.feed(without_inline_code("".join(ln for _, ln in block)))
            explicit.close()
            # Keep HTML parsing within a block, so omitted code cannot join tags.
            aset.update(explicit.anchors)
        headings = set()
        for _, ln, _ in scan_lines(path):
            m = HEADING.match(ln)
            if m:
                # strip an optional ATX closing sequence ("## Title ##")
                base = gh_anchor(re.sub(r"\s+#+\s*$", "", m.group(2)))
                a, i = base, 1
                while a in headings:
                    a = f"{base}-{i}"; i += 1
                headings.add(a)
        # Explicit anchors must not change GitHub's heading duplicate suffixes.
        aset.update(headings)
        anchors[path] = aset
    bad = 0
    for path in md:
        d = posixpath.dirname(path) or "."
        for lineno, _, ln in scan_lines(path):
            if REFDEF.match(ln):
                print(f"UNSUPPORTED REF-STYLE LINK DEF {path}:{lineno} -> {ln.strip()[:60]}")
                bad += 1
                continue
            if ANGLE.search(ln):
                print(f"UNSUPPORTED ANGLE-BRACKET DEST {path}:{lineno} -> {ln.strip()[:60]}")
                bad += 1
                continue
            # fail closed on any ]( this parser cannot consume on one line
            # (e.g. a link whose destination/title spans a line ending)
            if "](" in LINK.sub("", ln):
                print(f"UNSUPPORTED UNPARSED LINK {path}:{lineno} -> {ln.strip()[:60]}")
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
                if resolved == ".." or resolved.startswith("../"):
                    print(f"BROKEN FILE (escapes repo) {path}:{lineno} -> {target}")
                    bad += 1
                    continue
                if not os.path.exists(resolved):
                    print(f"BROKEN FILE {path}:{lineno} -> {target}")
                    bad += 1
                elif anchor and anchor != "#" and resolved.endswith(".md") and not resolved.startswith("docs/upstream"):
                    if anchor[1:] not in anchors.get(resolved, set()):
                        print(f"BROKEN ANCHOR {path}:{lineno} -> {target}{anchor}")
                        bad += 1
    print("broken links:", bad)
    return bad


if __name__ == "__main__":
    sys.exit(1 if check() else 0)
