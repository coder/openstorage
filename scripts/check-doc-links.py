#!/usr/bin/env python3
"""Offline docs link checker: validates every relative markdown link (file +
heading anchor) under README.md, CONTRIBUTING.md, CHANGELOG.md, and docs/.

Zero dependencies. Not wired into CI on purpose — run it when touching docs:

    python3 scripts/check-doc-links.py

Exits non-zero and prints one line per broken link. Anchor checking follows
GitHub's algorithm (lowercase, strip backticks/asterisks, spaces to hyphens,
dedupe with -1/-2 suffixes) and skips docs/upstream/ anchors (vendor copies).
"""
import os, re, sys, posixpath

os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

LINK = re.compile(r"\]\(([^)\s#]*)(#[^)\s]*)?\)")
FENCE = re.compile(r"^ {0,3}(```|~~~)")

HEADING = re.compile(r"^#+ +(.*)$")
def gh_anchor(h):
    h = re.sub(r"[`*]", "", h.strip().lower())
    h = re.sub(r"[^\w\- ]", "", h, flags=re.UNICODE)
    return h.replace(" ", "-")

def check():
    md = ["README.md", "CONTRIBUTING.md", "CHANGELOG.md"]
    for dirpath, dirs, files in os.walk("docs"):
        for f in files:
            if f.endswith(".md"):
                md.append(os.path.join(dirpath, f))
    anchors = {}
    for path in md:
        aset = set()
        in_code = False
        for ln in open(path):
            if FENCE.match(ln):
                in_code = not in_code
                continue
            if in_code: continue
            m = HEADING.match(ln)
            if m:
                base = gh_anchor(m.group(1))
                a, i = base, 1
                while a in aset:
                    a = f"{base}-{i}"; i += 1
                aset.add(a)
        anchors[path] = aset
    bad = 0
    for path in md:
        if path.startswith("docs/upstream"):  # vendor copies: check file targets only
            pass
        d = posixpath.dirname(path) or "."
        in_code = False
        for lineno, ln in enumerate(open(path), 1):
            if FENCE.match(ln):
                in_code = not in_code
                continue
            if in_code: continue
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
