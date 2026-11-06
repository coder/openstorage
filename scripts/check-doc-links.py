#!/usr/bin/env python3
"""Offline checker for the relative links and code citations in this repository's docs.

    python3 scripts/check-doc-links.py

Reads the Markdown files that git tracks or that are untracked and not
ignored (`git ls-files`), minus vendor/ and files deleted from the working
tree: every README.md, the root CONTRIBUTING.md and CHANGELOG.md, and every
.md file under docs/. Prints one line per failure, then `broken links: N`.
Exits 1 when N > 0. Exits 2, without checking anything, when git cannot list
the files (for example, outside a checkout). Zero dependencies besides git.
CI runs it in the required lint job of .github/workflows/ci.yml.

Every relative INLINE link must name an existing file that git's index
tracks, or a directory that holds one. Stage a new target with `git add`
before you link to it: a clean CI checkout holds only committed files, and an
untracked or intent-to-add (`git add -N`) target would pass here and fail there. A #fragment on
a .md target must name one of that file's anchors, even when the target is not
a scanned file. Anchors of docs/upstream/ targets are not checked, because those
files are verbatim upstream copies. Anchor
checking follows GitHub's algorithm (lowercase, strip backticks/asterisks,
spaces to hyphens, dedupe with -1/-2 suffixes). Explicit <a name="..."> and
<a id="..."> anchors retain their exact values; anchors demonstrated in
fenced, indented, or inline code do not create destinations.

In a README.md, [path:symbol] or [path] cites code when path ends in a
letter-led extension or names a Dockerfile, and no "(" or "[" follows the
bracket. path is relative to the README's directory and must resolve, after
symlinks, to a tracked file inside the repository. symbol must be a name that the
file's LANGUAGES reader returns. The Go, Terraform, Python, and JavaScript
readers return declarations only, with comments and string literals blanked
first. The shell reader accepts any whole-token phrase outside comments. An
empty symbol, a line number ([path:12], [path:3-9]), or a symbol in a file
kind with no reader fails. Citations count inside code spans, not inside
fenced or indented code. The Go reader assumes gofmt layout. A reader that
misses a rare declaration form fails closed: cite another name or the file.
The JavaScript reader lexes the file and stops reading where it cannot be
sure of its place, for example at a `/` that may open a regex literal or
divide. A name below that line fails, and the message gives the reason.

Unsupported-but-valid Markdown fails closed rather than silently passing:
reference-style links ([text][ref] with a [ref]: destination line) and
angle-bracket destinations ([text](<file.md>)) are not resolved — this tree
uses neither, and any occurrence outside code is itself reported as a
failure so it cannot bypass validation.

Deliberately minimal: it parses the Markdown this tree actually writes.
Destinations may contain balanced (single-level) parentheses and an optional
quoted title; fences track their opening character and length per CommonMark.
"""
import functools, os, posixpath, re, subprocess, sys
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


def git_files(*args, command=("ls-files",)):
    try:
        listed = subprocess.run(["git", *command, "-z", *args], capture_output=True)
        failure = listed.returncode and (listed.stderr.decode(errors="replace").strip()
                                         or f"git {command[0]} exited {listed.returncode}")
    except OSError as err:
        failure = str(err)
    if failure:
        print(f"check-doc-links.py needs a git checkout: {failure}", file=sys.stderr)
        sys.exit(2)
    return {os.fsdecode(name) for name in listed.stdout.split(b"\0") if name}


def doc_files():
    # Untracked files that .gitignore does not hide are read too, so a new
    # README is checked before `git add`. A clean CI checkout has none.
    names = git_files("--cached", "--others", "--exclude-standard", "--", "*.md")
    return sorted(name for name in names if not name.startswith("vendor/") and os.path.isfile(name))


def repo_path(path):
    """The path relative to the repository root after symlinks resolve, or None outside it."""
    root, real = os.path.realpath(os.curdir), os.path.realpath(path)
    return os.path.relpath(real, root) if os.path.commonpath([root, real]) == root else None


def indexed(path):
    """True when git's index holds the file that path names and every symlink that resolving it follows.

    A clean checkout has no untracked symlink, so a path through one breaks there even when it resolves to a
    tracked file. Each symlink is followed by hand, so a symlink that another symlink points at is checked too.
    Both callers check that the path exists first, so the OS has already refused a symlink loop.
    """
    pending, current = path.split("/")[::-1], "."
    while pending:
        name = pending.pop()
        if name in ("", "."):
            continue
        if name == "..":
            if current == ".":
                return False
            current = posixpath.dirname(current) or "."
            continue
        entry = posixpath.normpath(posixpath.join(current, name))
        if not os.path.islink(entry):
            current = entry
            continue
        target = os.readlink(entry)
        if entry not in tracked() or posixpath.isabs(target):
            return False
        pending.extend(reversed(target.split("/")))
    return current in tracked()


def intent_to_add():
    """The paths that `git add -N` put in the index without content: a commit holds none of them."""
    return git_files("--name-only", "--diff-filter=A", command=("diff-files",))


@functools.cache
def tracked():
    """The paths in git's index, minus intent-to-add entries, and every directory that holds one."""
    files = git_files() - intent_to_add()
    found = set(files) | {"."}
    for name in files:
        parent = posixpath.dirname(name)
        while parent and parent not in found:
            found.add(parent)
            parent = posixpath.dirname(parent)
    return found


def is_readme(path):
    return posixpath.basename(path) == "README.md"


def links_apply(path):
    return is_readme(path) or path in ("CONTRIBUTING.md", "CHANGELOG.md") or path.startswith("docs/")


@functools.cache
def anchors(path):
    found = set()
    for block in scan_blocks(path):
        explicit = ExplicitAnchors()
        explicit.feed(without_inline_code("".join(ln for _, ln in block)))
        explicit.close()
        # Keep HTML parsing within a block, so omitted code cannot join tags.
        found.update(explicit.anchors)
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
    return found | headings


def link_failures(path, lineno, ln):
    d = posixpath.dirname(path) or "."
    if REFDEF.match(ln):
        return [f"UNSUPPORTED REF-STYLE LINK DEF {path}:{lineno} -> {ln.strip()[:60]}"]
    if ANGLE.search(ln):
        return [f"UNSUPPORTED ANGLE-BRACKET DEST {path}:{lineno} -> {ln.strip()[:60]}"]
    # fail closed on any ]( this parser cannot consume on one line
    # (e.g. a link whose destination/title spans a line ending)
    if "](" in LINK.sub("", ln):
        return [f"UNSUPPORTED UNPARSED LINK {path}:{lineno} -> {ln.strip()[:60]}"]
    failures = []
    for m in LINK.finditer(ln):
        target, anchor = m.group(1), m.group(2)
        if re.match(r"^[a-z]+:", target) or target.startswith("/"):
            continue
        if not target and not anchor:
            continue
        # fragment-only links resolve to the current file
        resolved = path if not target else posixpath.normpath(posixpath.join(d, target))
        if resolved == ".." or resolved.startswith("../"):
            failures.append(f"BROKEN FILE (escapes repo) {path}:{lineno} -> {target}")
        elif not os.path.exists(resolved):
            failures.append(f"BROKEN FILE {path}:{lineno} -> {target}")
        elif target and not indexed(resolved):
            kind = "escapes repo" if repo_path(resolved) is None else "not tracked by git"
            failures.append(f"BROKEN FILE ({kind}) {path}:{lineno} -> {target}")
        elif (anchor and anchor != "#" and resolved.endswith(".md")
              and not resolved.startswith("docs/upstream") and anchor[1:] not in anchors(resolved)):
            failures.append(f"BROKEN ANCHOR {path}:{lineno} -> {target}{anchor}")
    return failures


def masked(text, lexer):
    # Same length and newlines as text, so line, column, and offset survive.
    # Quoted literals keep their quotes, so go_names and tf_names can find a
    # flag name or block label here and read its value from text.
    def blank(m):
        token, keep = m.group(), 1 if m.groupdict().get("quoted") else 0
        return token[:keep] + re.sub(r"[^\n]", " ", token[keep:len(token) - keep]) + token[len(token) - keep:]
    return lexer.sub(blank, text)


GO_LEXER = re.compile(r"(?P<blank>//[^\n]*|/\*.*?\*/|`[^`]*`|'(?:\\.|[^'\\\n])*')"
                      r'|(?P<quoted>"(?:\\.|[^"\\\n])*")', re.S)
TF_LEXER = re.compile(r"(?P<blank>#[^\n]*|//[^\n]*|/\*.*?\*/"
                      r"|<<(?P<dash>-)?(?P<tag>[\w-]+)[^\n]*\n.*?^(?(dash)[ \t]*)(?P=tag)[ \t]*$)"
                      r'|(?P<quoted>"(?:\\.|\$\{(?:"(?:\\.|[^"\\])*"|[^"}])*\}|[^"\\\n])*")', re.S | re.M)
PY_LEXER = re.compile(r"(?P<blank>#[^\n]*|\"\"\"(?:\\.|[^\\])*?\"\"\"|'''(?:\\.|[^\\])*?'''"
                      r"|\"(?:\\.|[^\"\\\n])*\"|'(?:\\.|[^'\\\n])*')", re.S)


def go_names(text):
    # Assumes gofmt layout: declarations in column 0, members one tab in.
    code = masked(text, GO_LEXER)
    names = set()
    owner, close, member = None, None, None
    group, group_close = None, None
    for line in code.split("\n"):
        line = line.rstrip()
        if owner:
            if line == close:
                owner = None
            elif "|" not in line and not line.lstrip().startswith("~"):
                m = (re.match(member + r"(\w+(?:, \w+)*)(?:\(| )", line)
                     or re.match(member + r"\*?(?:\w+\.)?(\w+)(?:\[.*\])?$", line))
                if m:
                    for name in m.group(1).split(", "):
                        names |= {name, f"{owner}.{name}"}
            continue
        if group:
            if line == group_close:
                group = None
                continue
            m = re.match(r"\t(\w+(?:, \w+)*)\b", line)
            if m:
                names.update(m.group(1).split(", "))
                if group == "type" and re.search(r"\b(struct|interface) \{$", line):
                    owner, close, member = m.group(1), "\t}", r"\t\t"
            continue
        m = re.match(r"func (?:\((?:\w+ )?\*?(\w+)(?:\[[^\]]*\])?\) )?(\w+)", line)
        if m:
            names.add(m.group(2))
            if m.group(1):
                names.add(f"{m.group(1)}.{m.group(2)}")
            continue
        m = re.match(r"(type|const|var) \($", line)
        if m:
            group, group_close = m.group(1), ")"
            continue
        m = re.match(r"type (\w+)", line)
        if m:
            names.add(m.group(1))
            if re.search(r"\b(struct|interface) \{$", line):
                owner, close, member = m.group(1), "}", r"\t"
            continue
        m = re.match(r"(?:const|var) (\w+(?:, \w+)*)", line)
        if m:
            names.update(m.group(1).split(", "))
    flag_sets = {"flag"}
    for m in re.finditer(r"\b(\w+) :?= flag\.NewFlagSet\(|\b(\w+) \*flag\.FlagSet\b", code):
        flag_sets.add(m.group(1) or m.group(2))
    for m in re.finditer(r"\b(\w+)\.(?:BoolFunc|Bool|Duration|Float64|Func|Int|Int64|String|"
                         r"TextVar|Uint|Uint64|Var)(?:Var)?\((?:&[\w.]+, )?\"", code):
        flag = re.match(r'"([\w-]+)"', text[m.end() - 1:])
        if flag and m.group(1) in flag_sets:
            names.add(f"flag -{flag.group(1)}")
    return names


def py_names(text):
    names, scopes, depth = set(), [], 0
    for line, source in zip(masked(text, PY_LEXER).split("\n"), text.split("\n")):
        line = line.rstrip()
        stripped = line.lstrip()
        continued = depth > 0
        depth += len(re.findall(r"[(\[{]", line)) - len(re.findall(r"[)\]}]", line))
        if continued or not stripped:
            continue
        indent = len(line) - len(stripped)
        while scopes and indent <= scopes[-1][0]:
            scopes.pop()
        m = re.match(r"(?:async )?(def|class) (\w+)", stripped)
        if m:
            kind, name = m.groups()
            in_class = scopes and scopes[-1][2]
            qualified = f"{scopes[-1][1]}.{name}" if in_class else name
            names |= {name, qualified}
            scopes.append((indent, qualified, kind == "class"))
            continue
        if indent:
            continue
        m = re.match(r"(\w+(?:\s*,\s*\w+)*)\s*(?::[^=]*)?=(?!=)", line)
        if m:
            names.update(re.split(r"\s*,\s*", m.group(1)))
        m = re.match(r"([A-Za-z_][\w.]*)\(", line)
        if m:
            names.add(m.group(1))
        if line.startswith("if __name__ == ") and re.match(r"if __name__ == [\"']__main__[\"']", source):
            names.add("__main__")
    return names


def tf_names(text):
    names, depth, top = set(), 0, None
    for line, source in zip(masked(text, TF_LEXER).split("\n"), text.split("\n")):
        block = re.match(r'\s*([\w-]+)((?:\s+"[^"]*")*)\s*\{', line)
        if block:
            labels = re.findall(r'"([^"]*)"', source[block.start(2):block.end(2)])
            kind = block.group(1)
            if depth == 0:
                if kind == "resource" and len(labels) == 2:
                    top = ".".join(labels)
                    names |= {labels[0], top}
                elif kind in ("data", "ephemeral") and len(labels) == 2:
                    top = f"{kind}." + ".".join(labels)
                    names.add(top)
                elif kind in ("variable", "output") and len(labels) == 1:
                    top = None
                    names |= {labels[0], f"{'var' if kind == 'variable' else 'output'}.{labels[0]}"}
                elif kind in ("provider", "module") and len(labels) == 1:
                    top = None
                    names.add(f"{kind}.{labels[0]}")
                elif not labels:
                    top = "local" if kind == "locals" else None
                    if kind != "locals":
                        names.add(kind)
            elif top and top != "local":
                names.add(f"{top}.{kind}")
        elif depth == 1 and top == "local":
            m = re.match(r"\s*([\w-]+)\s*=", line)
            if m:
                names.add(f"local.{m.group(1)}")
        depth += line.count("{") - line.count("}")
        if depth == 0:
            top = None
    return names


# A line that ends in one of these leaves its expression open, so the next
# line continues it and a function or class there is not a declaration. A
# trailing `+` or `-` does too, and so does a `++` or `--` that is not a
# postfix operator after a name or `]` (js_continues), because a prefix one
# takes the next line as its operand. `return`, `throw` and `yield` also end
# at a line break, so they are left out. A trailing `/` is read as division,
# never as the end of a regex literal: a statement that ends in a regex
# literal and has no `;` fails closed. A string keeps its quotes in the
# masked text, so a line that ends in a string never looks like a dangling
# `=`.
JS_CONTINUES = re.compile(r"[=(\[,?:*/%&|^<>!~.]$"
                          r"|(?<![\w$])(?:(?<!\.)|(?<=\.\.\.))(?:await|case|delete|extends|in|instanceof|new|of|typeof|void)$")


# JavaScript is lexed, not pattern-matched. A `/` divides after a value and
# opens a regex literal after an operator, and a regex literal, a string, or a
# template can hold the quotes, backticks, and braces that end the others, so
# a reader that guesses reads a declaration inside a template as code.
# js_mask follows the template and `${}` nesting and decides regex or division
# from the previous token. Where it cannot be sure, it stops, and js_names
# reads only the lines above the stop: a name below it fails closed, with the
# reason in its message.
JS_TOKEN = re.compile(
    r"(?P<skip>[\s\ufeff]+|//[^\n\r\u2028\u2029]*|/\*.*?\*/|\A\#![^\n\r\u2028\u2029]*)"
    r"|(?P<string>\"(?:\\.|[^\"\\\n])*\"|'(?:\\.|[^'\\\n])*')"
    r"|(?P<word>\#?(?:[^\W\d]|\$)[\w$]*|\.?\d[\w$]*(?:\.[\w$]*)?)"
    r"|(?P<html><!--|-->)|(?P<incr>\+\+|--)|(?P<open>[{(\[])|(?P<close>[})\]])"
    r"|(?P<op>\.\.\.|[!%&*+,.:;<=>?@^|~-])|(?P<tick>`)|(?P<slash>/)", re.S)
JS_TEMPLATE = re.compile(r"\\.|`|\$\{", re.S)
JS_REGEX = re.compile(r"/(?:\\[^\n\r\u2028\u2029]|\[(?:\\[^\n\r\u2028\u2029]|[^\]\\\n\r\u2028\u2029])*\]"
                      r"|[^/\\\[\n\r\u2028\u2029])+(?P<end>/)[\w$]*")
JS_BEFORE_REGEX = {"case", "default", "delete", "do", "else", "extends", "in", "instanceof", "new", "return", "throw",
                   "typeof", "void"}
JS_UNKNOWN = {"await", "of", "yield"}
JS_LINE_BREAK = re.compile(r"[\n\r\u2028\u2029]")
JS_NEWLINE = re.compile(r"\r\n?|[\n\u2028\u2029]")
JS_SIGN = re.compile(r"\+\+|--|[+-]")
# No declaration can name one of these: `let in x` and `let instanceof x` use
# `let` as a variable, and `class extends X {}` has no name.
JS_RESERVED = {"break", "case", "catch", "class", "const", "continue", "debugger", "default", "delete", "do", "else", "enum",
               "export", "extends", "false", "finally", "for", "function", "if", "import", "in", "instanceof", "new", "null",
               "return", "super", "switch", "this", "throw", "true", "try", "typeof", "var", "void", "while", "with"}


def js_mask(text):
    """Blank comments, strings, regex literals, and templates, as masked does.

    Returns the masked text and where the lexer stopped being sure of its
    place, as (offset, what it cannot do), or None. Text from the line of that
    offset on is not masked and must not be read."""
    spans, templates, start, i = [], [], 0, 0
    # templates holds one entry per open template: None while the reader is in
    # its text, else the count of `{` open in its current `${}`. value says
    # whether the last token ends an operand, so that `/` divides: True, False
    # (it opens a regex literal), or None (it cannot be told). A line break
    # between an operand and `/` can insert a semicolon first, which makes the
    # `/` a regex literal, so the reader cannot tell then either.
    value, dot, newline = False, False, False

    def done(why=None, at=None):
        chars = list(text)
        for a, b in spans:
            chars[a:b] = [c if JS_LINE_BREAK.match(c) else " " for c in text[a:b]]
        return "".join(chars), (start if templates else at, why) if why else None

    while i < len(text):
        if templates and templates[-1] is None:
            part = JS_TEMPLATE.search(text, i)
            if part is None:
                break
            i = part.end()
            if part.group() == "${":
                templates[-1], value, dot = 0, False, False
            elif part.group() == "`":
                templates.pop()
                value = True
                if not templates:
                    spans.append((start + 1, part.start()))
            continue
        token = JS_TOKEN.match(text, i)
        kind = token and token.lastgroup
        if kind is None:
            return done("find the end of this string" if text[i] in "\"'" else "read this character", i)
        line_break = newline
        newline = kind == "skip" and (line_break or JS_LINE_BREAK.search(token.group()) is not None)
        if kind == "skip":
            if not templates and not text[i].isspace() and text[i] != "\ufeff":
                spans.append((i, token.end()))
        elif kind == "string":
            if not templates:
                spans.append((i + 1, token.end() - 1))
            value, dot = True, False
        elif kind == "word":
            word = token.group()
            value = True if dot else None if word in JS_UNKNOWN else word not in JS_BEFORE_REGEX
            dot = False
        elif kind == "html":
            return done("read an HTML-like comment", i)
        elif kind == "incr":
            value, dot = None, False
        elif kind == "op":
            value, dot = False, token.group() == "."
        elif kind == "open":
            if token.group() == "{" and templates:
                templates[-1] += 1
            value, dot = False, False
        elif kind == "close":
            if token.group() == "}" and templates:
                if templates[-1] == 0:
                    templates[-1], i = None, token.end()
                    continue
                templates[-1] -= 1
            value, dot = (token.group() == "]") or None, False
        elif kind == "tick":
            if not templates:
                start = i
            templates.append(None)
        else:
            if text.startswith("/*", i):
                return done("find the end of this comment", i)
            if value is None or value and line_break:
                return done("tell a regex literal from a division", i)
            if value:
                value = False
            else:
                regex = JS_REGEX.match(text, i)
                if regex is None:
                    return done("find the end of this regex literal", i)
                if not templates:
                    spans.append((i + 1, regex.start("end")))
                i, value, dot = regex.end(), True, False
                continue
        i = token.end()
    return done("find the end of this template") if templates else done()


class JsNames(set):
    note = ""


def js_continues(line):
    run = re.search(r"[+-]*$", line)
    signs = JS_SIGN.findall(run.group())
    if not signs:
        return bool(JS_CONTINUES.search(line))
    postfix = len(signs) == 1 and len(signs[0]) == 2 and re.search(
        r"(?:^|[^\w$])(?!(?:%s)$)[\w$]+$|\]$" % "|".join(JS_BEFORE_REGEX | JS_UNKNOWN), line[:run.start()].rstrip())
    return not postfix


def js_names(text):
    blanked, stop = js_mask(text)
    names, prior = JsNames(), ""
    if stop:
        offset, cannot = stop
        blanked = blanked[:max(blanked.rfind(c, 0, offset) for c in "\n\r\u2028\u2029") + 1]
        stopped = len(JS_NEWLINE.findall(text, 0, offset)) + 1
        names.note = f"; the JavaScript reader stops at line {stopped}, where it cannot {cannot}"
    for line in JS_NEWLINE.sub("\n", blanked.replace("\ufeff", " ")).split("\n"):
        code = line.rstrip()
        if not code:
            continue
        m = re.match(r"\s*(?:export (?:default )?)?(?:async )?(?:function\*? |class |(?:const|let|var) )([\w$]+)", line)
        if m and m.group(1) not in JS_RESERVED and not js_continues(prior):
            names.add(m.group(1))
        prior = code
    return names


# Shell commands, options, and file names have no declaration, so any
# whole-token phrase in code counts.
class ShellMentions:
    def __init__(self, text):
        self.code = "\n".join(re.sub(r"(?:^|(?<=[\s;&|()]))#.*", "", line) for line in text.splitlines())

    def __contains__(self, symbol):
        pattern = r"(?<![\w-])(?<!\w\.)" + re.escape(symbol) + r"(?![\w-])(?!\.\w)"
        return re.search(pattern, self.code) is not None


def unit_names(text):
    names, continued = set(), False
    for line in text.splitlines():
        if line.lstrip().startswith(("#", ";")):
            continue
        m = None if continued else re.match(r"\[([\w-]+)\]", line)
        if m:
            names.add(m.group(1))
        continued = line.endswith("\\")
    return names


LANGUAGES = {
    ".go": go_names, ".py": py_names, ".tf": tf_names, ".js": js_names, ".sh": ShellMentions,
    ".service": unit_names, ".timer": unit_names,
}
# Independent of LANGUAGES, so a citation of a file kind without a reader
# fails instead of going unchecked.
CITATION = re.compile(r"\[((?:[^\s\[\]():/`]+/)*(?:[^\s\[\]():/`]*\.[A-Za-z]\w*|Dockerfile[^\s\[\]():/`]*))"
                      r"(?::([^\]]*))?\](?![(\[])")
LINE_NUMBER = re.compile(r"\d+(?:-\d+)?")


def language(path):
    return LANGUAGES.get(posixpath.splitext(path)[1])


@functools.cache
def citable(path):
    with open(path, encoding="utf-8") as source:
        return language(path)(source.read())


def citation_problem(path, symbol):
    if symbol == "":
        return "EMPTY CITATION", " (name a symbol after the colon, or cite the file alone)"
    if symbol is not None and LINE_NUMBER.fullmatch(symbol):
        return "LINE NUMBER CITATION", " (cite a name; line numbers drift)"
    if symbol is not None and language(path) is None:
        return "UNSUPPORTED CITATION", " (LANGUAGES has no reader for this file kind; cite the file alone)"
    real = repo_path(path)
    if real is None:
        return "MISSING CITED FILE", " (outside the repository)"
    if not os.path.isfile(path):
        return "MISSING CITED FILE", ""
    if not indexed(path):
        return "MISSING CITED FILE", " (not tracked by git)"
    if symbol is not None and symbol not in (names := citable(path)):
        return "UNRESOLVED CITATION", f" (no such name in {path}{getattr(names, 'note', '')})"
    return None


def citation_failures(readme, lineno, raw):
    failures = []
    for m in CITATION.finditer(raw):
        cited, symbol = m.groups()
        problem = citation_problem(posixpath.normpath(posixpath.join(posixpath.dirname(readme), cited)), symbol)
        if problem:
            failures.append(f"{problem[0]} {readme}:{lineno} -> {m.group()}{problem[1]}")
    return failures


def check():
    failures = []
    for path in filter(links_apply, doc_files()):
        for lineno, raw, visible in scan_lines(path):
            failures += link_failures(path, lineno, visible)
            if is_readme(path):
                failures += citation_failures(path, lineno, raw)
    for failure in failures:
        print(failure)
    print("broken links:", len(failures))
    return len(failures)


if __name__ == "__main__":
    sys.exit(1 if check() else 0)
