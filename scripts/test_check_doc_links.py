"""Regression tests for the documented check-doc-links.py command."""
from pathlib import Path
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest


class CheckerTestCase(unittest.TestCase):
    def run_checker(self, readme, docs=None, files=None, untracked=None, deleted=(), symlinks=None, git=True,
                    untracked_symlinks=None, intent_to_add=(), index_flags=None, env=None):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "scripts").mkdir()
            (root / "docs").mkdir()
            checker = root / "scripts" / "check-doc-links.py"
            shutil.copyfile(Path(__file__).with_name("check-doc-links.py"), checker)
            (root / "README.md").write_text(readme, encoding="utf-8")
            for name in ["CONTRIBUTING.md", "CHANGELOG.md"]:
                (root / name).write_text("", encoding="utf-8")
            staged = {f"docs/{name}": text for name, text in (docs or {}).items()}
            for name, text in {**staged, **(files or {})}.items():
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text, encoding="utf-8")
            for name, target in (symlinks or {}).items():
                (root / name).parent.mkdir(parents=True, exist_ok=True)
                (root / name).symlink_to(target)
            if git:
                subprocess.run(["git", "init", "-q"], cwd=root, check=True)
                subprocess.run(["git", "add", "-f", "."], cwd=root, check=True)
            for name, text in (untracked or {}).items():
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text, encoding="utf-8")
            for name, target in (untracked_symlinks or {}).items():
                (root / name).parent.mkdir(parents=True, exist_ok=True)
                (root / name).symlink_to(target)
            for name in deleted:
                (root / name).unlink()
            if intent_to_add:
                subprocess.run(["git", "add", "-N", "--", *intent_to_add], cwd=root, check=True)
            for name, flag in (index_flags or {}).items():
                subprocess.run(["git", "update-index", flag, "--", name], cwd=root, check=True)
            return subprocess.run(
                [sys.executable, str(checker)], cwd=root, text=True,
                capture_output=True, timeout=10, env=env,
            )


class DocLinkCheckerTests(CheckerTestCase):
    def test_named_anchor_resolves_local_and_cross_file_links(self):
        result = self.run_checker(
            '<a name="try-local"></a>\n[Start here](#try-local)\n',
            {"guide.md": "[Try locally](../README.md#try-local)\n"},
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(result.stdout, "broken links: 0\n")

    def test_anchor_id_and_html_attribute_forms(self):
        result = self.run_checker(
            "<a id='by-id'></a>\n"
            '<A class="anchor" NAME="CaseSensitive"></A>\n'
            "<a name=unquoted></a>\n"
            '<a\n  name="multiline"\n></a>\n'
            "[ID](#by-id) [Case](#CaseSensitive) "
            "[Bare](#unquoted) [Multiline](#multiline)\n"
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_anchor_examples_do_not_create_destinations(self):
        examples = [
            '```html\n<a name="hidden"></a>\n[Ignored](missing.md)\n```\n',
            '~~~html\n<a id="hidden"></a>\n[Ignored](missing.md)\n~~~\n',
            # Neither a shorter fence nor a different marker closes the block.
            '````html\n```\n~~~\n<a name="hidden"></a>\n````\n',
            '`<a name="hidden"></a>`\n',
            '``<a name="hidden" title="a ` backtick"></a>``\n',
            '<!-- <a name="hidden"></a> -->\n',
            '<!--\n<a name="hidden"></a>\n-->\n',
            '&lt;a name="hidden"&gt;&lt;/a&gt;\n',
        ]
        for example in examples:
            with self.subTest(example=example):
                result = self.run_checker(example + '[Missing](#hidden)\n')
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertIn('BROKEN ANCHOR README.md:', result.stdout)
                self.assertIn(' -> #hidden\n', result.stdout)
                self.assertTrue(result.stdout.endswith('broken links: 1\n'))

    def test_indented_code_does_not_create_anchors(self):
        for indent in ['    ', '\t', '  \t']:
            with self.subTest(indent=repr(indent)):
                result = self.run_checker(
                    indent + '<a name="hidden"></a>\n\n[Missing](#hidden)\n'
                )
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertIn(' -> #hidden\n', result.stdout)

    def test_multiline_code_spans_do_not_create_anchors(self):
        for delimiter in ['`', '``', '```']:
            with self.subTest(delimiter=delimiter):
                result = self.run_checker(
                    'Example: ' + delimiter + 'before\n'
                    '<a name="hidden"></a>\n'
                    'after' + delimiter + '\n\n[Missing](#hidden)\n'
                )
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertIn(' -> #hidden\n', result.stdout)

    def test_code_spans_require_exact_runs_and_cannot_cross_blocks(self):
        hidden = [
            'Example: ``before\n` mismatched\n<a name="hidden"></a>\nlonger ```\nafter``\n',
            'Example: `before\n    <a name="hidden"></a>\nafter`\n',
            'Example: `before\n*\n<a name="hidden"></a>\nafter`\n',
            'Example: `before\n2. continuation\n<a name="hidden"></a>\nafter`\n',
        ]
        for example in hidden:
            with self.subTest(example=example):
                result = self.run_checker(example + '\n[Missing](#hidden)\n')
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertIn(' -> #hidden\n', result.stdout)
        visible = [
            'Example: `unmatched\n\n<a name="visible"></a>\nLater `\n',
            'Example: ``unmatched\n<a name="visible"></a>\nOnly a single `\n',
            'Example: `unmatched\n```text\ncode\n```\n<a name="visible"></a>\nLater `\n',
            'Example: `unmatched\n# Heading\n<a name="visible"></a>\nLater `\n',
            'Example: \\`escaped\n<a name="visible"></a>\nLater `\n',
        ]
        for example in visible:
            with self.subTest(example=example):
                result = self.run_checker(example + '\n[Valid](#visible)\n')
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_containers_preserve_fenced_and_indented_code_contexts(self):
        examples = [
            '> ```html\n> <a name="hidden"></a>\n> ```\n',
            '> ```html\n> <a name="hidden"></a>\n',  # unclosed quote fence
            '> ~~~~html\n> ~~~\n> <a name="hidden"></a>\n> ~~~~\n',
            '>     <a name="hidden"></a>\n',
            '- Example:\n\n  ```html\n  <a name="hidden"></a>\n  ```\n',
            '- Outer:\n  - Example:\n\n    ~~~~html\n    ~~~\n    <a name="hidden"></a>\n    ~~~~\n',
            '- Example:\n\n      <a name="hidden"></a>\n',
        ]
        for example in examples:
            with self.subTest(example=example):
                result = self.run_checker(example + '\n[Missing](#hidden)\n')
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertIn(' -> #hidden\n', result.stdout)

    def test_indentation_keeps_real_html_and_list_continuations(self):
        examples = [
            '<a\n    name="visible"\n></a>\n',
            'Paragraph continues\n    <a name="visible"></a>\n',
            '- Item\n\n    <a name="visible"></a>\n',
            '- Outer\n  - Inner\n\n    <a name="visible"></a>\n',
            '> <a\n>     name="visible"\n> ></a>\n',
        ]
        for example in examples:
            with self.subTest(example=example):
                result = self.run_checker(example + '\n[Valid](#visible)\n')
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_dedented_text_ends_a_container_code_block(self):
        for code in ['> ```html\n> code\n', '- ```html\n  code\n']:
            with self.subTest(code=code):
                result = self.run_checker(
                    code + '<a name="visible"></a>\n\n[Valid](#visible)\n'
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_setext_heading_ends_a_paragraph_before_indented_code(self):
        result = self.run_checker(
            'Heading\n=======\n    <a name="hidden"></a>\n\n[Missing](#hidden)\n'
        )
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn(' -> #hidden\n', result.stdout)
        result = self.run_checker('=======\n    <a name="visible"></a>\n\n[Valid](#visible)\n')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_thematic_break_is_not_a_list_continuation(self):
        for separator in ['* * *', '- - -', '_ _ _']:
            with self.subTest(separator=separator):
                result = self.run_checker(
                    separator + '\n    <a name="hidden"></a>\n\n[Missing](#hidden)\n'
                )
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)

    def test_nested_containers_and_tabs_match_rendered_code_context(self):
        cases = [
            ('> - ~~~html\n>   <a name="anchor"></a>\n>   ~~~\n', False),
            ('- > ```html\n  > <a name="anchor"></a>\n  > ```\n', False),
            ('- item\n\n\t\t<a name="anchor"></a>\n', False),
            ('- item\n\n\t<a name="anchor"></a>\n', True),
            ('- First `unmatched\n- <a name="anchor"></a> later `\n', True),
            ('> Example `before\n<a name="anchor"></a>\nafter`\n', False),
            ('- Example `before\n<a name="anchor"></a>\nafter`\n', False),
        ]
        for example, visible in cases:
            with self.subTest(example=example):
                result = self.run_checker(example + '\n[Probe](#anchor)\n')
                self.assertEqual(result.returncode, 0 if visible else 1,
                                 result.stdout + result.stderr)

    def test_html_comments_attributes_and_escaped_markup_keep_their_meaning(self):
        result = self.run_checker(
            '<!-- comment with `\n\n<a name="hidden"></a>\n-->\n'
            '<a\n    title="literal ` backtick" name="visible"\n></a>\n'
            'Escaped \\<a name="escaped"></a>\n'
            '&lt;a name="entity"&gt;&lt;/a&gt;\n\n'
            '[Valid](#visible) [Missing](#hidden) [Missing](#escaped) [Missing](#entity)\n'
        )
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertTrue(result.stdout.endswith('broken links: 3\n'), result.stdout)
        self.assertNotIn(' -> #visible', result.stdout)

    def test_explicit_anchors_do_not_change_heading_duplicate_suffixes(self):
        result = self.run_checker(
            '<a name="heading"></a>\n# Heading\n# Heading\n'
            '[First](#heading) [Second](#heading-1) [Missing](#heading-2)\n'
        )
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(
            result.stdout,
            'BROKEN ANCHOR README.md:4 -> #heading-2\nbroken links: 1\n',
        )

    def test_invalid_and_unsupported_links_still_fail_closed(self):
        cases = [
            ('[Missing](#absent)', 'BROKEN ANCHOR'),
            ('[Wrong case](#VALID)', 'BROKEN ANCHOR'),
            ('[Missing](missing.md)', 'BROKEN FILE'),
            ('[Escape](../README.md)', 'BROKEN FILE (escapes repo)'),
            ('[ref]: docs/guide.md', 'UNSUPPORTED REF-STYLE LINK DEF'),
            ('[Angle](<docs/guide.md>)', 'UNSUPPORTED ANGLE-BRACKET DEST'),
            ('[Multiline](docs/guide.md\n)', 'UNSUPPORTED UNPARSED LINK'),
        ]
        for link, diagnostic in cases:
            with self.subTest(link=link):
                result = self.run_checker(
                    '<a name="valid"></a>\n[Valid](#valid)\n' + link + '\n',
                    {'guide.md': '# Guide\n'},
                )
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertIn(diagnostic, result.stdout)
                self.assertTrue(result.stdout.endswith('broken links: 1\n'))


GO = """package demo

import "flag"

// Run is mentioned here and called below, but declared in another file.
const (
	Limit = 3
	lower, upper = 1, 2
)

type Slots struct {
	sync.Mutex
	*Pool
	Size  int
	a, b  string
}

type Pool interface {
	fmt.Stringer
	Acquire(n int) error
}

type Number interface {
	Int | Float
}

func NewSlots(n int) *Slots { return &Slots{Size: n} }

func (s *Slots) Close() error { Run(); return nil }

func main() {
	seed := flag.Uint64("seed", 0, "simulation seed")
	_ = seed
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	fs.Int("listen-port", 0, "")
	flag.BoolFunc("verbose", "", nil)
	parser.String("ghost-flag", 1)
}

func register(set *flag.FlagSet) { set.Bool("param-flag", false, "") }
"""


class CitationTests(CheckerTestCase):
    def check_readme(self, readme, **files):
        return self.run_checker("", files={"pkg/README.md": readme, **files})

    def assert_failures(self, result, *expected):
        lines = result.stdout.splitlines()
        self.assertEqual(lines[-1], f"broken links: {len(expected)}", result.stdout + result.stderr)
        for kind, citation in expected:
            self.assertTrue(any(l.startswith(kind) and f"-> {citation}" in l for l in lines),
                            f"{kind} {citation} not in:\n{result.stdout}")

    def test_go_citations_resolve_to_declarations(self):
        citations = ["[demo.go:NewSlots]", "[demo.go:Slots]", "[demo.go:Slots.Close]", "[demo.go:Close]",
                     "[demo.go:Slots.Size]", "[demo.go:Slots.b]", "[demo.go:Pool.Acquire]", "[demo.go:Limit]",
                     "[demo.go:upper]", "[demo.go:main]", "[demo.go:flag -seed]", "[demo.go]",
                     "[demo.go:Slots.Mutex]", "[demo.go:Slots.Pool]", "[demo.go:Pool.Stringer]",
                     "[demo.go:flag -listen-port]", "[demo.go:flag -param-flag]", "[demo.go:flag -verbose]"]
        result = self.check_readme(" ".join(citations) + "\n", **{"pkg/demo.go": GO})
        self.assert_failures(result)
        self.assertEqual(result.returncode, 0)

    def test_go_mentions_are_not_declarations(self):
        # `Run` is called and commented on, `demo` is the package clause, `seed` is a local.
        bad = ["[demo.go:Run]", "[demo.go:demo]", "[demo.go:seed]", "[demo.go:Slots.Acquire]",
               "[demo.go:Number.Int]", "[demo.go:flag -ghost-flag]"]
        result = self.check_readme(" ".join(bad) + "\n", **{"pkg/demo.go": GO})
        self.assertEqual(result.returncode, 1)
        self.assert_failures(result, *[("UNRESOLVED CITATION", c) for c in bad])

    def test_declarations_in_comments_and_strings_do_not_count(self):
        cases = {
            "demo.go": ('package demo\n\n/*\nfunc BlockComment() {}\n*/\n// func LineComment() {}\n'
                        'var (\n\tusage = `\n)\nfunc RawString() {}\n`\n\tAfter = 1\n)\n'
                        'var quoted = "func Quoted() {}"\n\nfunc main() {\n'
                        '\t// flag.Int("ghost", 0, "")\n\t_ = flag.Int("live", 0, "")\n}\n',
                        ["After", "main", "flag -live"],
                        ["BlockComment", "LineComment", "RawString", "Quoted", "flag -ghost"]),
            "main.tf": ('# resource "aws_hash" "comment" {}\n// resource "aws_slash" "comment" {}\n'
                        '/*\nresource "aws_block" "comment" {}\n*/\nlocals {\n  policy = <<-EOT\n'
                        '    resource "aws_heredoc" "doc" {\n  EOT\n  arn = "${aws_s3_bucket.data.arn}/*"\n}\n'
                        'resource "aws_lb" "after" {}\n'
                        'resource "aws_instance" "x" {\n  user_data = <<EOT\n  EOT\n  lifecycle {\n  }\nEOT\n'
                        '  tags = <<END-DATA\n  provisioner {\n  }\nEND-DATA\n}\n',
                        ["local.policy", "local.arn", "aws_lb.after", "aws_instance.x"],
                        ["aws_hash.comment", "aws_slash.comment", "aws_block.comment", "aws_heredoc.doc",
                         "aws_instance.x.lifecycle", "aws_instance.x.provisioner"]),
            "tool.py": ('"""Module docstring.\n\ndef docstring_def():\n"""\n# def comment_def():\n'
                        'TEMPLATE = """\nclass TemplateClass:\n"""\ndef real():\n    pass\n',
                        ["TEMPLATE", "real"], ["docstring_def", "comment_def", "TemplateClass"]),
            "flow.js": ('/* function blockComment() {} */\n// const lineComment = 1;\n'
                        'const tmpl = `\nfunction templateFn() {}\n`;\nconst str = "class StrClass {}";\n'
                        'function realFn() {}\n',
                        ["tmpl", "str", "realFn"], ["blockComment", "lineComment", "templateFn", "StrClass"]),
        }
        for name, (source, declared, mentioned) in cases.items():
            with self.subTest(file=name):
                cite = lambda symbol: f"[{name}:{symbol}]"
                result = self.check_readme(" ".join(map(cite, declared + mentioned)) + "\n",
                                           **{f"pkg/{name}": source})
                self.assert_failures(result, *[("UNRESOLVED CITATION", cite(s)) for s in mentioned])

    def test_terraform_citations_resolve_to_block_addresses(self):
        tf = ('locals {\n  cloud-init = 1\n  cloud_config = {\n    a = "{"\n  }\n}\n'
              'resource "aws_lb" "this" {\n  name = "x"\n  lifecycle {\n    precondition {\n'
              '      condition = true\n    }\n  }\n}\n'
              'data "aws_partition" "current" {}\nvariable "region" {}\nprovider "aws" {\n  region = var.region\n}\n'
              'ephemeral "random_password" "db" {\n  length = 16\n  lifecycle {\n    postcondition {\n    }\n  }\n}\n')
        ok = ["[main.tf:aws_lb]", "[main.tf:aws_lb.this]", "[main.tf:aws_lb.this.precondition]",
              "[main.tf:data.aws_partition.current]", "[main.tf:region]", "[main.tf:var.region]",
              "[main.tf:provider.aws]", "[main.tf:local.cloud_config]", "[main.tf:local.cloud-init]",
              "[main.tf:ephemeral.random_password.db]", "[main.tf:ephemeral.random_password.db.postcondition]"]
        bad = ["[main.tf:aws_lb.main]", "[main.tf:data]", "[main.tf:local.a]", "[main.tf:aws_lb.this.name]",
               "[main.tf:random_password.db]", "[main.tf:ephemeral.random_password.db.length]"]
        result = self.check_readme(" ".join(ok + bad) + "\n", **{"pkg/main.tf": tf})
        self.assert_failures(result, *[("UNRESOLVED CITATION", c) for c in bad])

    def test_shell_citations_are_whole_tokens_in_code(self):
        sh = ('#!/usr/bin/env bash\n# cleanup is documented here only\nset -euo pipefail\n'
              'destroy() {\n  docker compose down\n}\nprintf x >"$work/config.json"\ndestroy\ntrue;# obsolete_fn\n')
        ok = ["[run.sh:destroy]", "[run.sh:set -euo pipefail]", "[run.sh:config.json]", "[run.sh:compose down]"]
        bad = ["[run.sh:cleanup]", "[run.sh:config]", "[run.sh:pose]", "[run.sh:$1]", "[run.sh:obsolete_fn]"]
        result = self.check_readme(" ".join(ok + bad) + "\n", **{"pkg/run.sh": sh})
        self.assert_failures(result, *[("UNRESOLVED CITATION", c) for c in bad])

    def test_shell_comments_start_at_a_hash_even_inside_quotes(self):
        # Quotes are not read, so a quote that spans lines or a line separator
        # that bash does not split on can never turn comment words into code.
        sh = ("echo 'requires # quoted-word'\n"
              "echo 'first line\nsecond line' # don't forget\n"
              "echo 'x\x0cy' # feed-word 'z\n"
              "echo 'x\u2028y' # sep-word 'z\n"
              'echo "tag # double-word" # comment-one\n')
        ok = ["[run.sh:requires]", "[run.sh:second line]", "[run.sh:tag]"]
        bad = ["[run.sh:quoted-word]", "[run.sh:forget]", "[run.sh:feed-word]", "[run.sh:sep-word]",
               "[run.sh:double-word]", "[run.sh:comment-one]"]
        result = self.check_readme(" ".join(ok + bad) + "\n", **{"pkg/run.sh": sh})
        self.assert_failures(result, *[("UNRESOLVED CITATION", c) for c in bad])

    def test_only_a_line_feed_ends_a_line_of_a_shell_script_or_a_unit_file(self):
        # str.splitlines also splits on these, and neither bash nor systemd does: the rest of a comment stays a comment.
        for name, sep in [("vt", "\x0b"), ("ff", "\x0c"), ("fs", "\x1c"), ("gs", "\x1d"), ("rs", "\x1e"), ("nel", "\x85"),
                          ("ls", "\u2028"), ("ps", "\u2029"), ("cr", "\r")]:
            with self.subTest(separator=name):
                files = {"pkg/run.sh": f"echo ok # note{sep}hidden-word\nreal-word\n"}
                cited = ["[run.sh:hidden-word]", "[run.sh:real-word]"]
                if sep != "\r":  # systemd and Python end a line at a lone carriage return
                    files["pkg/app.service"] = f"# note{sep}[Hidden]\n[Real]\n"
                    cited += ["[app.service:Hidden]", "[app.service:Real]"]
                result = self.check_readme(" ".join(cited) + "\n", **files)
                self.assert_failures(result, *[("UNRESOLVED CITATION", c) for c in cited if "idden" in c])

    def test_a_carriage_return_line_feed_still_ends_a_line_in_every_reader(self):
        files = {
            "pkg/run.sh": "echo ok # note\r\nreal-word\r\n",
            "pkg/app.service": "# note\r\n[Real]\r\n",
            "pkg/demo.go": "package demo\r\n\r\n// func Hidden() {}\r\nfunc Real() {}\r\n",
            "pkg/tool.py": "# def hidden(): pass\r\ndef real():\r\n    pass\r\n",
            "pkg/main.tf": 'resource "aws_s3_bucket" "b" {\r\n  policy = <<EOT\r\n  hidden_block {\r\n  }\r\nEOT\r\n  lifecycle {\r\n  }\r\n}\r\n',
            "pkg/flow.js": "// function hidden() {}\r\nfunction real() {}\r\n",
        }
        ok = ["[run.sh:real-word]", "[app.service:Real]", "[demo.go:Real]", "[tool.py:real]", "[main.tf:aws_s3_bucket.b]",
              "[main.tf:aws_s3_bucket.b.lifecycle]", "[flow.js:real]"]
        bad = ["[demo.go:Hidden]", "[tool.py:hidden]", "[main.tf:aws_s3_bucket.b.hidden_block]", "[flow.js:hidden]"]
        result = self.check_readme(" ".join(ok + bad) + "\n", **files)
        self.assert_failures(result, *[("UNRESOLVED CITATION", c) for c in bad])

    def test_a_lone_carriage_return_ends_a_line_in_python_and_systemd_but_not_in_bash(self):
        files = {
            "pkg/run.sh": "echo ok # note\rhidden-word\nreal-word\n",
            "pkg/tool.py": 'x = 1  # c\r"""\ndef hidden(): pass\n"""\ndef real(): pass\n',
            "pkg/app.service": "[Unit]\nDescription=a \\\r# c\n[Hidden]\n[Service]\nExecStart=/bin/true\n",
        }
        ok = ["[run.sh:real-word]", "[tool.py:real]", "[tool.py:x]", "[app.service:Service]"]
        bad = ["[run.sh:hidden-word]", "[tool.py:hidden]", "[app.service:Hidden]"]
        result = self.check_readme(" ".join(ok + bad) + "\n", **files)
        self.assert_failures(result, *[("UNRESOLVED CITATION", c) for c in bad])

    def test_a_lone_carriage_return_does_not_end_a_go_or_terraform_comment(self):
        files = {
            "pkg/demo.go": "package demo\n\n// note\rfunc Hidden() {}\nfunc Real() {}\n",
            "pkg/main.tf": '# c\rresource "a" "hidden" {}\nlocals {\n  a = 1 # c\rb = 2\n  c = 3\n}\n',
        }
        ok = ["[demo.go:Real]", "[main.tf:local.a]", "[main.tf:local.c]"]
        bad = ["[demo.go:Hidden]", "[main.tf:a.hidden]", "[main.tf:local.b]"]
        result = self.check_readme(" ".join(ok + bad) + "\n", **files)
        self.assert_failures(result, *[("UNRESOLVED CITATION", c) for c in bad])

    def test_a_carriage_return_in_go_code_is_white_space_that_ends_a_line_for_the_reader(self):
        # Go reads the carriage return before `}` as white space, so the struct is closed and `println` is a call in init.
        go = "package demo\n\ntype T struct {\n\tF int\r}\nfunc init() {\n\tprintln(\"x\")\n}\n"
        ok = ["[demo.go:T]", "[demo.go:T.F]", "[demo.go:init]"]
        bad = ["[demo.go:T.println]", "[demo.go:println]"]
        result = self.check_readme(" ".join(ok + bad) + "\n", **{"pkg/demo.go": go})
        self.assert_failures(result, *[("UNRESOLVED CITATION", c) for c in bad])

    def test_a_line_feed_and_a_carriage_return_in_either_order_end_one_unit_file_line(self):
        for kind, section in [("service", "Service"), ("timer", "Timer")]:
            for name, end in [("lf_cr", "\n\r"), ("cr_lf", "\r\n"), ("cr", "\r"), ("lf", "\n")]:
                with self.subTest(kind=kind, end=name):
                    unit = f"[Unit]{end}Description=a \\{end}[Hidden]{end}[{section}]{end}Key=value{end}"
                    result = self.check_readme(f"[app.{kind}:Hidden] [app.{kind}:{section}]\n", **{f"pkg/app.{kind}": unit})
                    self.assert_failures(result, ("UNRESOLVED CITATION", f"[app.{kind}:Hidden]"))

    def test_a_unit_file_reads_a_section_header_and_a_comment_as_systemd_does(self):
        # systemd trims only a space and a tab, allows nothing after `]`, and ends a line at a NUL.
        files = {
            "pkg/ff_comment.service": "[Unit]\n\x0c# note \\\n[Hidden]\n[Service]\nExecStart=/bin/true\n",
            "pkg/after.service": "[Unit]\n[Hidden]x\n[Spaced] \t\n",
            "pkg/feed_after.service": "[Unit]\n[Hidden]\x0c\n",
            "pkg/nul.service": "[Unit]\nDescription=a \\\x00# c\n[Hidden]\n",
        }
        ok = ["[ff_comment.service:Service]", "[after.service:Spaced]", "[after.service:Unit]"]
        bad = ["[ff_comment.service:Hidden]", "[after.service:Hidden]", "[feed_after.service:Hidden]", "[nul.service:Hidden]"]
        result = self.check_readme(" ".join(ok + bad) + "\n", **files)
        self.assert_failures(result, *[("UNRESOLVED CITATION", c) for c in bad])

    def test_a_form_feed_resets_the_python_indent_so_the_reader_reads_nothing_after_it(self):
        py = "class A:\n    def m(self): pass\n\x0cdef hidden(): pass\ndef real(): pass\n"
        result = self.check_readme("[tool.py:A.m] [tool.py:A.hidden] [tool.py:real]\n", **{"pkg/tool.py": py})
        self.assert_failures(result, ("UNRESOLVED CITATION", "[tool.py:A.hidden]"))

    def test_a_name_next_to_a_backslash_and_a_line_feed_is_not_mentioned_in_a_shell_script(self):
        # `foo\\<LF>bar` is the word foobar in bash, and two literal lines inside single quotes. Neither reads as foo or bar.
        sh = "echo foo\\\nbar\necho 'baz\\\nqux'\necho spaced \\\n  after\nreal-word\n"
        ok = ["[run.sh:real-word]", "[run.sh:spaced]", "[run.sh:after]"]
        bad = ["[run.sh:foo]", "[run.sh:bar]", "[run.sh:baz]", "[run.sh:qux]"]
        result = self.check_readme(" ".join(ok + bad) + "\n", **{"pkg/run.sh": sh})
        self.assert_failures(result, *[("UNRESOLVED CITATION", c) for c in bad])

    def test_only_a_space_or_a_tab_puts_a_hash_at_the_start_of_a_shell_word(self):
        # In bash `a<FF>#` is one word, so the `#` starts no comment and the next word is code.
        sh = "echo a\x0c# real-word\necho b\t# hidden-word\n"
        result = self.check_readme("[run.sh:real-word] [run.sh:hidden-word]\n", **{"pkg/run.sh": sh})
        self.assert_failures(result, ("UNRESOLVED CITATION", "[run.sh:hidden-word]"))

    def test_a_shell_script_that_holds_a_nul_mentions_no_name(self):
        # Bash drops a NUL, so `echo a <NUL>#hidden` is a comment, and the reader would read `hidden` as code.
        files = {"pkg/nul.sh": "echo a \x00#hidden\nreal-word\n", "pkg/plain.sh": "echo a #hidden\nreal-word\n"}
        result = self.check_readme("[nul.sh:hidden] [nul.sh:real-word] [plain.sh:hidden] [plain.sh:real-word]\n", **files)
        self.assert_failures(result, *[("UNRESOLVED CITATION", c) for c in ("[nul.sh:hidden]", "[nul.sh:real-word]", "[plain.sh:hidden]")])

    def test_a_backtick_starts_a_shell_comment(self):
        sh = "echo `#hidden-word` shown-after\nreal-word\n"
        result = self.check_readme("[run.sh:real-word] [run.sh:hidden-word]\n", **{"pkg/run.sh": sh})
        self.assert_failures(result, ("UNRESOLVED CITATION", "[run.sh:hidden-word]"))

    def test_only_a_space_and_a_tab_make_a_markdown_line_blank_or_close_a_fence(self):
        # CommonMark counts a form feed or NEL as text, so each link below renders. str.strip() would treat it as blank.
        cases = {
            "fence": ("~~~\ncode\n~~~\x0c\n~~~\n[Gone](gone.md)\n", "README.md:5"),
            "blank": ("para\n\x0c\n    [Gone](gone.md)\n", "README.md:3"),
            "span": ("a `\n\x0c\nx` [Gone](gone.md) `y`\n", "README.md:3"),
            "item": ("- a\n\x0c\n      [Gone](gone.md)\n", "README.md:3"),
            "nel": ("para\n\x85\n    [Gone](gone.md)\n", "README.md:3"),
            "item-content": ("`[Gone](gone.md)\n- \x1e\n`\n", "README.md:1"),
            "quote": (">\x1d\n\t[Gone](gone.md)\n", "README.md:2"),
            "item-marker": ("-   \x0c\n\n      [Gone](gone.md)\n", "README.md:3"),
            "lazy-line": ("- a\n\x0c\n\n    [Gone](gone.md)\n", "README.md:4"),
        }
        for name, (readme, where) in cases.items():
            with self.subTest(case=name):
                result = self.run_checker(readme)
                self.assertEqual(result.stdout, f"BROKEN FILE {where} -> gone.md\nbroken links: 1\n")

    def test_a_list_item_that_starts_with_a_blank_line_has_its_content_one_column_past_the_marker(self):
        # CommonMark starts the content of such an item one column past its marker, however many spaces or tabs follow.
        # An item that starts with text keeps the spaces after its marker: four of them make the content column.
        cases = {"bullet": ("-\n     [Gone](gone.md)\n", 2), "ordered": ("1.   \n\t[Gone](gone.md)\n", 2), "tab": ("-\t\n     [Gone](gone.md)\n", 2),
                 "text": ("-   a\n\n      [Gone](gone.md)\n", 3)}
        for name, (readme, line) in cases.items():
            with self.subTest(case=name):
                result = self.run_checker(readme)
                self.assertEqual(result.stdout, f"BROKEN FILE README.md:{line} -> gone.md\nbroken links: 1\n")
        for name, readme in {"code": "-\n      [Gone](gone.md)\n", "ordered": "1.\n        [Gone](gone.md)\n", "blank": "-\n\n      [Gone](gone.md)\n"}.items():
            with self.subTest(case=f"{name} code"):
                self.assertEqual(self.run_checker(readme).stdout, "broken links: 0\n")

    def test_a_list_item_that_begins_with_two_blank_lines_is_empty(self):
        # CommonMark lets an item begin with one blank line. After a second one the item is empty, so the lines below
        # are a paragraph, and a paragraph line can start with `*` and four spaces. An item with text keeps them.
        link = "[\n    *     ](gone.md)\n"
        result = self.run_checker("1)  \n\n   " + link)
        self.assertEqual(result.stdout, "BROKEN FILE README.md:4 -> gone.md\nbroken links: 1\n")
        # A line that holds only a form feed is text, so it does not make the item empty either.
        for readme in ["- a\n\n  " + link, "-\n  b\n\n  " + link, "-\n   \x0c\n    *     ](gone.md)\n"]:
            with self.subTest(readme=readme):
                self.assertEqual(self.run_checker(readme).stdout, "broken links: 0\n")

    def test_only_spaces_and_tabs_separate_a_link_destination_from_its_title(self):
        # CommonMark takes a form feed or NEL after a destination as part of the text, so the link below renders.
        for name, sep in [("ff", "\x0c"), ("nel", "\x85"), ("ls", "\u2028")]:
            with self.subTest(separator=name):
                link = f"[a](README.md{sep}'[Gone](gone.md')"
                result = self.run_checker(link + "\n")
                self.assertEqual(result.stdout, f"UNSUPPORTED UNPARSED LINK README.md:1 -> {link}\nbroken links: 1\n")
        result = self.run_checker("[a](README.md 'a title') [b](README.md\t\"a title\")\n")
        self.assertEqual(result.stdout, "broken links: 0\n")

    def test_markdown_line_numbers_and_links_survive_a_separator_that_only_python_splits_on(self):
        for name, sep in [("vt", "\x0b"), ("ff", "\x0c"), ("fs", "\x1c"), ("nel", "\x85"), ("ls", "\u2028"), ("ps", "\u2029")]:
            with self.subTest(separator=name):
                result = self.run_checker(f"first{sep}half\n[Gone](gone.md)\n[Also](gone-too.md)\n")
                self.assertEqual(result.stdout, "BROKEN FILE README.md:2 -> gone.md\nBROKEN FILE README.md:3 -> gone-too.md\nbroken links: 2\n")

    def test_javascript_expressions_after_a_dangling_operator_are_not_declarations(self):
        js = ("const viaEquals =\n  function equalsNamed() {};\nconst viaClass =\n  class ClassNamed {};\n"
              "register(\n  function argNamed() {},\n  function listNamed() {},\n);\n"
              "const either = a ||\n  function orNamed() {};\nconst arrow = () =>\n  function arrowNamed() {};\n"
              "const pick = ok ?\n  function thenNamed() {} :\n  function elseNamed() {};\n"
              "const list = [\n  function bracketNamed() {},\n];\n"
              "function declared() {}\nconst plain = 'text'\nfunction afterString() {}\n"
              "export default\nfunction defaulted() {}\n"
              "counter++\nfunction afterIncrement() {}\ncounter--\nfunction afterDecrement() {}\n"
              "const quotient = 1 /\n  function divided() {}\n"
              "const re = /a+/\nfunction afterRegex() {}\n"
              "async function work() {\n  return\n  function afterReturn() {}\n}\n"
              "switch (x) {\ncase 1:\n  function inCase() {}\n}\n")
        ok = ["viaEquals", "viaClass", "either", "arrow", "pick", "list", "declared", "plain", "afterString",
              "defaulted", "afterIncrement", "afterDecrement", "work", "afterReturn"]
        bad = ["equalsNamed", "ClassNamed", "argNamed", "listNamed", "orNamed", "arrowNamed", "thenNamed",
               "elseNamed", "bracketNamed", "divided", "afterRegex", "inCase"]
        result = self.check_readme(" ".join(f"[flow.js:{name}]" for name in ok + bad) + "\n",
                                   **{"pkg/flow.js": js})
        self.assert_failures(result, *[("UNRESOLVED CITATION", f"[flow.js:{name}]") for name in bad])

    def test_javascript_continuation_skips_blank_and_comment_lines(self):
        js = ("const a =\n  // why\n  function viaComment() {}\n"
              "const b =\n\n  function viaBlank() {}\n"
              "const c =\n  /* why\n  still why */\n  function viaBlock() {}\n"
              "const d = // trailing\n  function viaTrailing() {}\n"
              "const e = 1\n// why\nfunction afterComment() {}\n")
        ok = ["a", "b", "c", "d", "e", "afterComment"]
        bad = ["viaComment", "viaBlank", "viaBlock", "viaTrailing"]
        result = self.check_readme(" ".join(f"[flow.js:{name}]" for name in ok + bad) + "\n",
                                   **{"pkg/flow.js": js})
        self.assert_failures(result, *[("UNRESOLVED CITATION", f"[flow.js:{name}]") for name in bad])

    def test_every_javascript_continuation_ending_keeps_the_next_line_in_its_expression(self):
        endings = ["=", "(", "[", ",", "?", ":", "+", "-", "*", "/", "%", "&", "|", "^", "<", ">", "!", "~", ".",
                   "&&", "||", "??", "=>", "+-", "-+", "+++", "---", "+ ++", "- --", "= ++", "( ++", "typeof ++", "...typeof",
                   "...new", "as", "await", "case", "delete", "extends", "in", "instanceof", "new", "of", "typeof", "void"]
        ends_a_statement = ["++", "--", "return", "throw", "yield", "default", "obj.await", "obj.new", "obj.of", "a[0]++",
                            "a?.new"]
        closer = lambda op: ")" * op.count("(") + "]" * op.count("[")
        js = "".join(f"const v{i} = x {op}\n  function open{i}() {{}}{closer(op)}\n" for i, op in enumerate(endings))
        js += "".join(f"x {op}\nfunction closed{i}() {{}}\n" for i, op in enumerate(ends_a_statement))
        cited = [f"[flow.js:open{i}]" for i in range(len(endings))]
        cited += [f"[flow.js:closed{i}]" for i in range(len(ends_a_statement))]
        result = self.check_readme(" ".join(cited) + "\n", **{"pkg/flow.js": js})
        self.assert_failures(result, *[("UNRESOLVED CITATION", f"[flow.js:open{i}]") for i in range(len(endings))])

    def test_javascript_regex_literals_and_nested_templates_cannot_shift_the_reader(self):
        trap = "const trap = `\nfunction stale() {}\n`;\nfunction real() {}\n"
        regexes_after = [
            "export default /`/.source;\n", "delete /`/.source;\n", "do /`/.test(x); while (0)\n", "if (a) {} else /`/.test(x);\n",
            "class A extends /`/.constructor {}\n", "const q = x in /`/;\n", "const q = x instanceof /`/.constructor;\n",
            "const q = new /`/.constructor();\n", "const q = typeof /`/;\n", "const q = void /`/;\n",
            "switch (x) {\n  case /`/.source:\n}\n", "function f() {\n  return /`/.test(x);\n}\n", "function g() {\n  throw /`/;\n}\n",
            "const a = [...typeof /`/];\n", "const a = [...void /`/];\n", "const a = {...new /`/.constructor()};\n",
            # A word after a member name is a keyword again once the name ends, so its `/` opens a regex literal.
            "a.b\ntypeof /`/.source;\n", "a.b\nnew /`/.constructor();\n", "const q = a.b in /`/;\n",
            "const q = a?.b instanceof /`/.constructor;\n", "function h() {\n  x.y\n  return /`/.test(x);\n}\n",
            "a?.(typeof /`/.source);\n", "a?.[typeof /`/.source];\n",
            # A keyword after a string, `++` or a regex literal that ended the line starts an operand again. A regex
            # literal can open the file or follow an opening bracket.
            "'a'\ntypeof /`/.source;\n", "x++\ntypeof /`/.source;\n", "const r = /a/\ntypeof /`/.source;\n",
            "/`/.test(x);\n", "f(/`/, 1);\n",
            # A keyword that opens a template substitution is not a member name, so its `/` opens a regex literal.
            "const q = `${ typeof /`/.source }`;\n", "const q = `${ delete /`/.source }`;\n",
            "const q = `${ new /`/.constructor() }`;\n",
        ]
        sources = [prefix + trap for prefix in [
            "const r = /`/;\n", "const r = /[`]/g;\n", "const r = /'`/;\n", "const r = /[/]`/;\n", "const r = /[/*]/;\n",
            "const t = `a ${ \"`\" } b`;\n", "const t = `a ${ '`' } b`;\n", "const t = `${ /`/.test(x) }`;\n",
            "const t = `${ /}/.test(x) } ${ {a: \"}\"}.a }`;\n", "const q = a / b + /'`/.test(c);\n",
            "const g = (x) => /`/.test(x);\n", *regexes_after,
        ]]
        # A `/` after a value divides, so the template below opens a template that the division's right side
        # starts and its closing brace ends: reading the `/` as a regex literal flips every backtick after it.
        sources += [f"const q = {operand} / `${{b / c}}` + `\nfunction stale() {{}}\n`;\nfunction real() {{}}\n"
                    for operand in ["'a'", '"a"', "`a`", "1.", "0x1f", "this", "a[0]", "a.in", "a.return"]]
        sources += [
            "const t = `a ${ `b\nfunction stale() {}\n` } c`;\nfunction real() {}\n",
            "const t = `${ `a` / 2 } a/b` + `\nfunction stale() {}\n`;\nfunction real() {}\n",
            "const q = /a/ / `b / c\nfunction stale() {}\n`;\nfunction real() {}\n",
            "const t = `\\${ `;\nconst u = `\nfunction stale() {}\n`;\nfunction real() {}\n",
            "{\n  for (;\n  function stale() {};) { break }\n}\nfunction real() {}\n",
            "f(/`/, 1);\nconst tt = `a ${\nfunction stale() {}\n}`;\nfunction real() {}\n",
            # A `{` inside a substitution opens a block or an object, and its `}` does not end the substitution.
            "const t = `${ `${ {a: () => {}} + /`/ }` }\nfunction stale() {}\n`;\nfunction real() {}\n",
            "const t = `${ `${ {} + {k: /`/} }` }\nfunction stale() {}\n`;\nfunction real() {}\n",
            "const t = `${ {a: 1} + `\nfunction stale() {}\n` }`;\nfunction real() {}\n",
            "const t = `${ {a: {b: 1}} + `\nfunction stale() {}\n` }`;\nfunction real() {}\n",
            "const t = `${ (() => { return 1; })() + `\nfunction stale() {}\n` }`;\nfunction real() {}\n",
            "const t = `${ {a: 1}.a + `\nfunction stale() {}\n` }`;\nfunction real() {}\n",
            "const s = 'a\\\nfunction stale() {}\\\n';\nfunction real() {}\n",
            "const t = `\\`\nfunction stale() {}\n`;\nfunction real() {}\n",
            "// comment\u2028const open = `\nfunction stale() {}\n`;\nfunction real() {}\n",
        ]
        files = {f"pkg/leak{i}.js": source for i, source in enumerate(sources)}
        cited = [f"[leak{i}.js:{name}]" for i in range(len(sources)) for name in ("stale", "real")]
        result = self.check_readme(" ".join(cited) + "\n", **files)
        self.assert_failures(result, *[("UNRESOLVED CITATION", f"[leak{i}.js:stale]") for i in range(len(sources))])

    def test_javascript_reader_reads_regex_literals_templates_and_divisions_it_can_lex(self):
        js = ('function slug(name) {\n'
              '  return name.toLowerCase().replace(/[^a-z0-9]+/g, " ").replace(/^-+|-+$/g, "");\n}\n'
              'const label = `${slug("a b")} and ${ {k: "}"}.k } ${ `nested ${1}` }`;\n'
              'const ratio = total / count / 2; // half\n'
              'const test = x => /a\\/b/.test(x) && y / z;\n'
              'const first = list[0] / 2;\n'
              "const quote = '\\'' + \"say \\\"hi\\\"\";\n"
              'const nested = `${ `${ {a: 1}.a }` + {b: {c: 1}}.b.c }`;\nconst deep = `${ `${ `${ {a: {b: 1}}.a.b }` }` }`;\n'
              'class Counter {\n  #count = 0;\n  get half() { return this.#count / 2; }\n}\n'
              'class Wide\n  extends Counter {\n}\nclass Tight{}\nconst pair = 1,\n  other = 2;\nlet late\n  = 3;\n'
              'function broken\n  (a) {}\n{\n  var bare }\n'
              'function afterAll() {}\n')
        ok = ["slug", "label", "ratio", "test", "first", "quote", "nested", "deep", "Counter", "Wide", "Tight", "pair", "late", "broken", "bare",
              "afterAll"]
        result = self.check_readme(" ".join(f"[flow.js:{name}]" for name in ok) + "\n", **{"pkg/flow.js": js})
        self.assert_failures(result)
        self.assertEqual(result.returncode, 0)

    def test_javascript_every_line_terminator_ends_a_line_and_blocks_a_postfix_operator(self):
        js = ("const r = x\r++\n  function viaCr() {}.x\n"
              "const a = x\u2028++\n  function viaLs() {}.x\n"
              "const b = x\u2029++\n  function viaPs() {}.x\n"
              "const c = x /*\u2029*/ ++\n  function viaComment() {}.x\n"
              "const d = a[0]\u2028++\n  function viaBracket() {}.x\n"
              "const e = x // note\u2028++\n  function viaLineComment() {}.x\n"
              "const f = x++\u2028function afterLs() {}\u2029const g = 1\nfunction afterPs() {}\rconst h = 1\r\nfunction afterCr() {}\n")
        ok = ["afterLs", "afterPs", "afterCr"]
        bad = ["viaCr", "viaLs", "viaPs", "viaComment", "viaBracket", "viaLineComment"]
        result = self.check_readme(" ".join(f"[flow.js:{name}]" for name in ok + bad) + "\n", **{"pkg/flow.js": js})
        self.assert_failures(result, *[("UNRESOLVED CITATION", f"[flow.js:{name}]") for name in bad])

    def test_javascript_stop_line_counts_every_line_terminator(self):
        js = "function before() {}\u2028const half = (a + b) / 2;\u2029function after() {}\n"
        result = self.check_readme("[flow.js:before] [flow.js:after]\n", **{"pkg/flow.js": js})
        self.assert_failures(result, ("UNRESOLVED CITATION", "[flow.js:after]"))
        self.assertIn("the JavaScript reader stops at line 2, where it cannot tell a regex literal from a division", result.stdout)
        js = "function before() {}\rfunction mid() {}\r\nconst half = (a + b) / 2;\nfunction after() {}\n"
        result = self.check_readme("[flow.js:mid] [flow.js:after]\n", **{"pkg/flow.js": js})
        self.assert_failures(result, ("UNRESOLVED CITATION", "[flow.js:after]"))
        self.assertIn("the JavaScript reader stops at line 3, where it cannot tell a regex literal from a division", result.stdout)

    def test_javascript_reader_stays_linear_on_a_long_run_of_signs(self):
        js = "const v = " + "+-" * 50000 + "a\nfunction afterSigns() {}\n"
        result = self.check_readme("[flow.js:afterSigns]\n", **{"pkg/flow.js": js})
        self.assert_failures(result)
        self.assertEqual(result.returncode, 0)

    def test_javascript_keywords_inside_brackets_or_import_lists_declare_nothing(self):
        # A `;` inside a `for (...)` header does not end a statement, and `function as x` in an import or export
        # list names a specifier. Each file ends in a real declaration.
        files = {
            "pkg/for_test.js": "for (;\nfunction viaTest() {};) { break }\n",
            "pkg/for_update.js": "for (;;\nfunction viaUpdate() {}) { break }\n",
            "pkg/for_class.js": "for (;\nclass ViaClass {};) { break }\n",
            "pkg/import.js": "import {\n  function as viaImport,\n} from 'm';\n",
            "pkg/export.js": "export {\n  class as viaExport,\n} from 'm';\n",
            "pkg/var.js": "import {\n  var as viaVar,\n} from 'm';\n",
            "pkg/break_after.js": "import {\n  class as\n  k\n} from 'm';\n",
            "pkg/break_comment.js": "export {\n  let as // note\n  k\n} from 'm';\n",
            "pkg/extends_name.js": "export {\n  class as extends\n} from 'm';\n",
            "pkg/star_var.js": "export * as\nvar from\n'm';\n",
            "pkg/star_function.js": "export * as // note\nfunction from\n() {}\n",
            "pkg/star_class.js": "export * as\nclass from\n'm';\n",
        }
        files = {name: source + "function after() {}\n" for name, source in files.items()}
        stale = ["[for_test.js:viaTest]", "[for_update.js:viaUpdate]", "[for_class.js:ViaClass]", "[import.js:as]",
                 "[export.js:as]", "[var.js:as]", "[break_after.js:as]", "[break_comment.js:as]", "[extends_name.js:as]",
                 "[star_var.js:from]", "[star_function.js:from]", "[star_class.js:from]"]
        result = self.check_readme(" ".join(stale + [f"[{name[4:]}:after]" for name in files]) + "\n", **files)
        self.assert_failures(result, *[("UNRESOLVED CITATION", c) for c in stale])

    def test_javascript_reserved_words_after_a_declaration_keyword_are_not_names(self):
        # `let` is an identifier here, and the class has no name: JavaScript declares none of these words.
        files = {"pkg/let_in.js": "let in obj\nfunction real() {}\n", "pkg/let_instanceof.js": "let instanceof Object\nfunction real() {}\n",
                 "pkg/anonymous.js": "export default class extends Object {}\nfunction real() {}\n"}
        cited = ["[let_in.js:in]", "[let_instanceof.js:instanceof]", "[anonymous.js:extends]",
                 "[let_in.js:real]", "[let_instanceof.js:real]", "[anonymous.js:real]"]
        result = self.check_readme(" ".join(cited) + "\n", **files)
        self.assert_failures(result, *[("UNRESOLVED CITATION", c) for c in cited[:3]])

    def test_javascript_reader_skips_a_hashbang_and_reads_a_byte_order_mark_as_white_space(self):
        result = self.check_readme("[hashbang.js:afterHashbang] [bom.js:bom] [bom.js:afterBom] [bom.js:viaBom]\n", **{
            "pkg/hashbang.js": "#!/usr/bin/env node\nfunction afterHashbang() {}\n",
            "pkg/bom.js": "\ufeffconst bom = 1;\nconst open = x +\ufeff\n  function viaBom() {}\nfunction afterBom() {}\n"})
        self.assert_failures(result, ("UNRESOLVED CITATION", "[bom.js:viaBom]"))

    def test_javascript_reader_stops_where_it_cannot_be_sure_of_the_code(self):
        division = "tell a regex literal from a division"
        triggers = {  # name: (source after `before`, line of the stop, what the reader cannot do)
            "paren": ("const half = (a + b) / 2;\n", 2, division), "brace": ("const q = {}\n/ 2;\n", 3, division),
            "incr": ("count++ / 2;\n", 2, division), "linestart": ("const ratio = total\n  / count;\n", 3, division),
            "yield": ("const y = yield / 2;\n", 2, division), "inside": ("const t = `\nfunction stale() {}\n${ (a) / 2 }`;\n", 2, division),
            "html": ("x <!-- `\n", 2, "read an HTML-like comment"), "arrow": ("while (n --> 0) {}\n", 2, "read an HTML-like comment"),
            "escape": ("function st\\u0061le() {}\n", 2, "read this character"), "unicode": ("const x = 1 \u2192 2;\n", 2, "read this character"),
            "quote": ("const s = 'open;\n", 2, "find the end of this string"), "comment": ("/* open\n", 2, "find the end of this comment"),
            "regex": ("const r = /open;\n", 2, "find the end of this regex literal"),
            "template": ("const t = `open\n", 2, "find the end of this template"),
        }
        files = {f"pkg/{name}.js": f"function before() {{}}\n{source}function after() {{}}\n" for name, (source, _, _) in triggers.items()}
        cited = [f"[{name}.js:{symbol}]" for name in triggers for symbol in ("before", "after")] + ["[escape.js:st]"]
        result = self.check_readme(" ".join(cited) + "\n", **files)
        self.assert_failures(result, *[("UNRESOLVED CITATION", f"[{name}.js:after]") for name in triggers],
                             ("UNRESOLVED CITATION", "[escape.js:st]"))
        for name, (_, line, cannot) in triggers.items():
            report = next(l for l in result.stdout.splitlines() if f"[{name}.js:after]" in l)
            self.assertTrue(report.endswith(f"; the JavaScript reader stops at line {line}, where it cannot {cannot})"), report)

    def test_javascript_reader_that_stops_on_the_first_line_reads_no_name(self):
        # No line lies above a stop on line 1, so the reader keeps none of the text.
        sources = {"html": "<!-- x\nfunction stale() {}\n", "regex": "await /x/;\nfunction stale() {}\n",
                   "template": "`\nfunction stale() {}\n"}
        files = {f"pkg/{name}.js": source for name, source in sources.items()}
        result = self.check_readme(" ".join(f"[{name}.js:stale]" for name in sources) + "\n", **files)
        self.assert_failures(result, *[("UNRESOLVED CITATION", f"[{name}.js:stale]") for name in sources])
        for name in sources:
            report = next(l for l in result.stdout.splitlines() if f"[{name}.js:stale]" in l)
            self.assertIn("; the JavaScript reader stops at line 1, where it cannot ", report)

    def test_javascript_slash_that_starts_a_line_after_an_operand_can_open_a_regex_literal(self):
        # A line break lets a semicolon end `var s`, `break`, or an import, so the `/` below opens a
        # regex literal that holds a backtick. The reader stops there instead of guessing.
        trap = "const t = `\nfunction stale() {}\n`;\nfunction real() {}\n"
        heads = ["var s\n  /`/.test(x);\n", "let s, u\n  /`/.test(x);\n", "var s /* one\n two */ /`/.test(x);\n", "for (;;) {\n  break\n  /`/.test(x);\n}\n",
                 "debugger\n  /`/.test(x);\n", "import a from 'm'\n  /`/.test(x);\n", "export * from 'm'\n  /`/.test(x);\n"]
        files = {f"pkg/asi{i}.js": head + trap for i, head in enumerate(heads)}
        cited = [f"[asi{i}.js:{name}]" for i in range(len(heads)) for name in ("stale", "real")]
        result = self.check_readme(" ".join(cited) + "\n", **files)
        self.assert_failures(result, *[("UNRESOLVED CITATION", f"[asi{i}.js:{name}]") for i in range(len(heads))
                                       for name in ("stale", "real")])

    def test_javascript_reader_never_reads_a_name_that_a_literal_or_comment_holds(self):
        rnd = random.Random(640)
        operands = ["a", "1", "1.", "this", "a.in", "'q'", '"`"', "'/*'", "/`/", "/[`]/", "/'/g", "/}/", "/\\//", "`${x}`", "`a${'`'}`",
                    "`${/`/.test(y)}`", "`${`n${1}`}`", "`//${'/*'}`"]
        def statement(kind, name):
            expr = "".join(rnd.choice(operands) + rnd.choice([" + ", " / ", " - ", ", "]) for _ in range(rnd.randint(0, 2)))
            expr += rnd.choice(operands)
            return {
                "noise": f"x = {expr};", "asi": f"var v\n{rnd.choice(['/`/', '/[`]/', '/\\//'])}.test(x);",
                "real": f"function {name}() {{}}", "template": f"const v{name} = `x ${{ {expr} }}\nfunction {name}() {{}}\n`;",
                "comment": f"/*\nfunction {name}() {{}}\n*/", "continued": f"const v{name} = 'a\\\nfunction {name}() {{}}\\\n';",
                "expression": f"const v{name} =\n  function {name}() {{}};",
            }[kind]
        files, real, trap = {}, set(), set()
        for i in range(120):
            lines = []
            for k in range(rnd.randint(2, 6)):
                kind = rnd.choice(["noise", "noise", "asi", "real", "template", "comment", "continued", "expression"])
                name = f"{'r' if kind == 'real' else 't'}{i}_{k}"
                lines.append(statement(kind, name))
                if kind in ("real", "template", "comment", "continued", "expression"):
                    (real if kind == "real" else trap).add(f"[g{i}.js:{name}]")
            files[f"pkg/g{i}.js"] = "\n".join(lines) + "\n"
        result = self.check_readme(" ".join(sorted(real | trap)) + "\n", **files)
        failed = set(re.findall(r"-> (\[[^\]]+\])", result.stdout))
        self.assertEqual(trap - failed, set())
        self.assertGreater(len(real - failed), len(real) // 4)

    def test_python_js_and_unit_citations(self):
        files = {
            "pkg/tool.py": 'import os\nLIMIT = 3\nos.chdir("/")\n\nclass Box:\n    def open(self):\n'
                           '        inner = 1\n        def helper():\n            pass\n\n'
                           'class Wide(\n    Box,\n):\n    def run(self):\n        pass\n\n'
                           'def main():\n    pass\n\nif __name__ == "__main__":\n    main()\n',
            "pkg/flow.js": 'export const meta = {\n  argsSchema: 1,\n};\n'
                           'function gate() {\n  const rule = 1;\n}\nconst $handler = 1;\n'
                           'const { actual = fn(stale, ghost), keep } = api;\n',
            "pkg/app.service": "[Unit]\nDescription=x \\\n# note\n[Install]\n[Service]\nExecStart=/bin/true\n",
        }
        ok = ["[tool.py:LIMIT]", "[tool.py:os.chdir]", "[tool.py:Box.open]", "[tool.py:Wide.run]",
              "[tool.py:main]", "[tool.py:__main__]", "[flow.js:meta]", "[flow.js:gate]",
              "[flow.js:rule]", "[flow.js:$handler]", "[app.service:Service]"]
        bad = ["[tool.py:inner]", "[tool.py:os]", "[tool.py:Box.helper]", "[flow.js:meta.argsSchema]",
               "[flow.js:ghost]", "[app.service:ExecStart]", "[app.service:Install]"]
        result = self.check_readme(" ".join(ok + bad) + "\n", **files)
        self.assert_failures(result, *[("UNRESOLVED CITATION", c) for c in bad])

    def test_cited_paths_are_relative_to_the_readme_and_stay_in_the_repo(self):
        files = {"cmd/dst/README.md": "[internal/sim/run.go:Run] [../../internal/sim/run.go:Run] "
                                      "[../../../run.go] [gone.go] [escape.py:CheckerTestCase]\n",
                 "internal/sim/run.go": "package sim\n\nfunc Run() {}\n"}
        result = self.run_checker("", files=files,
                                  symlinks={"cmd/dst/escape.py": Path(__file__).resolve()})
        self.assert_failures(result, *[("MISSING CITED FILE", c) for c in
                                       ["[internal/sim/run.go:Run]", "[../../../run.go]", "[gone.go]",
                                        "[escape.py:CheckerTestCase]"]])
        self.assertIn("cmd/dst/README.md:1 -> [internal/sim/run.go:Run]", result.stdout)

    def test_cited_files_must_be_tracked_even_through_a_symlink(self):
        source = "package p\n\nfunc Run() {}\n"
        result = self.run_checker(
            "", files={"pkg/README.md": "[new.go:Run] [new.go] [link.go:Run] [kept.go:Run]\n", "pkg/kept.go": source},
            symlinks={"pkg/link.go": "new.go"}, untracked={"pkg/new.go": source})
        self.assert_failures(result, *[("MISSING CITED FILE", c) for c in
                                       ["[new.go:Run]", "[new.go]", "[link.go:Run]"]])
        self.assertEqual(result.stdout.count(" (not tracked by git)\n"), 3, result.stdout)

    def test_an_untracked_symlink_is_not_a_tracked_cited_file(self):
        source = "package p\n\nfunc Run() {}\n"
        result = self.run_checker(
            "", files={"pkg/README.md": "[alias.go:Run] [dirlink/real.go:Run] [real.go:Run]\n", "pkg/real.go": source},
            untracked_symlinks={"pkg/alias.go": "real.go", "pkg/dirlink": "../pkg"})
        self.assert_failures(result, *[("MISSING CITED FILE", c) for c in ["[alias.go:Run]", "[dirlink/real.go:Run]"]])
        self.assertEqual(result.stdout.count(" (not tracked by git)\n"), 2, result.stdout)

    def test_malformed_and_unsupported_citations_fail_with_their_kind(self):
        result = self.check_readme(
            "[run.sh:8] [run.sh:4-7] [demo.go:] [new.rs:Missing] [notes.txt:Intro] [notes.txt] [gone.txt] "
            "[a+b.go:Handler] [Dockerfile.test:CMD]\n",
            **{"pkg/run.sh": "echo\n", "pkg/notes.txt": "Intro\n"})
        self.assert_failures(result, ("LINE NUMBER CITATION", "[run.sh:8]"),
                             ("LINE NUMBER CITATION", "[run.sh:4-7]"), ("EMPTY CITATION", "[demo.go:]"),
                             ("UNSUPPORTED CITATION", "[new.rs:Missing]"),
                             ("UNSUPPORTED CITATION", "[notes.txt:Intro]"), ("MISSING CITED FILE", "[gone.txt]"),
                             ("MISSING CITED FILE", "[a+b.go:Handler]"),
                             ("UNSUPPORTED CITATION", "[Dockerfile.test:CMD]"))

    def test_citations_count_in_code_spans_not_code_blocks(self):
        readme = ("Inline `[run.sh:gone]`.\n\n```text\n[run.sh:fenced]\n```\n\n    [run.sh:indented]\n\n"
                  "Not citations: [run.sh](run.sh) [!WARNING] `[a-z]` [-steps N] [1, 100] [v1.2] [run.sh][ref].\n")
        result = self.check_readme(readme, **{"pkg/run.sh": "echo\n"})
        self.assert_failures(result, ("UNRESOLVED CITATION", "[run.sh:gone]"))

    def test_only_readmes_carry_citations(self):
        result = self.run_checker("", docs={"guide.md": "[run.sh:gone]\n"}, files={"run.sh": "echo\n"})
        self.assert_failures(result)


class FileSelectionTests(CheckerTestCase):
    def test_nested_readmes_are_link_checked(self):
        result = self.run_checker("", files={"internal/pkg/README.md": "[Gone](gone.md)\n"})
        self.assertEqual(result.stdout, "BROKEN FILE internal/pkg/README.md:1 -> gone.md\nbroken links: 1\n")
        self.assertEqual(result.returncode, 1)

    def test_anchors_resolve_in_markdown_that_is_not_scanned(self):
        files = {".agents/skill/SKILL.md": "# Skill\n\n## Launch\n",
                 ".agents/skill/features/README.md": "[Launch](../SKILL.md#launch) [Gone](../SKILL.md#gone)\n"}
        result = self.run_checker("", files=files)
        self.assertEqual(result.stdout, "BROKEN ANCHOR .agents/skill/features/README.md:1 -> "
                                        "../SKILL.md#gone\nbroken links: 1\n")

    def test_untracked_markdown_is_read_unless_ignored_or_vendored(self):
        broken = "[Gone](gone.md)\n"
        result = self.run_checker(
            "",
            files={"vendor/mod/README.md": broken, "notes.md": broken, "docs/old.md": broken, ".gitignore": "ignored/\n"},
            untracked={"internal/new/README.md": broken, "docs/new.md": broken, "ignored/README.md": broken,
                       "vendor/new/README.md": broken},
            deleted=["docs/old.md"],
        )
        self.assertEqual(result.stdout, "BROKEN FILE docs/new.md:1 -> gone.md\n"
                                        "BROKEN FILE internal/new/README.md:1 -> gone.md\nbroken links: 2\n")
        self.assertEqual(result.returncode, 1)

    def test_links_resolve_only_to_tracked_files_and_directories(self):
        readme = ("[Dir](docs/) [Root](./) [Tracked](docs/old.md) [Self](#top) [Fragment](docs/guide.md#guide)\n"
                  "[New file](docs/new.md) [New dir](fresh/) [New with fragment](docs/new.md#x) [Gone](docs/gone.md)\n")
        result = self.run_checker(
            "# Top\n" + readme, docs={"old.md": "", "gone.md": "", "guide.md": "# Guide\n"},
            untracked={"docs/new.md": "# New\n", "fresh/file.txt": ""}, deleted=["docs/gone.md"])
        untracked = "BROKEN FILE (not tracked by git) README.md:3 -> "
        self.assertEqual(result.stdout, f"{untracked}docs/new.md\n{untracked}fresh/\n{untracked}docs/new.md\n"
                                        "BROKEN FILE README.md:3 -> docs/gone.md\nbroken links: 4\n")

    def test_links_follow_tracked_symlinks_like_citations(self):
        result = self.run_checker(
            "[Real](notes/real.txt) [Untracked](notes/alias.txt) [Outside](notes/out.txt) [Root](notes/root/) "
            "[Up](notes/up/)\n",
            files={"notes/real.txt": "x\n"},
            symlinks={"notes/alias.txt": "new.txt", "notes/out.txt": Path(__file__).resolve(), "notes/root": "/",
                      "notes/up": "../.."},
            untracked={"notes/new.txt": "x\n"})
        self.assertEqual(result.stdout, "BROKEN FILE (not tracked by git) README.md:1 -> notes/alias.txt\n"
                                        "BROKEN FILE (escapes repo) README.md:1 -> notes/out.txt\n"
                                        "BROKEN FILE (escapes repo) README.md:1 -> notes/root/\n"
                                        "BROKEN FILE (escapes repo) README.md:1 -> notes/up/\nbroken links: 4\n")

    def test_an_untracked_symlink_to_a_tracked_file_is_not_tracked(self):
        result = self.run_checker(
            "[Alias](alias.md) [Deep](dir/alias.md) [Through](dirlink/real.md) [Real](real.md)\n",
            files={"real.md": "# Real\n", "dir/real.md": "# Deep\n"},
            untracked_symlinks={"alias.md": "real.md", "dir/alias.md": "real.md", "dirlink": "dir"})
        self.assertEqual(result.stdout, "BROKEN FILE (not tracked by git) README.md:1 -> alias.md\n"
                                        "BROKEN FILE (not tracked by git) README.md:1 -> dir/alias.md\n"
                                        "BROKEN FILE (not tracked by git) README.md:1 -> dirlink/real.md\n"
                                        "broken links: 3\n")

    def test_every_symlink_that_resolving_a_link_follows_must_be_tracked(self):
        result = self.run_checker(
            "[Chain](alias/guide.md) [Tracked](full/guide.md) [Loop](loop-a.md)\n",
            files={"docs/guide.md": "# Guide\n"},
            symlinks={"alias": "hidden", "full": "middle", "middle": "docs", "loop-a.md": "loop-b.md",
                      "loop-b.md": "loop-a.md"},
            untracked_symlinks={"hidden": "docs"})
        self.assertEqual(result.stdout, "BROKEN FILE (not tracked by git) README.md:1 -> alias/guide.md\n"
                                        "BROKEN FILE README.md:1 -> loop-a.md\nbroken links: 2\n")

    def test_a_tracked_symlink_below_a_tracked_directory_symlink_resolves(self):
        result = self.run_checker(
            "[Chain](alias/inner/link.md) [Up](alias/inner/up.md)\n",
            files={"docs/inner/real.md": "# Real\n", "docs/top.md": "# Top\n"},
            symlinks={"alias": "docs", "docs/inner/link.md": "real.md", "docs/inner/up.md": "../top.md"})
        self.assertEqual(result.stdout, "broken links: 0\n", result.stderr)

    def test_a_link_through_a_tracked_directory_symlink_resolves_to_the_tracked_file(self):
        result = self.run_checker("[Guide](alias/guide.md) [Dir](alias/)\n", docs={"guide.md": "# Guide\n"},
                                  symlinks={"alias": "docs"})
        self.assertEqual(result.stdout, "broken links: 0\n", result.stderr)

    def test_an_intent_to_add_target_is_not_tracked(self):
        result = self.run_checker(
            "[Target](docs/new.txt) [Cited](pkg/README.md)\n",
            untracked={"docs/new.txt": "x\n", "pkg/README.md": "[run.go:Run]\n", "pkg/run.go": "func Run() {}\n"},
            intent_to_add=["docs/new.txt", "pkg/README.md", "pkg/run.go"])
        self.assertEqual(result.stdout, "BROKEN FILE (not tracked by git) README.md:1 -> docs/new.txt\n"
                                        "BROKEN FILE (not tracked by git) README.md:1 -> pkg/README.md\n"
                                        "MISSING CITED FILE pkg/README.md:1 -> [run.go:Run] (not tracked by git)\n"
                                        "broken links: 3\n")

    def test_a_modified_tracked_file_and_a_directory_with_a_tracked_file_stay_tracked(self):
        # Only the intent-to-add flag marks an intent-to-add entry. A staged file that was edited since has none.
        result = self.run_checker(
            "[Edited](docs/mod.txt) [Dir](docs/both/) [Cited](pkg/README.md)\n",
            files={"docs/both/b.txt": "b\n", "docs/mod.txt": "one\n", "pkg/README.md": "[run.go:Run]\n", "pkg/run.go": "func Run() {}\n"},
            untracked={"docs/both/a.txt": "a\n", "docs/mod.txt": "two\n", "pkg/run.go": "func Run() {}\n// edited\n"},
            intent_to_add=["docs/both/a.txt"])
        self.assertEqual(result.stdout, "broken links: 0\n")

    def test_an_intent_to_add_entry_is_not_tracked_whatever_other_flag_it_has(self):
        # `git diff-files` does not list an entry that is assume-unchanged or skip-worktree (a sparse checkout sets it).
        names = [f"docs/{name}.txt" for name in ("plain", "unchanged", "sparse")]
        result = self.run_checker(" ".join(f"[Doc]({name})" for name in names) + "\n",
                                  untracked={name: "x\n" for name in names}, intent_to_add=names,
                                  index_flags={names[1]: "--assume-unchanged", names[2]: "--skip-worktree"})
        self.assertEqual(result.stdout, "".join(f"BROKEN FILE (not tracked by git) README.md:1 -> {name}\n" for name in names)
                         + "broken links: 3\n", result.stderr)

    def test_an_index_listing_that_cannot_be_read_exits_2_without_checking(self):
        # A `git` on PATH that passes everything through, except that it damages the `--debug` listing of the index.
        wrapper = textwrap.dedent("""\
            #!%s
            import os, re, subprocess, sys
            done = subprocess.run([os.environ["REAL_GIT"], *sys.argv[1:]], capture_output=True)
            out, mode = done.stdout, os.environ["DAMAGE"]
            if "--debug" in sys.argv:
                entries = re.findall(rb"[^\\0]*\\0(?:  [^\\n]*\\n){4}  size: [^\\n]*\\tflags: [0-9a-f]+\\n", out)
                assert b"".join(entries) == out and len(entries) > 2
                out = {"cut": out[:-1], "gap": entries[0] + b"junk\\n" + b"".join(entries[1:]), "tail": out + b"junk\\n",
                       "last": b"".join(entries[:-1]), "none": b"", "intact": out,
                       "junk": entries[0] + b"junk\\0\\n" + b"".join(entries[1:])}[mode]
            sys.stdout.buffer.write(out)
            sys.stderr.buffer.write(done.stderr)
            sys.exit(done.returncode)
            """) % sys.executable
        with tempfile.TemporaryDirectory() as bin_dir:
            fake = Path(bin_dir) / "git"
            fake.write_text(wrapper, encoding="utf-8")
            fake.chmod(0o755)
            for mode in ["intact", "cut", "gap", "junk", "tail", "last", "none"]:
                with self.subTest(damage=mode):
                    env = {**os.environ, "PATH": bin_dir + os.pathsep + os.environ["PATH"], "REAL_GIT": shutil.which("git"), "DAMAGE": mode}
                    result = self.run_checker("[Doc](docs/a.md)\n", docs={"a.md": "# A\n", "b.md": "# B\n"}, env=env)
                    if mode == "intact":
                        self.assertEqual((result.returncode, result.stdout), (0, "broken links: 0\n"), result.stderr)
                        continue
                    self.assertEqual((result.returncode, result.stdout), (2, ""), result.stderr)
                    self.assertEqual(result.stderr, "check-doc-links.py cannot read the index flags from `git ls-files --debug`\n")

    def test_a_directory_that_holds_only_intent_to_add_files_is_not_tracked(self):
        result = self.run_checker("[Dir](docs/only/) [File](docs/only/a.txt)\n", untracked={"docs/only/a.txt": "a\n"},
                                  intent_to_add=["docs/only/a.txt"])
        self.assertEqual(result.stdout, "BROKEN FILE (not tracked by git) README.md:1 -> docs/only/\n"
                                        "BROKEN FILE (not tracked by git) README.md:1 -> docs/only/a.txt\n"
                                        "broken links: 2\n")

    def test_an_untracked_readme_can_link_to_itself(self):
        result = self.run_checker("", untracked={"pkg/README.md": "# Top\n[Self](#top) [Missing](#absent)\n"})
        self.assertEqual(result.stdout, "BROKEN ANCHOR pkg/README.md:2 -> #absent\nbroken links: 1\n")

    def test_outside_a_git_checkout_exits_2_without_checking(self):
        result = self.run_checker("[Gone](gone.md)\n", git=False)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertIn("needs a git checkout", result.stderr)


if __name__ == "__main__":
    unittest.main()
