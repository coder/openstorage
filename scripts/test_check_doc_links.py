"""Regression tests for the documented check-doc-links.py command."""
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


class CheckerTestCase(unittest.TestCase):
    def run_checker(self, readme, docs=None, files=None, untracked=None, deleted=(), symlinks=None, git=True):
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
            for name in deleted:
                (root / name).unlink()
            return subprocess.run(
                [sys.executable, str(checker)], cwd=root, text=True,
                capture_output=True, timeout=10,
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
              'data "aws_partition" "current" {}\nvariable "region" {}\nprovider "aws" {\n  region = var.region\n}\n')
        ok = ["[main.tf:aws_lb]", "[main.tf:aws_lb.this]", "[main.tf:aws_lb.this.precondition]",
              "[main.tf:data.aws_partition.current]", "[main.tf:region]", "[main.tf:var.region]",
              "[main.tf:provider.aws]", "[main.tf:local.cloud_config]", "[main.tf:local.cloud-init]"]
        bad = ["[main.tf:aws_lb.main]", "[main.tf:data]", "[main.tf:local.a]", "[main.tf:aws_lb.this.name]"]
        result = self.check_readme(" ".join(ok + bad) + "\n", **{"pkg/main.tf": tf})
        self.assert_failures(result, *[("UNRESOLVED CITATION", c) for c in bad])

    def test_shell_citations_are_whole_tokens_in_code(self):
        sh = ('#!/usr/bin/env bash\n# cleanup is documented here only\nset -euo pipefail\n'
              'destroy() {\n  docker compose down\n}\nprintf x >"$work/config.json"\ndestroy\ntrue;# obsolete_fn\n')
        ok = ["[run.sh:destroy]", "[run.sh:set -euo pipefail]", "[run.sh:config.json]", "[run.sh:compose down]"]
        bad = ["[run.sh:cleanup]", "[run.sh:config]", "[run.sh:pose]", "[run.sh:$1]", "[run.sh:obsolete_fn]"]
        result = self.check_readme(" ".join(ok + bad) + "\n", **{"pkg/run.sh": sh})
        self.assert_failures(result, *[("UNRESOLVED CITATION", c) for c in bad])

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

    def test_only_tracked_markdown_outside_vendor_is_read(self):
        broken = "[Gone](gone.md)\n"
        result = self.run_checker(
            "",
            files={"vendor/mod/README.md": broken, "notes.md": broken, "docs/old.md": broken},
            untracked={"internal/new/README.md": broken},
            deleted=["docs/old.md"],
        )
        self.assertEqual(result.stdout, "broken links: 0\n", result.stderr)
        self.assertEqual(result.returncode, 0)

    def test_outside_a_git_checkout_exits_2_without_checking(self):
        result = self.run_checker("[Gone](gone.md)\n", git=False)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertIn("needs a git checkout", result.stderr)


if __name__ == "__main__":
    unittest.main()
