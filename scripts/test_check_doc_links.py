"""Regression tests for the documented check-doc-links.py command.

Run with: python3 -B -m unittest discover -s scripts -p 'test_*.py'
"""
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


class CheckerTestCase(unittest.TestCase):
    def run_checker(self, readme, docs=None, files=None, worktree=None, git=True):
        """Run a copy of the checker in a scratch repository.

        README.md, CONTRIBUTING.md, CHANGELOG.md, docs (under docs/), and files
        (repo-relative) are staged with git add. worktree edits then change only
        the working tree: a text writes an untracked file, None deletes one.
        """
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
            if git:
                subprocess.run(["git", "init", "-q"], cwd=root, check=True)
                subprocess.run(["git", "add", "-f", "."], cwd=root, check=True)
            for name, text in (worktree or {}).items():
                path = root / name
                if text is None:
                    path.unlink()
                else:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(text, encoding="utf-8")
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
            worktree={"internal/new/README.md": broken, "docs/old.md": None},
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
