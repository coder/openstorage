"""Regression tests for check-squash-message.py.

Run with: python3 -B -m unittest discover -s scripts -p 'test_check_squash_message.py'
"""
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import unittest

SCRIPT = Path(__file__).with_name("check-squash-message.py")
spec = importlib.util.spec_from_file_location("check_squash_message", SCRIPT)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


def lines_found(body, subject="fix(auth): reject non-canonical base64url (#323)"):
    return [number for number, _ in checker.findings(f"{subject}\n\n{body}\n")]


class SquashMessageTests(unittest.TestCase):
    def test_rejects_the_lines_that_broke_v0_16_0(self):
        # #323 and #362 were dropped: the parser throws on an unclosed "(".
        self.assertEqual(lines_found(
            "Text.\n`RawURLEncoding.EncodeToString(RawURLEncoding.Strict().Decode(seg))`"), [2])
        self.assertEqual(lines_found("`CreateTemp(filepath.Dir(dest))`. Not affected."), [1])
        # #270 was listed twice: its body repeated the subject.
        self.assertEqual(lines_found(
            "fix(s3wal): preserve evidence in lease refusal diagnostics (#270)"), [1])

    def test_rejects_other_release_directives(self):
        self.assertEqual(lines_found("Text.\n\nsegment!: wrapped text"), [3])
        self.assertEqual(lines_found("Text.\n\nRelease-As: 1.0.0"), [3])
        self.assertEqual(lines_found("Text.\n\nfeat(api): another entry"), [3])
        self.assertEqual(lines_found("Note: wrapped\n  fix: indented continuation"), [2])
        self.assertEqual(lines_found("Text.\n\nBREAKING CHANGE: no bang in the title"), [3])
        self.assertEqual(lines_found("BREAKING CHANGE:", subject="feat(api)!: x"), [1])
        self.assertEqual(lines_found("Text.\n\ndocs: split here\nBREAKING CHANGE: y",
                                     subject="feat(api)!: x"), [3])

    def test_accepts_ordinary_bodies(self):
        body = "\r\n".join([
            "Before this change, these spellings were all accepted in every",
            "segment: non-zero padding bits, `=` padding, and CR/LF anywhere",
            "Docs impact: none. See [the guide](docs/a.md) and (#405).",
            "- fix: a list item is fine",
            "    indented(code(sample))",
            "",
            "Closes #405",
            "https://github.com/user-attachments/assets/3cf83530",
            "Signed-off-by: A Person <a@example.com>",
            "_Generated with [`xum`](https://github.com/coder/xum) • Model: `m`_",
        ])
        self.assertEqual(lines_found(body), [])
        self.assertEqual(lines_found("BREAKING CHANGE: the bang matches", subject="feat!: x"), [])

    def test_explicit_commit_blocks_only_check_parse_failures(self):
        body = "BEGIN_COMMIT_OVERRIDE\nfix(a): one\nfeat(b): two\nEND_COMMIT_OVERRIDE"
        self.assertEqual(lines_found(body), [])
        self.assertEqual(lines_found(body + "\ncall(a(b))"), [5])

    def test_cli_reports_line_numbers_without_echoing_the_body(self):
        message = "fix: x (#1)\n\nok\nadd-mask(x(y)) ::stop-commands::\n"
        env = dict(os.environ, GITHUB_ACTIONS="true")
        result = subprocess.run([sys.executable, str(SCRIPT)], input=message, text=True,
                                capture_output=True, env=env, timeout=10)
        self.assertEqual(result.returncode, 1)
        self.assertIn("::error title=Squash body line 2::", result.stdout)
        self.assertNotIn("add-mask", result.stdout)
        ok = subprocess.run([sys.executable, str(SCRIPT)], input="docs: x\n\nfine\n",
                            text=True, capture_output=True, timeout=10)
        self.assertEqual(ok.returncode, 0)


if __name__ == "__main__":
    unittest.main()
