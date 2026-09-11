import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "smoke.sh"


class SmokeAdapterTests(unittest.TestCase):
    def test_requires_explicit_opt_in_before_reading_state(self):
        env = dict(os.environ, OPENSTORAGE_AWS_SMOKE="")
        result = subprocess.run([str(SCRIPT)], env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("OPENSTORAGE_AWS_SMOKE=1", result.stderr)

    def test_hands_off_output_and_local_key_and_preserves_runner_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = root / "private.pem"
            key.write_text("PRIVATE TEST MATERIAL MUST NOT BE PRINTED")
            key.chmod(0o600)
            terraform = root / "terraform"
            terraform.write_text('#!/bin/sh\n[ "$2 $3 $4" = "output -json deployment" ] || exit 99\nprintf \'{"schema_version":1}\\n\'\n')
            terraform.chmod(0o700)
            runner = root / "runner"
            runner.write_text('''#!/usr/bin/env python3
import json, os, pathlib, stat, sys
assert sys.argv[1] == "--custom"
assert sys.argv[2] == "--deployment" and sys.argv[4] == "--signing-key"
path = pathlib.Path(sys.argv[3])
assert json.loads(path.read_text()) == {"schema_version": 1}
assert stat.S_IMODE(path.stat().st_mode) == 0o600
assert pathlib.Path(sys.argv[5]).read_text().startswith("PRIVATE TEST")
pathlib.Path(os.environ["HANDOFF_PATH"]).write_text(str(path))
sys.exit(17)
''')
            runner.chmod(0o700)
            handoff = root / "handoff"
            env = dict(os.environ, OPENSTORAGE_AWS_SMOKE="1", PATH=f"{root}:{os.environ['PATH']}", HANDOFF_PATH=str(handoff))
            result = subprocess.run([str(SCRIPT), str(key), str(runner), "--custom"], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 17, result.stderr)
            self.assertNotIn("PRIVATE TEST", result.stdout + result.stderr)
            self.assertFalse(Path(handoff.read_text()).exists())
            self.assertTrue(key.exists())


if __name__ == "__main__":
    unittest.main()
