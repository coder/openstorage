import json
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

    def test_hands_off_strict_runner_config_and_preserves_runner_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = root / "private.pem"
            key.write_text("PRIVATE TEST MATERIAL MUST NOT BE PRINTED")
            key.chmod(0o600)
            inputs = root / "inputs.json"
            inputs.write_text(json.dumps({
                "candidate_commit": "a" * 40, "binary": "/local/openstorage",
                "binary_sha256": "b" * 64, "private_key_file": str(key),
            }))
            deployment = {
                "schema_version": 1, "topology": "postgres-s3-single-node",
                "service_url": "https://example.elb.amazonaws.com", "org": "example",
                "tls_ca_certificate_pem": "PUBLIC TEST CA", "region": "us-east-1",
                "bucket": "example-data", "account_id": "123456789012",
                "release": {"tag": "v0.15.0", "sha256": "c" * 64},
            }
            terraform = root / "terraform"
            terraform.write_text('#!/bin/sh\n[ "$2 $3 $4" = "output -json deployment" ] || exit 99\nprintf \'%s\\n\' \'' + json.dumps(deployment) + "'\n")
            terraform.chmod(0o700)
            runner = root / "runner"
            runner.write_text('''#!/usr/bin/env python3
import json, os, pathlib, stat, sys
assert sys.argv[1] == "--smoke"
assert sys.argv[2] == "--config" and sys.argv[4] == "--output"
path = pathlib.Path(sys.argv[3])
config = json.loads(path.read_text())
assert set(config) == {"schema_version", "candidate_commit", "binary", "binary_sha256", "topology", "endpoint", "replica_urls", "org", "private_key_file", "aws", "ca_file"}
assert config["topology"] == "pg-s3-single-node" and config["replica_urls"] == []
assert config["endpoint"] == "https://example.elb.amazonaws.com"
assert config["binary_sha256"] == "b" * 64  # executable hash, never archive hash
assert config["aws"] == {"region": "us-east-1", "bucket": "example-data", "expected_bucket_owner": "123456789012"}
assert stat.S_IMODE(path.stat().st_mode) == 0o600
assert pathlib.Path(config["private_key_file"]).read_text().startswith("PRIVATE TEST")
ca = pathlib.Path(config["ca_file"]).read_text()
assert "BEGIN CERTIFICATE" in ca and ca.endswith("PUBLIC TEST CA")
assert sys.argv[5] == os.environ["RESULTS_PATH"]
pathlib.Path(os.environ["HANDOFF_PATH"]).write_text(str(path))
sys.exit(17)
''')
            runner.chmod(0o700)
            handoff = root / "handoff"
            env = dict(os.environ, OPENSTORAGE_AWS_SMOKE="1", PATH=f"{root}:{os.environ['PATH']}",
                       HANDOFF_PATH=str(handoff), RESULTS_PATH=str(root / "results"))
            args = [str(SCRIPT), str(inputs), env["RESULTS_PATH"], str(runner), "--smoke"]
            result = subprocess.run(args, env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 17, result.stderr)
            self.assertNotIn("PRIVATE TEST", result.stdout + result.stderr)
            self.assertFalse(Path(handoff.read_text()).exists())
            self.assertTrue(key.exists())

            # Never silently map another infrastructure topology into a single-node run.
            terraform.write_text('#!/bin/sh\nprintf \'{"schema_version":1,"topology":"other"}\\n\'\n')
            result = subprocess.run(args, env=env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Unsupported Terraform deployment schema/topology", result.stderr)


if __name__ == "__main__":
    unittest.main()
