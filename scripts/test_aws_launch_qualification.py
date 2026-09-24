"""Offline regression coverage for the artifact and read-only AWS preflight."""
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import aws_launch_qualification as q


COMMIT = "a" * 40
SECRET = "DO_NOT_LEAK_QUALIFICATION_SECRET"


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.binary = self.root / SECRET
        self.binary.write_bytes(b"verified helper")
        self.binary.chmod(0o700)
        self.config = dict(schema_version=1, candidate_commit=COMMIT,
                           binary=str(self.binary), binary_sha256=hashlib.sha256(self.binary.read_bytes()).hexdigest())
        self.config_path = self.root / "config.json"
        self.output = self.root / "evidence"

    def invoke(self, **kwargs):
        self.config_path.write_text(json.dumps(self.config))
        return q.execute(self.config_path, self.output, **kwargs)

    def result(self):
        return json.loads((self.output / "result.json").read_text())

    def command(self, argv, **kwargs):
        if argv[-1] == "version":
            return f"openstorage devel (commit {COMMIT}, built now, go1.26.0)\n".encode()
        return b""

    def preflight(self):
        return patch.object(q.Runner, "command", side_effect=self.command)

    def test_removed_smoke_flag_is_rejected_before_execution(self):
        with patch.object(q, "execute") as execute, patch.object(q.sys, "stderr"):
            with self.assertRaises(SystemExit) as stopped:
                q.main(["--config", str(self.config_path), "--output", str(self.output), "--smoke"])
        self.assertEqual(stopped.exception.code, 2)
        execute.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_offline_default_and_private_sanitized_manifest(self):
        with self.preflight() as command:
            self.assertEqual(self.invoke(), 0)
        self.assertEqual(command.call_count, 1)
        self.assertEqual(command.call_args.args[0][-1], "version")
        result = self.result()
        self.assertFalse(result["launch_qualified"])
        self.assertFalse(result["remote_artifact_verified"])
        self.assertNotIn("cleanup", result)
        self.assertNotIn("scratch_repo", result)
        self.assertEqual(set(q.SCENARIOS), {"config", "local_artifact", "aws_identity", "aws_bucket",
                                         "clean_source_provenance", "deployed_artifact", "live_smoke", "real_aws_qualification"})
        self.assertEqual(stat.S_IMODE(self.output.stat().st_mode), 0o700)
        for file in self.output.iterdir():
            self.assertEqual(stat.S_IMODE(file.stat().st_mode), 0o600)
            self.assertNotIn(SECRET, file.read_text())
        self.assertEqual({case["id"] for case in result["scenarios"]}, set(q.SCENARIOS))
        for case in result["scenarios"]:
            self.assertIn(case["status"], ("passed", "skipped"))
            if case["status"] == "skipped":
                self.assertTrue(case["reason"])

    def test_invalid_configuration_fails_without_execution(self):
        invalid = [
            {"unknown": SECRET}, {"schema_version": True}, {"candidate_commit": "a" * 39},
            {"binary_sha256": "A" * 64}, {"binary": ""}, {"binary": str(self.root)},
            {"topology": "local"}, {"endpoint": "http://127.0.0.1:8080"},
            {"replica_urls": []}, {"org": "test-org"}, {"private_key_file": SECRET},
            {"aws": {}}, {"aws": {"region": "us-east-1", "bucket": "bucket", "expected_bucket_owner": SECRET}},
        ]
        baseline = self.config.copy()
        for index, change in enumerate(invalid):
            with self.subTest(change=list(change)):
                self.config = baseline | change
                self.output = self.root / str(index)
                with self.preflight() as command:
                    self.assertEqual(self.invoke(), 1)
                command.assert_not_called()
                self.assertEqual(self.result()["status"], "failed")
                self.assertNotIn(SECRET, (self.output / "result.json").read_text())

    def test_hash_mismatch_never_executes_binary(self):
        self.config["binary_sha256"] = "0" * 64
        with self.preflight() as command:
            self.assertEqual(self.invoke(), 1)
        command.assert_not_called()

    def test_version_commit_must_match_exactly_and_reject_dirty_markers(self):
        for index, version in enumerate([COMMIT[:12], "b" * 40, COMMIT + "-dirty", COMMIT + "0"]):
            self.output = self.root / str(index)
            with patch.object(q.Runner, "command", return_value=f"openstorage v1 (commit {version}, go1)".encode()):
                self.assertEqual(self.invoke(), 1)
            self.assertEqual(next(case for case in self.result()["scenarios"] if case["id"] == "local_artifact")["reason"], "source_identity_mismatch")

    def test_existing_output_is_never_clobbered(self):
        self.output.mkdir()
        marker = self.output / "result.json"
        marker.write_text(SECRET)
        with self.preflight() as command:
            self.assertEqual(self.invoke(), 1)
        command.assert_not_called()
        self.assertEqual(marker.read_text(), SECRET)

    def test_aws_requested_failure_is_fatal_and_sanitized(self):
        self.config["aws"] = dict(region="us-east-1", bucket="existing-bucket", expected_bucket_owner="123456789012")
        def command(argv, **kwargs):
            if argv[0] == "aws":
                raise subprocess.CalledProcessError(1, argv, SECRET.encode(), SECRET.encode())
            return self.command(argv, **kwargs)
        with patch.object(q.Runner, "command", side_effect=command), patch.object(q.shutil, "which", return_value="/bin/fake"):
            self.assertEqual(self.invoke(aws_readonly=True), 1)
        for file in self.output.iterdir():
            self.assertNotIn(SECRET, file.read_text())

    def test_subprocess_timeout_kills_owned_process_and_sanitizes(self):
        with tempfile.TemporaryDirectory() as work:
            runner = q.Runner({}, work, 1)
            with patch.object(runner, "remaining", return_value=0.05):
                with self.assertRaises(subprocess.TimeoutExpired):
                    runner.command([q.sys.executable, "-c", "import time; time.sleep(20)"])

    def test_subprocess_output_limit_is_enforced_before_exit(self):
        runner = q.Runner({}, str(self.root), 1)
        script = f"import sys, time; sys.stdout.buffer.write(b'x' * {q.LIMIT + 1}); sys.stdout.flush(); time.sleep(20)"
        with self.assertRaisesRegex(q.Failure, "^command_output_too_large$"):
            runner.command([q.sys.executable, "-c", script])
        runner = q.Runner({}, str(self.root), 5)
        data = runner.command([q.sys.executable, "-c", f"import sys; sys.stdout.buffer.write(b'x' * {q.LIMIT})"])
        self.assertEqual(data, b"x" * q.LIMIT)

    def test_budget_range_is_enforced_before_execution(self):
        for index, budget in enumerate([0, -1, 901, True, "1"]):
            self.output = self.root / str(index)
            with self.preflight() as command:
                self.assertEqual(self.invoke(budget=budget), 1)
            command.assert_not_called()

    def test_hash_and_version_consistency_do_not_attest_clean_source(self):
        with self.preflight():
            self.assertEqual(self.invoke(), 0)
        result = self.result()
        self.assertFalse(result["clean_source_verified"])
        self.assertFalse(result["remote_artifact_verified"])
        self.assertFalse(result["launch_qualified"])
        cases = {case["id"]: case for case in result["scenarios"]}
        self.assertEqual(cases["local_artifact"]["status"], "passed")
        self.assertEqual(cases["clean_source_provenance"]["status"], "skipped")
        self.assertIn("consistency checks", cases["clean_source_provenance"]["reason"])

    def test_requested_aws_uses_only_readonly_expected_owner_operations(self):
        self.config["aws"] = dict(region="us-east-1", bucket="existing-bucket", expected_bucket_owner="123456789012")
        with self.preflight() as command, patch.object(q.shutil, "which", return_value="/bin/fake"):
            self.assertEqual(self.invoke(aws_readonly=True), 0)
        aws = [call.args[0] for call in command.call_args_list if call.args[0][0] == "aws"]
        self.assertEqual([args[1:3] for args in aws], [["sts", "get-caller-identity"], ["s3api", "head-bucket"]])
        self.assertIn("--expected-bucket-owner", aws[1])
        self.assertEqual(aws[1][aws[1].index("--expected-bucket-owner") + 1], "123456789012")
        self.output = self.root / "head-fails"
        def fail_head(argv, **kwargs):
            if "head-bucket" in argv:
                raise TimeoutError(SECRET)
            return self.command(argv, **kwargs)
        with patch.object(q.Runner, "command", side_effect=fail_head), patch.object(q.shutil, "which", return_value="/bin/fake"):
            self.assertEqual(self.invoke(aws_readonly=True), 1)
        cases = {case["id"]: case for case in self.result()["scenarios"]}
        self.assertEqual(cases["aws_identity"]["status"], "passed")
        self.assertEqual(cases["aws_bucket"]["status"], "failed")

    def test_offline_preflight_needs_no_go_toolchain(self):
        with self.preflight() as command, patch.object(q.shutil, "which", return_value=None) as which:
            self.assertEqual(self.invoke(), 0)
        which.assert_not_called()
        self.assertEqual(command.call_count, 1)
        self.assertEqual(command.call_args.args[0][-1], "version")

    def test_timeout_reaps_child_even_if_process_group_already_exited(self):
        runner = q.Runner({}, str(self.root), 1)
        read, write = os.pipe()
        os.close(write)
        with os.fdopen(read, "rb") as stdout, patch.object(q.subprocess, "Popen") as popen, patch.object(q.os, "killpg", side_effect=ProcessLookupError):
            popen.return_value.stdout = stdout
            popen.return_value.wait.side_effect = [subprocess.TimeoutExpired("child", 1), 0]
            with self.assertRaises(subprocess.TimeoutExpired):
                runner.command(["child"])
            self.assertEqual(popen.return_value.wait.call_count, 2)

    def test_aws_requires_config_and_dependency_before_execution(self):
        with self.preflight() as command, patch.object(q.shutil, "which", return_value="/bin/fake"):
            self.assertEqual(self.invoke(aws_readonly=True), 1)
        command.assert_not_called()
        self.config["aws"] = dict(region="us-east-1", bucket="existing-bucket", expected_bucket_owner="123456789012")
        self.output = self.root / "missing-aws"
        with self.preflight() as command, patch.object(q.shutil, "which", return_value=None):
            self.assertEqual(self.invoke(aws_readonly=True), 1)
        command.assert_not_called()

    def test_missing_or_nonexecutable_binary_fails_before_execution(self):
        self.binary.chmod(0o600)
        with self.preflight() as command:
            self.assertEqual(self.invoke(), 1)
        command.assert_not_called()
        self.binary.unlink()
        self.output = self.root / "missing-binary"
        with self.preflight() as command:
            self.assertEqual(self.invoke(), 1)
        command.assert_not_called()

    def test_duplicate_config_fields_fail_before_execution(self):
        self.config_path.write_text('{"schema_version":1,"schema_version":1}')
        with self.preflight() as command:
            self.assertEqual(q.execute(self.config_path, self.output), 1)
        command.assert_not_called()

    def test_preflight_does_not_install_global_signal_handlers(self):
        with self.preflight(), patch.object(q.signal, "signal") as signal, patch.object(q.signal, "setitimer") as timer:
            self.assertEqual(self.invoke(), 0)
        signal.assert_not_called()
        timer.assert_not_called()

    def test_aws_preserves_shared_config_and_sso_home(self):
        executable = self.root / "aws"
        executable.write_text(f"#!{q.sys.executable}\nimport os\nprint(os.environ['HOME'])\n")
        executable.chmod(0o700)
        with patch.dict(os.environ, {"HOME": str(self.root / "caller"), "PATH": str(self.root)}):
            runner = q.Runner({}, str(self.root / "scratch"), 5)
            runner.work.mkdir()
            self.assertEqual(runner.command(["aws", "sts", "get-caller-identity"]).decode().strip(), str(self.root / "caller"))


if __name__ == "__main__":
    unittest.main()
