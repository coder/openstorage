"""Offline regression coverage for the opt-in qualification runner."""
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
        self.binary = self.root / "openstorage"
        self.binary.write_bytes(b"verified helper")
        self.binary.chmod(0o700)
        self.key = self.root / SECRET
        self.key.write_text(SECRET)
        self.key.chmod(0o600)
        self.config = dict(schema_version=1, candidate_commit=COMMIT,
                           binary=str(self.binary), binary_sha256=hashlib.sha256(self.binary.read_bytes()).hexdigest(),
                           topology="local", endpoint="http://127.0.0.1:8080", replica_urls=[],
                           org="test-org", private_key_file=str(self.key))
        self.config_path = self.root / "config.json"
        self.output = self.root / "evidence"

    def invoke(self, **kwargs):
        self.config_path.write_text(json.dumps(self.config))
        return q.execute(self.config_path, self.output, **kwargs)

    def result(self):
        return json.loads((self.output / "result.json").read_text())

    def command(self, argv, **kwargs):
        if argv[-1] == "version" and argv[0] != "git":
            return f"openstorage devel (commit {COMMIT}, built now, go1.26.0)\n".encode()
        if "mint" in argv:
            return b"aaa.bbb.ccc\n"
        return b""

    def preflight(self):
        return patch.object(q.Runner, "command", side_effect=self.command)

    def test_offline_default_and_private_sanitized_manifest(self):
        with self.preflight(), patch.object(q.Runner, "http") as http:
            self.assertEqual(self.invoke(), 0)
        http.assert_not_called()
        result = self.result()
        self.assertFalse(result["launch_qualified"])
        self.assertFalse(result["remote_artifact_verified"])
        self.assertEqual(result["cleanup"]["status"], "not_needed")
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
            {"binary_sha256": "A" * 64}, {"topology": "other"},
            {"endpoint": "https://user:" + SECRET + "@example.com"},
            {"endpoint": "https://example.com/?token=" + SECRET},
            {"endpoint": "https://example.com/#" + SECRET},
            {"endpoint": "https://example.com/base"}, {"endpoint": "http://example.com"},
            {"endpoint": "https://example.com\\@evil.test"},
            {"topology": "pg-s3", "endpoint": "https://example.com", "replica_urls": []},
            {"topology": "s3wal", "replica_urls": ["https://example.com"]},
            {"aws": {"region": "us-east-1", "bucket": "bucket", "expected_bucket_owner": SECRET}},
        ]
        baseline = self.config.copy()
        for index, change in enumerate(invalid):
            with self.subTest(change=list(change)):
                self.config = baseline | change
                self.output = self.root / str(index)
                with self.preflight() as command:
                    self.assertEqual(self.invoke(smoke=True), 1)
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

    def test_missing_or_public_key_and_missing_dependency(self):
        self.key.chmod(0o644)
        with self.preflight() as command:
            self.assertEqual(self.invoke(), 1)
        command.assert_not_called()
        self.key.unlink()
        self.output = self.root / "missing"
        with self.preflight():
            self.assertEqual(self.invoke(), 1)
        self.key.write_text(SECRET)
        self.key.chmod(0o600)
        self.output = self.root / "dependency"
        with self.preflight(), patch.object(q.shutil, "which", return_value=None):
            self.assertEqual(self.invoke(smoke=True), 1)

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

    def test_smoke_interrupt_emits_partial_manifest_and_removes_temp(self):
        paths = []
        def smoke(runner):
            paths.append(runner.work)
            (runner.work / "temporary-token").write_text(SECRET)
            raise KeyboardInterrupt
        with self.preflight(), patch.object(q.shutil, "which", return_value="/bin/fake"), patch.object(q.Runner, "smoke", smoke):
            self.assertEqual(self.invoke(smoke=True), 1)
        self.assertEqual(self.result()["status"], "failed")
        self.assertTrue(paths)
        self.assertTrue(all(not path.exists() for path in paths))

    def fake_http(self, runner, base, path, token=None, method="GET", data=None, expected=200, git=False):
        if path == "/api/v1/repos" and method == "POST":
            self.assertEqual(json.loads(data), {"default_branch": "main"})
            return json.dumps({"repo_name": runner.repo, "repo_id": "generated-id"}).encode()
        if path.endswith("commit-pack"):
            self.assertEqual(json.loads(data.splitlines()[0])["metadata"]["target_branch"], "main")
            return json.dumps({"commit": {"commit_sha": COMMIT}, "result": {"success": True, "new_sha": COMMIT}}).encode()
        if path.startswith("/api/v1/repos/file"):
            return b"qualification Git\n"
        return b""

    def fake_command(self, argv, cwd=None):
        if argv[0] != "git":
            return self.command(argv, cwd=cwd)
        if "clone" in argv:
            clone = Path(argv[-1])
            clone.mkdir()
            if clone.name == "clone":
                (clone / "hello.txt").write_bytes(b"qualification API\n")
            else:
                for name in ("hello.txt", "payload.bin"):
                    (clone / name).write_bytes((clone.parent / "clone" / name).read_bytes())
        if "show" in argv:
            payload = (Path(cwd) / "payload.bin").read_bytes()
            return f"version https://git-lfs.github.com/spec/v1\noid sha256:{hashlib.sha256(payload).hexdigest()}\nsize {len(payload)}\n".encode()
        if "rev-parse" in argv:
            changed = (Path(cwd) / "hello.txt").read_bytes() == b"qualification Git\n"
            return (("b" * 40 if changed else COMMIT) + "\n").encode()
        return b""

    def smoke_with(self, http=None):
        with patch.object(q.Runner, "http", autospec=True, side_effect=http or self.fake_http) as requests, \
                patch.object(q.Runner, "command", side_effect=self.fake_command) as commands, \
                patch.object(q.shutil, "which", return_value="/bin/fake"):
            code = self.invoke(smoke=True)
        return code, requests.call_args_list, commands.call_args_list

    def test_successful_smoke_soft_deletes_only_owned_unique_repo(self):
        code, calls, commands = self.smoke_with()
        self.assertEqual(code, 0)
        result = self.result()
        self.assertRegex(result["scratch_repo"], r"^q222-[a-f0-9-]{36}$")
        self.assertFalse(result["launch_qualified"])
        self.assertEqual(result["cleanup"]["status"], "soft_deleted")
        deleted = [call for call in calls if call.args[2] == "/api/v1/repos/delete"]
        self.assertEqual(len(deleted), 1)
        self.assertTrue(deleted[0].args[0].created)
        argv = [call.args[0] for call in commands]
        self.assertTrue(any("git-lfs" in str(arg) or "lfs" in arg for arg in argv))
        self.assertTrue(any("push" in arg and "refs/tags/qualification" in arg for arg in argv))
        for arg in argv:
            self.assertNotIn("aaa.bbb.ccc", " ".join(arg))
        self.assertTrue(all(not call.args[0].work.exists() for call in calls))

    def test_create_conflict_timeout_and_bad_ack_never_delete(self):
        for index, outcome in enumerate([q.Failure("unexpected_http_status"), TimeoutError(SECRET), b"{}", b"null"]):
            self.output = self.root / str(index)
            def http(runner, base, path, *args, **kwargs):
                if path == "/api/v1/repos":
                    if isinstance(outcome, Exception):
                        raise outcome
                    return outcome
                return self.fake_http(runner, base, path, *args, **kwargs)
            code, calls, _ = self.smoke_with(http)
            self.assertEqual(code, 1)
            self.assertEqual(self.result()["cleanup"]["status"], "unknown")
            self.assertFalse(any(call.args[2] == "/api/v1/repos/delete" for call in calls))

    def test_cleanup_failure_fails_otherwise_successful_smoke(self):
        def http(runner, base, path, *args, **kwargs):
            if path == "/api/v1/repos/delete":
                raise OSError(SECRET)
            return self.fake_http(runner, base, path, *args, **kwargs)
        code, _, _ = self.smoke_with(http)
        self.assertEqual(code, 1)
        result = self.result()
        self.assertEqual(result["cleanup"]["status"], "unknown")
        cases = {case["id"]: case for case in result["scenarios"]}
        self.assertEqual(cases["lfs_roundtrip"]["status"], "passed")
        self.assertEqual(cases["cleanup"]["status"], "failed")
        self.assertNotIn(SECRET, (self.output / "runner.log").read_text())

    def test_api_unsuccessful_and_interrupt_after_creation_still_cleanup(self):
        for index, outcome in enumerate([b'{"commit":{"commit_sha":"' + COMMIT.encode() + b'"},"result":{"success":false}}', KeyboardInterrupt()]):
            self.output = self.root / str(index)
            def http(runner, base, path, *args, **kwargs):
                if path.endswith("commit-pack"):
                    if isinstance(outcome, BaseException):
                        raise outcome
                    return outcome
                return self.fake_http(runner, base, path, *args, **kwargs)
            code, calls, _ = self.smoke_with(http)
            self.assertEqual(code, 1)
            self.assertEqual(self.result()["cleanup"]["status"], "soft_deleted")
            self.assertTrue(any(call.args[2] == "/api/v1/repos/delete" for call in calls))

    def test_subprocess_timeout_kills_owned_process_and_sanitizes(self):
        with tempfile.TemporaryDirectory() as work:
            runner = q.Runner({}, work, 1)
            with patch.object(runner, "remaining", return_value=0.05):
                with self.assertRaises(subprocess.TimeoutExpired):
                    runner.command([q.sys.executable, "-c", "import time; time.sleep(20)"])

    def test_inherited_git_environment_is_removed(self):
        with patch.dict(os.environ, {"GIT_SSH_COMMAND": SECRET, "GIT_CONFIG_COUNT": "9", "GIT_ASKPASS": SECRET, "GIT_TRACE": SECRET}):
            runner = q.Runner({}, str(self.root), 1)
        for key in ("GIT_SSH_COMMAND", "GIT_CONFIG_COUNT", "GIT_ASKPASS", "GIT_TRACE"):
            self.assertNotIn(key, runner.env)
        self.assertEqual(runner.env["GIT_CONFIG_GLOBAL"], os.devnull)
        self.assertEqual(runner.env["HOME"], str(self.root))

    def test_urllib_redirect_does_not_forward_credentials(self):
        handler = q.NoRedirect()
        request = q.urllib.request.Request("https://example.test", headers={"Authorization": "Bearer " + SECRET})
        for code in (301, 302, 303, 307, 308):
            self.assertIsNone(handler.redirect_request(request, None, code, "", {}, "https://other.test"))

    def test_budget_range_is_enforced_before_execution(self):
        for index, budget in enumerate([0, -1, 901, True, "1"]):
            self.output = self.root / str(index)
            with self.preflight() as command:
                self.assertEqual(self.invoke(budget=budget), 1)
            command.assert_not_called()

    def test_overall_budget_interrupts_blocking_smoke_and_keeps_manifest(self):
        import time
        start = time.monotonic()
        def smoke(runner):
            runner.step("health")
            time.sleep(2)
        with self.preflight(), patch.object(q.shutil, "which", return_value="/bin/fake"), patch.object(q.Runner, "smoke", smoke):
            self.assertEqual(self.invoke(smoke=True, budget=1), 1)
        self.assertLess(time.monotonic() - start, 1.8)
        self.assertEqual(self.result()["status"], "failed")

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

    def test_aws_preserves_shared_config_and_sso_home_without_changing_git_home(self):
        with patch.dict(os.environ, {"HOME": str(self.root / "caller")}):
            runner = q.Runner({}, str(self.root / "scratch"), 5)
            runner.work.mkdir()
            with patch.object(q.subprocess, "Popen") as popen:
                popen.return_value.wait.return_value = 0
                runner.command(["aws", "sts", "get-caller-identity"])
                self.assertEqual(popen.call_args.kwargs["env"]["HOME"], str(self.root / "caller"))
                runner.command(["git", "version"])
                self.assertEqual(popen.call_args.kwargs["env"]["HOME"], str(self.root / "scratch"))

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

    def test_pg_s3_writes_first_replica_and_reads_second_and_public(self):
        self.config.update(topology="pg-s3", endpoint="https://public.test", replica_urls=["https://one.test", "https://two.test"])
        code, requests, commands = self.smoke_with()
        self.assertEqual(code, 0)
        create = next(call for call in requests if call.args[2] == "/api/v1/repos")
        self.assertEqual(create.args[1], "https://one.test")
        clones = [call.args[0] for call in commands if "clone" in call.args[0]]
        self.assertTrue(all(args[-2].startswith("https://two.test/") for args in clones))
        reads = {call.args[1] for call in requests if call.args[2].startswith("/api/v1/repos/file")}
        self.assertEqual(reads, {"https://public.test", "https://one.test", "https://two.test"})
        cases = {case["id"]: case for case in self.result()["scenarios"]}
        self.assertEqual(cases["cross_replica"]["status"], "passed")
        for name in ("standard_topology", "deployed_artifact", "infrastructure_teardown", "aws_temporary_credentials", "cold_cache_restart", "s3wal_qualification", "lfs_redirect_control"):
            self.assertEqual(cases[name]["status"], "skipped")
            self.assertTrue(cases[name]["reason"].startswith("not implemented:"))

    def test_duplicate_keys_and_normalized_replica_duplicates_are_invalid(self):
        self.config_path.write_text('{"schema_version":1,"schema_version":1}')
        with self.preflight() as command:
            self.assertEqual(q.execute(self.config_path, self.output), 1)
        command.assert_not_called()
        self.output = self.root / "duplicate-replicas"
        self.config.update(topology="pg-s3", endpoint="https://public.test", replica_urls=["https://REPLICA.test", "https://replica.test:443"])
        with self.preflight() as command:
            self.assertEqual(self.invoke(), 1)
        command.assert_not_called()

    def test_offline_preflight_needs_no_go_toolchain(self):
        with self.preflight() as command, patch.object(q.shutil, "which", return_value=None) as which:
            self.assertEqual(self.invoke(), 0)
        which.assert_not_called()
        self.assertEqual(command.call_count, 1)
        self.assertEqual(command.call_args.args[0][-1], "version")

    def test_credential_helper_is_private_and_origin_scoped(self):
        def command(argv, **kwargs):
            if "clone" in argv:
                work = Path(argv[-1]).parent
                helper = work / "credential"
                self.assertEqual(stat.S_IMODE(helper.stat().st_mode), 0o700)
                self.assertEqual(stat.S_IMODE((work / "credentials.json").stat().st_mode), 0o600)
                url = q.urllib.parse.urlsplit(argv[-2])
                fields = f"protocol={url.scheme}\nhost={url.netloc}\npath={url.path[1:]}\n\n"
                accepted = subprocess.run([str(helper), "get"], input=fields, text=True, capture_output=True, timeout=3, check=True)
                self.assertEqual(accepted.stdout, "username=t\npassword=aaa.bbb.ccc\n")
                denied = subprocess.run([str(helper), "get"], input=fields.replace(url.netloc, "untrusted.test"), text=True, capture_output=True, timeout=3, check=True)
                self.assertEqual(denied.stdout, "")
            return self.fake_command(argv, **kwargs)
        with patch.object(q.Runner, "http", autospec=True, side_effect=self.fake_http), patch.object(q.Runner, "command", side_effect=command), patch.object(q.shutil, "which", return_value="/bin/fake"):
            self.assertEqual(self.invoke(smoke=True), 0)
        for file in self.output.iterdir():
            self.assertNotIn("aaa.bbb.ccc", file.read_text())

    def test_timeout_reaps_child_even_if_process_group_already_exited(self):
        runner = q.Runner({}, str(self.root), 1)
        with patch.object(q.subprocess, "Popen") as popen, patch.object(q.os, "killpg", side_effect=ProcessLookupError):
            popen.return_value.wait.side_effect = [subprocess.TimeoutExpired("child", 1), 0]
            with self.assertRaises(subprocess.TimeoutExpired):
                runner.command(["child"])
            self.assertEqual(popen.return_value.wait.call_count, 2)

    def test_lfs_uses_client_policy_on_trusted_endpoints(self):
        with self.preflight():
            self.assertEqual(self.invoke(), 0)
        case = next(case for case in self.result()["scenarios"] if case["id"] == "lfs_redirect_control")
        self.assertEqual(case["status"], "skipped")
        self.assertIn("trusted endpoints", case["reason"])
        self.assertIn("client redirect policy", case["reason"])
        self.assertNotIn("must not redirect", case["reason"])


if __name__ == "__main__":
    unittest.main()
