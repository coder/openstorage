"""Offline regression coverage for the opt-in qualification runner."""
import base64
import hashlib
import hmac
import itertools
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
REPO_ID = "repo_0123456789ab"
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

    def test_equivalent_replica_spellings_fail_offline_without_execution(self):
        pairs = [
            ["https://[2001:db8::1]", "https://[2001:0db8:0:0:0:0:0:1]:443"],
            ["https://REPLICA.test.", "https://replica.test"],
            ["https://192.0.2.1", "https://[::ffff:c000:201]"],
            ["https://[fe80::1%25eth0]", "https://[fe80::2%25eth0]"],
        ]
        for index, replicas in enumerate(pairs):
            with self.subTest(replicas=replicas):
                self.config.update(topology="pg-s3", endpoint="https://public.test", replica_urls=replicas)
                self.output = self.root / str(index)
                with self.preflight() as command, patch.object(q.Runner, "http") as http:
                    self.assertEqual(self.invoke(), 1)
                command.assert_not_called()
                http.assert_not_called()

    def test_replica_resolution_fails_closed_before_http_or_creation(self):
        self.config.update(topology="pg-s3", endpoint="https://public.test", replica_urls=["https://one.test", "https://two.test"])
        cases = [
            [["192.0.2.1", 0]],
            [["192.0.2.2", 0], ["::ffff:192.0.2.1", 0]],
            [], [["not-an-ip", 0]], [["fe80::1%eth0", 0]], [["fe80::1", 2]],
            [["192.0.2.2"]], {"address": "192.0.2.2"},
            subprocess.TimeoutExpired(["resolver"], 1),
        ]
        for index, answers in enumerate(cases):
            with self.subTest(answers=answers):
                self.output = self.root / str(index)
                resolved = []
                def command(argv, **kwargs):
                    if argv[0] == q.sys.executable:
                        resolved.append(argv[-2])
                        if argv[-2] == "two.test":
                            if isinstance(answers, Exception):
                                raise answers
                            return json.dumps(answers).encode()
                        return b'[["192.0.2.1",0]]'
                    return self.fake_command(argv, **kwargs)
                with patch.object(q.Runner, "command", side_effect=command), \
                        patch.object(q.Runner, "http") as http, patch.object(q.shutil, "which", return_value="/bin/fake"):
                    self.assertEqual(self.invoke(smoke=True), 1)
                self.assertEqual(resolved, ["one.test", "two.test"])
                http.assert_not_called()
                self.assertEqual(self.result()["cleanup"]["status"], "not_needed")

    def test_disjoint_replica_answers_preserve_urls_and_offline_does_not_resolve(self):
        self.config.update(topology="pg-s3", endpoint="https://public.test", replica_urls=["https://one.test.", "https://two.test"])
        resolved = []
        def command(argv, **kwargs):
            if argv[0] == q.sys.executable:
                resolved.append(argv[-2:])
                answers = [["192.0.2.1", 0], ["2001:db8::1", 0]] if argv[-2] == "one.test." else [["192.0.2.2", 0], ["2001:db8::2", 0]]
                return json.dumps(answers).encode()
            return self.fake_command(argv, **kwargs)
        with patch.object(q.Runner, "command", side_effect=command), patch.object(q.Runner, "http") as http:
            self.assertEqual(self.invoke(), 0)
        self.assertEqual(resolved, [])
        http.assert_not_called()
        self.output = self.root / "smoke"
        with patch.object(q.Runner, "command", side_effect=command), \
                patch.object(q.Runner, "http", autospec=True, side_effect=self.fake_http) as http, \
                patch.object(q.shutil, "which", return_value="/bin/fake"):
            self.assertEqual(self.invoke(smoke=True), 0)
        self.assertEqual(resolved, [["one.test.", "443"], ["two.test", "443"]])
        self.assertEqual({call.args[1] for call in http.call_args_list}, {"https://public.test", "https://one.test.", "https://two.test"})

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
            return json.dumps({"repo_name": runner.repo, "repo_id": REPO_ID,
                               "http_url": runner.config["endpoint"] + "/" + REPO_ID + ".git"}).encode()
        if path.endswith("commit-pack"):
            self.assertEqual(json.loads(data.splitlines()[0])["metadata"]["target_branch"], "main")
            return json.dumps({"commit": {"commit_sha": COMMIT}, "result": {"success": True, "new_sha": COMMIT}}).encode()
        if path.startswith("/api/v1/repos/file"):
            return b"qualification Git\n"
        return b""

    def fake_command(self, argv, cwd=None):
        if argv[0] == q.sys.executable:
            return json.dumps([[{"one.test": "192.0.2.1", "two.test": "192.0.2.2"}[argv[-2]], 0]]).encode()
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
        if "ls-remote" in argv:
            return (COMMIT + "\trefs/heads/main\n").encode()
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

    def test_create_identity_and_url_must_match_canonical_contract(self):
        canonical = self.config["endpoint"] + "/" + REPO_ID + ".git"
        invalid = [
            None, {"repo_name": "another-repo"}, {"repo_id": None}, {"repo_id": "generated-id"},
            {"repo_id": "repo_0123456789AB"}, {"repo_id": "repo_0123456789abc"},
            {"repo_id": "../" + REPO_ID}, {"http_url": None}, {"http_url": ""},
            {"http_url": "https://untrusted.test/" + REPO_ID + ".git"},
            {"http_url": canonical.replace(REPO_ID, "repo_abcdef012345")},
            {"http_url": canonical + "?token=" + SECRET}, {"http_url": canonical + "#fragment"},
            {"http_url": canonical.replace("127.0.0.1", "user@127.0.0.1")},
            {"http_url": canonical.replace("repo_", "%72epo_")},
        ]
        for index, change in enumerate(invalid):
            with self.subTest(change=change):
                self.output = self.root / str(index)
                def http(runner, base, path, *args, **kwargs):
                    body = self.fake_http(runner, base, path, *args, **kwargs)
                    if path == "/api/v1/repos":
                        acknowledgment = json.loads(body)
                        if change is None:
                            acknowledgment.pop("http_url")
                        else:
                            acknowledgment.update(change)
                        body = json.dumps(acknowledgment).encode()
                    return body
                code, calls, commands = self.smoke_with(http)
                self.assertEqual(code, 1)
                self.assertEqual(self.result()["cleanup"]["status"], "unknown")
                self.assertEqual(next(case for case in self.result()["scenarios"] if case["id"] == "create_repo")["status"], "failed")
                self.assertFalse(any(call.args[0][0] == "git" for call in commands))
                self.assertFalse(any(call.args[2] == "/api/v1/repos/delete" for call in calls))
                self.assertTrue(all(call.args[1] == self.config["endpoint"] for call in calls))
                self.assertNotIn(SECRET, (self.output / "runner.log").read_text())

    def test_create_url_accepts_equivalent_origin_spellings(self):
        for index, (endpoint, advertised) in enumerate([
            ("https://PUBLIC.test:443", "https://PUBLIC.test:443"),
            ("https://public.test", "https://PUBLIC.test:443"),
            ("http://LOCALHOST:80", "http://LOCALHOST:80"),
            ("http://[::1]:80", "http://[::1]:80"),
        ]):
            with self.subTest(endpoint=endpoint, advertised=advertised):
                self.config["endpoint"] = endpoint
                self.output = self.root / str(index)
                def http(runner, base, path, *args, **kwargs):
                    body = self.fake_http(runner, base, path, *args, **kwargs)
                    if path == "/api/v1/repos":
                        ack = json.loads(body)
                        ack["http_url"] = advertised + "/" + REPO_ID + ".git"
                        body = json.dumps(ack).encode()
                    return body
                code, calls, commands = self.smoke_with(http)
                self.assertEqual(code, 0)
                normalized = q.origin(endpoint, True)
                canonical = normalized + "/" + REPO_ID + ".git"
                self.assertEqual([call.args[0][-2] for call in commands if "ls-remote" in call.args[0]], [canonical])
                self.assertTrue(all(call.args[1] == normalized for call in calls))
                self.assertEqual(self.result()["cleanup"]["status"], "soft_deleted")

    def test_smoke_uses_canonical_git_url_and_direct_replica_paths(self):
        baseline = self.config.copy()
        for topology in ("local", "pg-s3"):
            with self.subTest(topology=topology):
                self.config = baseline.copy()
                self.output = self.root / topology
                if topology == "pg-s3":
                    self.config.update(topology=topology, endpoint="https://public.test", replica_urls=["https://one.test", "https://two.test"])
                code, calls, commands = self.smoke_with()
                self.assertEqual(code, 0)
                public = self.config["endpoint"]
                writer, reader = self.config["replica_urls"] or (public, public)
                path = "/" + REPO_ID + ".git"
                argv = [call.args[0] for call in commands]
                advertised = [args[-2] for args in argv if "ls-remote" in args]
                self.assertEqual(advertised, [public + path])
                self.assertEqual([args[-2] for args in argv if "clone" in args], [reader + path, reader + path])
                self.assertEqual([args[-1] for args in argv if "set-url" in args], [writer + path])
                for args in argv:
                    for arg in args:
                        if arg.startswith(("https://", "http://")):
                            self.assertIn(arg, {base + path for base in (public, writer, reader)})
                    if args[0] == "git":
                        self.assertIn("http.followRedirects=false", args)
                origins = 3 if topology == "pg-s3" else 1
                name = [call.args[2] for call in calls if call.kwargs.get("expected") == 404 and call.kwargs.get("git")]
                self.assertEqual(len(name), origins)
                self.assertTrue(all(p.startswith("/q222-") and p.endswith(".git/info/refs?service=git-upload-pack") for p in name))
                canonical = [call.args[2] for call in calls if call.kwargs.get("expected") == 403 and call.args[2].startswith(path)]
                self.assertEqual(canonical, [path + "/info/refs?service=git-upload-pack"] * origins)

    def test_cleanup_matches_git_status_rules_after_soft_delete(self):
        # Mirrors internal/githttp authenticate(): the token names the repository, so a
        # deleted name path is a 404, while a repo-ID path that no longer resolves is a
        # 403 (claim mismatch, never an existence oracle).
        self.config.update(topology="pg-s3", endpoint="https://public.test", replica_urls=["https://one.test", "https://two.test"])
        deleted = []
        observed = []
        def http(runner, base, path, token=None, method="GET", data=None, expected=200, git=False):
            if path == "/api/v1/repos/delete":
                deleted.append(base)
            elif deleted and path == "/api/v1/repo":
                observed.append((base, "api", expected))
                q.require(expected == 404, "unexpected_http_status")
            elif deleted and git:
                name_path = path.startswith("/" + runner.repo + ".git/")
                self.assertTrue(name_path or path.startswith("/" + REPO_ID + ".git/"))
                observed.append((base, "name" if name_path else "id", expected))
                q.require(expected == (404 if name_path else 403), "unexpected_http_status")
            return self.fake_http(runner, base, path, token, method, data, expected, git)
        code, _, _ = self.smoke_with(http)
        self.assertEqual(code, 0)
        self.assertEqual(deleted, ["https://one.test"])
        self.assertEqual(observed, [(base, kind, status) for base in ("https://public.test", "https://one.test", "https://two.test")
                                    for kind, status in (("api", 404), ("name", 404), ("id", 403))])
        result = self.result()
        self.assertEqual(result["cleanup"]["status"], "soft_deleted")
        self.assertEqual(next(case for case in result["scenarios"] if case["id"] == "cleanup")["status"], "passed")

    def test_minted_tokens_outlive_maximum_run_and_cleanup(self):
        code, _, commands = self.smoke_with()
        self.assertEqual(code, 0)
        minted = [call.args[0] for call in commands if "mint" in call.args[0]]
        self.assertEqual(len(minted), 2)
        for argv in minted:
            ttl = argv[argv.index("--ttl") + 1]
            self.assertRegex(ttl, r"^[0-9]+m$")
            self.assertGreater(int(ttl[:-1]) * 60, 900 + 45)

    def test_auth_denials_detect_forged_signature_acceptance_on_api_and_git(self):
        self.config.update(topology="pg-s3", endpoint="https://public.test", replica_urls=["https://one.test", "https://two.test"])
        # A local HMAC fixture provides an independent signature oracle, not a deployment auth test.
        key = b"offline qualification signature fixture"
        def encode(data):
            return base64.urlsafe_b64encode(data).decode().rstrip("=")
        def decode(data):
            return base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))
        def command(argv, **kwargs):
            if "mint" in argv:
                claims = {"iss": "test-org", "repo": argv[argv.index("--repo") + 1],
                          "scopes": argv[argv.index("--scopes") + 1].split(",")}
                message = encode(b'{"alg":"HS256","typ":"JWT"}') + "." + encode(json.dumps(claims).encode())
                return (message + "." + encode(hmac.digest(key, message.encode(), "sha256"))).encode()
            return self.fake_command(argv, **kwargs)
        for accepts_forged in (None, "api", "git"):
            with self.subTest(accepts_forged=accepts_forged):
                self.output = self.root / str(accepts_forged)
                forged_calls = []
                def http(runner, base, path, token=None, method="GET", data=None, expected=200, git=False):
                    if runner.active == "auth_denial" and token not in (None, "invalid"):
                        header, payload, signature = token.split(".")
                        authentic = hmac.digest(key, (header + "." + payload).encode(), "sha256")
                        if not hmac.compare_digest(decode(signature), authentic):
                            self.assertEqual((header, payload), tuple(runner.token.split(".")[:2]))
                            self.assertEqual(json.loads(decode(header))["alg"], "HS256")
                            self.assertEqual(json.loads(decode(payload))["scopes"], ["repo:write", "git:read", "git:write"])
                            self.assertEqual(len(decode(signature)), len(authentic))
                            interface = "git" if git else "api"
                            forged_calls.append((base, interface, expected))
                            q.require((200 if accepts_forged == interface else 401) == expected, "unexpected_http_status")
                    return self.fake_http(runner, base, path, token, method, data, expected, git)
                with patch.object(q.Runner, "http", autospec=True, side_effect=http) as requests, \
                        patch.object(q.Runner, "command", side_effect=command), patch.object(q.shutil, "which", return_value="/bin/fake"):
                    code = self.invoke(smoke=True)
                if accepts_forged is None:
                    self.assertEqual(code, 0)
                    self.assertEqual(forged_calls, [(base, interface, 401) for base in
                                     ("https://public.test", "https://one.test", "https://two.test") for interface in ("api", "git")])
                    for token in (None, "invalid"):
                        self.assertEqual(sum(call.args[3] == token and call.kwargs.get("expected") == 401 for call in requests.call_args_list if len(call.args) > 3), 6)
                    self.assertEqual(sum(call.kwargs.get("expected") == 403 and not call.args[2].startswith("/" + REPO_ID)
                                         for call in requests.call_args_list), 6)
                else:
                    self.assertEqual(code, 1)
                    self.assertEqual(forged_calls[-1][1:], (accepts_forged, 401))
                    case = next(case for case in self.result()["scenarios"] if case["id"] == "auth_denial")
                    self.assertEqual((case["status"], case["reason"]), ("failed", "unexpected_http_status"))
                    self.assertEqual(self.result()["cleanup"]["status"], "not_needed")
                    self.assertFalse(any(call.args[2] == "/api/v1/repos" for call in requests.call_args_list))
                for call in requests.call_args_list:
                    if len(call.args) > 3 and isinstance(call.args[3], str) and call.args[3].count(".") == 2:
                        for file in self.output.iterdir():
                            self.assertNotIn(call.args[3], file.read_text())

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

    def test_sigterm_after_creation_records_failure_and_cleans_up(self):
        script = '''import os, signal, sys, tempfile
from pathlib import Path
from test_aws_launch_qualification import RunnerTests

tempfile.tempdir = sys.argv[2]
case = RunnerTests()
case.setUp()
case.output = Path(sys.argv[1])
previous = signal.getsignal(signal.SIGTERM)
def http(runner, base, path, *args, **kwargs):
    if path.endswith("commit-pack"):
        os.kill(os.getpid(), signal.SIGTERM)
    return case.fake_http(runner, base, path, *args, **kwargs)
try:
    code, _, _ = case.smoke_with(http)
    assert signal.getsignal(signal.SIGTERM) == previous
finally:
    case.doCleanups()
sys.exit(code)
'''
        env = os.environ | {"PYTHONPATH": str(Path(__file__).resolve().parent)}
        child = subprocess.run([q.sys.executable, "-B", "-c", script, str(self.output), str(self.root)],
                               env=env, capture_output=True, timeout=10)
        self.assertEqual(child.returncode, 1, child.stderr.decode())
        result = self.result()
        self.assertEqual(result["status"], "failed")
        self.assertRegex(result["scratch_repo"], r"^q222-")
        self.assertEqual(result["cleanup"]["status"], "soft_deleted")
        cases = {case["id"]: case for case in result["scenarios"]}
        self.assertEqual(cases["api_commit"]["reason"], "interrupted")
        self.assertNotIn(SECRET, (self.output / "runner.log").read_text())

    def test_main_alarm_cannot_skip_cleanup_during_handler_handoff(self):
        install = q.signal.signal
        for interrupt_at in (q.signal.SIGTERM, q.signal.SIGINT):
            with self.subTest(interrupt_at=interrupt_at):
                self.output = self.root / str(interrupt_at)
                fired = []
                def signal_handler(sig, handler):
                    if sig == interrupt_at and getattr(handler, "__name__", "") == "defer_cleanup_interrupt":
                        if q.signal.getitimer(q.signal.ITIMER_REAL)[0] > 0:
                            # Model the main timer expiring: a one-shot timer is no
                            # longer armed when its signal is delivered.
                            q.signal.setitimer(q.signal.ITIMER_REAL, 0)
                            fired.append(True)
                            os.kill(os.getpid(), q.signal.SIGALRM)
                    return install(sig, handler)
                with patch.object(q.signal, "signal", side_effect=signal_handler):
                    code, calls, _ = self.smoke_with()
                self.assertEqual(code, 1 if fired else 0)
                self.assertTrue(any(call.args[2] == "/api/v1/repos/delete" for call in calls))
                self.assertEqual(self.result()["cleanup"]["status"], "soft_deleted")

    def test_main_alarm_during_disarm_still_reaches_cleanup(self):
        install = q.signal.signal
        fired = []
        previous = [q.signal.getsignal(sig) for sig in (q.signal.SIGALRM, q.signal.SIGTERM, q.signal.SIGINT)]
        def signal_handler(sig, handler):
            if sig == q.signal.SIGALRM and handler == q.signal.SIG_IGN and not fired:
                q.signal.setitimer(q.signal.ITIMER_REAL, 0)
                fired.append(True)
                os.kill(os.getpid(), q.signal.SIGALRM)
            return install(sig, handler)
        with patch.object(q.signal, "signal", side_effect=signal_handler):
            code, calls, _ = self.smoke_with()
        self.assertEqual(fired, [True])
        self.assertEqual(code, 1)
        self.assertEqual(self.result()["status"], "failed")
        self.assertTrue(any(call.args[2] == "/api/v1/repos/delete" for call in calls))
        self.assertEqual(self.result()["cleanup"]["status"], "soft_deleted")
        self.assertEqual([q.signal.getsignal(sig) for sig in (q.signal.SIGALRM, q.signal.SIGTERM, q.signal.SIGINT)], previous)

    def test_cleanup_keeps_an_independent_alarm_deadline(self):
        arm = q.signal.setitimer
        reached = []
        def timer(which, seconds, interval=0):
            return arm(which, 0.02 if seconds == 45 else seconds, interval)
        def http(runner, base, path, *args, **kwargs):
            if path == "/api/v1/repos/delete":
                reached.append("delete")
                q.time.sleep(2)
                reached.append("unbounded")
            return self.fake_http(runner, base, path, *args, **kwargs)
        with patch.object(q.signal, "setitimer", side_effect=timer):
            code, _, _ = self.smoke_with(http)
        self.assertEqual(code, 1)
        self.assertEqual(reached, ["delete"])
        self.assertEqual(self.result()["cleanup"]["status"], "unknown")
        self.assertEqual(next(case for case in self.result()["scenarios"] if case["id"] == "cleanup")["status"], "failed")

    def test_signals_during_cleanup_finishes_delete_and_replica_verification(self):
        self.config.update(topology="pg-s3", endpoint="https://public.test", replica_urls=["https://one.test", "https://two.test"])
        expected = [("https://one.test", "DELETE", False)] + [
            (base, "GET", git) for base in ("https://public.test", "https://one.test", "https://two.test") for git in (False, True, True)
        ]
        for sig, interrupt_at in itertools.product((q.signal.SIGTERM, q.signal.SIGINT), range(len(expected))):
            with self.subTest(signal=sig, interrupt_at=interrupt_at):
                self.output = self.root / f"{sig}-{interrupt_at}"
                completed = []
                previous = [q.signal.getsignal(sig) for sig in (q.signal.SIGTERM, q.signal.SIGINT, q.signal.SIGALRM)]
                def http(runner, base, path, token=None, method="GET", data=None, **kwargs):
                    if runner.active == "cleanup":
                        if len(completed) == interrupt_at:
                            os.kill(os.getpid(), sig)
                        completed.append((base, method, kwargs.get("git", False)))
                    return self.fake_http(runner, base, path, token, method, data, **kwargs)
                with patch.object(q.signal, "setitimer", wraps=q.signal.setitimer) as timer:
                    code, _, _ = self.smoke_with(http)
                self.assertEqual(code, 1)
                self.assertEqual(next(case for case in self.result()["scenarios"] if case["id"] == "lfs_roundtrip")["status"], "passed")
                self.assertEqual(completed, expected)
                self.assertEqual(self.result()["cleanup"]["status"], "soft_deleted")
                case = next(case for case in self.result()["scenarios"] if case["id"] == "cleanup")
                self.assertEqual((case["status"], case["reason"]), ("failed", "interrupted"))
                self.assertEqual([call.args for call in timer.call_args_list], [
                    (q.signal.ITIMER_REAL, 300), (q.signal.ITIMER_REAL, 0),
                    (q.signal.ITIMER_REAL, 45), (q.signal.ITIMER_REAL, 0),
                ])
                self.assertEqual([q.signal.getsignal(sig) for sig in (q.signal.SIGTERM, q.signal.SIGINT, q.signal.SIGALRM)], previous)
                self.assertNotIn(SECRET, (self.output / "runner.log").read_text())

    def test_signals_during_cleanup_preserves_failures_and_alarm_deadline(self):
        for sig, failure in itertools.product((q.signal.SIGTERM, q.signal.SIGINT), ("http", "alarm")):
            with self.subTest(signal=sig, failure=failure):
                self.output = self.root / f"{sig}-{failure}"
                after_signal = []
                previous = [q.signal.getsignal(sig) for sig in (q.signal.SIGTERM, q.signal.SIGINT, q.signal.SIGALRM)]
                def http(runner, base, path, *args, **kwargs):
                    if path == "/api/v1/repos/delete":
                        os.kill(os.getpid(), sig)
                        after_signal.append(True)
                        if failure == "alarm":
                            os.kill(os.getpid(), q.signal.SIGALRM)
                            after_signal.append(False)  # The active cleanup alarm must interrupt here.
                        raise OSError(SECRET)
                    return self.fake_http(runner, base, path, *args, **kwargs)
                code, calls, _ = self.smoke_with(http)
                self.assertEqual(code, 1)
                self.assertEqual(after_signal, [True])
                self.assertEqual(self.result()["cleanup"]["status"], "unknown")
                case = next(case for case in self.result()["scenarios"] if case["id"] == "cleanup")
                self.assertEqual((case["status"], case["reason"]), ("failed", "cleanup_failed_or_interrupted"))
                self.assertEqual([q.signal.getsignal(sig) for sig in (q.signal.SIGTERM, q.signal.SIGINT, q.signal.SIGALRM)], previous)
                self.assertFalse(any(call.kwargs.get("expected") == 404 for call in calls))
                self.assertNotIn(SECRET, (self.output / "runner.log").read_text())

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

    def test_inherited_git_environment_is_removed(self):
        with patch.dict(os.environ, {"GIT_SSH_COMMAND": SECRET, "GIT_CONFIG_COUNT": "9", "GIT_ASKPASS": SECRET, "GIT_TRACE": SECRET}):
            runner = q.Runner({}, str(self.root), 1)
        for key in ("GIT_SSH_COMMAND", "GIT_CONFIG_COUNT", "GIT_ASKPASS", "GIT_TRACE"):
            self.assertNotIn(key, runner.env)
        self.assertEqual(runner.env["GIT_CONFIG_GLOBAL"], os.devnull)
        self.assertEqual(runner.env["HOME"], str(self.root))

    def test_git_children_do_not_inherit_proxy_settings(self):
        proxies = {name: SECRET for name in ("http_proxy", "https_proxy", "all_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "no_proxy")}
        git = self.root / "git"
        git.write_text(f"#!{q.sys.executable}\nimport json, os\nprint(json.dumps({{k: v for k, v in os.environ.items() if k.lower().endswith('_proxy')}}))\n")
        git.chmod(0o700)
        with patch.dict(os.environ, proxies | {"PATH": str(self.root)}):
            runner = q.Runner({}, str(self.root), 5)
        self.assertEqual(json.loads(runner.command(["git", "version"])), {})

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
        for name in ("aws", "git"):
            executable = self.root / name
            executable.write_text(f"#!{q.sys.executable}\nimport os\nprint(os.environ['HOME'])\n")
            executable.chmod(0o700)
        with patch.dict(os.environ, {"HOME": str(self.root / "caller"), "PATH": str(self.root)}):
            runner = q.Runner({}, str(self.root / "scratch"), 5)
            runner.work.mkdir()
            self.assertEqual(runner.command(["aws", "sts", "get-caller-identity"]).decode().strip(), str(self.root / "caller"))
            self.assertEqual(runner.command(["git", "version"]).decode().strip(), str(self.root / "scratch"))

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
                self.assertEqual(url.path, "/" + REPO_ID + ".git")
                fields = f"protocol={url.scheme}\nhost={url.netloc}\npath={url.path[1:]}\n\n"
                for accepted_fields in (fields, fields.replace(url.path[1:], url.path[1:] + "/info/lfs")):
                    accepted = subprocess.run([str(helper), "get"], input=accepted_fields, text=True, capture_output=True, timeout=3, check=True)
                    self.assertEqual(accepted.stdout, "username=t\npassword=aaa.bbb.ccc\n")
                for denied_fields in (fields.replace(url.netloc, "untrusted.test"),
                                      fields.replace(url.path[1:], "another-repo.git"),
                                      fields.replace(url.path[1:], url.path[1:] + "-other")):
                    denied = subprocess.run([str(helper), "get"], input=denied_fields, text=True, capture_output=True, timeout=3, check=True)
                    self.assertEqual(denied.stdout, "")
            return self.fake_command(argv, **kwargs)
        with patch.object(q.Runner, "http", autospec=True, side_effect=self.fake_http), patch.object(q.Runner, "command", side_effect=command), patch.object(q.shutil, "which", return_value="/bin/fake"):
            self.assertEqual(self.invoke(smoke=True), 0)
        for file in self.output.iterdir():
            self.assertNotIn("aaa.bbb.ccc", file.read_text())

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
