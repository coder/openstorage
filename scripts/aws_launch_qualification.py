#!/usr/bin/env python3
"""Bounded, opt-in external endpoint smoke; never a launch qualification verdict.

API and Git redirects are disabled. LFS uses the client's redirect policy.
All configured endpoints must be trusted scratch-deployment endpoints. Untrusted
endpoints and pre-existing remote repositories are outside this runner's scope.
Hash/version consistency checks do not attest clean source or deployed artifacts.
"""
import argparse
import base64
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


SCENARIOS = {
    "config": "not reached", "local_artifact": "not reached",
    "aws_identity": "--aws-readonly not requested", "aws_bucket": "--aws-readonly not requested",
    "health": "--smoke not requested", "auth_denial": "--smoke not requested",
    "create_repo": "--smoke not requested", "api_commit": "--smoke not requested",
    "git_clone": "--smoke not requested", "git_push": "--smoke not requested",
    "lfs_roundtrip": "--smoke not requested", "cross_replica": "requires pg-s3 smoke",
    "lfs_redirect_control": "not implemented: LFS-wide redirect suppression; trusted endpoints use the stock git-lfs client redirect policy, which may retain Authorization on same-host redirects",
    "cleanup": "no acknowledged scratch repository creation",
    "sandbox_safety": "not implemented: sandbox isolation, least-privilege role, short-lived credentials, spending limit and teardown plan require operator evidence",
    "clean_source_provenance": "not implemented: hash/version are consistency checks, not build provenance; version omits dirty state; operator must clean-build the pinned commit",
    "deployed_artifact": "not implemented: local helper identity does not identify deployed binaries or containers",
    "ec2_same_region": "not implemented: existing endpoint and runner placement are not attested",
    "standard_topology": "not implemented: configured replica URLs do not attest PostgreSQL and real S3 deployment",
    "experimental_s3wal_topology": "not implemented: backend deployment unverified; S3-only remains experimental under issue #192",
    "aws_temporary_credentials": "not implemented: STS success does not prove valid/invalid/missing/forged session-token handling",
    "aws_invalid_credentials": "not implemented: no invalid-credential probes",
    "aws_session_refresh": "not implemented: no session expiry or refresh drill",
    "aws_iam_denials": "not implemented: no denied-permission IAM probes",
    "aws_expected_owner_enforcement": "not implemented: HeadBucket checks the configured owner only; wrong-owner denial is untested",
    "aws_presigned_urls": "not implemented: valid/tampered presigned URLs are not separately tested",
    "s3wal_conditional_writes": "not implemented: no real-S3 CAS/conditional-write drill",
    "concurrent_writes": "not implemented: smoke writes are sequential",
    "cold_cache_restart": "not implemented: runner cannot restart servers or remove caches",
    "interrupted_push_recovery": "not implemented: no server-side push interruption or recovery drill",
    "s3wal_qualification": "not implemented: real-S3 qualification campaign and remaining issue #192 criteria required",
    "s3wal_lease_restart": "not implemented: no lease/restart drills",
    "eks_deployment": "not implemented: EKS is a follow-up unless included in the launch support claim",
    "evidence_publication": "not implemented: local evidence only; no publication or supported-deployment claim",
    "release_failure_resolution": "not implemented: release-blocking campaign failures require separate resolution",
    "infrastructure_teardown": "not implemented: runner owns no infrastructure; deletion is soft only",
}
LIMIT = 1024 * 1024


class Failure(Exception):
    """Only fixed, non-secret diagnostic codes may be used as messages."""


def require(condition, reason):
    if not condition:
        raise Failure(reason)


def private_write(path, data):
    with open(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as file:
        file.write(data)


def origin(value, local):
    require(isinstance(value, str) and value.isascii() and not re.search(r"[\s\\%?#]", value), "invalid_url")
    url = urllib.parse.urlsplit(value)
    require(url.scheme in ("http", "https") and url.hostname and not url.username and not url.password
            and not url.path and url.port != 0 and "@" not in url.netloc, "invalid_url")
    try:
        loopback = ipaddress.ip_address(url.hostname).is_loopback
    except ValueError:
        loopback = url.hostname == "localhost"
    require(url.scheme == "https" or (local and loopback), "https_required")
    host = url.hostname.lower()
    require(re.fullmatch(r"[a-z0-9.-]+", host) or ":" in host, "invalid_url_host")
    host = "[" + host + "]" if ":" in host else host
    port = url.port
    require(not url.netloc.endswith(":"), "invalid_url_port")
    suffix = "" if port is None or port == (443 if url.scheme == "https" else 80) else ":" + str(port)
    return url.scheme + "://" + host + suffix


def unique_fields(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate_config_field")
        result[key] = value
    return result


def load_config(path):
    with open(path, "rb") as file:
        data = file.read(LIMIT + 1)
    require(len(data) <= LIMIT, "config_too_large")
    config = json.loads(data, object_pairs_hook=unique_fields)
    required = {"schema_version", "candidate_commit", "binary", "binary_sha256", "topology",
                "endpoint", "replica_urls", "org", "private_key_file"}
    require(isinstance(config, dict) and required <= config.keys() and config.keys() <= required | {"aws"}, "invalid_fields")
    require(type(config["schema_version"]) is int and config["schema_version"] == 1, "invalid_schema")
    for field, pattern in (("candidate_commit", r"[a-f0-9]{40}"), ("binary_sha256", r"[a-f0-9]{64}"),
                           ("org", r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")):
        require(isinstance(config[field], str) and re.fullmatch(pattern, config[field]), "invalid_identity")
    require(config["topology"] in ("local", "pg-s3", "s3wal"), "invalid_topology")
    config["endpoint"] = origin(config["endpoint"], config["topology"] == "local")
    replicas = config["replica_urls"]
    require(isinstance(replicas, list) and len(replicas) == (2 if config["topology"] == "pg-s3" else 0), "invalid_replicas")
    config["replica_urls"] = [origin(url, False) for url in replicas]
    require(len(set(config["replica_urls"])) == len(replicas), "duplicate_replicas")
    for field in ("binary", "private_key_file"):
        require(isinstance(config[field], str) and config[field], "invalid_path")
        path = Path(config[field]).resolve(strict=True)
        require(stat.S_ISREG(path.stat().st_mode), "not_regular_file")
        config[field] = str(path)
    require(stat.S_IMODE(Path(config["private_key_file"]).stat().st_mode) == 0o600, "key_mode_must_be_0600")
    require(os.access(config["binary"], os.X_OK), "binary_not_executable")
    if "aws" in config:
        aws = config["aws"]
        require(isinstance(aws, dict) and set(aws) == {"region", "bucket", "expected_bucket_owner"}, "invalid_aws_fields")
        for field, pattern in (("region", r"[a-z]{2}(?:-[a-z]+)+-\d"), ("bucket", r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]"),
                               ("expected_bucket_owner", r"[0-9]{12}")):
            require(isinstance(aws[field], str) and re.fullmatch(pattern, aws[field]), "invalid_aws_config")
    return config


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Runner:
    def __init__(self, result, work, budget):
        self.result, self.work = result, Path(work)
        self.deadline = time.monotonic() + budget
        self.active = "config"
        self.created = False
        self.log = []
        self.env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
        self.env.update(HOME=work, XDG_CONFIG_HOME=work, GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                        GIT_TERMINAL_PROMPT="0", GIT_LFS_SKIP_SMUDGE="1", AWS_PAGER="", AWS_MAX_ATTEMPTS="1")
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def step(self, name):
        self.active = name
        self.result["scenarios"][name] = {"id": name, "status": "failed", "reason": "incomplete"}
        self.log.append(name + ": started")

    def passed(self):
        self.result["scenarios"][self.active].update(status="passed", reason="completed")
        self.log.append(self.active + ": passed")

    def remaining(self):
        remaining = self.deadline - time.monotonic()
        require(remaining > 0, "budget_exhausted")
        return min(30, remaining)

    def command(self, argv, cwd=None):
        timeout = self.remaining()
        # AWS shared config and SSO caches live in the caller's home, not Git's sandbox.
        env = self.env | {"HOME": str(Path.home())} if argv[0] == "aws" else self.env
        with tempfile.TemporaryFile(dir=self.work) as output:
            process = subprocess.Popen(argv, cwd=cwd or self.work, env=env, stdin=subprocess.DEVNULL,
                                       stdout=output, stderr=subprocess.DEVNULL, start_new_session=True)
            try:
                code = process.wait(timeout=timeout)
            except BaseException:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait(timeout=5)
                raise
            self.log.append(f"{self.active}: process expected=0 actual={code}")
            require(code == 0, "command_failed")
            output.seek(0)
            data = output.read(LIMIT + 1)
            require(len(data) <= LIMIT, "command_output_too_large")
            return data

    def http(self, base, path, token=None, method="GET", data=None, expected=200, git=False):
        headers = {"Content-Type": "application/x-ndjson" if path.endswith("commit-pack") else "application/json"}
        if token:
            headers["Authorization"] = ("Basic " + base64.b64encode(("t:" + token).encode()).decode()) if git else "Bearer " + token
        request = urllib.request.Request(base + path, data=data, headers=headers, method=method)
        try:
            response = self.opener.open(request, timeout=self.remaining())
        except urllib.error.HTTPError as error:
            response = error
        with response:
            self.log.append(f"{self.active}: HTTP expected={expected} actual={response.code}")
            require(response.code == expected, "unexpected_http_status")
            body = response.read(LIMIT + 1)
            require(len(body) <= LIMIT, "http_response_too_large")
            return body

    def preflight(self, path, smoke, aws_readonly):
        self.step("config")
        self.config = load_config(path)
        config = self.config
        self.result["config"] = {key: config[key] for key in ("candidate_commit", "binary_sha256", "topology")}
        for tool in (["git", "git-lfs"] if smoke else []) + (["aws"] if aws_readonly else []):
            require(shutil.which(tool), "missing_dependency")
        require(not aws_readonly or "aws" in config, "aws_config_required")
        self.passed()
        self.step("local_artifact")
        helper = self.work / "openstorage"
        shutil.copyfile(config["binary"], helper)
        with open(helper, "rb") as file:
            require(hashlib.file_digest(file, "sha256").hexdigest() == config["binary_sha256"], "binary_hash_mismatch")
        helper.chmod(0o700)
        config["binary"] = str(helper)
        banner = self.command([config["binary"], "version"]).decode("utf-8")
        require(re.fullmatch(r"openstorage [^\r\n]+ \(commit " + config["candidate_commit"] + r", [^\r\n]+\)\n?", banner)
                and "dirty" not in banner.lower(), "source_identity_mismatch")
        with open(config["private_key_file"], "r") as file:
            key = file.read(65537)
        require(len(key) <= 65536, "key_too_large")
        private_write(self.work / "key.pem", key)
        config["private_key_file"] = str(self.work / "key.pem")
        self.passed()

    def aws(self):
        aws = self.config["aws"]
        common = ["--region", aws["region"], "--cli-connect-timeout", "10", "--cli-read-timeout", "20"]
        self.step("aws_identity")
        self.command(["aws", "sts", "get-caller-identity", *common])
        self.passed()
        self.step("aws_bucket")
        self.command(["aws", "s3api", "head-bucket", "--bucket", aws["bucket"],
                      "--expected-bucket-owner", aws["expected_bucket_owner"], *common])
        self.passed()

    def mint(self, scopes):
        token = self.command([self.config["binary"], "mint", "--key", self.config["private_key_file"],
                              "--iss", self.config["org"], "--repo", self.repo, "--scopes", scopes, "--ttl", "15m"]).decode().strip()
        require(re.fullmatch(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", token), "invalid_minted_token")
        return token

    def git(self, *args, cwd=None):
        return self.command(["git", "-c", "http.followRedirects=false", "-c", "credential.helper=",
                             "-c", "credential.helper=" + str(self.work / "credential"),
                             "-c", "credential.useHttpPath=true", "-c", "protocol.file.allow=never", *args], cwd=cwd)

    def smoke(self):
        self.repo = "q222-" + str(uuid.uuid4())
        self.result["scratch_repo"] = self.repo
        public = self.config["endpoint"]
        replicas = self.config["replica_urls"]
        self.writer, reader = replicas if replicas else (public, public)
        self.step("health")
        for base in dict.fromkeys([public, self.writer, reader]):
            self.http(base, "/healthz")
        self.passed()
        self.step("auth_denial")
        self.token = self.mint("repo:write,git:read,git:write")
        limited = self.mint("repo:write")
        self.gitpath = "/" + self.repo + ".git"
        for base in dict.fromkeys([public, self.writer, reader]):
            for token, expected in ((None, 401), ("invalid", 401), (limited, 403)):
                self.http(base, "/api/v1/repo", token, expected=expected)
                self.http(base, self.gitpath + "/info/refs?service=git-upload-pack", token, expected=expected, git=True)
        self.passed()
        self.step("create_repo")
        self.result["cleanup"].update(status="unknown", reason="creation attempted; acknowledgment not yet verified")
        response = json.loads(self.http(self.writer, "/api/v1/repos", self.token, "POST", b'{"default_branch":"main"}'))
        require(response.get("repo_name") == self.repo and isinstance(response.get("repo_id"), str) and response["repo_id"], "invalid_create_acknowledgment")
        self.created = True
        self.passed()
        self.step("api_commit")
        initial = b"qualification API\n"
        content_id = str(uuid.uuid4())
        metadata = {"target_branch": "main", "commit_message": "qualification API", "author": {"name": "Qualification", "email": "qualification@example.invalid"},
                    "files": [{"path": "hello.txt", "content_id": content_id, "operation": "upsert", "mode": "100644"}]}
        frames = [{"metadata": metadata}, {"blob_chunk": {"content_id": content_id, "data": base64.b64encode(initial).decode(), "eof": True}}]
        response = json.loads(self.http(self.writer, "/api/v1/repos/commit-pack", self.token, "POST", ("\n".join(map(json.dumps, frames)) + "\n").encode()))
        sha = response["commit"]["commit_sha"]
        require(response["result"]["success"] is True and re.fullmatch(r"[a-f0-9]{40}", sha)
                and response["result"]["new_sha"] == sha, "api_commit_not_successful")
        self.passed()
        private_write(self.work / "credentials.json", json.dumps({"token": self.token, "origins": [public, self.writer, reader], "path": self.gitpath[1:]}))
        private_write(self.work / "credential", f"#!{sys.executable}\n" + '''import json, pathlib, sys
if sys.argv[1:] == ['get']:
    data = json.loads(pathlib.Path(__file__).with_name('credentials.json').read_text())
    fields = dict(line.rstrip('\\n').split('=', 1) for line in sys.stdin if '=' in line)
    origin = fields.get('protocol', '') + '://' + fields.get('host', '')
    path = fields.get('path', '')
    if origin in data['origins'] and (path == data['path'] or path.startswith(data['path'] + '/')):
        print('username=t\\npassword=' + data['token'])
''')
        (self.work / "credential").chmod(0o700)
        self.step("git_clone")
        clone = self.work / "clone"
        self.git("clone", reader + self.gitpath, str(clone))
        require(self.git("rev-parse", "HEAD", cwd=clone).decode().strip() == sha and (clone / "hello.txt").read_bytes() == initial, "clone_mismatch")
        self.git("fsck", "--full", cwd=clone)
        self.passed()
        self.step("git_push")
        self.git("remote", "set-url", "origin", self.writer + self.gitpath, cwd=clone)
        self.git("config", "user.name", "Qualification", cwd=clone)
        self.git("config", "user.email", "qualification@example.invalid", cwd=clone)
        self.git("lfs", "install", "--local", cwd=clone)
        self.git("lfs", "track", "payload.bin", cwd=clone)
        changed, payload = b"qualification Git\n", os.urandom(128 * 1024)
        (clone / "hello.txt").write_bytes(changed)
        (clone / "payload.bin").write_bytes(payload)
        self.git("add", "hello.txt", "payload.bin", ".gitattributes", cwd=clone)
        self.git("commit", "-m", "qualification Git and LFS", cwd=clone)
        pointer = f"version https://git-lfs.github.com/spec/v1\noid sha256:{hashlib.sha256(payload).hexdigest()}\nsize {len(payload)}\n".encode()
        require(self.git("show", "HEAD:payload.bin", cwd=clone) == pointer, "not_an_lfs_pointer")
        sha = self.git("rev-parse", "HEAD", cwd=clone).decode().strip()
        require(re.fullmatch(r"[a-f0-9]{40}", sha), "invalid_git_sha")
        self.git("branch", "qualification", cwd=clone)
        self.git("tag", "qualification", cwd=clone)
        self.git("push", "origin", "main", "refs/heads/qualification", "refs/tags/qualification", cwd=clone)
        self.git("fetch", reader + self.gitpath, "main", "refs/heads/qualification", "refs/tags/qualification", cwd=clone)
        for base in dict.fromkeys([public, self.writer, reader]):
            require(self.http(base, "/api/v1/repos/file?path=hello.txt&ref=main", self.token) == changed, "api_read_mismatch")
        self.passed()
        self.step("lfs_roundtrip")
        fresh = self.work / "fresh"
        self.git("clone", reader + self.gitpath, str(fresh))
        self.git("lfs", "install", "--local", cwd=fresh)
        self.git("lfs", "pull", cwd=fresh)
        require((fresh / "payload.bin").read_bytes() == payload and (fresh / "hello.txt").read_bytes() == changed, "lfs_bytes_mismatch")
        for ref in ("HEAD", "refs/remotes/origin/qualification", "refs/tags/qualification"):
            require(self.git("rev-parse", ref, cwd=fresh).decode().strip() == sha, "git_ref_mismatch")
        self.git("fsck", "--full", cwd=fresh)
        self.passed()
        if replicas:
            self.step("cross_replica")
            self.passed()

    def cleanup(self):
        if not self.created:
            return
        self.deadline = time.monotonic() + 45
        self.step("cleanup")
        self.result["cleanup"].update(status="unknown", reason="soft deletion pending verification")
        self.http(self.writer, "/api/v1/repos/delete", self.token, "DELETE")
        for base in dict.fromkeys([self.config["endpoint"], *self.config["replica_urls"]]):
            self.http(base, "/api/v1/repo", self.token, expected=404)
            self.http(base, self.gitpath + "/info/refs?service=git-upload-pack", self.token, expected=404, git=True)
        self.result["cleanup"].update(status="soft_deleted", reason="API and Git inaccessible; physical blobs and tombstone may remain")
        self.passed()


def deadline_expired(_signum, _frame):
    raise Failure("budget_exhausted")


def execute(config, output, smoke=False, aws_readonly=False, budget=300):
    result = {"schema_version": 1, "status": "failed", "launch_qualified": False, "remote_artifact_verified": False, "clean_source_verified": False,
              "config": {}, "scratch_repo": None, "cleanup": {"status": "not_needed", "reason": "no creation attempted"},
              "scenarios": {name: {"id": name, "status": "skipped", "reason": reason} for name, reason in SCENARIOS.items()}}
    for case in result["scenarios"].values():
        if (smoke and case["reason"] == "--smoke not requested") or (aws_readonly and case["reason"] == "--aws-readonly not requested"):
            case["reason"] = "not reached after earlier failure"
    try:
        output = Path(output)
        output.mkdir(mode=0o700)
    except OSError:
        print("evidence directory unavailable; existing evidence was not modified", file=sys.stderr)
        return 1
    with tempfile.TemporaryDirectory(prefix="q222-") as work:
        runner = Runner(result, work, budget if type(budget) is int else 0)
        old_alarm = None
        try:
            require(type(budget) is int and 1 <= budget <= 900, "invalid_budget")
            require(os.name == "posix", "posix_required")
            old_alarm = signal.signal(signal.SIGALRM, deadline_expired)
            signal.setitimer(signal.ITIMER_REAL, budget)
            runner.preflight(config, smoke, aws_readonly)
            if aws_readonly:
                runner.aws()
            if smoke:
                runner.smoke()
            result["status"] = "passed"
        except (Exception, KeyboardInterrupt) as error:
            reason = str(error) if isinstance(error, Failure) else "interrupted" if isinstance(error, KeyboardInterrupt) else "operation_failed"
            result["scenarios"][runner.active].update(status="failed", reason=reason)
            runner.log.append(runner.active + ": " + reason)
        finally:
            try:
                if old_alarm is not None:
                    signal.setitimer(signal.ITIMER_REAL, 45)
                runner.cleanup()
            except (Exception, KeyboardInterrupt):
                result["status"] = "failed"
                result["scenarios"]["cleanup"].update(status="failed", reason="cleanup_failed_or_interrupted")
                runner.log.append("cleanup: failed")
            finally:
                if old_alarm is not None:
                    signal.setitimer(signal.ITIMER_REAL, 0)
                    signal.signal(signal.SIGALRM, old_alarm)
            result["scenarios"] = list(result["scenarios"].values())
            private_write(output / "result.json", json.dumps(result, indent=2) + "\n")
            private_write(output / "runner.log", "\n".join(runner.log) + "\n")
    return 0 if result["status"] == "passed" else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True, help="new private evidence directory")
    parser.add_argument("--smoke", action="store_true", help="mutate a unique scratch repository on an existing endpoint")
    parser.add_argument("--aws-readonly", action="store_true", help="request bounded STS and expected-owner HeadBucket probes")
    parser.add_argument("--budget-seconds", type=int, default=300)
    args = parser.parse_args(argv)
    try:
        return execute(args.config, args.output, args.smoke, args.aws_readonly, args.budget_seconds)
    except (Exception, KeyboardInterrupt):
        print("runner failed; private evidence could not be finalized", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
