#!/usr/bin/env python3
"""Local artifact checks and opt-in read-only AWS preflight, not qualification.

The supplied executable and AWS credential configuration must be trusted.
Hash/version consistency checks do not attest clean source or deployed artifacts.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time


SCENARIOS = {
    "config": "not reached", "local_artifact": "not reached",
    "aws_identity": "--aws-readonly not requested", "aws_bucket": "--aws-readonly not requested",
    "clean_source_provenance": "not verified: hash/version are consistency checks, not build provenance",
    "deployed_artifact": "not verified: local helper identity does not identify deployed artifacts",
    "live_smoke": "deferred under issue #222 until user-authorized account and campaign work",
    "real_aws_qualification": "deferred under issue #222; STS/HeadBucket alone cannot qualify AWS support",
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
    required = {"schema_version", "candidate_commit", "binary", "binary_sha256"}
    require(isinstance(config, dict) and required <= config.keys() and config.keys() <= required | {"aws"}, "invalid_fields")
    require(type(config["schema_version"]) is int and config["schema_version"] == 1, "invalid_schema")
    for field, pattern in (("candidate_commit", r"[a-f0-9]{40}"), ("binary_sha256", r"[a-f0-9]{64}")):
        require(isinstance(config[field], str) and re.fullmatch(pattern, config[field]), "invalid_identity")
    require(isinstance(config["binary"], str) and config["binary"], "invalid_path")
    binary = Path(config["binary"]).resolve(strict=True)
    require(stat.S_ISREG(binary.stat().st_mode), "not_regular_file")
    config["binary"] = str(binary)
    require(os.access(config["binary"], os.X_OK), "binary_not_executable")
    if "aws" in config:
        aws = config["aws"]
        require(isinstance(aws, dict) and set(aws) == {"region", "bucket", "expected_bucket_owner"}, "invalid_aws_fields")
        for field, pattern in (("region", r"[a-z]{2}(?:-[a-z]+)+-\d"), ("bucket", r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]"),
                               ("expected_bucket_owner", r"[0-9]{12}")):
            require(isinstance(aws[field], str) and re.fullmatch(pattern, aws[field]), "invalid_aws_config")
    return config


class Runner:
    def __init__(self, result, work, budget):
        self.result, self.work = result, Path(work)
        self.deadline = time.monotonic() + budget
        self.active = "config"
        self.log = []
        # Preserve the caller's shared AWS config and SSO credential chain.
        self.env = dict(os.environ, AWS_PAGER="", AWS_MAX_ATTEMPTS="1")

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

    def command(self, argv):
        timeout = self.remaining()
        process = subprocess.Popen(argv, cwd=self.work, env=self.env, stdin=subprocess.DEVNULL,
                                   stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, start_new_session=True)
        try:
            data = bytearray()
            deadline = time.monotonic() + timeout
            with process.stdout, selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or not selector.select(remaining):
                        raise subprocess.TimeoutExpired(argv, timeout)
                    chunk = os.read(process.stdout.fileno(), min(65536, LIMIT + 1 - len(data)))
                    if not chunk:
                        break
                    data.extend(chunk)
                    require(len(data) <= LIMIT, "command_output_too_large")
            code = process.wait(timeout=max(0, deadline - time.monotonic()))
        except BaseException:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=5)
            raise
        self.log.append(f"{self.active}: process expected=0 actual={code}")
        require(code == 0, "command_failed")
        return bytes(data)

    def preflight(self, path, aws_readonly):
        self.step("config")
        self.config = load_config(path)
        config = self.config
        self.result["config"] = {key: config[key] for key in ("candidate_commit", "binary_sha256")}
        if aws_readonly:
            require(shutil.which("aws"), "missing_dependency")
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


def execute(config, output, aws_readonly=False, budget=300):
    result = {"schema_version": 1, "status": "failed", "launch_qualified": False,
              "remote_artifact_verified": False, "clean_source_verified": False, "config": {},
              "scenarios": {name: {"id": name, "status": "skipped", "reason": reason} for name, reason in SCENARIOS.items()}}
    for case in result["scenarios"].values():
        if aws_readonly and case["reason"] == "--aws-readonly not requested":
            case["reason"] = "not reached after earlier failure"
    try:
        output = Path(output)
        output.mkdir(mode=0o700)
    except OSError:
        print("evidence directory unavailable; existing evidence was not modified", file=sys.stderr)
        return 1
    with tempfile.TemporaryDirectory(prefix="q222-") as work:
        runner = Runner(result, work, budget if type(budget) is int else 0)
        try:
            require(type(budget) is int and 1 <= budget <= 900, "invalid_budget")
            require(os.name == "posix", "posix_required")
            runner.preflight(config, aws_readonly)
            if aws_readonly:
                runner.aws()
            result["status"] = "passed"
        except (Exception, KeyboardInterrupt) as error:
            reason = str(error) if isinstance(error, Failure) else "interrupted" if isinstance(error, KeyboardInterrupt) else "operation_failed"
            result["scenarios"][runner.active].update(status="failed", reason=reason)
            runner.log.append(runner.active + ": " + reason)
        finally:
            result["scenarios"] = list(result["scenarios"].values())
            private_write(output / "result.json", json.dumps(result, indent=2) + "\n")
            private_write(output / "runner.log", "\n".join(runner.log) + "\n")
    return 0 if result["status"] == "passed" else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True, help="new private evidence directory")
    parser.add_argument("--aws-readonly", action="store_true", help="request bounded STS and expected-owner HeadBucket probes")
    parser.add_argument("--budget-seconds", type=int, default=300)
    args = parser.parse_args(argv)
    try:
        return execute(args.config, args.output, aws_readonly=args.aws_readonly, budget=args.budget_seconds)
    except (Exception, KeyboardInterrupt):
        print("runner failed; private evidence could not be finalized", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
