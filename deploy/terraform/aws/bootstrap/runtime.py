#!/usr/bin/python3
"""Root-only RDS credential preparation; also used by the rotation timer."""

import fcntl
import ipaddress
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
from urllib.parse import quote, urlsplit

CONFIG_DIR = Path("/etc/openstorage")
RUN_DIR = Path("/run/openstorage")
CA_FILE = CONFIG_DIR / "rds-ca.pem"


def parse_json(data):
    def unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON field")
            result[key] = value
        return result

    return json.loads(data, object_pairs_hook=unique_pairs)


def validate_config(cfg):
    patterns = {
        "region": r"[a-z]{2}-[a-z]+-\d+",
        "account_id": r"\d{12}",
        "org": r"[^\x00-\x1f\x7f]+",
        "service_url": r"https://[^\s]+",
        "artifact_bucket": r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]",
        "artifact_key": r"[^\x00-\x1f\x7f]{1,1024}",
        "release_sha256": r"[a-fA-F0-9]{64}",
        "release_tag": r"v\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?",
        "postgres_host": r"[a-zA-Z0-9]+(?:[.-][a-zA-Z0-9]+)*",
        "postgres_database": r"openstorage",
        "postgres_secret_arn": r"arn:aws:secretsmanager:[a-z0-9-]+:\d{12}:secret:[a-zA-Z0-9/_+=.!@-]+",
        "data_bucket": r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]",
    }
    if not isinstance(cfg, dict) or set(cfg) != set(patterns) | {"postgres_port", "trusted_proxy_cidrs"}:
        raise ValueError("unexpected configuration fields")
    for key, pattern in patterns.items():
        if not isinstance(cfg[key], str) or not re.fullmatch(pattern, cfg[key], re.ASCII):
            raise ValueError("invalid configuration field: " + key)
    if type(cfg["postgres_port"]) is not int or cfg["postgres_port"] != 5432:
        raise ValueError("postgres_port must be 5432")
    url = urlsplit(cfg["service_url"])
    if not url.hostname or url.username is not None or url.password is not None or url.path not in ("", "/") or url.query or url.fragment:
        raise ValueError("service_url must be an HTTPS origin")
    arn_prefix = "arn:aws:secretsmanager:{region}:{account_id}:secret:".format(**cfg)
    if not cfg["postgres_secret_arn"].startswith(arn_prefix):
        raise ValueError("secret must belong to the configured region and account")
    proxies = cfg["trusted_proxy_cidrs"]
    if not isinstance(proxies, list) or not proxies:
        raise ValueError("trusted_proxy_cidrs must list the ALB subnet CIDRs")
    for cidr in proxies:
        if not isinstance(cidr, str) or "/" not in cidr or ipaddress.ip_network(cidr).prefixlen == 0:
            raise ValueError("invalid trusted proxy CIDR")
    return cfg


def load_config():
    return validate_config(parse_json((CONFIG_DIR / "config.json").read_bytes()))


def run(args, timeout=45):
    # Never inherit developer profiles, static credentials, endpoint overrides, or
    # subprocess output: AWS errors can contain the secret response itself.
    env = {
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin",
        "HOME": "/nonexistent", "LANG": "C.UTF-8",
        "AWS_CONFIG_FILE": "/dev/null", "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
        "AWS_EC2_METADATA_DISABLED": "false", "AWS_MAX_ATTEMPTS": "3", "AWS_PAGER": "",
    }
    try:
        return subprocess.run(args, check=True, stdin=subprocess.DEVNULL,
                              capture_output=True, timeout=timeout, env=env).stdout
    except (OSError, subprocess.SubprocessError):
        raise RuntimeError("subprocess failed (output withheld)") from None


def atomic_write(path, content, mode=0o600):
    fd, name = tempfile.mkstemp(prefix="." + path.name + "-", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "wb") as stream:
            os.fchmod(stream.fileno(), mode)
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def environment(cfg, response):
    response = parse_json(response)
    if not isinstance(response, dict) or not isinstance(response.get("SecretString"), str):
        raise ValueError("missing SecretString")
    version = response.get("VersionId")
    if not isinstance(version, str) or not re.fullmatch(r"[A-Za-z0-9-]{32,64}", version):
        raise ValueError("invalid secret version")
    credentials = parse_json(response["SecretString"])
    if not isinstance(credentials, dict):
        raise ValueError("invalid database credentials")
    for key in ("username", "password"):
        value = credentials.get(key)
        if not isinstance(value, str) or not value or "\x00" in value:
            raise ValueError("invalid database credentials")
    # Quote each userinfo component separately; secrets never appear in argv.
    dsn = "postgresql://{}:{}@{}:{}/{}?sslmode=verify-full&sslrootcert={}".format(
        quote(credentials["username"], safe=""), quote(credentials["password"], safe=""),
        cfg["postgres_host"], cfg["postgres_port"], cfg["postgres_database"], quote(str(CA_FILE), safe="/"),
    )
    values = {
        "AWS_REGION": cfg["region"], "AWS_DEFAULT_REGION": cfg["region"],
        "AWS_CONFIG_FILE": "/dev/null", "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
        "AWS_EC2_METADATA_DISABLED": "false",
        "OPENSTORAGE_ORG": cfg["org"], "OPENSTORAGE_ORG_PUBKEYS": str(CONFIG_DIR / "org.pub.pem"),
        "OPENSTORAGE_BASE_URL": cfg["service_url"], "OPENSTORAGE_LISTEN": "0.0.0.0:8080",
        "OPENSTORAGE_PG_URL": dsn,
        "OPENSTORAGE_S3_URL": "s3://{}?region={}".format(cfg["data_bucket"], cfg["region"]),
        "OPENSTORAGE_S3_EXPECTED_BUCKET_OWNER": cfg["account_id"],
        "OPENSTORAGE_RATE_RPS": "20", "OPENSTORAGE_RATE_BURST": "40",
        "OPENSTORAGE_TRUSTED_PROXIES": ",".join(cfg["trusted_proxy_cidrs"]),
        "OPENSTORAGE_SYNC_ALLOW_PRIVATE": "false", "OPENSTORAGE_WEBHOOK_ALLOW_PRIVATE": "false",
        "TMPDIR": "/var/lib/openstorage/tmp",
    }
    lines = ["# Secret version: " + version]
    for key, value in sorted(values.items()):
        # EnvironmentFile is systemd syntax, not shell syntax. It performs no
        # variable expansion. Double quotes preserve whitespace and literal $.
        assert not any(ord(char) < 32 or ord(char) == 127 for char in value)
        lines.append('{}="{}"'.format(key, value.replace("\\", "\\\\").replace('"', '\\"')))
    return ("\n".join(lines) + "\n").encode()


def refresh(cfg, directory=RUN_DIR, restart=False):
    directory.mkdir(mode=0o700, exist_ok=True)
    info = directory.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid():
        raise ValueError("runtime directory must be owned by the current user")
    directory.chmod(0o700)
    fd = os.open(directory / "refresh.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        path = directory / "service.env"
        previous = None
        try:
            info = path.lstat()
        except FileNotFoundError:
            pass
        else:
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600:
                raise ValueError("runtime environment must be a private owned regular file")
            previous = path.read_bytes()
        response = run([
            "aws", "secretsmanager", "get-secret-value", "--region", cfg["region"],
            "--secret-id", cfg["postgres_secret_arn"], "--version-stage", "AWSCURRENT",
            "--output", "json", "--cli-connect-timeout", "5", "--cli-read-timeout", "10",
        ])
        content = environment(cfg, response)
        if content == previous:
            return False
        atomic_write(path, content)
        if restart:
            try:
                # Never wait while holding the lock: ExecStartPre also takes it.
                run(["systemctl", "--no-block", "try-restart", "openstorage.service"])
            except RuntimeError:
                # Do not acknowledge a version whose restart was not enqueued.
                if previous is None:
                    path.unlink()
                else:
                    atomic_write(path, previous)
                raise
        return True


def main(args=None):
    args = sys.argv[1:] if args is None else args
    try:
        if os.geteuid() != 0 or args not in (["prepare"], ["refresh"]):
            raise ValueError("root and prepare/refresh command required")
        refresh(load_config(), restart=args == ["refresh"])
    except Exception:
        # Do not print exception details or tracebacks from secret parsing.
        print("openstorage runtime configuration failed (details withheld)", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
