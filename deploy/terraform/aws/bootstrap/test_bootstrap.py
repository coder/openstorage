"""Offline tests: python3 -m unittest discover -s deploy/terraform/aws/bootstrap -v."""

import base64
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import tarfile
import tempfile
import unittest
from unittest import mock
from urllib.parse import parse_qs, unquote, urlsplit

import bootstrap
import runtime


def config():
    return {
        "region": "us-east-1", "account_id": "123456789012", "org": "example",
        "service_url": "https://git.example.com", "artifact_bucket": "release-bucket",
        "artifact_key": "releases/openstorage.tar.gz", "release_sha256": "a" * 64,
        "release_tag": "v0.10.0", "postgres_host": "db.example.rds.amazonaws.com",
        "postgres_port": 5432, "postgres_database": "openstorage",
        "postgres_secret_arn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:rds!db-test",
        "data_bucket": "data-bucket", "trusted_proxy_cidrs": ["10.0.1.0/24", "10.0.2.0/24"],
    }


def secret(password="secret", version="a" * 32, username="openstorage"):
    return json.dumps({"VersionId": version, "SecretString": json.dumps({
        "username": username, "password": password,
    })}).encode()


class ConfigTests(unittest.TestCase):
    def test_valid_config(self):
        self.assertEqual(runtime.validate_config(config()), config())

    def test_invalid_config(self):
        for field, value in [
            ("release_sha256", "g" * 64), ("release_sha256", "a" * 63),
            ("release_sha256", "a" * 64 + "\n"), ("release_tag", "v0.10.0\nexecute"),
            ("account_id", "123"), ("org", "injected\nENV=1"),
            ("service_url", "http://git.example.com"),
            ("service_url", "https://user:pass@git.example.com"),
            ("service_url", "https://git.example.com/path"),
            ("postgres_host", "db/?sslmode=disable"), ("postgres_port", True),
            ("postgres_port", 5433), ("postgres_database", "other"),
            ("postgres_secret_arn", "arn:aws:secretsmanager:us-west-2:123456789012:secret:db"),
            ("data_bucket", "bucket?endpoint=http://attacker"),
            ("trusted_proxy_cidrs", "10.0.1.0/24"), ("trusted_proxy_cidrs", ["0.0.0.0/0"]),
            ("trusted_proxy_cidrs", ["not-a-cidr"]), ("private_key", "PRIVATE KEY"),
        ]:
            with self.subTest(field=field, value=value):
                candidate = config()
                candidate[field] = value
                with self.assertRaises(ValueError):
                    runtime.validate_config(candidate)

    def test_no_duplicate_json_fields(self):
        with self.assertRaises(ValueError):
            runtime.parse_json('{"password":"a","password":"b"}')

    def test_public_key_rejects_nonpublic_material_before_subprocess(self):
        public = "-----BEGIN PUBLIC KEY-----\n" + base64.b64encode(b"der").decode() + "\n-----END PUBLIC KEY-----\n"
        for data in ["", "PRIVATE KEY", public + "private bytes", public.replace("PUBLIC", "PRIVATE"),
                     public + "-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----", public.replace("ZGVy", "@@@@")]:
            with self.subTest(data=data), tempfile.TemporaryDirectory() as temp:
                path = Path(temp) / "org.pub.pem"
                path.write_text(data)
                with mock.patch.object(bootstrap, "run") as run:
                    with self.assertRaises(ValueError):
                        bootstrap.validate_public_key(path)
                    run.assert_not_called()

    def test_public_key_is_cryptographically_parsed_as_public(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "org.pub.pem"
            path.write_text("-----BEGIN PUBLIC KEY-----\nZGVy\n-----END PUBLIC KEY-----\n")
            with mock.patch.object(bootstrap, "run", side_effect=RuntimeError("invalid public DER")) as run:
                with self.assertRaises(RuntimeError):
                    bootstrap.validate_public_key(path)
            self.assertIn("-pubin", run.call_args.args[0])
            self.assertIn("-pubcheck", run.call_args.args[0])


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.archive = self.root / "release.tar.gz"
        self.destination = self.root / "openstorage"
        self.destination.write_bytes(b"previous release")

    def archive_with(self, entries):
        with tarfile.open(self.archive, "w:gz") as archive:
            for name, kind, content in entries:
                member = tarfile.TarInfo(name)
                member.type = kind
                member.size = len(content) if kind == tarfile.REGTYPE else 0
                member.linkname = "../../escaped"
                archive.addfile(member, io.BytesIO(content))
        return hashlib.sha256(self.archive.read_bytes()).hexdigest()

    def valid_entries(self):
        return [("openstorage", tarfile.REGTYPE, b"binary"), ("LICENSE", tarfile.REGTYPE, b"license")]

    def test_hash_precedes_archive_read_or_execution(self):
        self.archive.write_bytes(b"not even an archive")
        with mock.patch.object(bootstrap.tarfile, "open") as extract, mock.patch.object(bootstrap, "run") as run:
            with self.assertRaises(ValueError):
                bootstrap.install_release(self.archive, "0" * 64, "v0.10.0", self.destination)
            extract.assert_not_called()
            run.assert_not_called()
        self.assertEqual(self.destination.read_bytes(), b"previous release")

    def test_rejects_unsafe_or_unexpected_archive_without_executing(self):
        for name, kind in [("../openstorage", tarfile.REGTYPE), ("/openstorage", tarfile.REGTYPE),
                           ("./openstorage", tarfile.REGTYPE), ("extra", tarfile.REGTYPE),
                           ("openstorage", tarfile.SYMTYPE), ("openstorage", tarfile.LNKTYPE),
                           ("LICENSE", tarfile.DIRTYPE), ("openstorage", tarfile.FIFOTYPE)]:
            with self.subTest(name=name, kind=kind):
                digest = self.archive_with(self.valid_entries() + [(name, kind, b"bad")])
                with mock.patch.object(bootstrap, "run") as run:
                    with self.assertRaises(ValueError):
                        bootstrap.install_release(self.archive, digest, "v0.10.0", self.destination)
                    run.assert_not_called()
                self.assertEqual(self.destination.read_bytes(), b"previous release")

    def test_binary_only_published_release(self):
        # v0.15.0's published archive predates the LICENSE packaging change.
        digest = self.archive_with(self.valid_entries()[:1])
        with mock.patch.object(bootstrap, "run", return_value=b"openstorage v0.15.0 (release)\n"):
            license_content = bootstrap.install_release(self.archive, digest, "v0.15.0", self.destination)
        self.assertIsNone(license_content)
        self.assertEqual(self.destination.read_bytes(), b"binary")
        self.assertEqual(stat.S_IMODE(self.destination.stat().st_mode), 0o755)

    def test_missing_or_duplicate_entries(self):
        for entries in [[], self.valid_entries()[1:], self.valid_entries() + self.valid_entries()[:1]]:
            digest = self.archive_with(entries)
            with self.assertRaises(ValueError):
                bootstrap.install_release(self.archive, digest, "v0.10.0", self.destination)

    def test_exact_version_and_atomic_install(self):
        digest = self.archive_with(self.valid_entries())
        for version in [b"openstorage v0.10.01 (go1.26)", b"openstorage v0.9.0 (go1.26)", b"v0.10.0"]:
            with mock.patch.object(bootstrap, "run", return_value=version):
                with self.assertRaises(ValueError):
                    bootstrap.install_release(self.archive, digest, "v0.10.0", self.destination)
            self.assertEqual(self.destination.read_bytes(), b"previous release")
        with mock.patch.object(bootstrap, "run", return_value=b"openstorage v0.10.0 (go1.26)\n") as run:
            bootstrap.install_release(self.archive, digest, "v0.10.0", self.destination)
            self.assertEqual(run.call_args.args[0][1:], ["--version"])
        self.assertEqual(self.destination.read_bytes(), b"binary")
        self.assertEqual(stat.S_IMODE(self.destination.stat().st_mode), 0o755)


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "run"

    def refresh(self, payload, restart=True):
        with mock.patch.object(runtime, "run", return_value=payload) as run:
            changed = runtime.refresh(config(), self.root, restart=restart)
        return changed, run

    def test_password_and_username_quoting(self):
        username = 'user@name:/?#%" $'
        password = 'SECRET@:/?#%"\\\n$` ü'
        content = runtime.environment(config(), secret(password, username=username)).decode()
        line = next(line for line in content.splitlines() if line.startswith("OPENSTORAGE_PG_URL="))
        dsn = line.split('"')[1]
        url = urlsplit(dsn)
        self.assertEqual(unquote(url.username), username)
        self.assertEqual(unquote(url.password), password)
        self.assertEqual(url.hostname, config()["postgres_host"])
        self.assertEqual(url.port, 5432)
        self.assertEqual(url.path, "/openstorage")
        self.assertEqual(parse_qs(url.query), {"sslmode": ["verify-full"], "sslrootcert": ["/etc/openstorage/rds-ca.pem"]})
        self.assertNotIn(password, content)
        self.assertNotIn("SECRET@", content)

    def test_safe_runtime_settings(self):
        content = runtime.environment(config(), secret()).decode()
        for setting in [
            'OPENSTORAGE_S3_URL="s3://data-bucket?region=us-east-1"',
            'OPENSTORAGE_S3_EXPECTED_BUCKET_OWNER="123456789012"',
            'OPENSTORAGE_SYNC_ALLOW_PRIVATE="false"', 'OPENSTORAGE_RATE_RPS="20"',
            'OPENSTORAGE_RATE_BURST="40"', 'TMPDIR="/var/lib/openstorage/tmp"',
            'OPENSTORAGE_ORG_PUBKEYS="/etc/openstorage/org.pub.pem"',
        ]:
            self.assertIn(setting, content)
        self.assertNotIn("AWS_ACCESS_KEY_ID", content)
        self.assertNotIn("SECRETS_KEY", content)

    def test_prepare_permissions_and_no_restart(self):
        changed, run = self.refresh(secret(), restart=False)
        self.assertTrue(changed)
        run.assert_called_once()
        self.assertEqual(stat.S_IMODE(self.root.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE((self.root / "service.env").stat().st_mode), 0o600)
        self.assertEqual((self.root / "service.env").stat().st_uid, os.geteuid())
        self.assertEqual(stat.S_IMODE((self.root / "refresh.lock").stat().st_mode), 0o600)
        self.assertIn("--version-stage", run.call_args.args[0])
        self.assertIn("AWSCURRENT", run.call_args.args[0])

    def test_only_changed_value_or_version_restarts(self):
        self.refresh(secret(), restart=False)
        before = (self.root / "service.env").stat().st_ino
        changed, run = self.refresh(secret())
        self.assertFalse(changed)
        run.assert_called_once()
        self.assertEqual(before, (self.root / "service.env").stat().st_ino)
        for payload in [secret("rotated"), secret("rotated", "b" * 32)]:
            changed, run = self.refresh(payload)
            self.assertTrue(changed)
            self.assertEqual(run.call_args.args[0], ["systemctl", "--no-block", "try-restart", "openstorage.service"])

    def test_invalid_secret_and_fetch_failures_preserve_previous_file(self):
        self.refresh(secret(), restart=False)
        before = (self.root / "service.env").read_bytes()
        invalid = [b"not json", b"{}", b'{"SecretBinary":"private"}',
                   secret(""), secret(username=""), secret(version="bad\nINJECT=1"),
                   json.dumps({"VersionId": "a" * 32, "SecretString": '{"username":"user","password":42}'}).encode()]
        for payload in invalid:
            with self.subTest(payload=payload):
                with mock.patch.object(runtime, "run", return_value=payload) as run:
                    with self.assertRaises(ValueError):
                        runtime.refresh(config(), self.root, restart=True)
                    run.assert_called_once()
                self.assertEqual(before, (self.root / "service.env").read_bytes())
        with mock.patch.object(runtime, "run", side_effect=RuntimeError("AWS failure")):
            with self.assertRaises(RuntimeError):
                runtime.refresh(config(), self.root, restart=False)
        self.assertEqual(before, (self.root / "service.env").read_bytes())

    def test_failed_restart_enqueue_retries_next_refresh(self):
        self.refresh(secret(), restart=False)
        before = (self.root / "service.env").read_bytes()
        with mock.patch.object(runtime, "run", side_effect=[secret("rotated"), RuntimeError("systemctl failed")]):
            with self.assertRaises(RuntimeError):
                runtime.refresh(config(), self.root, restart=True)
        self.assertEqual(before, (self.root / "service.env").read_bytes())
        changed, run = self.refresh(secret("rotated"))
        self.assertTrue(changed)
        self.assertEqual(run.call_count, 2)

    def test_atomic_write_failure_keeps_old_file_and_cleans_temporary(self):
        self.refresh(secret(), restart=False)
        before = (self.root / "service.env").read_bytes()
        with mock.patch.object(runtime.os, "replace", side_effect=OSError("disk failure")):
            with self.assertRaises(OSError):
                self.refresh(secret("rotated"))
        self.assertEqual(before, (self.root / "service.env").read_bytes())
        self.assertEqual({p.name for p in self.root.iterdir()}, {"service.env", "refresh.lock"})

    def test_rejects_symlinks_and_nonprivate_existing_env(self):
        self.refresh(secret(), restart=False)
        path = self.root / "service.env"
        path.chmod(0o644)
        with self.assertRaises(ValueError):
            self.refresh(secret())
        path.unlink()
        path.symlink_to(self.root.parent / "elsewhere")
        with self.assertRaises(ValueError):
            self.refresh(secret())

    def test_command_errors_and_cli_never_print_secrets(self):
        leak = "SUPER_PRIVATE_password"
        error = subprocess.CalledProcessError(1, ["aws"], output=leak.encode(), stderr=leak.encode())
        with mock.patch.object(runtime.subprocess, "run", side_effect=error):
            with self.assertRaises(RuntimeError) as caught:
                runtime.run(["aws", "secretsmanager", "get-secret-value"])
        self.assertNotIn(leak, str(caught.exception))
        with mock.patch.object(runtime.os, "geteuid", return_value=0), mock.patch.object(runtime, "load_config", side_effect=ValueError(leak)):
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                self.assertEqual(runtime.main(["prepare"]), 1)
            self.assertEqual(out.getvalue(), "")
            self.assertNotIn(leak, err.getvalue())

    def test_subprocess_cannot_inherit_static_aws_credentials(self):
        with mock.patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "static", "AWS_SECRET_ACCESS_KEY": "secret", "AWS_PROFILE": "personal", "AWS_ENDPOINT_URL": "http://evil"}):
            with mock.patch.object(runtime.subprocess, "run", return_value=mock.Mock(stdout=b"ok")) as run:
                self.assertEqual(runtime.run(["aws", "s3", "ls"]), b"ok")
            env = run.call_args.kwargs["env"]
            self.assertNotIn("AWS_ACCESS_KEY_ID", env)
            self.assertNotIn("AWS_SECRET_ACCESS_KEY", env)
            self.assertNotIn("AWS_PROFILE", env)
            self.assertNotIn("AWS_ENDPOINT_URL", env)
            self.assertEqual(env["AWS_SHARED_CREDENTIALS_FILE"], "/dev/null")
            self.assertEqual(env["AWS_CONFIG_FILE"], "/dev/null")
            self.assertFalse(run.call_args.kwargs.get("shell", False))


class CATests(unittest.TestCase):
    def test_ca_download_is_fixed_https_and_refuses_redirects(self):
        self.assertEqual(bootstrap.CA_URL, "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem")
        with self.assertRaises(ValueError):
            bootstrap.NoRedirect().redirect_request(None, None, 302, "redirect", {}, "http://evil/ca.pem")

    def test_invalid_or_oversized_ca_does_not_replace_trust(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "ca.pem"
            path.write_bytes(b"previous CA")
            for content in [b"not a certificate", b"x" * (2 * 1024 * 1024 + 1)]:
                with mock.patch.object(bootstrap.urllib.request, "build_opener") as build:
                    build.return_value.open.return_value.__enter__.return_value.read.return_value = content
                    with self.assertRaises((ValueError, bootstrap.ssl.SSLError)):
                        bootstrap.install_ca(path)
                    build.return_value.open.assert_called_once_with(bootstrap.CA_URL, timeout=30)
                self.assertEqual(path.read_bytes(), b"previous CA")


class UnitTests(unittest.TestCase):
    def test_units_use_root_preparation_and_unprivileged_server_without_cycle(self):
        directory = Path(__file__).parent
        service = (directory / "openstorage.service").read_text()
        self.assertEqual(bootstrap.UNITS, ("openstorage.service", "openstorage-refresh.service", "openstorage-refresh.timer"))
        refresh = (directory / "openstorage-refresh.service").read_text()
        timer = (directory / "openstorage-refresh.timer").read_text()
        self.assertIn("Unit=openstorage-refresh.service", timer)
        for setting in ["User=openstorage", "Group=openstorage", "UMask=0077", "LimitCORE=0",
                        "ExecStartPre=+/usr/bin/python3 /etc/openstorage/runtime.py prepare",
                        "EnvironmentFile=-/run/openstorage/service.env",
                        "ExecStart=/usr/local/bin/openstorage serve --cache-dir=/var/cache/openstorage",
                        "Restart=on-failure"]:
            self.assertIn(setting, service)
        self.assertIn("User=root", refresh)
        self.assertIn("runtime.py refresh", refresh)
        self.assertNotIn("After=openstorage.service", refresh)
        self.assertNotIn("Requires=openstorage", service)
        self.assertIn("OnUnitInactiveSec=1min", timer)


if __name__ == "__main__":
    unittest.main()
