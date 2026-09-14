"""Destructive local-filesystem probe. Run ONLY in Dockerfile.test's container.

AWS and systemctl are stubbed. All package tools, filesystem permissions,
archive verification, public-key parsing, and unit-file validation are real.
"""

import base64
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import ssl
import tarfile
import tempfile
import unittest
from unittest import mock

import bootstrap
import runtime
from test_bootstrap import config, secret


class ContainerProbe(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.geteuid() != 0 or not Path("/.dockerenv").exists() or not Path("/etc/amazon-linux-release").exists():
            raise RuntimeError("container probe requires a disposable root AL2023 Docker container")

    def test_bootstrap_and_runtime(self):
        cfg = config()
        directory = Path("/etc/openstorage")
        directory.mkdir(exist_ok=True)
        for name in ("runtime.py", "bootstrap.py") + bootstrap.UNITS:
            shutil.copyfile(Path(__file__).parent / name, directory / name)
        # RFC 8032 public test vector, encoded as Ed25519 SubjectPublicKeyInfo.
        der = bytes.fromhex("302a300506032b6570032100d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
        (directory / "org.pub.pem").write_text("-----BEGIN PUBLIC KEY-----\n" + base64.b64encode(der).decode() + "\n-----END PUBLIC KEY-----\n")
        ca = ssl.DER_cert_to_PEM_cert(ssl.create_default_context().get_ca_certs(binary_form=True)[0]).encode()
        with tempfile.TemporaryDirectory() as temp:
            archive = Path(temp) / "fixture.tar.gz"
            with tarfile.open(archive, "w:gz") as output:
                for name, content in [("openstorage", b"#!/bin/sh\nprintf 'openstorage v0.10.0 (fixture)\\n'\n"), ("LICENSE", b"test license")]:
                    member = tarfile.TarInfo(name)
                    member.size = len(content)
                    output.addfile(member, io.BytesIO(content))
            cfg["release_sha256"] = hashlib.sha256(archive.read_bytes()).hexdigest()
            (directory / "config.json").write_text(json.dumps(cfg))
            real_run = runtime.run

            def command(args, **kwargs):
                if args[0] == "dnf":
                    self.assertIn("awscli-2", args)
                    self.assertIn("--allowerasing", args)
                    return b""
                if args[:3] == ["aws", "s3api", "get-object"]:
                    self.assertEqual(args[args.index("--expected-bucket-owner") + 1], cfg["account_id"])
                    self.assertEqual(args[args.index("--key") + 1], cfg["artifact_key"])
                    shutil.copyfile(archive, args[-1])
                    return b"{}"
                if args[0] in ("aws", "systemctl"):
                    self.assertEqual(args[0], "systemctl", "unexpected AWS call")
                    return b""
                return real_run(args, **kwargs)

            with mock.patch.object(bootstrap, "run", side_effect=command), mock.patch.object(bootstrap.urllib.request, "build_opener") as opener:
                opener.return_value.open.return_value.__enter__.return_value.read.return_value = ca
                bootstrap.bootstrap()
                bootstrap.bootstrap()  # Re-running cloud-init installation is safe.
                temporary = Path("/var/lib/openstorage/tmp")
                saved = temporary.with_name("tmp.saved")
                temporary.rename(saved)
                try:
                    temporary.symlink_to(temp, target_is_directory=True)
                    with self.assertRaises(OSError):
                        bootstrap.bootstrap()
                    self.assertEqual(Path(temp).stat().st_uid, 0)
                finally:
                    temporary.unlink()
                    saved.rename(temporary)
        for executable in ("git", "gpg", "ssh-keygen", "aws", "openssl"):
            self.assertIsNotNone(shutil.which(executable))
        self.assertEqual(real_run(["/usr/local/bin/openstorage", "--version"]), b"openstorage v0.10.0 (fixture)\n")
        real_run(["systemd-analyze", "verify"] + [str(Path("/etc/systemd/system") / name) for name in bootstrap.UNITS])
        self.assertEqual((directory / "config.json").stat().st_mode & 0o777, 0o600)
        self.assertEqual((directory / "org.pub.pem").stat().st_mode & 0o777, 0o644)
        self.assertEqual((directory / "rds-ca.pem").read_bytes(), ca)
        with mock.patch.object(runtime, "run", return_value=secret()):
            self.assertTrue(runtime.refresh(cfg, restart=False))
        env = Path("/run/openstorage/service.env")
        self.assertEqual(env.stat().st_uid, 0)
        self.assertEqual(env.stat().st_mode & 0o777, 0o600)
        script = "import os; assert not os.access('/run/openstorage/service.env', os.R_OK); assert not os.access('/etc/openstorage/config.json', os.R_OK); assert os.access('/etc/openstorage/org.pub.pem', os.R_OK); assert os.access('/var/cache/openstorage', os.W_OK); assert os.access('/var/lib/openstorage/tmp', os.W_OK)"
        real_run(["runuser", "-u", "openstorage", "--", "python3", "-c", script])
        with mock.patch.object(runtime, "run", return_value=secret("rotated", "b" * 32)) as run:
            self.assertTrue(runtime.refresh(cfg, restart=True))
            self.assertEqual(run.call_args.args[0], ["systemctl", "--no-block", "try-restart", "openstorage.service"])
        previous = env.read_bytes()
        with mock.patch.object(runtime, "run", return_value=b"{}"):
            with self.assertRaises(ValueError):
                runtime.refresh(cfg, restart=False)
        self.assertEqual(env.read_bytes(), previous)


if __name__ == "__main__":
    unittest.main(verbosity=2)
