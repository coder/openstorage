"""Local TLS transport regressions; no cloud or external network requests."""
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import ssl
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

import aws_launch_qualification as q


class TLSTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        cls.root = Path(cls.temp.name)
        for name in ("ca", "unknown"):
            subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                            "-subj", "/CN=" + name, "-addext", "basicConstraints=critical,CA:TRUE",
                            "-addext", "keyUsage=critical,keyCertSign,cRLSign",
                            "-keyout", str(cls.root / (name + ".key")), "-out", str(cls.root / (name + ".pem"))],
                           check=True, capture_output=True, timeout=15)
        subprocess.run(["openssl", "req", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost",
                        "-keyout", str(cls.root / "server.key"), "-out", str(cls.root / "server.csr")],
                       check=True, capture_output=True, timeout=15)
        (cls.root / "extensions").write_text("subjectAltName=DNS:localhost\nbasicConstraints=critical,CA:FALSE\n")
        subprocess.run(["openssl", "x509", "-req", "-in", str(cls.root / "server.csr"),
                        "-CA", str(cls.root / "ca.pem"), "-CAkey", str(cls.root / "ca.key"), "-CAcreateserial",
                        "-days", "1", "-extfile", str(cls.root / "extensions"), "-out", str(cls.root / "server.pem")],
                       check=True, capture_output=True, timeout=15)
        cls.requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                cls.requests.append(q.urllib.parse.urlsplit(self.path).path)
                if self.path == "/healthz":
                    body, content = b"ok", "text/plain"
                elif self.path.startswith("/repo.git/info/refs?"):
                    body, content = b"001e# service=git-upload-pack\n00000000", "application/x-git-upload-pack-advertisement"
                elif q.urllib.parse.urlsplit(self.path).path == "/repo.git/info/lfs/locks":
                    body, content = b'{"locks": []}', "application/vnd.git-lfs+json"
                else:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Type", content)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def handle(self):
                try:
                    super().handle()
                except (BrokenPipeError, ConnectionResetError):
                    pass  # Clients rejecting the certificate may close before sending HTTP.

            def log_message(self, *_args):
                pass

        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.addClassCleanup(cls.server.server_close)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cls.root / "server.pem", cls.root / "server.key")
        cls.server.socket = context.wrap_socket(cls.server.socket, server_side=True)
        thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        thread.start()
        cls.addClassCleanup(thread.join, 5)
        cls.addClassCleanup(cls.server.shutdown)
        cls.endpoint = "https://localhost:" + str(cls.server.server_port)

    def setUp(self):
        self.work = tempfile.TemporaryDirectory()
        self.addCleanup(self.work.cleanup)
        self.path = Path(self.work.name)
        binary = self.path / "helper"
        binary.write_text(f"#!{sys.executable}\nprint('openstorage devel (commit {'a' * 40}, built test)')\n")
        binary.chmod(0o700)
        key = self.path / "key.pem"
        key.write_text("private test input")
        key.chmod(0o600)
        self.config = dict(schema_version=1, candidate_commit="a" * 40, binary=str(binary),
                           binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                           topology="pg-s3-single-node", endpoint=self.endpoint, replica_urls=[],
                           org="test", private_key_file=str(key), ca_file=str(self.root / "ca.pem"))
        self.config_path = self.path / "config.json"

    def runner(self, config):
        self.config_path.write_text(json.dumps(config))
        work = self.path / str(len(list(self.path.iterdir())))
        work.mkdir()
        runner = q.Runner({"scenarios": {name: {} for name in q.SCENARIOS}}, str(work), 30)
        runner.preflight(self.config_path, smoke=True, aws_readonly=False)
        runner.git("init", str(work / "repo"))
        return runner

    def test_custom_ca_verifies_all_transports_and_keeps_public_roots(self):
        bundle = self.path / "input-ca.pem"
        roots = ssl.create_default_context().get_ca_certs(binary_form=True)
        bundle.write_text("".join(ssl.DER_cert_to_PEM_cert(cert) for cert in roots) + (self.root / "ca.pem").read_text())
        runner = self.runner(self.config | {"ca_file": str(bundle)})
        # Removing the caller's file after preflight must not change transport trust.
        bundle.unlink()
        snapshot = Path(runner.config["ca_file"])
        self.assertNotEqual(snapshot, bundle)
        actual = ssl.create_default_context(cafile=str(snapshot)).get_ca_certs(binary_form=True)
        self.assertTrue(set(roots) <= set(actual))
        self.assertEqual(runner.http(self.endpoint, "/healthz"), b"ok")
        self.assertEqual(runner.git("ls-remote", self.endpoint + "/repo.git"), b"")
        runner.git("-c", "lfs.url=" + self.endpoint + "/repo.git/info/lfs", "lfs", "locks", cwd=runner.work / "repo")
        self.assertIn("/repo.git/info/lfs/locks", self.requests)

    def test_unknown_ca_and_hostname_mismatch_fail_every_transport(self):
        for change in ({"ca_file": str(self.root / "unknown.pem")},
                       {"endpoint": self.endpoint.replace("localhost", "127.0.0.1")}, {"ca_file": None}):
            with self.subTest(change=change):
                config = self.config | change
                if config["ca_file"] is None:
                    del config["ca_file"]
                with patch.dict(os.environ, {"GIT_SSL_NO_VERIFY": "1"}):
                    runner = self.runner(config)
                endpoint = config["endpoint"]
                with self.assertRaises(q.urllib.error.URLError):
                    runner.http(endpoint, "/healthz")
                with self.assertRaisesRegex(q.Failure, "^command_failed$"):
                    runner.git("ls-remote", endpoint + "/repo.git")
                with self.assertRaisesRegex(q.Failure, "^command_failed$"):
                    runner.git("-c", "lfs.url=" + endpoint + "/repo.git/info/lfs", "lfs", "locks", cwd=runner.work / "repo")

    def test_invalid_ca_input_fails_before_helper_execution(self):
        ca = self.path / "invalid.pem"
        for index, data in enumerate((b"", b"garbage", b"-----BEGIN CERTIFICATE-----\nYWJj\n-----END CERTIFICATE-----\n",
                                      (self.root / "ca.pem").read_bytes() + (self.root / "ca.key").read_bytes(),
                                      (self.root / "server.pem").read_bytes(), b"x" * (q.LIMIT + 1))):
            ca.write_bytes(data)
            self.config_path.write_text(json.dumps(self.config | {"ca_file": str(ca)}))
            with self.subTest(index=index), patch.object(q.Runner, "command") as command:
                self.assertEqual(q.execute(self.config_path, self.path / ("invalid-" + str(index))), 1)
                command.assert_not_called()
        for index, value in enumerate((None, 42, "", str(self.path), str(self.path / "missing"))):
            self.config_path.write_text(json.dumps(self.config | {"ca_file": value}))
            with self.subTest(value=value), patch.object(q.Runner, "command") as command:
                self.assertEqual(q.execute(self.config_path, self.path / ("path-" + str(index))), 1)
                command.assert_not_called()


if __name__ == "__main__":
    unittest.main()
