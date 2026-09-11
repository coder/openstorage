#!/usr/bin/python3
"""Install the pinned release on AL2023; invoked once by cloud-init as root."""

import base64
import hashlib
import os
from pathlib import Path
import platform
import pwd
import re
import shutil
import ssl
import sys
import tarfile
import tempfile
import urllib.request

from runtime import CA_FILE, CONFIG_DIR, atomic_write, load_config, run

UNITS = ("openstorage.service", "openstorage-refresh.service", "openstorage-refresh.timer")
CA_URL = "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem"


def validate_public_key(path):
    content = path.read_text(encoding="ascii")
    match = re.fullmatch(r"\s*-----BEGIN PUBLIC KEY-----\s+([A-Za-z0-9+/=\s]+)-----END PUBLIC KEY-----\s*", content)
    if not match:
        raise ValueError("org.pub.pem must contain only one public key")
    base64.b64decode("".join(match[1].split()), validate=True)
    run(["openssl", "pkey", "-pubin", "-in", str(path), "-pubcheck", "-noout"])


def install_release(archive_path, expected_sha256, tag, destination):
    digest = hashlib.sha256()
    with archive_path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != expected_sha256.lower():
        raise ValueError("release checksum mismatch")
    # Do not use extract/extractall: ignore archive ownership, permissions, and
    # paths, and reject links or anything outside the two-file release layout.
    with tarfile.open(archive_path, "r:gz") as archive:
        members = archive.getmembers()
        if len(members) != 2 or {member.name for member in members} != {"openstorage", "LICENSE"}:
            raise ValueError("unexpected release archive layout")
        if any(not member.isreg() or member.issparse() or not 0 < member.size <= 512 * 1024 * 1024 for member in members):
            raise ValueError("release archive must contain only bounded regular files")
        with tempfile.TemporaryDirectory(prefix=".openstorage-", dir=destination.parent) as temp:
            for member in members:
                with archive.extractfile(member) as source, (Path(temp) / member.name).open("wb") as target:
                    shutil.copyfileobj(source, target)
            binary = Path(temp) / "openstorage"
            binary.chmod(0o755)
            output = run([str(binary), "--version"]).decode("utf-8").strip()
            if not output.startswith("openstorage " + tag + " (") or "\n" in output:
                raise ValueError("release binary version mismatch")
            license_content = (Path(temp) / "LICENSE").read_bytes()
            os.replace(binary, destination)
            return license_content


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("RDS CA download must not redirect")


def install_ca(destination=CA_FILE):
    opener = urllib.request.build_opener(NoRedirect(), urllib.request.HTTPSHandler(context=ssl.create_default_context()))
    with opener.open(CA_URL, timeout=30) as response:
        content = response.read(2 * 1024 * 1024 + 1)
    if len(content) > 2 * 1024 * 1024:
        raise ValueError("RDS CA bundle exceeds size limit")
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.load_verify_locations(cadata=content.decode("ascii"))
    atomic_write(destination, content, 0o644)


def bootstrap():
    if os.geteuid() != 0:
        raise ValueError("bootstrap requires root")
    release = Path("/etc/os-release").read_text()
    if not re.search(r'^ID="?amzn"?$', release, re.MULTILINE) or not re.search(r'^VERSION_ID="?2023"?$', release, re.MULTILINE) or platform.machine() != "x86_64":
        raise ValueError("only Amazon Linux 2023 x86_64 is supported")
    CONFIG_DIR.chmod(0o755)
    (CONFIG_DIR / "config.json").chmod(0o600)
    public_key = CONFIG_DIR / "org.pub.pem"
    public_key.chmod(0o600)
    cfg = load_config()
    run(["dnf", "install", "-y", "--allowerasing", "git", "gnupg2", "openssh-clients", "python3", "awscli-2", "openssl", "shadow-utils", "ca-certificates"], timeout=600)
    validate_public_key(public_key)
    public_key.chmod(0o644)
    try:
        user = pwd.getpwnam("openstorage")
    except KeyError:
        run(["useradd", "--system", "--user-group", "--home-dir", "/var/lib/openstorage", "--shell", "/sbin/nologin", "openstorage"])
        user = pwd.getpwnam("openstorage")
    if user.pw_uid == 0 or user.pw_gid == 0:
        raise ValueError("openstorage account must not be root")
    for name in ("/var/cache/openstorage", "/var/lib/openstorage", "/var/lib/openstorage/tmp"):
        directory = Path(name)
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        # A previous service process can replace tmp with a symlink. Change
        # ownership through a no-follow descriptor, never through that path.
        fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fchmod(fd, 0o700)
            os.fchown(fd, user.pw_uid, user.pw_gid)
        finally:
            os.close(fd)
    with tempfile.TemporaryDirectory(prefix="openstorage-release-") as temp:
        archive = Path(temp) / "release.tar.gz"
        # AL2023's high-level `s3 cp` does not support expected-bucket-owner.
        run(["aws", "s3api", "get-object", "--bucket", cfg["artifact_bucket"], "--key", cfg["artifact_key"],
             "--region", cfg["region"], "--expected-bucket-owner", cfg["account_id"], str(archive)], timeout=300)
        license_content = install_release(archive, cfg["release_sha256"], cfg["release_tag"], Path("/usr/local/bin/openstorage"))
    atomic_write(CONFIG_DIR / "LICENSE", license_content, 0o644)
    install_ca()
    for name in UNITS:
        atomic_write(Path("/etc/systemd/system") / name, (CONFIG_DIR / name).read_bytes(), 0o644)
    run(["systemctl", "daemon-reload"])
    run(["systemctl", "enable", "--now", "openstorage.service", "openstorage-refresh.timer"], timeout=120)


def main():
    try:
        bootstrap()
    except Exception:
        print("openstorage bootstrap failed (details withheld)", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
