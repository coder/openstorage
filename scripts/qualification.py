#!/usr/bin/env python3
"""Linux-only, single-command qualification supervision and exact-session cleanup.

Not a sandbox. Run only cooperative qualification commands on the local daemon.
See docs/how-to/qualification-adapter.md for prerequisites and HOLD recovery.
"""
import argparse
import ctypes
import errno
import fcntl
import http.client
import json
import math
import os
from pathlib import Path
import re
import signal
import socket
import stat
import subprocess
import sys
import time
from urllib.parse import urlencode

GRACE = 2
POLL = 0.05
HOLD_POLL = 5
CAPACITY_INTERVAL = 5
QUERY_TIMEOUT = 5
MIN_AVAILABLE = 60_696_476_550
MAX_RESPONSE = 1024 * 1024
LABEL = "org.testcontainers.sessionId"
KINDS = ("containers", "networks", "volumes")
WALL = 0x40000000  # __WALL includes Linux clone children with other exit signals.
WAIT = os.WEXITED | os.WNOWAIT | os.WNOHANG | WALL


def require(condition, message):
    if not condition:
        raise ValueError(message)


def request_timeout(signum, frame):
    raise TimeoutError("Docker request deadline")


class Docker:
    """Only the fixed local endpoints needed by this adapter; no helper processes."""
    def __init__(self, args, session):
        self.args, self.session = args, session

    def request(self, method, path):
        connection = http.client.HTTPConnection("localhost", timeout=QUERY_TIMEOUT)
        transport = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        transport.settimeout(QUERY_TIMEOUT)
        signal.setitimer(signal.ITIMER_REAL, QUERY_TIMEOUT)
        try:
            transport.connect(self.args.docker_socket)
            connection.sock = transport
            connection.request(method, path)
            response = connection.getresponse()
            data = response.read(MAX_RESPONSE + 1)
            require(200 <= response.status < 300, "Docker HTTP failure")
            require(len(data) <= MAX_RESPONSE, "Docker response too large")
            require(response.length in (None, 0), "truncated Docker response")
            return json.loads(data) if data else None
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            connection.close()
            transport.close()

    def identity(self):
        info = self.request("GET", "/info")
        require(isinstance(info, dict) and info.get("ID") == self.args.daemon_id,
                "Docker daemon identity mismatch")
        require(info.get("DockerRootDir") == self.args.backing_directory,
                "Docker backing directory mismatch")

    def inventory(self):
        found = {}
        for kind in KINDS:
            query = {"filters": json.dumps({"label": [LABEL + "=" + self.session]})}
            path = "/" + kind
            if kind == "containers":
                path += "/json"
                query["all"] = "1"
            rows = self.request("GET", path + "?" + urlencode(query))
            if kind == "volumes":
                require(isinstance(rows, dict) and "Volumes" in rows and not rows.get("Warnings"),
                        "incomplete volume inventory")
                rows = rows["Volumes"]
                if rows is None:  # Docker uses null for an empty volume list.
                    rows = []
            require(isinstance(rows, list), "invalid Docker inventory")
            identifiers = []
            for row in rows:
                require(isinstance(row, dict), "invalid Docker resource")
                ident = row.get("Name" if kind == "volumes" else "Id")
                pattern = r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,254}" if kind == "volumes" else r"[0-9a-f]{64}"
                require(isinstance(ident, str) and re.fullmatch(pattern, ident), "invalid resource identity")
                require(isinstance(row.get("Labels"), dict) and row["Labels"].get(LABEL) == self.session,
                        "inventory label mismatch")
                identifiers.append(ident)
            require(len(set(identifiers)) == len(identifiers), "duplicate resource identity")
            found[kind] = identifiers
        return found

    def cleanup(self):
        self.identity()
        inventory = self.inventory()
        for kind, identifiers in inventory.items():
            for ident in identifiers:
                path = "/" + kind + "/" + ident
                resource = self.request("GET", path + ("/json" if kind == "containers" else ""))
                require(isinstance(resource, dict), "invalid resource inspection")
                require(resource.get("Name" if kind == "volumes" else "Id") == ident,
                        "inspection identity mismatch")
                config = resource.get("Config") if kind == "containers" else resource
                require(isinstance(config, dict) and isinstance(config.get("Labels"), dict)
                        and config["Labels"].get(LABEL) == self.session, "inspection label mismatch")
                # Volumes are separate, individually inspected resources. Never v=1.
                self.request("DELETE", path + ("?force=1" if kind == "containers" else ""))
        require(not any(self.inventory().values()), "owned resources remain")


class Qualification:
    def __init__(self, args):
        self.args = args
        session = os.environ.get("TESTCONTAINERS_SESSION_ID", "")
        require(re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", session), "explicit session ID required")
        artifact = os.environ.get("OPENSTORAGE_TEST_ARTIFACT_DIR", "")
        require(artifact and Path(artifact).is_absolute(), "absolute artifact directory required")
        self.artifact = Path(artifact)
        st = self.artifact.lstat()
        require(stat.S_ISDIR(st.st_mode) and stat.S_IMODE(st.st_mode) == 0o700 and st.st_uid == os.getuid(),
                "artifact directory must be caller-owned mode 0700")
        require(Path(args.backing_directory).is_absolute() and Path(args.docker_socket).is_absolute(),
                "absolute local daemon paths required")
        require(os.stat(args.backing_directory).st_dev == st.st_dev,
                "artifacts and Docker backing directory must share a filesystem")
        require(not (Path.home() / ".testcontainers.properties").exists(),
                "private HOME without Testcontainers properties required")
        host = "unix://" + args.docker_socket
        require(os.environ.get("DOCKER_HOST", host) == host, "conflicting Docker host")
        for key in ("DOCKER_CONTEXT", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH", "TESTCONTAINERS_HOST_OVERRIDE"):
            require(not os.environ.get(key), "conflicting Docker configuration")
        require(os.environ.get("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", args.docker_socket) == args.docker_socket,
                "conflicting Docker socket override")
        self.env = dict(os.environ, DOCKER_HOST=host, TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=args.docker_socket)
        self.receipt = dict(version=1, session_id=session, native_status=None, quiescent=True,
                            state="admission", error="", interrupted=0, timed_out=False,
                            lock_held=False, capacity_available=0, capacity_required=0,
                            capacity_samples=0, capacity_min_available=None, reaped=0)
        self.path = self.artifact / "qualification.json"
        with self.path.open("x"):
            pass  # Exclusive attempt. Never replace evidence from another run.
        self.control_path = self.artifact / "qualification.control"
        os.mkfifo(self.control_path, 0o600)
        self.control = os.open(self.control_path, os.O_RDWR | os.O_NONBLOCK | os.O_NOFOLLOW)
        self.lock = None
        self.next_capacity = 0
        self.docker = Docker(args, session)
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(sig, self.interrupt)
        signal.signal(signal.SIGALRM, request_timeout)

    def interrupt(self, signum, frame=None):
        if not self.receipt["interrupted"]:
            self.receipt["interrupted"] = signum

    def error(self, stage, exc):
        if not self.receipt["error"]:
            # Do not copy command arguments, response bodies, or environment values.
            detail = str(exc) if type(exc) is ValueError else str(getattr(exc, "errno", ""))
            self.receipt["error"] = (stage + ":" + type(exc).__name__ + ":" + detail)[:240]
            print("qualification HOLD: " + self.receipt["error"], file=sys.stderr, flush=True)
        self.receipt["state"] = "hold"

    def save(self):
        try:
            temporary = self.path.with_suffix(".tmp")
            with temporary.open("w") as out:
                json.dump(self.receipt, out, sort_keys=True)
                out.write("\n")
                out.flush()
                os.fsync(out.fileno())
            temporary.replace(self.path)
            return True
        except OSError as exc:
            self.error("receipt", exc)
            return False

    def capacity(self):
        st = os.statvfs(self.args.backing_directory)
        available = st.f_bavail * st.f_frsize
        required = max(MIN_AVAILABLE, (st.f_blocks * st.f_frsize + 99) // 100 + self.args.headroom_bytes)
        minimum = self.receipt["capacity_min_available"]
        self.receipt.update(capacity_available=available, capacity_required=required,
                            capacity_min_available=available if minimum is None else min(minimum, available),
                            capacity_samples=self.receipt["capacity_samples"] + 1)
        self.next_capacity = time.monotonic() + CAPACITY_INTERVAL
        require(available >= required, "insufficient backing filesystem capacity")

    def tick(self):
        try:
            data = os.read(self.control, 4096)
            for byte in data:
                if byte in (ord("T"), ord("I")):
                    self.interrupt(signal.SIGTERM if byte == ord("T") else signal.SIGINT)
                    break
        except BlockingIOError:
            pass
        if time.monotonic() >= self.next_capacity:
            try:
                self.capacity()
            except OSError as exc:
                self.error("capacity", exc)
            except ValueError as exc:
                self.error("capacity", exc)
        self.save()

    def supervise(self, command):
        # Popen is only for exec's error handshake. Never poll/wait it: this sole
        # waiter records the real leader status and drains every adopted child.
        process = subprocess.Popen(command, env=self.env, stdin=subprocess.DEVNULL, start_new_session=True)
        self.receipt.update(quiescent=False, state="running")
        handles = {}
        sent = {}
        end = time.monotonic() + self.args.timeout
        stopping = None
        while True:
            try:
                self.tick()
            except OSError as exc:
                self.error("control", exc)
            now = time.monotonic()
            if stopping is None and (process.returncode is not None or self.receipt["error"]
                                     or self.receipt["interrupted"] or self.receipt["timed_out"]):
                stopping = now
            try:
                # Discovery is not authority or an emptiness proof. An unreaped
                # kernel child relationship plus P_PIDFD validation pins identity.
                candidates = Path(f"/proc/self/task/{os.getpid()}/children").read_text().split()
                for child in map(int, candidates):
                    if child not in handles:
                        fd = os.pidfd_open(child)
                        try:
                            os.waitid(os.P_PIDFD, fd, WAIT)
                        except BaseException:
                            os.close(fd)
                            raise
                        handles[child] = fd
                for child, fd in list(handles.items()):
                    status = os.waitid(os.P_PIDFD, fd, WAIT)
                    if status is not None:
                        actual = os.waitid(os.P_PIDFD, fd, os.WEXITED | WALL)
                        assert status == actual, "sole-waiter status changed"
                        self.receipt["reaped"] += 1
                        if child == process.pid:
                            native = status.si_status if status.si_code == os.CLD_EXITED else -status.si_status
                            self.receipt["native_status"] = process.returncode = native
                        os.close(fd)
                        del handles[child]
                        sent.pop(child, None)
                    elif stopping is not None:
                        sig = signal.SIGKILL if now - stopping >= GRACE else self.receipt["interrupted"] or signal.SIGTERM
                        if sent.get(child) != sig:
                            try:
                                signal.pidfd_send_signal(fd, sig)
                            except ProcessLookupError:
                                pass  # It exited after the wait check; reap next time.
                            sent[child] = sig
                # Reap available leader status before this poll's timeout decision.
                if time.monotonic() >= end and process.returncode is None:
                    self.receipt["timed_out"] = True
                try:
                    os.waitid(os.P_ALL, 0, WAIT)
                except ChildProcessError as exc:
                    assert exc.errno == errno.ECHILD and not handles and process.returncode is not None
                    self.receipt["quiescent"] = True
                    return
            except (OSError, ValueError) as exc:
                self.error("supervision", exc)
            # A drain deadline is not permission to abandon producers. Continue
            # owning handles and the lock until kernel quiescence is established.
            if stopping is not None and now - stopping > GRACE + QUERY_TIMEOUT:
                self.error("drain-deadline", TimeoutError())
            time.sleep(POLL)

    def run(self, command):
        require(sys.platform == "linux" and len(list(Path("/proc/self/task").iterdir())) == 1,
                "dedicated single-thread Linux process required")
        require(signal.getsignal(signal.SIGCHLD) == signal.SIG_DFL, "SIGCHLD must not auto-reap")
        # Refuse unsupported proc mounts before owning a lock or any producer.
        Path(f"/proc/self/task/{os.getpid()}/children").read_text()
        libc = ctypes.CDLL(None, use_errno=True)
        require(libc.prctl(36, 1, 0, 0, 0) == 0, "child subreaper unavailable")
        # Probe pidfd wait support and reject unrelated inherited children before
        # taking a lock or starting a producer. ECHILD is expected for our own fd.
        fd = os.pidfd_open(os.getpid())
        try:
            try:
                os.waitid(os.P_PIDFD, fd, WAIT)
            except ChildProcessError:
                pass
            else:
                raise ValueError("unexpected self wait")
        finally:
            os.close(fd)
        try:
            os.waitid(os.P_ALL, 0, WAIT)
        except ChildProcessError:
            pass
        else:
            raise ValueError("inherited children are not allowed")
        self.capacity()  # Pre-lock admission; no producer or resource ownership yet.
        require(self.save(), "receipt unavailable")
        self.lock = open(self.args.lock, "r")  # Existing coordinator-owned inode only.
        lock_stat = os.fstat(self.lock.fileno())
        while True:
            self.tick()
            if self.receipt["interrupted"] or self.receipt["error"]:
                return self.exit_code()
            try:
                fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                time.sleep(POLL)
        self.receipt["lock_held"] = True
        try:
            current = os.stat(self.args.lock)
            require((current.st_dev, current.st_ino) == (lock_stat.st_dev, lock_stat.st_ino),
                    "lifecycle lock inode changed")
            self.capacity()  # Capacity may have changed while waiting.
            require(self.save(), "receipt unavailable")
            self.docker.identity()
            require(not any(self.docker.inventory().values()), "session already exists")
            self.capacity()  # Heavy-phase admission.
            self.tick()
            if not self.receipt["interrupted"] and not self.receipt["error"]:
                self.supervise(command)
            assert self.receipt["quiescent"], "cleanup before producer quiescence"
            self.receipt["state"] = "cleanup"
            while not self.save():
                time.sleep(HOLD_POLL)
            if not self.receipt["error"]:
                self.docker.cleanup()
        except (OSError, ValueError, http.client.HTTPException) as exc:
            self.error("qualification", exc)
        assert self.receipt["quiescent"], "ownership must survive incomplete supervision"
        if self.receipt["error"]:
            while True:
                self.tick()
                try:
                    self.docker.identity()
                    empty = not any(self.docker.inventory().values())
                except (OSError, ValueError, http.client.HTTPException) as exc:
                    self.error("reconciliation", exc)
                    empty = False
                if empty and self.save():
                    break
                time.sleep(HOLD_POLL)
        # Never leave the lock scope by exception on a query error or deadline.
        self.receipt.update(state="complete", lock_held=False)
        while not self.save():
            self.receipt["lock_held"] = True
            time.sleep(HOLD_POLL)
            self.receipt["lock_held"] = False
        fcntl.flock(self.lock, fcntl.LOCK_UN)
        return self.exit_code()

    def exit_code(self):
        if self.receipt["error"]:
            return 70
        if self.receipt["interrupted"]:
            return 128 + self.receipt["interrupted"]
        if self.receipt["timed_out"]:
            return 124
        native = self.receipt["native_status"]
        return 0 if native is None else native if native >= 0 else 128 - native


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("lock", "docker-socket", "daemon-id", "backing-directory"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--headroom-bytes", type=int, required=True)
    parser.add_argument("--timeout", type=float, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    runner = None
    try:
        require(command and args.headroom_bytes > 0 and math.isfinite(args.timeout) and args.timeout > 0,
                "command, positive headroom, and finite positive timeout required")
        runner = Qualification(args)
        return runner.run(command)
    except (OSError, ValueError) as exc:
        if runner is not None:
            runner.error("admission", exc)
            runner.save()
        print("qualification refused: " + type(exc).__name__, file=sys.stderr)
        return 70
    finally:
        if runner is not None:
            assert not runner.receipt["lock_held"], "cannot unwind owned lifecycle lock"
            if runner.lock is not None:
                runner.lock.close()
            os.close(runner.control)
            runner.control_path.unlink()


if __name__ == "__main__":
    sys.exit(main())
