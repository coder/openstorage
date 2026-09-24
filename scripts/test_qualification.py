"""Harmless Linux adapter tests: python3 -B -m unittest discover -s scripts.

All servers, locks, resources, and commands are private fakes. No Docker access.
"""
import ctypes
import fcntl
import http.server
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import socketserver
import subprocess
import sys
import tempfile
import time
import unittest
from urllib.parse import parse_qs, urlsplit

HERE = Path(__file__).resolve()
LABEL = "org.testcontainers.sessionId"
KINDS = ("containers", "networks", "volumes")
IDS = dict(containers="a" * 64, networks="b" * 64, volumes="private-volume")


def write(path, value):
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(value))
    temp.replace(path)


def server(root):
    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            self.respond()

        def do_DELETE(self):
            self.respond()

        def respond(self):
            parsed = urlsplit(self.path)
            parts = parsed.path.strip("/").split("/")
            mode = (root / "mode").read_text() if (root / "mode").exists() else ""
            if mode == "outage" and (root / "ready").exists():
                self.send_error(503)
                return
            if mode == "deadline" and (root / "ready").exists():
                time.sleep(0.7)
            receipt = json.loads((root / "artifacts/qualification.json").read_text())
            query = parse_qs(parsed.query)
            row = dict(method=self.command, path=parsed.path, query=query,
                       quiescent=receipt["quiescent"], lock_held=receipt["lock_held"])
            with (root / "http.jsonl").open("a") as out:
                out.write(json.dumps(row) + "\n")
            if parts == ["info"]:
                response = dict(ID="private-daemon", DockerRootDir=str(root))
            else:
                kind = parts[0]
                assert kind in KINDS, self.path
                labels = {LABEL: "wrong" if mode == "identity" else "private-session"}
                resource = dict(Name=IDS[kind], Labels=labels) if kind == "volumes" else dict(Id=IDS[kind], Labels=labels)
                # "vanish" models a competing cleaner, such as Ryuk or container
                # auto-removal: it wins the container inspection and network
                # DELETE races, then finishes its sweep before any later inventory.
                if self.command == "DELETE":
                    assert receipt["quiescent"] and receipt["lock_held"], row
                    assert query.get("v") is None, "implicit volume deletion"
                    (root / ("removed-" + kind)).touch()
                    if mode == "vanish":
                        self.send_error(404)
                        return
                    if mode == "partial":
                        (root / "mode").write_text("outage")
                    response = {}
                elif query:
                    assert json.loads(query["filters"][0]) == {"label": [LABEL + "=private-session"]}
                    if mode == "vanish" and (root / "removed-containers").exists():
                        for other in KINDS:
                            (root / ("removed-" + other)).touch()
                    present = (root / "ready").exists() and not (root / ("removed-" + kind)).exists()
                    response = [resource] if present else []
                    if kind == "volumes":
                        response = dict(Volumes=response, Warnings=[])
                elif mode == "vanish" and kind != "networks":
                    (root / ("removed-" + kind)).touch()
                    self.send_error(404)
                    return
                else:
                    response = resource
                    if kind == "containers":
                        response = dict(Id=IDS[kind], Config=dict(Labels=labels))
            data = json.dumps(response).encode()
            try:
                self.send_response(200)
                self.send_header("Content-Length", str(len(data) + (5 if mode == "truncated" else 0)))
                self.end_headers()
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass

    with socketserver.UnixStreamServer(str(root / "docker.sock"), Handler) as listener:
        (root / "server-ready").touch()
        listener.serve_forever()


def fixture(root, case):
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.alarm(10)
    r, w = os.pipe()
    middle = os.fork()
    if middle == 0:
        os.setpgid(0, 0)
        signal.alarm(10)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        leaf = os.fork()
        if leaf == 0:
            os.setsid()
            signal.alarm(10)
            os.write(w, b"L")
        else:
            os.write(w, b"M")
            if case == "reparented":
                os._exit(0)
        while True:
            signal.pause()
    ready = b""
    while len(ready) < 2:
        ready += os.read(r, 2 - len(ready))
    write(root / "ready", dict(session=os.environ.get("TESTCONTAINERS_SESSION_ID"),
                              artifacts=os.environ.get("OPENSTORAGE_TEST_ARTIFACT_DIR")))
    while not (root / "go").exists():
        time.sleep(0.005)
    if case in ("zero", "seven", "reparented", "deadline-exit", "late-signal"):
        os._exit(7 if case == "seven" else 0)
    while True:
        signal.pause()


def adapter(root, case):
    spec = importlib.util.spec_from_file_location("qualification", HERE.with_name("qualification.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.GRACE = 0.05
    module.POLL = 0.005
    module.HOLD_POLL = 0.05
    module.CAPACITY_INTERVAL = 0.05
    module.QUERY_TIMEOUT = 0.2
    # A deterministic filesystem, not a weakened production threshold. Reserved
    # f_bfree is deliberately enormous; only f_bavail may authorize admission.
    available = 1 if case == "capacity" else 100_000_000_000
    original = os.statvfs(root)
    def capacity(path):
        dropped = case == "capacity-drop" and (root / "ready").exists() and not (root / "capacity-recovered").exists()
        return os.statvfs_result((1, 1, 1_000_000_000_000, 900_000_000_000,
                                 1 if dropped else available, *tuple(original)[5:]))
    os.statvfs = capacity
    actual_wait, actual_signal, actual_close = os.waitid, signal.pidfd_send_signal, os.close
    authorized, reaped = set(), set()
    def trace(event, **fields):
        with (root / "native.jsonl").open("a") as out:
            out.write(json.dumps(dict(event=event, **fields)) + "\n")
    def wait(kind, ident, options):
        assert kind in (os.P_ALL, os.P_PIDFD), "numeric wait target"
        assert options & 0x40000000, "missing complete Linux child wait set"
        assert len(list(Path("/proc/self/task").iterdir())) == 1
        enabled = ctypes.c_int()
        assert ctypes.CDLL(None).prctl(37, ctypes.byref(enabled), 0, 0, 0) == 0 and enabled.value == 1
        try:
            result = actual_wait(kind, ident, options)
        except ChildProcessError:
            trace("echild", kind=kind)
            raise
        if kind == os.P_PIDFD:
            authorized.add(ident)
        if result is not None and not options & os.WNOWAIT:
            reaped.add(ident)
            trace("reap", kind=kind, fd=ident, native=result.si_status)
        return result
    def send(fd, sig, *args):
        assert fd in authorized and fd not in reaped, "signal without live kernel parenthood"
        trace("signal", fd=fd, signal=sig)
        return actual_signal(fd, sig, *args)
    def close(fd):
        authorized.discard(fd)
        reaped.discard(fd)
        return actual_close(fd)
    def forbidden(*args, **kwargs):
        raise AssertionError("numeric signaling is forbidden")
    os.waitid, signal.pidfd_send_signal, os.close = wait, send, close
    os.kill = os.killpg = forbidden
    if case == "missing-children":
        actual_read = Path.read_text
        def read(path, *args, **kwargs):
            if str(path) == f"/proc/self/task/{os.getpid()}/children":
                raise FileNotFoundError("child list unavailable")
            return actual_read(path, *args, **kwargs)
        def unexpected_launch(*args, **kwargs):
            (root / "launch-attempt").touch()
            raise OSError("producer reached without child discovery")
        Path.read_text = read
        subprocess.Popen = unexpected_launch  # Fail safely if admission is broken.
    elif case == "deadline-exit":
        actual_sleep = time.sleep
        paused = False
        def sleep(seconds):
            nonlocal paused
            if not paused:
                paused = True
                # At the first poll boundary, let the real leader exit without
                # reaping it, then force the next poll beyond the deadline.
                children = Path(f"/proc/self/task/{os.getpid()}/children").read_text().split()
                assert len(children) == 1
                fd = os.pidfd_open(int(children[0]))
                try:
                    deadline = time.monotonic() + 2
                    while (status := actual_wait(os.P_PIDFD, fd, module.WAIT)) is None:
                        assert time.monotonic() < deadline, "leader exit barrier"
                        actual_sleep(0.001)
                    assert status.si_code == os.CLD_EXITED and status.si_status == 0
                    trace("waitable-before-deadline-check", native=status.si_status)
                finally:
                    actual_close(fd)
                actual_sleep(0.35)
            else:
                actual_sleep(seconds)
        time.sleep = sleep
    elif case == "late-signal":
        actual_flock = fcntl.flock
        def flock(lock, operation):
            if operation == fcntl.LOCK_UN:
                # The final receipt is durable. Deliver a handled signal before
                # the unlock and exit-status selection.
                final = json.loads((root / "artifacts/qualification.json").read_text())
                assert final["state"] == "complete" and not final["interrupted"], final
                (root / "late-signal").touch()
                signal.raise_signal(signal.SIGTERM)
            return actual_flock(lock, operation)
        fcntl.flock = flock
    timeout = "0.3" if case in ("timeout", "deadline-exit") else "3"
    sys.exit(module.main(["--lock", str(root / "lock"), "--docker-socket", str(root / "docker.sock"),
                          "--daemon-id", "private-daemon", "--backing-directory", str(root),
                          "--headroom-bytes", "1000", "--timeout", timeout, "--",
                          sys.executable, str(HERE), "fixture", str(root), case]))


@unittest.skipUnless(sys.platform == "linux", "requires Linux pidfds and child subreaper")
class QualificationTests(unittest.TestCase):
    def setUp(self):
        self.assertEqual(ctypes.CDLL(None).prctl(36, 1, 0, 0, 0), 0)
        self.temp = tempfile.TemporaryDirectory(prefix="q282-")
        self.root = Path(self.temp.name)
        (self.root / "artifacts").mkdir(mode=0o700)
        (self.root / "home").mkdir(mode=0o700)
        (self.root / "lock").touch()
        self.children = []
        self.addCleanup(self.cleanup)
        self.start("server", str(self.root))
        self.until(lambda: (self.root / "server-ready").exists())
        self.sentinel = self.start("sentinel")

    def start(self, *args, session="private-session"):
        env = {k: v for k, v in os.environ.items() if not k.startswith(("DOCKER_", "TESTCONTAINERS_"))}
        env.update(HOME=str(self.root / "home"), TESTCONTAINERS_SESSION_ID=session,
                   OPENSTORAGE_TEST_ARTIFACT_DIR=str(self.root / "artifacts"))
        log = (self.root / (str(len(self.children)) + ".log")).open("w+")
        proc = subprocess.Popen([sys.executable, "-B", str(HERE), *args], env=env, stdout=log, stderr=log)
        self.children.append((proc, os.pidfd_open(proc.pid), log))
        return proc

    def until(self, predicate, timeout=5, proc=None):
        end = time.monotonic() + timeout
        while not predicate():
            if proc is not None and proc.poll() is not None:
                self.fail("adapter exited before checkpoint: " + self.logs())
            self.assertLess(time.monotonic(), end, self.logs())
            time.sleep(0.01)

    def logs(self):
        return "\n".join(p.read_text()[-2000:] for p in self.root.glob("*.log"))

    def receipt(self):
        try:
            return json.loads((self.root / "artifacts/qualification.json").read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            return {}  # The exclusive first receipt can still be initializing.

    def locked(self):
        with (self.root / "lock").open() as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            return False

    def cleanup(self):
        for proc, fd, log in reversed(self.children):
            if proc.poll() is None:
                signal.pidfd_send_signal(fd, signal.SIGKILL)
            proc.wait(timeout=2)
            os.close(fd)
            log.close()
        # Finite alarms bound failed-test fixtures; they reparent here, not PID1.
        rescued = []
        end = time.monotonic() + 11
        while True:
            try:
                status = os.waitid(os.P_ALL, 0, os.WEXITED | os.WNOHANG | 0x40000000)
            except ChildProcessError:
                break
            if status:
                rescued.append(status.si_pid)
            self.assertLess(time.monotonic(), end, "fixture fallback reap deadline")
            time.sleep(0.01)
        self.temp.cleanup()
        self.assertEqual(rescued, [], "adapter did not reap all fixtures")

    def run_case(self, case, expected):
        proc = self.start("adapter", str(self.root), case)
        self.until(lambda: (self.root / "ready").exists(), proc=proc)
        self.assertTrue(self.locked())
        self.assertEqual(json.loads((self.root / "ready").read_text()),
                         dict(session="private-session", artifacts=str(self.root / "artifacts")))
        (self.root / "go").touch()
        if case.startswith("direct-"):
            fd = next(fd for child, fd, log in self.children if child is proc)
            signal.pidfd_send_signal(fd, signal.SIGINT if case == "direct-int" else signal.SIGTERM)
        elif case in ("term", "int"):
            fd = os.open(self.root / "artifacts/qualification.control", os.O_WRONLY | os.O_NONBLOCK)
            try:
                os.write(fd, b"I" if case == "int" else b"T")
            finally:
                os.close(fd)
        self.assertEqual(proc.wait(timeout=5), expected, str(self.receipt()) + self.logs())
        receipt = self.receipt()
        self.assertTrue(receipt["quiescent"])
        self.assertEqual(receipt["state"], "complete")
        self.assertGreaterEqual(receipt["capacity_samples"], 3)
        self.assertEqual(receipt["capacity_min_available"], 100_000_000_000)
        self.assertFalse(receipt["lock_held"])
        self.assertFalse(self.locked())
        self.assertIsNone(self.sentinel.poll())
        native = -2 if case.endswith("int") else -15 if case in ("term", "direct-term", "timeout") else expected
        self.assertEqual(receipt["native_status"], native)
        events = [json.loads(line) for line in (self.root / "native.jsonl").read_text().splitlines()]
        self.assertTrue(any(e["event"] == "signal" and e["signal"] == signal.SIGKILL for e in events))
        self.assertEqual(events[-1]["event"], "echild")
        requests = [json.loads(line) for line in (self.root / "http.jsonl").read_text().splitlines()]
        deletes = [r for r in requests if r["method"] == "DELETE"]
        self.assertEqual(len(deletes), 3)
        self.assertTrue(all(r["quiescent"] and r["lock_held"] for r in deletes))

    def test_native_zero_secondary_groups_and_resistance(self):
        self.run_case("zero", 0)

    def test_native_nonzero(self):
        self.run_case("seven", 7)

    def test_reparented(self):
        self.run_case("reparented", 0)

    def test_term(self):
        self.run_case("term", 143)

    def test_int(self):
        self.run_case("int", 130)

    def test_timeout(self):
        self.run_case("timeout", 124)

    def test_waitable_leader_is_reaped_before_timeout(self):
        self.run_case("deadline-exit", 0)
        self.assertFalse(self.receipt()["timed_out"])
        events = [json.loads(line) for line in (self.root / "native.jsonl").read_text().splitlines()]
        self.assertIn(dict(event="waitable-before-deadline-check", native=0), events)

    def test_late_signal_cannot_contradict_final_receipt(self):
        self.run_case("late-signal", 0)
        self.assertTrue((self.root / "late-signal").exists())
        self.assertEqual(self.receipt()["interrupted"], 0)

    def test_direct_term(self):
        self.run_case("direct-term", 143)

    def test_direct_int(self):
        self.run_case("direct-int", 130)

    def test_lock_contention_starts_no_producer(self):
        with (self.root / "lock").open() as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            proc = self.start("adapter", str(self.root), "zero")
            self.until(lambda: bool(self.receipt()), proc=proc)
            self.assertFalse((self.root / "ready").exists())
            self.assertFalse(self.receipt()["lock_held"])
        self.until(lambda: (self.root / "ready").exists(), proc=proc)
        (self.root / "go").touch()
        self.assertEqual(proc.wait(timeout=5), 0, self.logs())
        self.assertFalse(self.locked())

    def test_query_outage_retains_lock_until_scoped_empty(self):
        self.hold_case("outage")

    def test_identity_mismatch_retains_lock(self):
        self.hold_case("identity")

    def test_query_absolute_deadline_retains_lock(self):
        self.hold_case("deadline")

    def test_partial_cleanup_outage_does_not_retry_deletion(self):
        self.hold_case("partial")

    def test_truncated_inventory_is_not_success(self):
        self.hold_case("truncated")

    def test_periodic_capacity_failure_stops_producer_before_hold(self):
        self.hold_case("capacity-drop")

    def test_concurrent_removal_is_not_cleanup_failure(self):
        proc = self.start("adapter", str(self.root), "zero")
        self.until(lambda: (self.root / "ready").exists(), proc=proc)
        (self.root / "mode").write_text("vanish")
        (self.root / "go").touch()
        self.assertEqual(proc.wait(timeout=5), 0, str(self.receipt()) + self.logs())
        receipt = self.receipt()
        self.assertEqual((receipt["error"], receipt["state"], receipt["native_status"]), ("", "complete", 0))
        self.assertTrue(receipt["quiescent"])
        self.assertFalse(receipt["lock_held"])
        self.assertFalse(self.locked())
        self.assertIsNone(self.sentinel.poll())
        requests = [json.loads(line) for line in (self.root / "http.jsonl").read_text().splitlines()]
        # A 404 inspection never authorizes deletion; the one DELETE lost its race.
        deletes = [r for r in requests if r["method"] == "DELETE"]
        self.assertEqual([r["path"] for r in deletes], ["/networks/" + IDS["networks"]])
        self.assertTrue(all(r["quiescent"] and r["lock_held"] for r in deletes))
        # Success rests on the final scoped inventory, not on the 404 responses.
        final = requests[-3:]
        self.assertEqual([r["path"] for r in final], ["/containers/json", "/networks", "/volumes"])
        self.assertTrue(all(r["method"] == "GET" and "filters" in r["query"] for r in final))

    def test_fixture_incompatible_session_refused_before_lock(self):
        # The existing fixture capture rejects these; admission must refuse them.
        for session in ("dot.session", "a" * 65, "a" * 128):
            with self.subTest(session=session):
                proc = self.start("adapter", str(self.root), "zero", session=session)
                self.assertEqual(proc.wait(timeout=5), 70, self.logs())
                self.assertFalse((self.root / "artifacts/qualification.json").exists())
                self.assertFalse((self.root / "http.jsonl").exists())
                self.assertFalse((self.root / "ready").exists())
                self.assertFalse(self.locked())
        self.assertIsNone(self.sentinel.poll())

    def hold_case(self, mode):
        proc = self.start("adapter", str(self.root), "capacity-drop" if mode == "capacity-drop" else "zero")
        self.until(lambda: (self.root / "ready").exists(), proc=proc)
        (self.root / "mode").write_text(mode)
        (self.root / "go").touch()
        self.until(lambda: self.receipt().get("state") == "hold" and self.receipt().get("quiescent"), proc=proc)
        self.assertTrue(self.locked())
        self.assertTrue(self.receipt()["quiescent"])
        self.assertIsNone(proc.poll())
        if mode == "partial":
            self.assertTrue((self.root / "removed-containers").exists())
            time.sleep(0.15)  # Several HOLD iterations; never retry mutations.
            self.assertFalse((self.root / "removed-networks").exists())
            self.assertFalse((self.root / "removed-volumes").exists())
        # External fake-resource cleanup is distinct from automatic deletion.
        (self.root / "capacity-recovered").touch()
        for kind in KINDS:
            (self.root / ("removed-" + kind)).touch()
        (self.root / "mode").write_text("")
        self.assertEqual(proc.wait(timeout=5), 70, self.logs())
        self.assertFalse(self.locked())
        self.assertTrue(self.receipt()["error"])
        self.assertIsNone(self.sentinel.poll())

    def test_missing_child_discovery_refuses_before_lock_or_launch(self):
        proc = self.start("adapter", str(self.root), "missing-children")
        self.assertEqual(proc.wait(timeout=5), 70, self.logs())
        self.assertFalse((self.root / "launch-attempt").exists(), self.logs())
        self.assertFalse((self.root / "http.jsonl").exists())
        receipt = self.receipt()
        self.assertIsNone(receipt["native_status"])
        self.assertTrue(receipt["quiescent"])
        self.assertFalse(receipt["lock_held"])
        self.assertFalse(self.locked())
        self.assertIsNone(self.sentinel.poll())

    def test_reserved_blocks_cannot_authorize_admission(self):
        proc = self.start("adapter", str(self.root), "capacity")
        self.assertNotEqual(proc.wait(timeout=5), 0, self.logs())
        self.assertFalse((self.root / "ready").exists())
        self.assertFalse(self.locked())


class SessionContractTests(unittest.TestCase):
    def test_session_contract_matches_fixture_capture(self):
        spec = importlib.util.spec_from_file_location("qualification", HERE.with_name("qualification.py"))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        go = (HERE.parents[1] / "internal/testutil/seaweedcapture/capture.go").read_text()
        match = re.search(r"sessionPattern\s*=\s*regexp\.MustCompile\(`\^(.*)\$`\)", go)
        self.assertIsNotNone(match, "fixture session contract not found")
        self.assertEqual(module.SESSION_PATTERN, match.group(1))


if __name__ == "__main__":
    if len(sys.argv) == 1:
        unittest.main()
    elif sys.argv[1] == "server":
        server(Path(sys.argv[2]))
    elif sys.argv[1] == "fixture":
        fixture(Path(sys.argv[2]), sys.argv[3])
    elif sys.argv[1] == "adapter":
        adapter(Path(sys.argv[2]), sys.argv[3])
    elif sys.argv[1] == "sentinel":
        signal.alarm(60)
        while True:
            signal.pause()
