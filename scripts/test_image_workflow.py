"""Runs the publish job's promote and check steps of image.yml the way an Actions runner does.

A step with no `shell:` runs as `bash -e {0}`, which has no pipefail, and under it
`[ -n "$x" ] && [ "$x" = "$y" ]` does not stop when its first test is false. The steps run
here with that shell, against a stand-in `docker` that models `buildx imagetools inspect`
and `create` over a small registry: it builds the dry-run plan and the new tag from the
arguments the step passes, so a wrong source, a missing `--tag` or a split argument shows.
It does not read annotation values.
"""
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import tempfile
import unittest

WORKFLOW = Path(__file__).resolve().parent.parent / ".github" / "workflows" / "image.yml"
REFUSE = "Refuse to replace an existing version tag"
PROMOTE = "Give the checked digests the version tag"
CHECK = "Check that the index holds exactly the checked digests"

STAND_IN_DOCKER = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import re
import sys

world = Path(os.environ["WORLD"])
args = sys.argv[1:]
state = json.loads((world / "state.json").read_text())
with open(world / "log", "a") as log:
    log.write(json.dumps(args) + "\n")


def fail(code, message):
    print(message, file=sys.stderr)
    sys.exit(code)


def registry_key(ref):
    """The registry holds one repository, $IMAGE. A reference to another repository never matches."""
    repository, key = ref.rsplit("@", 1) if "@" in ref else ref.rsplit(":", 1)
    key = key if "@" in ref else "tag-" + key
    return key if repository == os.environ["IMAGE"] else f"{repository}|{key}"


def lookup(ref):
    doc = state["inspect"].get(registry_key(ref))
    if doc is None:
        fail(1, f"ERROR: {ref}: not found")
    return doc


if args[:2] == ["buildx", "version"]:
    print("github.com/docker/buildx v0.0.0 stand-in")
    sys.exit(0)
if args[:2] != ["buildx", "imagetools"]:
    fail(97, f"unexpected docker call: {args}")
verb, rest = args[2], args[3:]
if verb == "inspect":
    if rest[1:] != ["--format", "{{json .Manifest}}"]:
        fail(98, f"inspect without --format '{{{{json .Manifest}}}}': {args}")
    doc = lookup(rest[0])
    print(doc if isinstance(doc, str) else json.dumps(doc))
    sys.exit(0)
if verb != "create":
    fail(97, f"unexpected docker call: {args}")
dry, tags, sources = False, [], []
i = 0
while i < len(rest):
    if rest[i] == "--dry-run":
        dry = True
    elif rest[i] in ("--tag", "--annotation"):
        i += 1
        if rest[i - 1] == "--tag":
            tags.append(rest[i])
    elif rest[i].startswith("-"):
        fail(97, f"unexpected create option: {rest[i]}")
    elif re.fullmatch(r"\S+@sha256:[0-9a-f]{64}", rest[i]):
        sources.append(rest[i])
    else:
        fail(1, f"ERROR: invalid reference format: {rest[i]}")
    i += 1
if not sources or (not dry and not tags):
    fail(1, "ERROR: create needs sources and, unless --dry-run, a tag")
children = []
for source in sources:
    doc = lookup(source)
    if not isinstance(doc, dict):
        fail(1, f"ERROR: {source}: not a manifest")
    found = doc["manifests"] if "manifests" in doc else [
        {"mediaType": doc["mediaType"], "digest": doc["digest"], "size": doc["size"]}]
    children += [c for c in found if c["digest"] not in {d["digest"] for d in children}]
plan = {"schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json", "manifests": children}
if dry:
    override = state["plan_override"]
    if override == "fail":
        fail(1, "ERROR: dry run failed")
    print(json.dumps(plan if override is None else override))
    sys.exit(0)
mode = state["create"]
if mode != "no read-back":
    made = dict(plan, size=1, digest=state["published"])
    if mode == "no digest":
        del made["digest"]
    for tag in tags:
        state["inspect"][registry_key(tag)] = made
    (world / "state.json").write_text(json.dumps(state))
'''


def block_lines(text, matches, what):
    """The lines from the one line that `matches` to the next line that is not indented deeper."""
    lines = text.splitlines()
    heads = [i for i, line in enumerate(lines) if matches(line)]
    assert len(heads) == 1, f"{what} found {len(heads)} times"
    margin = indent(lines[heads[0]])
    end = next((j for j in range(heads[0] + 1, len(lines)) if lines[j].strip() and indent(lines[j]) <= margin),
               len(lines))
    return lines[heads[0]:end]


def step_lines(text, name):
    return block_lines(text, lambda line: line.strip() == f"- name: {name}", f"step {name!r}")


def job_lines(text, name):
    return block_lines(text, lambda line: line == f"  {name}:", f"job {name!r}")


def indent(line):
    return len(line) - len(line.lstrip())


def run_text(text, name):
    """The `run: |` block of the step called `name`, as the runner writes it to a file."""
    lines = step_lines(text, name)
    starts = [i for i, line in enumerate(lines) if line.strip() == "run: |"]
    assert len(starts) == 1, f"step {name!r} has {len(starts)} run blocks"
    run_indent = indent(lines[starts[0]])
    rest = lines[starts[0] + 1:]
    body = rest[:next((j for j, line in enumerate(rest) if line.strip() and indent(line) <= run_indent), len(rest))]
    assert any(line.strip() for line in body), f"step {name!r} has an empty run block"
    margin = run_indent + 2
    assert all(indent(line) >= margin for line in body if line.strip()), f"step {name!r}: uneven indent"
    return "\n".join(line[margin:] for line in body) + "\n"


def read_if_any(path):
    return path.read_text(encoding="utf-8") if path.exists() else ""


def sha(n):
    return "sha256:" + f"{n:x}".rjust(64, "0")


VERSION = "9.9.9"
AMD, ARM = sha(1), sha(2)
A_IMG, A_ATT, B_IMG, B_ATT = sha(0x11), sha(0x12), sha(0x21), sha(0x22)
PUBLISHED = sha(0x99)
OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_INDEX = "application/vnd.oci.image.index.v1+json"


def child(digest, arch):
    """An index entry. `unknown/unknown` is how buildx lists an attestation manifest."""
    return {"mediaType": OCI_MANIFEST, "digest": digest, "size": 1,
            "platform": {"architecture": "unknown" if arch is None else arch,
                         "os": "unknown" if arch is None else "linux"}}


def index(digest, *children):
    """What `inspect --format '{{json .Manifest}}'` prints for an index."""
    return {"schemaVersion": 2, "mediaType": OCI_INDEX, "digest": digest, "size": 1, "manifests": list(children)}


def plan_index(*children):
    """What `create --dry-run` prints: the index that would be pushed, with no digest yet."""
    return {"schemaVersion": 2, "mediaType": OCI_INDEX, "manifests": list(children)}


def plain_manifest(digest):
    return {"mediaType": OCI_MANIFEST, "digest": digest, "size": 1}


def world():
    """The registry as the steps see it after `stage` pushed both platforms."""
    a = [child(A_IMG, "amd64"), child(A_ATT, None)]
    b = [child(B_IMG, "arm64"), child(B_ATT, None)]
    return {
        "digests": [AMD, ARM],
        "inspect": {AMD: index(AMD, *a), ARM: index(ARM, *b), PUBLISHED: index(PUBLISHED, *a, *b)},
        "plan_override": None,
        "create": "ok",
        "published": PUBLISHED,
    }


class ImageWorkflowStepsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        text = WORKFLOW.read_text(encoding="utf-8")
        # The steps run as `bash -e` only while no default shell is set and neither step sets one.
        assert not re.search(r"^\s*defaults:", text, re.M), "image.yml sets a default shell: run the steps with it"
        for name in (PROMOTE, CHECK):
            assert not re.search(r"^\s*shell:", "\n".join(step_lines(text, name)), re.M), f"step {name!r} sets a shell"
        assert "${{" not in run_text(text, PROMOTE) + run_text(text, CHECK), "a run block holds an expression"
        # This test sets the variables that the two steps read. The file must bind each of them once, to the same things.
        publish = [line.strip() for line in job_lines(text, "publish")]
        bound = [line for line in publish if re.match(r"(IMAGE|TAG|VERSION|COMMIT):", line)]
        assert sorted(bound) == ["COMMIT: ${{ needs.resolve.outputs.commit }}", "TAG: ${{ needs.resolve.outputs.tag }}",
                                 "VERSION: ${{ needs.resolve.outputs.version }}"], f"the publish job binds {bound}"
        # The tag may be created only after the existing-tag check, and the later jobs read the digest that this
        # job reports.
        order = [line.strip()[len("- name: "):] for line in job_lines(text, "publish") if line.strip().startswith("- name: ")]
        assert [n for n in order if n in (REFUSE, PROMOTE, CHECK)] == [REFUSE, PROMOTE, CHECK], f"publish steps run as {order}"
        assert '[ "$code" = 404 ]' in run_text(text, REFUSE), "the existing-tag check no longer requires a 404"
        assert [line.strip() for line in job_lines(text, "publish")].count("digest: ${{ steps.promote.outputs.digest }}") == 1, \
            "the publish job does not report the promote step's digest"
        assert [line.strip() for line in job_lines(text, "verify-published")].count("DIGEST: ${{ needs.publish.outputs.digest }}") == 1, \
            "verify-published does not read the publish job's digest"
        assert "id: promote" in [line.strip() for line in step_lines(text, PROMOTE)], "promote step lost its id"
        assert "DIGEST: ${{ steps.promote.outputs.digest }}" in [line.strip() for line in step_lines(text, CHECK)], \
            "check step does not read the promote step's digest output"
        image = re.findall(r"^env:\n  IMAGE: (\S+)$", text, re.M)
        assert len(image) == 1, "image.yml has no single top-level IMAGE"
        cls.image = image[0]
        assert shutil.which("jq"), "jq is required"
        cls.tmp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.tmp.cleanup)
        root = Path(cls.tmp.name)
        for name, key in (("promote", PROMOTE), ("check", CHECK)):
            (root / f"{name}.sh").write_text(run_text(text, key), encoding="utf-8")
        (root / "bin").mkdir()
        docker = root / "bin" / "docker"
        docker.write_text(STAND_IN_DOCKER, encoding="utf-8")
        docker.chmod(docker.stat().st_mode | stat.S_IXUSR)
        cls.root = root

    def run_step(self, step, scenario):
        """Run `step` in a fresh directory. Returns the process, the docker calls (one argument list each),
        and the contents of GITHUB_OUTPUT and GITHUB_STEP_SUMMARY."""
        work = Path(tempfile.mkdtemp(dir=self.root))
        (work / "digest").mkdir()
        (work / "digest" / "amd64").write_text(scenario["digests"][0] + "\n", encoding="utf-8")
        (work / "digest" / "arm64").write_text(scenario["digests"][1] + "\n", encoding="utf-8")
        (work / "world").mkdir()
        (work / "world" / "state.json").write_text(
            json.dumps({k: v for k, v in scenario.items() if k != "digests"}), encoding="utf-8")
        env = {
            "PATH": f"{self.root / 'bin'}{os.pathsep}{os.environ['PATH']}",
            "WORLD": str(work / "world"),
            "IMAGE": self.image, "VERSION": VERSION, "TAG": "v9.9.9",
            "COMMIT": "0" * 40, "GITHUB_REPOSITORY": "coder/openstorage",
            "GITHUB_OUTPUT": str(work / "output"), "GITHUB_STEP_SUMMARY": str(work / "summary"),
        }
        if step == "check":
            # What the workflow wires from the promote step's output.
            env["DIGEST"] = PUBLISHED
        done = subprocess.run(["bash", "-e", str(self.root / f"{step}.sh")], cwd=work, env=env,
                              capture_output=True, text=True, timeout=60, check=False)
        calls = [json.loads(line) for line in read_if_any(work / "world" / "log").splitlines()]
        return done, calls, read_if_any(work / "output"), read_if_any(work / "summary")

    @staticmethod
    def creates(calls):
        return [c for c in calls if c[2:3] == ["create"] and "--dry-run" not in c]

    def promote(self, name, edit=lambda w: None, created=False, calls_docker=True):
        scenario = world()
        edit(scenario)
        done, calls, output, _ = self.run_step("promote", scenario)
        creates = self.creates(calls)
        detail = f"{name}: rc={done.returncode} creates={len(creates)}\n{done.stderr}"
        if created:
            self.assertEqual((done.returncode, len(creates)), (0, 1), detail)
            self.assertIn(f"digest={PUBLISHED}", output, detail)
            # The index that is pushed is the one the dry run showed.
            dry = [c for c in calls if "--dry-run" in c]
            self.assertEqual(len(dry), 1, detail)
            self.assertEqual(creates[0], [a for a in dry[0] if a != "--dry-run"], detail)
            # It gets the one tag that the "Refuse to replace" step checked.
            tags = [creates[0][i + 1] for i, a in enumerate(creates[0]) if a == "--tag"]
            self.assertEqual(tags, [f"{self.image}:{VERSION}"], detail)
        else:
            self.assertNotEqual(done.returncode, 0, detail)
            self.assertEqual(creates, [], detail)
            self.assertNotIn("digest=", output, detail)
        if not calls_docker:
            self.assertEqual(calls, [], detail)

    def pushes_then_fails(self, name, edit):
        scenario = world()
        edit(scenario)
        done, calls, output, _ = self.run_step("promote", scenario)
        detail = f"{name}: rc={done.returncode}\n{done.stderr}"
        self.assertNotEqual(done.returncode, 0, detail)
        self.assertEqual(len(self.creates(calls)), 1, detail)
        self.assertNotIn("digest=", output, detail)

    def check(self, name, edit=lambda w: None, passes=False):
        scenario = world()
        edit(scenario)
        done, _, _, summary = self.run_step("check", scenario)
        detail = f"{name}: rc={done.returncode}\n{done.stderr}"
        if passes:
            self.assertEqual(done.returncode, 0, detail)
            self.assertIn(PUBLISHED, summary, detail)
        else:
            self.assertNotEqual(done.returncode, 0, detail)
            self.assertEqual(summary, "", detail)

    def test_promote_creates_the_tag_when_the_plan_holds_exactly_the_staged_manifests(self):
        self.promote("happy path", created=True)

    def test_promote_refuses_when_the_staged_inspect_calls_fail(self):
        def edit(w):
            del w["inspect"][AMD], w["inspect"][ARM]
        self.promote("staged inspect fails", edit)

    def test_promote_refuses_when_the_staged_list_and_the_plan_are_both_empty(self):
        def edit(w):
            w["inspect"][AMD], w["inspect"][ARM] = index(AMD), index(ARM)
        self.promote("both empty", edit)

    def test_promote_refuses_when_the_plan_drops_a_manifest(self):
        def edit(w):
            w["plan_override"] = plan_index(child(A_IMG, "amd64"), child(B_IMG, "arm64"), child(B_ATT, None))
        self.promote("plan drops one", edit)

    def test_promote_refuses_when_the_plan_adds_a_manifest(self):
        def edit(w):
            w["plan_override"] = plan_index(
                child(A_IMG, "amd64"), child(A_ATT, None), child(B_IMG, "arm64"), child(B_ATT, None),
                child(sha(0x77), "amd64"))
        self.promote("plan adds one", edit)

    def test_promote_refuses_when_the_plan_is_empty(self):
        def edit(w):
            w["plan_override"] = plan_index()
        self.promote("plan empty", edit)

    def test_promote_refuses_when_the_dry_run_fails(self):
        def edit(w):
            w["plan_override"] = "fail"
        self.promote("dry run fails", edit)

    def test_promote_refuses_a_digest_that_is_a_platform_manifest_and_not_an_index(self):
        def edit(w):
            w["digests"][1] = B_IMG
            w["inspect"][B_IMG] = plain_manifest(B_IMG)
        self.promote("one platform manifest", edit)

    def test_promote_refuses_when_both_digests_are_platform_manifests(self):
        def edit(w):
            w["digests"] = [A_IMG, B_IMG]
            w["inspect"] = {A_IMG: plain_manifest(A_IMG), B_IMG: plain_manifest(B_IMG)}
        self.promote("both platform manifests", edit)

    def test_promote_refuses_a_digest_that_the_registry_lacks(self):
        def edit(w):
            w["digests"][1] = sha(0)
        self.promote("missing digest", edit)

    def test_promote_refuses_equal_digests(self):
        def edit(w):
            w["digests"][1] = AMD
        self.promote("equal digests", edit, calls_docker=False)

    def test_promote_refuses_a_malformed_digest(self):
        def edit(w):
            w["digests"][0] = "latest"
        self.promote("malformed digest", edit, calls_docker=False)

    def test_promote_fails_after_the_push_when_the_new_tag_cannot_be_read_back(self):
        # `create` has run by then, so the tag exists and the step must not report a digest.
        def edit(w):
            w["create"] = "no read-back"
        self.pushes_then_fails("tag missing", edit)

    def test_promote_fails_after_the_push_when_the_tag_read_back_has_no_digest(self):
        def edit(w):
            w["create"] = "no digest"
        self.pushes_then_fails("no digest", edit)

    def test_check_passes_when_the_index_holds_exactly_the_staged_manifests(self):
        self.check("happy path", passes=True)

    def test_check_fails_when_the_staged_inspect_calls_fail(self):
        def edit(w):
            del w["inspect"][AMD], w["inspect"][ARM]
        self.check("staged inspect fails", edit)

    def test_check_fails_when_the_index_lacks_an_attestation_manifest(self):
        def edit(w):
            del w["inspect"][PUBLISHED]["manifests"][3]
        self.check("attestation missing", edit)

    def test_check_fails_when_the_index_holds_an_extra_manifest(self):
        def edit(w):
            w["inspect"][PUBLISHED]["manifests"].append(child(sha(0x77), None))
        self.check("extra manifest", edit)

    def test_check_fails_when_a_platform_is_missing(self):
        def edit(w):
            w["inspect"][PUBLISHED]["manifests"] = w["inspect"][PUBLISHED]["manifests"][:2]
        self.check("one platform", edit)

    def test_check_fails_when_the_digests_match_but_a_platform_is_mislabeled(self):
        def edit(w):
            w["inspect"][PUBLISHED]["manifests"][2]["platform"]["architecture"] = "amd64"
        self.check("arm64 labeled amd64", edit)


class RunTextTest(unittest.TestCase):
    TEXT = "jobs:\n  a:\n    steps:\n      - name: one\n        run: |\n          echo 1\n\n          echo 2\n      - name: two\n        run: |\n          echo 3\n"

    def test_reads_a_block_up_to_the_next_step(self):
        self.assertEqual(run_text(self.TEXT, "one"), "echo 1\n\necho 2\n")

    def test_stops_at_a_key_after_the_block(self):
        text = "      - name: one\n        run: |\n          echo 1\n        env:\n          A: b\n"
        self.assertEqual(run_text(text, "one"), "echo 1\n")

    def test_reads_the_last_block(self):
        self.assertEqual(run_text(self.TEXT, "two"), "echo 3\n")

    def test_refuses_a_missing_step(self):
        with self.assertRaises(AssertionError):
            run_text(self.TEXT, "three")


if __name__ == "__main__":
    unittest.main()
