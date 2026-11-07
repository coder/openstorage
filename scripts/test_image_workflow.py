"""Runs the publish job's promote and check steps of image.yml the way an Actions runner does.

A step with no `shell:` runs as `bash -e {0}`, which has no pipefail, and under it
`[ -n "$x" ] && [ "$x" = "$y" ]` does not stop when its first test is false. The steps run
here with that shell, against a stand-in `docker` that serves canned `imagetools` output.
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
PROMOTE = "Give the checked digests the version tag"
CHECK = "Check that the index holds exactly the checked digests"

STAND_IN_DOCKER = """#!/bin/bash
w=$WORLD
echo "$*" >> "$w/log"
if [ "$1 $2" = "buildx version" ]; then echo "github.com/docker/buildx v0.0.0 stand-in"; exit 0; fi
[ "$1 $2" = "buildx imagetools" ] || { echo "unexpected docker call: $*" >&2; exit 97; }
case $3 in
inspect)
  ref=$4
  [ "$5 $6" = "--format {{json .Manifest}}" ] || { echo "inspect without --format {{json .Manifest}}: $*" >&2; exit 98; }
  if [[ $ref == *@* ]]; then key=${ref##*@}; else key=tag-${ref##*:}; fi
  [ -f "$w/inspect/$key" ] || { echo "ERROR: $ref: not found" >&2; exit 1; }
  cat "$w/inspect/$key" ;;
create)
  if [[ " $* " == *" --dry-run "* ]]; then
    echo "$*" > "$w/dry"
    [ -f "$w/plan" ] || { echo "ERROR: dry run failed" >&2; exit 1; }
    cat "$w/plan"
  else
    echo "$*" >> "$w/calls"
  fi ;;
*) echo "unexpected docker call: $*" >&2; exit 97 ;;
esac
"""


def run_text(text, name):
    """The `run: |` block of the one step called `name`, as the runner writes it to a file."""
    lines = text.splitlines()
    heads = [i for i, line in enumerate(lines) if line.strip() == f"- name: {name}"]
    assert len(heads) == 1, f"step {name!r} found {len(heads)} times"

    def indent(line):
        return len(line) - len(line.lstrip())

    step_indent = indent(lines[heads[0]])
    body = []
    run_indent = None
    for line in lines[heads[0] + 1:]:
        if line.strip() and indent(line) <= step_indent:
            break
        if run_indent is None:
            if line.strip() == "run: |":
                run_indent = indent(line)
            continue
        if line.strip() and indent(line) <= run_indent:
            break
        body.append(line)
    assert run_indent is not None and any(line.strip() for line in body), f"step {name!r} has no run block"
    margin = run_indent + 2
    assert all(indent(line) >= margin for line in body if line.strip()), f"step {name!r}: uneven indent"
    return "\n".join(line[margin:] for line in body) + "\n"


def read_if_any(path):
    return path.read_text(encoding="utf-8") if path.exists() else ""


def sha(n):
    return "sha256:" + f"{n:x}".rjust(64, "0")


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
        "inspect": {AMD: index(AMD, *a), ARM: index(ARM, *b), "tag-9.9.9": index(PUBLISHED),
                    PUBLISHED: index(PUBLISHED, *a, *b)},
        "plan": plan_index(*a, *b),
    }


class ImageWorkflowStepsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        text = WORKFLOW.read_text(encoding="utf-8")
        # The steps run as `bash -e` only while nothing in the file sets a shell.
        assert not re.search(r"^\s*(shell|defaults):", text, re.M), "image.yml sets a shell: run the steps with it"
        assert "${{" not in run_text(text, PROMOTE) + run_text(text, CHECK), "a run block holds an expression"
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

    def run_step(self, step, scenario, digest=None):
        """Run `step` in a fresh directory. Returns the process, the docker calls, the real `create` calls,
        the dry-run call, and the contents of GITHUB_OUTPUT and GITHUB_STEP_SUMMARY."""
        work = Path(tempfile.mkdtemp(dir=self.root))
        (work / "digest").mkdir()
        (work / "digest" / "amd64").write_text(scenario["digests"][0] + "\n", encoding="utf-8")
        (work / "digest" / "arm64").write_text(scenario["digests"][1] + "\n", encoding="utf-8")
        (work / "world" / "inspect").mkdir(parents=True)
        for key, doc in scenario["inspect"].items():
            text = doc if isinstance(doc, str) else json.dumps(doc)
            (work / "world" / "inspect" / key).write_text(text, encoding="utf-8")
        if scenario["plan"] is not None:
            (work / "world" / "plan").write_text(json.dumps(scenario["plan"]), encoding="utf-8")
        env = {
            "PATH": f"{self.root / 'bin'}{os.pathsep}{os.environ['PATH']}",
            "WORLD": str(work / "world"),
            "IMAGE": "ghcr.io/coder/openstorage", "VERSION": "9.9.9", "TAG": "v9.9.9",
            "COMMIT": "0" * 40, "GITHUB_REPOSITORY": "coder/openstorage",
            "GITHUB_OUTPUT": str(work / "output"), "GITHUB_STEP_SUMMARY": str(work / "summary"),
            "DIGEST": digest or PUBLISHED,
        }
        done = subprocess.run(["bash", "-e", str(self.root / f"{step}.sh")], cwd=work, env=env,
                              capture_output=True, text=True, timeout=60, check=False)
        world_dir = work / "world"
        return (done, read_if_any(world_dir / "log").splitlines(), read_if_any(world_dir / "calls").splitlines(),
                read_if_any(world_dir / "dry").strip(), read_if_any(work / "output"), read_if_any(work / "summary"))

    def promote(self, name, edit=lambda w: None, created=False, calls_docker=True):
        scenario = world()
        edit(scenario)
        done, log, creates, dry, output, _ = self.run_step("promote", scenario)
        detail = f"{name}: rc={done.returncode} creates={len(creates)}\n{done.stderr}"
        if created:
            self.assertEqual((done.returncode, len(creates)), (0, 1), detail)
            self.assertIn(f"digest={PUBLISHED}", output, detail)
            # The index that is pushed is the one the dry run showed.
            self.assertEqual(creates[0], dry.replace(" --dry-run", "", 1), detail)
        else:
            self.assertNotEqual(done.returncode, 0, detail)
            self.assertEqual(creates, [], detail)
            self.assertNotIn("digest=", output, detail)
        if not calls_docker:
            self.assertEqual(log, [], detail)
        return done, creates

    def check(self, name, edit=lambda w: None, passes=False):
        scenario = world()
        edit(scenario)
        done, _, _, _, _, summary = self.run_step("check", scenario)
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
            w["inspect"][AMD], w["inspect"][ARM], w["plan"] = index(AMD), index(ARM), plan_index()
        self.promote("both empty", edit)

    def test_promote_refuses_when_the_plan_drops_a_manifest(self):
        def edit(w):
            del w["plan"]["manifests"][1]
        self.promote("plan drops one", edit)

    def test_promote_refuses_when_the_plan_adds_a_manifest(self):
        def edit(w):
            w["plan"]["manifests"].append(child(sha(0x77), "amd64"))
        self.promote("plan adds one", edit)

    def test_promote_refuses_when_the_plan_is_empty(self):
        def edit(w):
            w["plan"] = plan_index()
        self.promote("plan empty", edit)

    def test_promote_refuses_when_the_dry_run_fails(self):
        def edit(w):
            w["plan"] = None
        self.promote("dry run fails", edit)

    def test_promote_refuses_a_digest_that_is_a_platform_manifest_and_not_an_index(self):
        def edit(w):
            w["digests"][1] = B_IMG
            w["inspect"][B_IMG] = plain_manifest(B_IMG)
            w["plan"] = plan_index(child(A_IMG, "amd64"), child(A_ATT, None), child(B_IMG, "arm64"))
        self.promote("one platform manifest", edit)

    def test_promote_refuses_when_both_digests_are_platform_manifests(self):
        def edit(w):
            w["digests"] = [A_IMG, B_IMG]
            w["inspect"] = {A_IMG: plain_manifest(A_IMG), B_IMG: plain_manifest(B_IMG), "tag-9.9.9": index(PUBLISHED)}
            w["plan"] = plan_index(child(A_IMG, "amd64"), child(B_IMG, "arm64"))
        self.promote("both platform manifests", edit)

    def test_promote_refuses_a_digest_that_the_registry_lacks(self):
        def edit(w):
            w["digests"][1] = sha(0)
            w["plan"] = None
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
        def edit(w):
            del w["inspect"]["tag-9.9.9"]
        # `create` has run by then, so the tag exists and the step must not report a digest.
        self.run_readback_failure("tag missing", edit)

    def test_promote_fails_after_the_push_when_the_tag_read_back_has_no_digest(self):
        def edit(w):
            w["inspect"]["tag-9.9.9"] = {"schemaVersion": 2, "mediaType": OCI_INDEX}
        self.run_readback_failure("no digest", edit)

    def run_readback_failure(self, name, edit):
        scenario = world()
        edit(scenario)
        done, _, creates, _, output, _ = self.run_step("promote", scenario)
        detail = f"{name}: rc={done.returncode} creates={len(creates)}\n{done.stderr}"
        self.assertNotEqual(done.returncode, 0, detail)
        self.assertEqual(len(creates), 1, detail)
        self.assertNotIn("digest=", output, detail)

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


class RunTextTest(unittest.TestCase):
    TEXT = "jobs:\n  a:\n    steps:\n      - name: one\n        run: |\n          echo 1\n\n          echo 2\n      - name: two\n        run: |\n          echo 3\n"

    def test_reads_a_block_up_to_the_next_step(self):
        self.assertEqual(run_text(self.TEXT, "one"), "echo 1\n\necho 2\n")

    def test_reads_the_last_block(self):
        self.assertEqual(run_text(self.TEXT, "two"), "echo 3\n")

    def test_refuses_a_missing_step(self):
        with self.assertRaises(AssertionError):
            run_text(self.TEXT, "three")


if __name__ == "__main__":
    unittest.main()
