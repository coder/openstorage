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
if [ "$1 $2" = "buildx version" ]; then echo "github.com/docker/buildx v0.0.0 stand-in"; exit 0; fi
[ "$1 $2" = "buildx imagetools" ] || { echo "unexpected docker call: $*" >&2; exit 97; }
case $3 in
inspect)
  ref=$4
  if [[ $ref == *@* ]]; then key=${ref##*@}; else key=tag-${ref##*:}; fi
  [ -f "$w/inspect/$key" ] || { echo "ERROR: $ref: not found" >&2; exit 1; }
  cat "$w/inspect/$key" ;;
create)
  if [[ " $* " == *" --dry-run "* ]]; then
    [ -f "$w/plan" ] || { echo "ERROR: dry run failed" >&2; exit 1; }
    cat "$w/plan"
  else
    echo create >> "$w/calls"
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
    return {"mediaType": OCI_INDEX, "digest": digest, "size": 1, "manifests": list(children)}


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
        "plan": index(PUBLISHED, *a, *b),
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
        """Run `step` in a fresh directory. Returns (returncode, create calls, output file, summary file)."""
        work = Path(tempfile.mkdtemp(dir=self.root))
        (work / "digest").mkdir()
        (work / "digest" / "amd64").write_text(scenario["digests"][0] + "\n", encoding="utf-8")
        (work / "digest" / "arm64").write_text(scenario["digests"][1] + "\n", encoding="utf-8")
        (work / "world" / "inspect").mkdir(parents=True)
        for key, doc in scenario["inspect"].items():
            (work / "world" / "inspect" / key).write_text(json.dumps(doc), encoding="utf-8")
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
        calls = work / "world" / "calls"
        creates = len(calls.read_text().splitlines()) if calls.exists() else 0
        return done, creates, read_if_any(work / "output"), read_if_any(work / "summary")

    def promote(self, name, edit=lambda w: None, created=False):
        scenario = world()
        edit(scenario)
        done, creates, output, _ = self.run_step("promote", scenario)
        detail = f"{name}: rc={done.returncode} creates={creates}\n{done.stderr}"
        if created:
            self.assertEqual((done.returncode, creates), (0, 1), detail)
            self.assertIn(f"digest={PUBLISHED}", output, detail)
        else:
            self.assertNotEqual(done.returncode, 0, detail)
            self.assertEqual(creates, 0, detail)

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
            w["inspect"][AMD], w["inspect"][ARM], w["plan"] = index(AMD), index(ARM), index(PUBLISHED)
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
            w["plan"] = index(PUBLISHED)
        self.promote("plan empty", edit)

    def test_promote_refuses_when_the_dry_run_fails(self):
        def edit(w):
            w["plan"] = None
        self.promote("dry run fails", edit)

    def test_promote_refuses_a_digest_that_is_a_platform_manifest_and_not_an_index(self):
        def edit(w):
            w["digests"][1] = B_IMG
            w["inspect"][B_IMG] = plain_manifest(B_IMG)
            w["plan"] = index(PUBLISHED, child(A_IMG, "amd64"), child(A_ATT, None), child(B_IMG, "arm64"))
        self.promote("one platform manifest", edit)

    def test_promote_refuses_when_both_digests_are_platform_manifests(self):
        def edit(w):
            w["digests"] = [A_IMG, B_IMG]
            w["inspect"] = {A_IMG: plain_manifest(A_IMG), B_IMG: plain_manifest(B_IMG), "tag-9.9.9": index(PUBLISHED)}
            w["plan"] = index(PUBLISHED, child(A_IMG, "amd64"), child(B_IMG, "arm64"))
        self.promote("both platform manifests", edit)

    def test_promote_refuses_a_digest_that_the_registry_lacks(self):
        def edit(w):
            w["digests"][1] = sha(0)
            w["plan"] = None
        self.promote("missing digest", edit)

    def test_promote_refuses_equal_digests(self):
        def edit(w):
            w["digests"][1] = AMD
        self.promote("equal digests", edit)

    def test_promote_refuses_a_malformed_digest(self):
        def edit(w):
            w["digests"][0] = "latest"
        self.promote("malformed digest", edit)

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
