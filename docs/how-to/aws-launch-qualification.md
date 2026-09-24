# Run artifact and read-only AWS preflight

**This is preflight tooling, not release qualification.** The
[runner](../../scripts/aws_launch_qualification.py) checks a local executable's
checksum and reported commit. With explicit opt-in, it also requests AWS STS
identity and an expected-owner `HeadBucket` check. It does not exercise a server.

Live smoke and real-AWS qualification are deferred under
[issue #222](https://github.com/coder/openstorage/issues/222), which remains open.
The live mutating `--smoke` path is not available. It returns only with work on
an account and campaign explicitly authorized by the user. Neither local tests
nor a successful read-only AWS preflight satisfy #222.

## 1. Prepare the local artifact and configuration

Requirements: Python 3.11+ on POSIX and a locally runnable, trusted `openstorage`
executable. The AWS CLI is needed only for `--aws-readonly`. Running preflight
needs no Go installation, Git, Git LFS, Python packages, or signing key.

Supply a JSON file with these fields:

```json
{
  "schema_version": 1,
  "candidate_commit": "<full 40-character lowercase source SHA>",
  "binary": "/private/rc/openstorage",
  "binary_sha256": "<64-character lowercase SHA256 of that executable>"
}
```

Use an independently verified checksum, not the archive checksum. The runner
copies the executable to a private temporary directory, hashes that copy, and
runs its `version` command only if the checksum matches. Paths are relative to
the invocation directory unless absolute. Unknown or duplicate fields fail;
old fields such as `endpoint`, `topology`, and `private_key_file` are not accepted.

The checksum and reported commit are consistency checks, not build provenance.
Use a trusted binary: preflight executes it and does not sandbox it. Verify
clean-source provenance and installed artifacts separately. The result always
has `clean_source_verified`, `remote_artifact_verified`, and `launch_qualified`
set to `false`.

## 2. Run offline preflight

```sh
python3 scripts/aws_launch_qualification.py \
  --config /private/run.json --output /private/evidence/preflight
```

The output directory must be new and its parent must exist. The runner creates
it with mode 0700 and writes `result.json` and `runner.log` with mode 0600. It
records fixed diagnostic codes, not raw child output or configuration paths.
A zero exit means only the requested checks passed. Unrequested AWS checks and
deferred qualification work are recorded as skips.

`--budget-seconds` accepts 1–900 and defaults to 300. Each child command gets at
most 30 seconds or the remaining shared budget, whichever is smaller. Child
stdout is limited to 1 MiB. Timeout or excess output fails the check. The budget
does not interrupt local file copying or hashing, and is not a whole-process or
cloud-spending limit. Forced termination can prevent evidence from being written.

## 3. Opt into read-only AWS checks only when authorized

Do not infer permission from credentials being available. First obtain approval
for the account, region, existing bucket, expected owner, and credential chain.
Add an `aws` object to the JSON:

```json
"aws": {
  "region": "<approved region>",
  "bucket": "<approved existing bucket>",
  "expected_bucket_owner": "<12-digit account ID>"
}
```

Use the approved AWS CLI credential chain, including shared configuration or SSO.
Do not put credentials in the JSON or command arguments. Then run:

```sh
python3 scripts/aws_launch_qualification.py \
  --config /private/run.json --output /private/evidence/aws-preflight --aws-readonly
```

This requests only `aws sts get-caller-identity` and `aws s3api head-bucket` with
`--expected-bucket-owner`. Both use the configured region; a failed command
returns a nonzero runner exit. The AWS CLI inherits the caller's configuration,
including endpoint overrides and credential helpers. Trust and verify that
configuration; the runner does not attest which endpoint answered.

These calls do not prove runtime read/write permissions, wrong-owner denial,
credential expiry or refresh, presigned transfers, deployment topology, or
real-S3 recovery. No bucket or repository is created by these checks.

## 4. Rehearse locally and preserve evidence

Use a clean committed checkout and keep rehearsal files outside it. For a local
build, use the repository's pinned Go version:

```sh
mise install
export REHEARSAL=$(mktemp -d "$HOME/openstorage-preflight.XXXXXX")
GOROOT=$(mise where go)
export GOROOT
"$GOROOT/bin/go" build -o "$REHEARSAL/openstorage" ./cmd/openstorage
"$REHEARSAL/openstorage" version
git rev-parse HEAD
sha256sum "$REHEARSAL/openstorage"
python3 -m unittest discover -s scripts -p test_aws_launch_qualification.py -v
```

Use those local values in the JSON, omit `aws`, and run section 2. Check for two
passed scenarios, six skips, no failures, and `launch_qualified: false`. A wrong
checksum must fail before execution; `--smoke` must be rejected. Unit tests use
local fixtures for AWS calls; they do not establish AWS permissions or behavior.
Capture a screenshot **and** video of the actual invocation and results, plus
the two evidence files. Review them for secrets before publication. Use `gh`
2.99+ `--attach` for images and video; never record environment or credential dumps.

The separate #222 campaign still needs approved in-region deployment, exact
candidate provenance, live API/Git/LFS coverage, AWS credential and IAM negative
cases, recovery evidence, and published deployment limits. The
[AWS EC2 reference](deploy-aws-terraform.md) is a separate deployment path, not
this runner's input contract. S3WAL promotion remains
[#192](https://github.com/coder/openstorage/issues/192)-owned. Keep #222 open until
its full acceptance criteria are met; this guide does not authorize cloud work.
