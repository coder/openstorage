# Prepare and run AWS launch acceptance

**Status: readiness tooling, not release qualification.**
[Issue #222](https://github.com/coder/openstorage/issues/222) requires fresh,
in-region AWS evidence for an exact release candidate. The August 27 campaign
and emulator lanes are regression evidence, not current release proof.

The [runner](../../scripts/aws_launch_qualification.py) checks a pinned local
binary and exercises an **existing, trusted scratch deployment** through its
public API, Git, and Git LFS interfaces. It does not provision infrastructure,
restart servers, change IAM, schedule jobs, publish releases, or approve launch.
Its result always has `launch_qualified: false`.

> [!IMPORTANT]
> **Having this runner in the repository is not real-AWS qualification.** It has
> run only against offline unit-test fixtures and a local loopback server. It has
> not run against AWS. #222 stays open until an approved in-region campaign
> produces the evidence in [section 5](#5-complete-the-separate-cloud-gate).

## 1. Establish the deployment and authorization boundary

Use the [AWS EC2 Terraform reference](deploy-aws-terraform.md) owned by
[issue #224](https://github.com/coder/openstorage/issues/224), including its
normal bootstrap, authentication, and teardown path. Do not create a separate
acceptance-only Terraform stack. That reference deploys one PostgreSQL + S3
node, and its smoke adapter emits a `pg-s3-single-node` configuration with a
`ca_file`. **This runner rejects that configuration.** It accepts only `local`,
two-replica `pg-s3`, and `s3wal`. Supporting the reference is separate #222 work;
never label that node `local`, list one server twice, or disable TLS to get past
the check. Until a compatible deployment and runner exist, use the local
rehearsal below; do not substitute local success for AWS qualification.

Before anyone provisions resources, record and approve these concrete inputs:

| Prerequisite | Required decision or evidence |
|---|---|
| Sandbox | Dedicated non-production account or explicitly isolated sandbox; no customer data. Unique run ID and resource tags. |
| Region and compute | EC2 runner, bucket, and servers in one approved region; record instance identity document and bucket region. No off-region laptop campaign. |
| Standard topology | Two independently addressable PG+S3 replicas sharing the same Postgres database and bucket, plus the advertised public endpoint. |
| Experimental topology | Separate single-node S3WAL deployment and scratch bucket; retain the instance lease. Never describe this as promoted support. |
| Buckets | Exact approved names and expected owner for dedicated empty PG blob and S3WAL buckets; versioning must never have been enabled and Object Lock must be disabled. No customer data. |
| Candidate | Full 40-character source SHA, immutable archive checksum or image digest, extracted binary checksum, build provenance, and installed checksum/digest on **every** node. |
| Access | Short-lived client/test credentials and renewable instance-role credentials. No administrator role for the runtime. Client JWT private key stays off servers and out of Terraform state. |
| Budget | Approved monetary ceiling, instance sizes/count, maximum cloud session length, accountable teardown operator, and an independent deadline/kill procedure. The runner's 300-second default bounds only the smoke process, not AWS billing. |
| Teardown | Explicit treatment of bucket objects, multipart uploads, database storage/backups, volumes, load balancers, network resources, IAM resources, and Terraform state. Verify deletion/retention individually. |

A practical approval request is one sandbox, one region, two standard-mode
replicas and one separate experimental node, a co-located EC2 runner, and a
single bounded session. The parent supplies the actual region, role, approved
runtime, and spending ceiling before apply. Do not infer authorization from
credentials merely being available.

Separate provisioning permissions from runtime permissions. For the runtime,
start with the [documented bucket permissions](deploy-production.md#backing-services):
`s3:ListBucket` and `s3:GetBucketVersioning` on the dedicated bucket;
`s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, and
`s3:AbortMultipartUpload` on its objects. Constrain access with
`aws:ResourceAccount`; configure `OPENSTORAGE_S3_EXPECTED_BUCKET_OWNER` too.
The reference provisioning role creates the buckets; the runner never calls
`CreateBucket`. **The serve role for these pre-existing scratch buckets must
explicitly deny `s3:CreateBucket`.** Production `serve` calls `blob.EnsureBucket`,
which attempts `CreateBucket` before falling back from `AccessDenied` to
`HeadBucket`. A successful runner `HeadBucket` preflight does not prevent that
startup attempt. Verify the effective serve-role policy before starting either
mode; reject broad provisioning credentials rather than relying on preflight
or changing startup behavior for the measurement.

Use separate, narrowly scoped test sessions for denied-read/write/delete cases;
do not mutate a shared production role. Versioned or version-suspended buckets
are unsuitable for the existing external test suites' deletion-based cleanup.

## 2. Supply the reference deployment contract

The runner consumes a small JSON file, not raw Terraform state. A #224
adapter should select only the following non-secret outputs and locally held
client inputs. Output names in Terraform need not match these JSON keys.

```json
{
  "schema_version": 1,
  "candidate_commit": "<full lowercase source SHA>",
  "binary": "/private/rc/openstorage",
  "binary_sha256": "<SHA256 of that executable, not the archive>",
  "topology": "pg-s3",
  "endpoint": "https://public.example.test",
  "replica_urls": ["https://replica-a.example.test", "https://replica-b.example.test"],
  "org": "acceptance",
  "private_key_file": "/private/client/org.pem",
  "aws": {
    "region": "<approved region>",
    "bucket": "<dedicated scratch bucket>",
    "expected_bucket_owner": "<12-digit account ID>"
  }
}
```

1. `endpoint` is the advertised origin; replica URLs directly select different
   servers, not two names for the same load balancer. They must have trusted TLS
   certificates. URLs have no credentials, path, trailing slash, query, or
   fragment. Local rehearsal alone permits loopback HTTP.
2. `pg-s3` requires exactly two distinct replica origins. `s3wal` and `local`
   require an empty `replica_urls` array. Declaring a topology does not prove the
   backing stores or physical replica identity.
3. Generate the organization key with `openstorage keygen`; deploy only its
   public half through #224. The private key must be a regular mode-0600 file.
   Paths are relative to the invocation directory unless absolute. Do not put
   tokens, AWS keys, private PEM contents, or database URLs in this JSON.
4. Pin the binary checksum independently of the downloaded file, and verify its
   release provenance. A checksum and the binary's reported commit are consistency
   checks, not an attestation that the source was clean or the remote servers run
   that binary. `clean_source_verified` and `remote_artifact_verified` remain
   false in this slice. The deployment owner must collect node-specific evidence.
5. Keep `terraform version`, provider lockfile/revision, sanitized input choices,
   topology, endpoint-to-instance mapping, artifact provenance, region checks,
   and teardown evidence alongside the runner result. Do not publish raw state,
   cloud-init output, credentials, or presigned URLs.

## 3. Run preflight, then opt into the smoke

Requirements: Python 3.11+ on POSIX; a locally runnable candidate binary;
Git and Git LFS for smoke. The AWS CLI is needed only for `--aws-readonly`.
No Python packages or Go installation are needed to use the runner.

```sh
# Offline by default: no endpoint request, repository mutation, or AWS call.
python3 scripts/aws_launch_qualification.py \
  --config /private/run.json --output /private/evidence/preflight

# Explicit read-only AWS opt-in: STS identity and expected-owner HeadBucket.
# Use the approved short-lived credential chain; never put keys in arguments.
python3 scripts/aws_launch_qualification.py \
  --config /private/run.json --output /private/evidence/aws-preflight --aws-readonly

# After preflight and approval of scratch-repository writes on this deployment:
python3 scripts/aws_launch_qualification.py \
  --config /private/run.json --output /private/evidence/smoke --smoke
```

Each output directory must be new. It is private (0700), with private (0600)
`result.json` and `runner.log` files. The default budget is 300 seconds;
`--budget-seconds` accepts 1–900. Remote cleanup has a separate bounded budget.
Requested checks failing, timing out, or failing cleanup return nonzero. A
zero exit means only the requested readiness checks passed, **not** launch
acceptance. Unrequested and unimplemented checks remain explicit skips.

The smoke creates its own unique repository. It checks health, authorization
denials, API commit → Git clone, Git push → API read, branch/tag handling,
and a real Git LFS round-trip. The standard-mode path also exercises direct
replica reads. It uses the supplied signing key to mint short-lived JWTs.
Raw child output, tokens, and response bodies are not copied into evidence.
API/Git redirects are disabled; Git LFS retains its own redirect policy.
Use only trusted scratch endpoints, not arbitrary third-party repositories.

Cleanup soft-deletes only a repository whose creation was acknowledged. A
conflict never authorizes deletion of an existing repository. An ambiguous
creation or failed deletion leaves an explicit operator cleanup obligation.
Even successful soft deletion **retains physical objects and metadata**;
it is not bucket cleanup or Terraform teardown. SIGKILL, host loss, and power
loss cannot run a process's cleanup handler: inspect the recorded scratch name
and deployment inventory after an interrupted campaign.

## 4. Rehearse locally

This verifies the runner and product interfaces on SQLite + file blobs. It
cannot verify AWS IAM, session credentials, presigning, or two-replica storage.
Use a clean committed checkout; keep all scratch state outside it.

```sh
mise install
export REHEARSAL=$(mktemp -d "$HOME/openstorage-acceptance.XXXXXX")
chmod 700 "$REHEARSAL"
mise exec -- go build -o "$REHEARSAL/openstorage" ./cmd/openstorage
"$REHEARSAL/openstorage" keygen --out "$REHEARSAL/org"
# In a dedicated terminal; stop this process with Ctrl-C after the smoke.
OPENSTORAGE_ORG=acceptance OPENSTORAGE_ORG_PUBKEYS="$REHEARSAL/org.pub.pem" \
  OPENSTORAGE_BASE_URL=http://127.0.0.1:18080 \
  "$REHEARSAL/openstorage" serve --listen 127.0.0.1:18080 \
  --data-dir "$REHEARSAL/data"
```

In another terminal, create the JSON from section 2 with `topology: "local"`,
`endpoint: "http://127.0.0.1:18080"`, `replica_urls: []`, and no `aws` field.
Use `git rev-parse HEAD` for the candidate commit, `sha256sum` of the built
executable, and the paths printed/selected above. Run offline preflight first,
then `--smoke`. Assert `status` is passed, no scenario failed, cleanup is
`soft_deleted`, and `launch_qualified` is false. Stop the owned server before
removing the private rehearsal directory; retain only sanitized evidence.

Capture a screenshot **and** video of the actual invocation and result, plus
`runner.log` and `result.json`. Review evidence for secrets before publication.
Use `gh` 2.99+ `--attach` for images/video on the implementation PR. Never
record a terminal printing environment values, mint output, or Terraform state.

## 5. Complete the separate cloud gate

The fixed result inventory makes missing evidence visible; this slice does not
provide commands that fault or control cloud infrastructure. The campaign owner
must add and verify these evidence sets on the **same pinned candidate**:

| Required evidence | Acceptance oracle |
|---|---|
| Deployment | In-region EC2 identity, both standard replicas, separate experimental node, artifact/provenance match on every node. |
| Temporary credentials | Valid temporary session succeeds; invalid key/secret, absent/forged/expired session tokens fail. Record safe error classes, never credential values. |
| Refresh | The running service survives real role-session refresh and still reads acknowledged Git/LFS data; initial temporary-key success alone is insufficient. |
| IAM and owner | Each deliberately denied operation fails; mismatched expected owner fails. A successful HeadBucket is not proof of all runtime owner checks. |
| Presigned transfers | Valid GET/PUT works; tampered signature fails and rejected PUT leaves no accepted bytes. |
| Contention/recovery | Concurrent writes have consistent acknowledged refs; cold-cache restart, interrupted push, and recovery retain every acknowledged commit and LFS object. |
| S3WAL | Existing external CAS qualification plus lease/restart drills on real S3; record failed, skipped, and record-only surfaces, not just a green exit. |
| Cleanup | Reference teardown succeeds; resource and data inventory confirms deletion or explicitly approved retention. |

The [external blob tests](../reference/testing.md#the-floci-s3-lane) and
`TestQualifyExternalEndpoint` in `internal/ref/s3wal` already accept real S3.
They mutate scratch keys and require separate execution authorization. Run them
from the pinned source and preserve the full case census; package-test success
is not evidence that the deployed binary was tested. The existing githttp
`TestMain` builds its own binaries and backends, so do not relabel its emulator
lease/restart runs as deployed-RC AWS tests.

Coordinate shared session evidence with
[#192](https://github.com/coder/openstorage/issues/192) (S3WAL promotion) and
[#182](https://github.com/coder/openstorage/issues/182) (performance), without
changing their criteria. The #182 packet prepares three paired, sequential
single-writer runs: 20 warmups and 60 measured pushes per lane per pair (480
pushes plus six seed pushes and setup/final reads). Approve that traffic and
its separate diagnostic traffic within the shared request/time/spend limits.
Keep this comparison separate from two-replica acceptance. Local performance
windows are not AWS proof; re-pin the eventual candidate and rerun applicable
gates before cloud execution. Starvation, separate-day repetition, and support
promotion remain #192-owned.

EKS is a follow-up unless the launch claims Kubernetes support; such a claim
requires workload identity, ingress/TLS, replacement, and rollout evidence.
Keep #222 open until the cloud criteria, publication of
sanitized evidence and deployment limits, and verified teardown are complete.
