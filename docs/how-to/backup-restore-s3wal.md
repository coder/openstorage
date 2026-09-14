# Back up and restore a stopped S3WAL node (experimental)

This is a **manual, offline, same-binary** recovery procedure for the
[one-bucket S3WAL topology](deploy-production.md#zero-infrastructure-mode-experimental-one-bucket-one-node).
It covers loss of the serving bucket using a separately protected copy, not just
replacement of the node. It does not make S3WAL supported or qualify AWS disaster
recovery. Promotion gates remain in [#192](https://github.com/coder/openstorage/issues/192).

Before changing binaries, read the
[S3WAL format stability policy](../explanation/storage-engine.md#s3wal-format-stability-policy).
A stopped-node backup is an upgrade precaution, not qualification of
cross-version rollback.

## Recovery contract

1. Stop and fence **every writer** for the entire backup window. Blocking HTTP
   ingress alone is insufficient: hooks, background jobs, direct S3 clients,
   outstanding presigned uploads, replication and lifecycle automation can still
   change objects. Stop the supervisor from restarting the node. Drain requests,
   send `SIGTERM`, wait for process exit and its hook children, and revoke or wait
   out external upload access. Confirm any in-flight storage operations have
   finished. If you cannot establish quiescence, do not call the copy consistent.
2. Copy **every object**, preserving its exact key, bytes and user metadata. Do not
   select only currently referenced packs. The bucket includes `_wal/meta.json`,
   repository manifests, permanent purge tombstones, scan state, packs, LFS bytes
   and sweep quarantine objects. Do not edit JSON or discard unfamiliar keys.
3. Use a new, empty destination for each snapshot and each restore. Never merge a
   backup into an existing serving bucket or overwrite the only good backup.
   Verify the complete key/body/metadata inventory before starting a node.
4. Keep a matching external configuration backup: binary/image digest, both
   storage URLs, region/endpoint, expected bucket owner, organization, public-key
   PEM files, and `OPENSTORAGE_SECRETS_KEY` if used. Store credentials and private
   signing keys separately in your secret manager. The node cache is disposable;
   it is not a backup of the authority.

A stopped source prevents interleaving copies of different committed states.
Normal publication uploads pack/LFS bytes before registering them in the
manifest; this does **not** establish that a previously damaged source is healthy.
Inventory equality proves copy fidelity, not application consistency or repair.
Clone/fsck/LFS checks below are a separate acceptance gate.

**Recovery point and time:** the recovery point is the verified stopped snapshot.
Changes accepted after the source resumes are absent from that snapshot. Downtime
includes draining, copying and verification; recovery time includes provisioning,
copying back, verification and cold-cache hydration. Measure these for your data
size. This small local drill establishes no fleet-wide RPO/RTO guarantee.

## Prepare a protected snapshot

Use a backup identity that can read the source, but whose backup destination
cannot be modified or deleted by the serving node's credentials. Protect the
inventory/configuration record independently too. A second bucket writable by a
compromised serving identity is not ransomware protection. Choose retention and
off-site protection outside the serving authority; this procedure does not test
cross-region disaster recovery.

The source and restored **serving** buckets must have versioning and object lock
disabled, as required by the [store contract](deploy-production.md#zero-infrastructure-mode-experimental-one-bucket-one-node).
Do not enable them to implement this procedure. A separately managed archival
system must export a complete ordinary-object snapshot into an empty compliant
serving bucket; archival policies and encryption keys need their own recovery
validation. Bucket policies, ACLs, lifecycle rules, KMS keys, tags and provider
settings are not reproduced by the object inventory below. Provision destination
permissions/encryption deliberately; never blindly restore source access policy.

The following Bash/AWS CLI v2 examples copy between pre-provisioned buckets on
**one endpoint in one region**. They are not a cross-endpoint migration tool.
Use independently scoped backup credentials (including KMS decrypt/encrypt rights
where applicable); configure `AWS_PROFILE` or the normal credential chain without
putting secrets in command arguments. For a local SeaweedFS drill set `ENDPOINT`
to its loopback URL. For AWS leave it empty. Do not run against production until
the quiescence and permission requirements above are satisfied.

```bash
set -euo pipefail
umask 077
export AWS_PAGER=''
export REGION='us-east-1' ENDPOINT='http://127.0.0.1:8333'
export SOURCE='source-bucket' BACKUP='new-snapshot-bucket' RESTORE='new-restore-bucket'
export SOURCE_OWNER='expected-account-id' BACKUP_OWNER='expected-account-id'
export RESTORE_OWNER='expected-account-id'
[[ "$SOURCE" != "$BACKUP" && "$SOURCE" != "$RESTORE" && "$BACKUP" != "$RESTORE" ]]
opts=(--region "$REGION")
if [[ -n "$ENDPOINT" ]]; then opts+=(--endpoint-url "$ENDPOINT"); fi
# For local emulators, use empty *_OWNER values if owner checks are unsupported.
# AWS: set real owner IDs; do not disable the checks.
```

Save this inventory helper as `inventory.py` in a restricted working directory
**outside** the bucket. It uses Python 3.11+ and the AWS CLI, paginates the whole
bucket, downloads one object at a time and computes SHA-256 locally. It needs
scratch disk for the largest object and memory for the inventory. Its output is
sensitive: keys and user metadata may disclose repository information. Do not
upload real inventories to issues or include credentials in transcripts.

```python
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
from urllib.parse import unquote

bucket = os.environ["BUCKET"]
options = ["--region", os.environ["REGION"]]
if os.environ.get("ENDPOINT"):
    options += ["--endpoint-url", os.environ["ENDPOINT"]]
owner = os.environ.get("EXPECTED_OWNER", "")
if owner:
    options += ["--expected-bucket-owner", owner]

def s3api(*args):
    result = subprocess.run(
        ["aws", "s3api", *args, "--bucket", bucket, *options, "--output", "json"],
        check=True, capture_output=True,
    )
    return json.loads(result.stdout or b"{}")

s3api("head-bucket")
listing = s3api("list-objects-v2", "--encoding-type", "url")
entries = {}
with tempfile.TemporaryDirectory() as scratch:
    body = Path(scratch) / "object"
    for item in listing.get("Contents", []):
        key = unquote(item["Key"])
        info = s3api("get-object", "--key", key, str(body))
        with body.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        entries[key] = {"bytes": body.stat().st_size, "sha256": digest,
                        "metadata": info.get("Metadata", {})}
        body.unlink()
print(json.dumps(entries, sort_keys=True, indent=2))
```

The CLI must be allowed to paginate (no `--no-paginate` or `--max-items`). A
failure aborts the helper; a failed or incomplete inventory is never accepted.
Do not replace body hashes with ETags: multipart/encrypted object ETags are not
portable content hashes.

With the source stopped/fenced and the destination inaccessible to other writers:

```bash
BUCKET="$SOURCE" EXPECTED_OWNER="$SOURCE_OWNER" python3 inventory.py > source-before.json
python3 -c 'import json, sys; "_wal/meta.json" in json.load(open("source-before.json")) or sys.exit("source has no metadata authority")'
BUCKET="$BACKUP" EXPECTED_OWNER="$BACKUP_OWNER" python3 inventory.py > backup-empty.json
python3 -c 'import json, sys; json.load(open("backup-empty.json")) == {} or sys.exit("backup is not empty")'
aws s3 cp "s3://$SOURCE/" "s3://$BACKUP/" --recursive --copy-props default "${opts[@]}"
BUCKET="$SOURCE" EXPECTED_OWNER="$SOURCE_OWNER" python3 inventory.py > source-after.json
BUCKET="$BACKUP" EXPECTED_OWNER="$BACKUP_OWNER" python3 inventory.py > backup.json
cmp source-before.json source-after.json
cmp source-before.json backup.json
```

`--copy-props default` preserves user metadata, including multipart copies; it
may require additional object-tagging permissions. The high-level `s3 cp` command
does not pin expected owners per request; use bucket-scoped permissions for both
sides. The inventory helper pins the owner on its AWS reads. Validate the copy
implementation on your endpoint, including large objects, before operational use.
The local fixture below does not exercise AWS CLI multipart copying or AWS IAM/KMS.

Record stop/copy/verify times, the binary digest, object count, total bytes and a
hash of `source-before.json` in the protected backup catalog. Retain the verified
inventory with the snapshot. A source mismatch invalidates the snapshot: find and
stop the writer, then take a new snapshot. Matching before/after inventories are
an additional check, **not a substitute for writer fencing**. Only after all gates
pass may the original node resume against its original bucket.

## Restore and validate before cutover

1. Keep the original node stopped/fenced during a real recovery cutover. Do not
   start two copies of the deployment. The instance lease only detects nodes
   sharing one bucket; it cannot detect two restored buckets. During a drill,
   isolate the restored node from users **and deny nonessential outbound
   traffic before boot**. Restored pending upstream-sync/forwarding markers can
   enqueue work at boot, and later writes can deliver restored webhooks.
2. Use the exact saved binary and configuration, changing both storage URLs and
   owner settings to the restored bucket and using an empty cache directory.
   This is not an upgrade/downgrade procedure. Do not use an older binary merely
   because the JSON says `v: 1`; cross-release compatibility is a separate #192
   gate. Keep the original backup immutable while investigating failed recovery.
3. Verify the backup still matches its saved inventory. Copy into the empty
   restore bucket, then verify again, **before any serve process starts**:

   ```bash
   BUCKET="$BACKUP" EXPECTED_OWNER="$BACKUP_OWNER" python3 inventory.py > backup-now.json
   cmp source-before.json backup-now.json
   BUCKET="$RESTORE" EXPECTED_OWNER="$RESTORE_OWNER" python3 inventory.py > restore-empty.json
   python3 -c 'import json, sys; json.load(open("restore-empty.json")) == {} or sys.exit("restore is not empty")'
   aws s3 cp "s3://$BACKUP/" "s3://$RESTORE/" --recursive --copy-props default "${opts[@]}"
   BUCKET="$RESTORE" EXPECTED_OWNER="$RESTORE_OWNER" python3 inventory.py > restored.json
   cmp source-before.json restored.json
   ```

4. A copied `_wal/instance-lease` can look fresh because copying resets its
   Last-Modified time. Prefer waiting at least the saved lease TTL **plus two
   seconds** after copying, allowing for whole-second S3 timestamp resolution.
   If startup still reports a held lease, keep the node fenced and retry after
   its expiry. If necessary, remove **only that key from the restore bucket**,
   only after confirming every old serving process is dead/fenced; record this
   intentional post-verification mutation. Never disable the instance lease for recovery.
   Do not rewrite embedded clocks, claims or scan cursors. New object modification
   times can delay age-based cleanup; copied leases/claims can delay work. Monitor
   lease refusals and clock-skew warnings using the [deployment guide](deploy-production.md#zero-infrastructure-mode-experimental-one-bucket-one-node).
5. Start one isolated restored node. Check authenticated repository metadata and
   expected refs; make fresh clones, run `git fsck --strict`, fetch/check LFS and
   compare payload bytes or SHA-256 with known-good expectations. Exercise a
   disposable repository's push and re-clone to check new CAS writes. `/healthz`
   alone is not recovery proof. A real incident requires checks across the affected
   repository inventory, not just the small fixture below.
6. Before admitting users or allowing external side effects, reconcile security
   changes since the snapshot: deleted/purged repository names, revoked keys,
   credentials and webhook/upstream settings may have been rolled back. Unexpired
   JWTs accepted by restored keys can work again. Preserve purge tombstones in the
   snapshot, but do not assume it contains revocations made later. After compromise,
   rotate credentials/keys and assess the backup's trustworthiness separately.
   Authorize a single cutover only after data and security checks pass.

## Reproduce the local recovery drill

Install the [contributor prerequisites](../../CONTRIBUTING.md#prerequisites):
pinned Go, Docker and git-lfs. From the repository root:

```bash
OPENSTORAGE_TEST_BACKEND=s3wal go test ./internal/githttp \
  -run '^TestS3WAL_OfflineBackupRestore$' -count=1 -race -timeout=5m -v
```

The drill uses separate source, backup and restore buckets on disposable
SeaweedFS. It stops the source, verifies whole-bucket copies (including an opaque
metadata-bearing object), rejects a damaged restore inventory before boot, removes
source data/cache, and checks fresh-cache Git/LFS recovery and a subsequent push.
The copier/verifier is test-only. Capture the stage log plus a terminal screenshot
and recording for review; use only generated test credentials/data.

This is **local fixture proof**, not evidence for AWS permissions, account/region
loss, retention enforcement, large-object transfer, hostile-writer fencing or
fleet-wide corruption recovery. It does not satisfy A1's conformant real-S3
starvation rerun, A2's separate-day repeats, A3's in-region campaign or A4/#182's
measurement refresh. If the conformant rerun again trips the recorded 30-second
starvation criterion, the kill/deprecation recommendation remains unchanged.
