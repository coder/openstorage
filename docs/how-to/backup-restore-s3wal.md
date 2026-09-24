# Back up and restore a stopped S3WAL node (experimental)

Snapshot a stopped
[one-bucket S3WAL node](deploy-production.md#zero-infrastructure-mode-experimental-one-bucket-one-node)
and restore it into a new bucket, for example after losing the serving bucket. The
procedure is **manual, offline, and same-binary**.

> [!IMPORTANT]
> S3WAL stays experimental; this does not qualify AWS disaster recovery (gates:
> [#192](https://github.com/coder/openstorage/issues/192)). A stopped-node backup is
> an upgrade precaution, not proof of cross-version rollback. Read the [S3WAL format stability policy](../explanation/storage-engine.md#s3wal-format-stability-policy)
> before changing binaries.

## Prerequisites

- Bash, AWS CLI v2, and Python 3.11+.
- Pre-provisioned, empty backup and restore buckets on the source's endpoint and
  region.
- Independently scoped backup credentials (see below).
- Authority to stop the node and every other writer.

## Recovery contract

1. **Stop and fence every writer** for the whole backup window. Blocking HTTP is
   not enough: hooks, jobs, direct S3 clients, presigned uploads, replication, and
   lifecycle automation can still write.
   - Stop the supervisor from restarting the node.
   - Drain requests, send `SIGTERM`, and wait for the process and hook children.
   - Revoke or wait out external upload access and in-flight storage operations.
   - Without proven quiescence, the copy is not consistent.
2. **Copy every object** with exact key, bytes, and user metadata, not just
   referenced packs: `_wal/meta.json`, manifests, permanent purge tombstones, scan
   state, packs, LFS bytes, sweep quarantine. Never edit JSON or drop unknown keys.
3. **Use a new, empty destination** for every snapshot and restore. Never merge into
   a serving bucket or overwrite the only good backup. Verify the full
   key/body/metadata inventory before starting a node.
4. **Back up the configuration too:** binary/image digest, both storage URLs,
   region/endpoint, expected bucket owner, organization, public-key PEMs, and
   `OPENSTORAGE_SECRETS_KEY` if used. Keep credentials and signing keys in your
   secret manager. The node cache is disposable, not a backup.

A stopped source prevents copying mixed committed states. Inventory equality proves
copy fidelity, not application consistency: a damaged source stays damaged. The
clone/fsck/LFS checks are a separate gate.

**Recovery point and time:** the recovery point is the verified snapshot. Downtime
covers draining, copying, and verifying; recovery time covers provisioning, copying
back, verifying, and cold-cache hydration. Measure both for your data; the local drill sets no
RPO/RTO.

## Prepare a protected snapshot

- **Backup identity:** can read the source; the serving node's credentials cannot
  modify or delete its destination. (A bucket a compromised serving identity can
  write is not ransomware protection.) Protect the inventory/config record too, and
  choose retention and off-site protection outside the serving authority.
- **Serving buckets** (source and restored) must have versioning and object lock
  disabled ([store contract](deploy-production.md#zero-infrastructure-mode-experimental-one-bucket-one-node)).
  Do not enable either to implement this procedure.
  An archival system must instead export a complete ordinary-object snapshot into
  an empty compliant bucket, with its own policy/key recovery validated.
- **Not reproduced by the object inventory:** bucket policies, ACLs, lifecycle rules,
  KMS keys, tags, and provider settings. Provision them deliberately; never blindly
  copy source access policy. (`--copy-props default` can copy object tags, but the
  inventory only checks keys, bodies, and metadata.)
- **Scope:** the Bash/AWS CLI v2 examples copy between pre-provisioned buckets on
  **one endpoint in one region**. No cross-endpoint migration or cross-region
  disaster recovery.
- **Credentials:** independently scoped (with KMS rights where needed), via
  `AWS_PROFILE` or the normal chain, never in arguments.
- **Endpoint:** loopback URL for a local SeaweedFS drill; empty for AWS.

Do not run against production until the quiescence and permission requirements are
met.

### 1. Set variables

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

### 2. Save the inventory helper

Save as `inventory.py` in a restricted directory **outside** the bucket. It needs
Python 3.11+, the AWS CLI, scratch disk for the largest object, and memory for the
inventory. It paginates the whole bucket and SHA-256-hashes one object at a time.

> [!WARNING]
> Inventories are sensitive: keys and metadata reveal repository details. Never
> upload real ones to issues or put credentials in transcripts.

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

Let the CLI paginate (no `--no-paginate` or `--max-items`). Any failure aborts;
never accept an incomplete inventory. Do not substitute ETags for body hashes:
multipart/encrypted ETags are not portable.

### 3. Copy and verify

With the source stopped/fenced and no other writer able to reach the destination:

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

**Verify:** both `cmp` commands succeed silently.

- `--copy-props default` keeps user metadata, also on multipart copies; it may need
  object-tagging permissions.
- `s3 cp` does not pin expected owners per request, so scope permissions per bucket.
  The inventory helper does pin owners.
- Validate copying on your endpoint, including large objects. The local drill does
  not exercise AWS CLI multipart copying or IAM/KMS.

### 4. Record and resume

> [!IMPORTANT]
> Matching before/after inventories are an extra check, **not a substitute for
> writer fencing**.

1. In the protected catalog, record stop/copy/verify times, binary digest, object
   count, total bytes, and a hash of `source-before.json`. Keep the inventory with
   the snapshot.
2. Source inventories differ? The snapshot is invalid: find and stop the writer,
   then start over.
3. Only after all checks pass may the original node resume on its bucket.

## Restore and validate before cutover

1. **Keep the original node stopped/fenced** during a real cutover. Never run two
   copies: the instance lease detects nodes sharing one bucket, not two buckets.
   In a drill, isolate the restored node from users **and deny nonessential
   outbound traffic before boot**: restored pending sync/forwarding markers can
   enqueue work at boot, and later writes can deliver restored webhooks.
2. **Use the exact saved binary and configuration**, with both storage URLs and owner
   settings pointed at the restore bucket and an empty cache directory. This is not
   an upgrade/downgrade: `v: 1` in the JSON does not make an older binary safe
   (cross-release compatibility is a #192 gate). Keep the backup immutable while
   investigating a failed recovery.
3. **Verify, copy, and verify again, before any serve process starts:**

   ```bash
   BUCKET="$BACKUP" EXPECTED_OWNER="$BACKUP_OWNER" python3 inventory.py > backup-now.json
   cmp source-before.json backup-now.json
   BUCKET="$RESTORE" EXPECTED_OWNER="$RESTORE_OWNER" python3 inventory.py > restore-empty.json
   python3 -c 'import json, sys; json.load(open("restore-empty.json")) == {} or sys.exit("restore is not empty")'
   aws s3 cp "s3://$BACKUP/" "s3://$RESTORE/" --recursive --copy-props default "${opts[@]}"
   BUCKET="$RESTORE" EXPECTED_OWNER="$RESTORE_OWNER" python3 inventory.py > restored.json
   cmp source-before.json restored.json
   ```

4. **Handle the copied instance lease.** Copying resets `_wal/instance-lease`'s
   Last-Modified time, so it can look fresh.
   - Wait at least the saved lease TTL **plus two seconds** (S3 timestamps have
     whole-second resolution).
   - Still held at startup? Keep the node fenced and retry after expiry.
   - Last resort: remove **only that key from the restore bucket**, after confirming
     every old serving process is dead/fenced. Record this mutation.

   **Never disable the instance lease for recovery.** Do not rewrite embedded
   clocks, claims, or scan cursors.

   New modification times can delay age-based cleanup; copied leases/claims can delay
   work. Monitor lease refusals and clock-skew warnings
   ([deployment guide](deploy-production.md#zero-infrastructure-mode-experimental-one-bucket-one-node)).
5. **Start one isolated restored node and validate data:**
   - authenticated repository metadata and expected refs;
   - fresh clones plus `git fsck --strict`;
   - LFS fetch, with payload bytes or SHA-256 compared to known-good values;
   - a push and re-clone on a disposable repository, to test new CAS writes.

   `/healthz` alone is not proof. A real incident needs checks across all affected
   repositories, not just the fixture below.
6. **Reconcile security changes since the snapshot** before admitting users or
   external side effects. Deleted or purged repository names, revoked keys, credentials, and
   webhook/upstream settings may be rolled back; unexpired JWTs from restored keys
   can work again. The snapshot keeps purge tombstones but may lack later
   revocations. After a compromise,
   rotate credentials/keys and assess the backup's trustworthiness separately.
   Authorize a single cutover only after data and security checks pass.

## Reproduce the local recovery drill

Install the [contributor prerequisites](../../CONTRIBUTING.md#prerequisites)
(pinned Go, Docker, git-lfs). From the repository root:

```bash
OPENSTORAGE_TEST_BACKEND=s3wal go test ./internal/githttp \
  -run '^TestS3WAL_OfflineBackupRestore$' -count=1 -race -timeout=5m -v
```

The drill uses separate source, backup, and restore buckets on disposable
SeaweedFS:

1. stops the source and verifies whole-bucket copies, including an opaque
   metadata-bearing object;
2. rejects a damaged restore inventory before boot;
3. removes source data and cache;
4. checks fresh-cache Git/LFS recovery and a later push.

The copier/verifier is test-only. For review, capture the stage log, a screenshot,
and a recording, with generated test credentials and data only.

> [!NOTE]
> This is **local fixture proof**, not evidence for AWS permissions, account/region
> loss, retention enforcement, large-object transfer, hostile-writer fencing, or fleet-wide
> corruption recovery. It does not satisfy A1's conformant
> real-S3 starvation rerun, A2's separate-day repeats, A3's in-region campaign, or
> A4/#182's measurement refresh. If the conformant rerun again trips the recorded
> 30-second starvation criterion, the kill/deprecation recommendation stands.
