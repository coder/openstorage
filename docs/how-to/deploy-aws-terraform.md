# Deploy the AWS EC2 reference with Terraform

Deploy one open.storage server on AWS: an HTTPS Application Load Balancer (ALB), one
Amazon Linux 2023 x86-64 EC2 instance, private single-AZ RDS PostgreSQL, S3, networking,
and IAM. Release smoke tests use the same [Terraform root](../../deploy/terraform/aws).
No existing VPC, DNS zone, certificate, or organization-managed infrastructure is
needed.

> [!IMPORTANT]
> This is an **evaluation reference, not a highly available fleet**: standard
> PostgreSQL + S3, no Kubernetes, no experimental S3-only storage. Local/mock checks
> do not prove an AWS deployment works; qualification is tracked in [#224](https://github.com/coder/openstorage/issues/224)
> and [#222](https://github.com/coder/openstorage/issues/222).

## 1. Prerequisites and cost boundary

> [!WARNING]
> **The commands below create billable resources.** This guide does not authorize
> that spend.

1. **Approval:** an isolated AWS account/sandbox, a commercial region with two
   available AZs, a spending limit, and teardown authority. Use short-lived
   credentials (`aws sso login` or an assumed role).
2. **Provisioning permissions:** create/read/update/delete VPCs, subnets, routes,
   security groups, an Internet gateway, EC2/EBS, ALB/target group/listener,
   RDS/subnet groups, S3 buckets/objects/policies, IAM instance roles/policies/
   profiles, and imported ACM certificates. Plus `iam:PassRole` for the EC2 role,
   reads of the public AL2023 SSM parameter, and describe/tag permissions. A clean
   account may need `iam:CreateServiceLinkedRole` for ELB, RDS, and RDS-managed
   Secrets Manager. Boundaries, SCPs, region opt-in, quotas, or denied
   service-linked roles can block provisioning. No administrator policy is
   embedded, and the runtime role is not meant to provision the stack.
3. **Tools:** [mise](https://mise.jdx.dev/) plus `mise install` at the repo root
   (pins Terraform; provider locks are committed), AWS CLI v2, Git, Git LFS, `gh`,
   Python 3, OpenSSL, `curl`, and GNU `timeout`.
4. **Release:** the exact release/RC archive and its trusted checksum. Use
   authenticated `gh` **on your client** while downloads need repository access. The
   instance never gets a GitHub token or the signing key.
5. **Client CIDR:** your public egress IPv4 CIDR (normally `/32`); mind the privacy
   of address-discovery services. The ALB accepts only these CIDRs on port 443. No
   SSH ingress.

**Default bill:** a `t3.small`, a 50-GiB encrypted gp3 root/cache disk,
`db.t4g.micro` PostgreSQL (20 GiB, seven-day automated backups), an ALB, public IPv4
addresses, Secrets Manager, and S3 storage/request/transfer. Check regional prices.
No NAT gateway, autoscaling, scheduled job, or automatic shutdown. Budget alarms are
not hard caps.

## 2. Prepare release and organization authentication locally

Download and verify the release into a private directory **outside the checkout**:

```bash
set -euo pipefail
umask 077
export CLIENT_DIR="$HOME/openstorage-evaluation"
mkdir -p "$CLIENT_DIR"
chmod 700 "$CLIENT_DIR"
export RELEASE_TAG=v0.15.0 # example only: select the approved release/RC to test
export ARCHIVE="openstorage_${RELEASE_TAG#v}_linux_amd64.tar.gz"
gh release download "$RELEASE_TAG" --repo coder/openstorage \
  --pattern "$ARCHIVE" --pattern checksums.txt --dir "$CLIENT_DIR"
export ARCHIVE_PATH="$CLIENT_DIR/$ARCHIVE"
RELEASE_SHA256=$(python3 - <<'PY'
import hashlib, os
from pathlib import Path
path = Path(os.environ["ARCHIVE_PATH"])
lines = (path.parent / "checksums.txt").read_text().splitlines()
matches = [line.split()[0] for line in lines if line.split()[1] == path.name]
assert len(matches) == 1, "missing or ambiguous release checksum"
assert hashlib.sha256(path.read_bytes()).hexdigest() == matches[0], "archive mismatch"
print(matches[0])
PY
)
export RELEASE_SHA256
```

Install the matching client CLI ([instructions](../../CONTRIBUTING.md#authenticated-installation)).
On Linux amd64, the verified archive runs directly:

```bash
tar -xzf "$ARCHIVE_PATH" -C "$CLIENT_DIR" openstorage
export PATH="$CLIENT_DIR:$PATH"
openstorage version
```

Other clients need their own platform archive and checksum (EC2 still uses Linux
amd64). Record `openstorage version`, including commit, with the evidence. An RC may
be built with [`scripts/release-archives.sh`](../../scripts/release-archives.sh);
record its source commit and archive digest. This publishes nothing.

Generate the org keypair and write the Terraform inputs:

```bash
openstorage keygen --out "$CLIENT_DIR/org" --alg ES256
# org.pem is the private signing key (0600). Only org.pub.pem goes to Terraform.
export AWS_REGION=us-east-1
export DEPLOYMENT_NAME=openstorage-eval
export OPENSTORAGE_ORG=example
export CLIENT_CIDR=192.0.2.1/32 # REPLACE with your actual permitted egress CIDR
python3 - <<'PY'
import json, os
from pathlib import Path
client = Path(os.environ["CLIENT_DIR"])
inputs = {
    "region": os.environ["AWS_REGION"], "name": os.environ["DEPLOYMENT_NAME"],
    "org": os.environ["OPENSTORAGE_ORG"],
    "org_public_key_pem": (client / "org.pub.pem").read_text(),
    "release_tag": os.environ["RELEASE_TAG"],
    "release_archive": os.environ["ARCHIVE_PATH"],
    "release_sha256": os.environ["RELEASE_SHA256"],
    "allowed_client_cidrs": [os.environ["CLIENT_CIDR"]],
}
(client / "inputs.tfvars.json").write_text(json.dumps(inputs, indent=2) + "\n")
PY
```

> [!WARNING]
> Never paste a private key into a tfvar, env template, PR, or log. (Terraform
> rejects private PEM in the public-key input; bootstrap parses the public key.)
> Without the signing key you cannot mint tokens: back it up outside Terraform.

## 3. Protect state, plan, and apply

> [!WARNING]
> **Terraform state contains the transport CA and server TLS private keys.**
> "Sensitive" only redacts normal CLI output; state, saved plans,
> `terraform show -json`, and provider debug logs can still contain them.

- Restrict local permissions and backups. Never commit or upload state or plans.
- For shared or persistent use, configure an encrypted, access-controlled, locking remote backend
  yourself (left out to avoid a state bootstrap cycle).
- The org JWT signing key never enters state.
- RDS generates and rotates its master password in Secrets Manager; Terraform stores
  only the ARN. The instance keeps the connection string in a root-only `/run` file
  and the service environment, readable by instance administrators.
- Repository data and RDS snapshots can contain application secrets:
  `OPENSTORAGE_SECRETS_KEY` encryption for webhook/upstream credentials is not
  enabled. See [production security guidance](deploy-production.md#backing-services).

Check the account and region, then plan, review, and apply:

```bash
# From the repository root. Verify account/region before any mutation.
aws sts get-caller-identity
mise exec -- terraform -chdir=deploy/terraform/aws init
mise exec -- terraform -chdir=deploy/terraform/aws plan \
  -var-file="$CLIENT_DIR/inputs.tfvars.json" -out="$CLIENT_DIR/deploy.tfplan"
# Review account, resource changes and estimated cost before approving.
timeout --signal=INT --kill-after=60s 60m \
  mise exec -- terraform -chdir=deploy/terraform/aws apply "$CLIENT_DIR/deploy.tfplan"
mise exec -- terraform -chdir=deploy/terraform/aws output -json deployment \
  > "$CLIENT_DIR/deployment.json"
```

> [!IMPORTANT]
> A timeout does **not** guarantee rollback or cleanup. Preserve state, inspect AWS and the next plan,
> then finish or tear down. Never run concurrent apply/destroy or blindly remove a
> state lock.

<details><summary>What the stack looks like</summary>

**Network and IAM**

- The instance has a public address for outbound HTTPS but accepts port 8080 only
  from the ALB security group.
- PostgreSQL is in private subnets, port 5432 open only to the instance.
- ALB-to-instance is plain HTTP inside that boundary, not end-to-end TLS.
- IMDSv2 is required.
- Runtime IAM covers only the deployment's S3 data, the release object, and the RDS
  secret; S3 access is pinned to the owning account.
- Repository S3 is unversioned, matching the destructive-sweep semantics.

**Bootstrap**

- Cloud-init fetches the release from the private artifact bucket, verifies SHA-256
  and embedded version, and starts a non-root systemd service with automatic
  restart and a managed-password refresh timer.
- The service stops with `KillMode=mixed` and `TimeoutStopSec=30s`: systemd sends
  `SIGTERM` to the server only, and the server drains and stops its own Git processes
  ([rolling upgrades and restarts](deploy-production.md#rolling-upgrades-and-restarts)).
- Bootstrap needs HTTPS egress to AWS package repos, S3, Secrets Manager, and the
  RDS CA endpoint.
- The AMI comes from AWS's AL2023 parameter; pass output `ami_id` back as an input
  to pin it. OS packages and RDS minor versions are not byte-pinned.

</details>

## 4. Trust TLS and use the service

The service URL is the **ALB DNS name** (not a stable IP); no domain is needed.
Terraform creates a private CA and imports a matching ALB certificate into ACM.
Browsers do not trust it.

> [!WARNING]
> Trust the public CA from your protected Terraform output. **Never use `curl -k` or
> disable Git TLS verification.**

The leaf expires after one year. An apply within 30 days of expiry renews it; no
job renews it unattended. Publicly trusted custom domains are a future extension.

Build a CA bundle, check liveness, then create, push to, and read a demo repository:

```bash
export SERVICE_URL=$(python3 -c 'import json,os; print(json.load(open(os.environ["CLIENT_DIR"]+"/deployment.json"))["service_url"])')
python3 - <<'PY'
import json, os, ssl
from pathlib import Path
client = Path(os.environ["CLIENT_DIR"])
info = json.loads((client / "deployment.json").read_text())
# Retain system trust for presigned transfers to AWS S3, as well as our ALB CA.
roots = ssl.get_default_verify_paths().cafile
assert roots, "install your OS CA certificate bundle"
(client / "ca.pem").write_text(Path(roots).read_text() + "\n" + info["tls_ca_certificate_pem"])
PY
export CURL_CA_BUNDLE="$CLIENT_DIR/ca.pem"
export GIT_SSL_CAINFO="$CLIENT_DIR/ca.pem"
# Initial ALB/process liveness; bounded retries. This is NOT storage readiness.
curl --fail --silent --show-error --retry 60 --retry-all-errors \
  --retry-delay 5 --retry-max-time 360 --max-time 10 "$SERVICE_URL/healthz"

# One token scoped to a new demonstration repository; keep it out of URLs/logs.
export TOKEN=$(openstorage mint --key "$CLIENT_DIR/org.pem" --iss "$OPENSTORAGE_ORG" \
  --repo demo --scopes repo:write,git:read,git:write --ttl 15m)
printf 'header = "Authorization: Bearer %s"\n' "$TOKEN" | \
  curl --config - --fail --silent --show-error --max-time 30 \
  -X POST "$SERVICE_URL/api/v1/repos" -d '{"default_branch":"main"}' \
  > "$CLIENT_DIR/demo.json"
# Use the canonical clone URL returned by repository creation.
CLONE_URL=$(python3 -c 'import json,os; print(json.load(open(os.environ["CLIENT_DIR"]+"/demo.json"))["http_url"])')
# Supply Git auth in process environment, not a token-bearing remote URL.
export GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0="http.${SERVICE_URL}/.extraHeader"
export GIT_CONFIG_VALUE_0="Authorization: Basic $(printf 't:%s' "$TOKEN" | openssl base64 -A)"
git clone "$CLONE_URL" "$CLIENT_DIR/demo"
(cd "$CLIENT_DIR/demo" && git -c user.name=example -c user.email=example@example.com \
  commit --allow-empty -m 'reference deployment' && git push origin HEAD:main)
printf 'header = "Authorization: Bearer %s"\n' "$TOKEN" | \
  curl --config - --fail --silent --show-error --max-time 30 "$SERVICE_URL/api/v1/repo"
unset TOKEN GIT_CONFIG_COUNT GIT_CONFIG_KEY_0 GIT_CONFIG_VALUE_0
```

- No `set -x`, `GIT_TRACE`, curl verbosity, or header logging. Same-user processes
  can inspect tokens while in use.
- The create/write/read flow is the real connectivity check. `/healthz` is liveness
  only; an ALB-healthy node can still fail PG/S3 operations.
- Debug startup with EC2 console/cloud-init output and ALB target health. There is
  no SSH/SSM; changing bootstrap inputs replaces the disposable node.

## 5. Release-smoke integration and evidence

`terraform output -json deployment` is the **schema version 1** handoff:
`service_url`, `health_url`, `tls_ca_certificate_pem`, `org`, `region`, `account_id`,
`instance_ids`, `ami_id`, `bucket`, `database_id`, ALB/target-group ARNs, and
`release: {tag, sha256}`. No private key, token, or database password; the runner
supplies its own signing-key path. The opt-in
[smoke adapter](../../deploy/terraform/aws/scripts/smoke.sh) maps it to the #222
runner's `--config`/`--output` interface.

> [!IMPORTANT]
> The runner must support **`pg-s3-single-node` with empty `replica_urls` and
> optional `ca_file`**; runners with only `local`, two-replica `pg-s3`, and `s3wal`
> reject it. Compatible integration is still a #222 gate (stub tests do not prove
> it). Never label this node `local`, list the ALB twice, or disable TLS to bypass.

Prepare runner inputs separately from Terraform. Use the approved source commit and
the executable SHA for **your client platform, not the Linux archive SHA**. Paths
resolve from the invoking directory. The runner's hash/version check does not attest
deployed binaries.

```bash
# Replace these values with the independently verified local helper identity.
export CANDIDATE_COMMIT='REPLACE_WITH_FULL_40_CHARACTER_RELEASE_COMMIT'
export CLIENT_BINARY_SHA256='REPLACE_WITH_VERIFIED_64_CHARACTER_EXECUTABLE_SHA256'
python3 - <<'PY'
import json, os, shutil
from pathlib import Path
client = Path(os.environ["CLIENT_DIR"])
(client / "runner-inputs.json").write_text(json.dumps({
    "candidate_commit": os.environ["CANDIDATE_COMMIT"],
    "binary": shutil.which("openstorage"),
    "binary_sha256": os.environ["CLIENT_BINARY_SHA256"],
    "private_key_file": str(client / "org.pem"),
}))
PY
# Requires the compatible #222 runner, approved testing, and an existing stack.
# results must not exist. Omit --smoke for config/local-artifact checks only.
OPENSTORAGE_AWS_SMOKE=1 timeout --signal=TERM --kill-after=30s 20m \
  mise exec -- deploy/terraform/aws/scripts/smoke.sh "$CLIENT_DIR/runner-inputs.json" \
  "$CLIENT_DIR/results" python3 /path/to/aws_launch_qualification.py \
  --smoke --budget-seconds 600
```

The adapter reads state but never provisions. It builds a temporary CA bundle
(system CAs plus the ALB CA) for the runner, keeps the signing key local, removes
temporary config/CA files, and returns the runner's exit
status. `--aws-readonly` enables the runner's STS/HeadBucket probes with your
approved credentials. Keep `deployment.json` beside the results; the runner config
omits its resource and archive metadata. Single-node smoke cannot prove
cross-replica behavior.

**RC qualification** must record: the reference Git commit, exact release commit and
artifact SHA, Terraform/provider lock, region/account, sanitized inputs, timestamps,
logs, screenshot/video, pass/fail/skipped cases, and teardown/resource checks.

A plan, bootstrap unit test, or apply is not enough: #222 still needs real
same-region Git/API/LFS/auth/IAM/recovery and two-replica evidence. No recurring
cloud schedule runs; cadence, budget, ownership, notifications, recovery breadth,
and leak detection remain #224 follow-ups.

## 6. Persistence and deliberate teardown

RDS and S3 hold the durable repository; losing the EC2 disk does not deliberately delete them.
This is not a tested recovery guarantee. The cache grows without eviction (50 GiB is
evaluation capacity). There is no HA, instance replacement, disk alarm, or restore
automation, and RDS backups are not a verified backup of both stores together.

By default RDS deletion protection is on and nonempty S3 cannot be force deleted,
so `terraform destroy` can partially succeed and then fail.

> [!WARNING]
> **Do not use destroy to pause a deployment**; retained resources keep costing
> money. Export and verify backups first: these commands delete all data with no
> final snapshot.

```bash
# This first apply permanently opts the stack into data deletion on destroy.
timeout --signal=INT --kill-after=60s 60m \
  mise exec -- terraform -chdir=deploy/terraform/aws apply \
  -var-file="$CLIENT_DIR/inputs.tfvars.json" -var=allow_destroy_persistent_data=true
# No final RDS snapshot; automated backups and all S3 data are deleted.
timeout --signal=INT --kill-after=60s 60m \
  mise exec -- terraform -chdir=deploy/terraform/aws destroy \
  -var-file="$CLIENT_DIR/inputs.tfvars.json" -var=allow_destroy_persistent_data=true
mise exec -- terraform -chdir=deploy/terraform/aws state list
```

**Verify cleanup:**

1. Confirm the state list is empty.
2. Inspect EC2/EBS, ELB, RDS/backups/Secrets Manager, S3, IAM, ACM, and VPC in the
   recorded region for the deployment name/tags. Empty state does not prove no leaks.
3. On timeout, keep state and rerun the inspected destroy. Never delete state to
   hide a failed cleanup.

Terraform-managed artifact storage is destroyed too. Manual snapshots/backups and
your state backend are outside this destroy.

## Local contributor checks (no AWS account)

```bash
mise install
mise exec -- terraform -chdir=deploy/terraform/aws fmt -check -recursive
mise exec -- terraform -chdir=deploy/terraform/aws init -backend=false -lockfile=readonly
mise exec -- terraform -chdir=deploy/terraform/aws validate
mise exec -- terraform -chdir=deploy/terraform/aws test
python3 -m unittest discover -s deploy/terraform/aws/bootstrap -p 'test_*.py' -v
python3 -m unittest discover -s deploy/terraform/aws/tests -p 'test_*.py' -v
python3 scripts/check-doc-links.py
```

These use mocked providers (**plan only**) and local stubs. They do not prove AWS
IAM, RDS, networking, certificate import, ALB readiness, clean-account provisioning,
or reliable cleanup.
