# Deploy the AWS EC2 reference with Terraform

**One user deployment, also used by release smoke tests.** The
[Terraform root](../../deploy/terraform/aws) provisions an HTTPS Application Load
Balancer (ALB), one Amazon Linux 2023 x86-64 EC2 server, private single-AZ RDS
PostgreSQL, S3 repository storage, networking, and IAM. No existing VPC, DNS zone,
certificate, or organization-managed infrastructure is required.

This is an **evaluation reference, not a highly available fleet**. PostgreSQL and
S3 are the standard durable topology. It does not deploy Kubernetes or promote
experimental S3-only storage. Local/mock verification is not evidence of a
successful AWS deployment; clean-account and release qualification remain tracked
in [#224](https://github.com/coder/openstorage/issues/224) and
[#222](https://github.com/coder/openstorage/issues/222).

## 1. Prerequisites and cost boundary

1. Obtain approval for an isolated AWS account/sandbox, a commercial AWS region
   with two available AZs, a spending limit, and teardown authority. Use
   short-lived credentials (`aws sso login` or an assumed role). **The commands
   below create billable resources.** This guide does not authorize that spend.
2. The provisioning role needs to create/read/update/delete VPCs, subnets, routes,
   security groups, an Internet gateway, EC2/EBS, an ALB/target group/listener,
   RDS/subnet groups, S3 buckets/objects/policies, IAM instance roles/policies and
   profiles, and imported ACM certificates. It needs `iam:PassRole` for the
   reference EC2 role, reads of the public AL2023 SSM parameter, and the usual
   describe/tag permissions for those services. A clean account may also need
   `iam:CreateServiceLinkedRole` for ELB, RDS, and RDS-managed Secrets Manager
   integration. Permission boundaries, SCPs, region opt-in, quotas, or denied
   service-linked role creation can block provisioning. There is no embedded
   administrator policy or claim that the runtime role can provision the stack.
3. Install [mise](https://mise.jdx.dev/), then run `mise install` at the repository
   root. Terraform is pinned in `mise.toml`; provider versions and checksums are
   committed beside the reference. Also install AWS CLI v2, Git, Git LFS, `gh`,
   Python 3, OpenSSL, `curl`, and GNU `timeout` for bounded automation.
4. Have access to the exact release or RC archive and its trusted checksum. While
   downloads require repository access, use authenticated `gh` **on your client**.
   The instance receives neither a GitHub token nor the signing private key.
5. Know your client's public egress IPv4 CIDR (normally `/32`). Do not use an
   address-discovery service without considering its privacy implications. The
   ALB accepts only these CIDRs on port 443. There is no SSH ingress.

The default bill includes a `t3.small`, a 50-GiB encrypted gp3 root/cache disk,
`db.t4g.micro` PostgreSQL with 20 GiB and seven-day automated backups, an ALB,
public IPv4 addresses, Secrets Manager, and S3/storage/request/transfer charges.
Check regional prices before applying. There is no NAT gateway, autoscaling,
scheduled job, or automatic shutdown. Budget alarms are not hard spending caps.

## 2. Prepare release and organization authentication locally

Use a private directory **outside the checkout** for signing keys and downloads:

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

Install the matching `openstorage` CLI on your client using the
[installation instructions](../../CONTRIBUTING.md#authenticated-installation).
For a Linux amd64 client, the verified archive above is directly runnable:

```bash
tar -xzf "$ARCHIVE_PATH" -C "$CLIENT_DIR" openstorage
export PATH="$CLIENT_DIR:$PATH"
openstorage version
```

Other clients need their own matching platform archive and checksum as well as the
Linux amd64 archive for EC2. Record `openstorage version` (including commit) with
the evidence. An RC may be built
with [`scripts/release-archives.sh`](../../scripts/release-archives.sh); its exact
source commit and archive digest must be recorded. This does not publish an artifact.

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

Do not paste a private key into a tfvar, environment template, PR, or log.
Terraform rejects private-key PEM in the public-key input; bootstrap additionally
parses the actual public key. Losing the signing key loses the ability to mint
new tokens. Back it up securely outside Terraform.

## 3. Protect state, plan, and apply

**Terraform state contains the transport CA and server TLS private keys.** They
are unrelated to the organization's JWT signing key, which never enters state.
Marking a value sensitive only redacts normal CLI rendering; state, saved plans,
`terraform show -json`, and provider debug logs can still contain it. Restrict
local file permissions/backups, never commit or upload state/plans, and use an
encrypted, access-controlled, locking remote backend for shared/persistent use.
The root intentionally leaves backend selection to the operator; configuring that
backend is separate from the reference resources to avoid a state bootstrap cycle.

RDS generates and rotates its master password in Secrets Manager. Terraform stores
its ARN/metadata, not its value. The EC2 runtime fetches it with its role and holds
the connection string in a root-only `/run` file and the service process environment.
Instance administrators can access it. Repository data and RDS snapshots can
contain application secrets: this slice does not enable application-level
`OPENSTORAGE_SECRETS_KEY` encryption for webhook/upstream credentials. See the
[production security guidance](deploy-production.md#backing-services).

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

A timeout does **not** guarantee rollback or cleanup. Preserve state, inspect AWS
and the next plan, and finish or tear down the partial deployment. Never run a
second apply/destroy concurrently or blindly remove a state lock.

The instance has a public address for outbound HTTPS packages/AWS APIs but accepts
port 8080 only from the ALB security group. PostgreSQL is in private subnets and
accepts port 5432 only from the instance. ALB-to-instance HTTP stays inside that
security-group boundary; this is not end-to-end TLS. IMDSv2 is required. Runtime
IAM is limited to the deployment's S3 data, the exact release object, and the RDS
secret; S3 access is pinned to the owning account. Repository S3 is unversioned to
preserve the documented destructive-sweep semantics.

Cloud-init downloads the staged release from the private artifact bucket, verifies
SHA-256 and embedded version before installation, and starts a non-root systemd
service. Automatic restart and a managed-password refresh timer handle process
failure/credential rotation. Bootstrap needs working HTTPS egress to AWS package
repositories, S3, Secrets Manager, and the RDS CA endpoint. The root AMI resolves
from AWS's AL2023 parameter; replay its output `ami_id` as an input to pin it.
OS package/security updates and RDS minor versions are intentionally not byte-pinned.

## 4. Trust TLS and use the service

No custom domain is needed. The output URL uses the **ALB DNS name**, not a stable
IP. Terraform creates a private CA and imports an ALB certificate with that DNS
SAN into ACM. Browsers do not trust it automatically. Trust the public CA obtained
from your protected Terraform output; **never use `curl -k` or disable Git TLS
verification**. The leaf expires after one year and is renewed by a subsequent
Terraform apply when within 30 days of expiry. There is no unattended renewal job.
A publicly trusted custom-domain deployment is a later extension, not a hidden
manual prerequisite for this path.

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

Do not run this shell with `set -x`, `GIT_TRACE`, curl verbosity, or HTTP-header
logging. Tokens remain accessible to same-user process inspection while in use.
The create/write/read flow is an authenticated connectivity check. `/healthz`
returns process liveness after initial startup, not continuous PG/S3 health; an
ALB-healthy node can still fail data operations. Inspect EC2 console/cloud-init
output and ALB target health for startup failures. There is no SSH/SSM management
channel in this slice; changing bootstrap inputs replaces the disposable node.

## 5. Release-smoke integration and evidence

`terraform output -json deployment` is the **schema version 1** handoff. It includes
`service_url`, `health_url`, `tls_ca_certificate_pem`, `org`, `region`, `account_id`,
`instance_ids`, `ami_id`, `bucket`, `database_id`, ALB/target-group ARNs, and
`release: {tag, sha256}`. It contains no private key, token, or database password.
The client/runner supplies its local signing-key path separately.

The opt-in [smoke adapter](../../deploy/terraform/aws/scripts/smoke.sh) translates
this output to the separately owned #222 runner's `--config`/`--output` interface.
It requires a runner supporting **`pg-s3-single-node` with empty `replica_urls` and
optional `ca_file`**. Runner versions supporting only `local`, two-replica `pg-s3`,
and `s3wal` reject this handoff. Compatible runner integration is a remaining
#222 gate, not established by the adapter's stub tests. Do not label this node
`local`, repeat the ALB URL as two replicas, or disable TLS verification to bypass it.

Prepare four local inputs, separately from Terraform. Use the approved source
commit and executable SHA for your matching client platform, **not the Linux
archive SHA**. Paths resolve relative to the invoking directory. The runner checks
executable hash/version consistency; that does not attest deployed binaries.

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

The adapter reads state but never provisions resources. It combines system CA
trust with the public ALB CA, keeps the signing key local, removes temporary
config/CA files, and propagates the runner's exit status. Keep the earlier exported
`deployment.json` beside the runner results for resource and archive metadata;
the runner config intentionally excludes those extra fields. `--aws-readonly`
optionally enables the runner's STS/HeadBucket probes using the caller's approved
AWS credentials. Scenarios and result manifests belong to #222, not another
framework here. Single-node smoke explicitly cannot prove cross-replica behavior.

RC qualification must record the reference Git commit, exact release commit and
artifact SHA, Terraform/provider lock, region/account, sanitized inputs, timestamps,
logs, screenshot/video, pass/fail/skipped cases, and teardown/resource checks.
Real same-region Git/API/LFS/auth/IAM/recovery and two-replica evidence are still
required by #222. A successful plan, bootstrap unit test, or apply is insufficient.
No recurring cloud schedule is enabled. Daily/weekly/RC cadence, budget, ownership,
notifications, recovery breadth, and leaked-resource detection remain follow-ups
in #224 pending approved execution resources.

## 6. Persistence and deliberate teardown

RDS and S3 hold the durable repository. EC2 root/cache replacement is disposable;
losing it does not deliberately delete PG/S3. This is not a tested recovery or
backup-restore guarantee. Cache grows without eviction; 50 GiB is only evaluation
capacity. There is no HA, automatic instance replacement, disk alarm, or restore
automation. RDS's seven-day backups are not an independently verified backup of
both stores together.

By default, RDS deletion protection is on and nonempty S3 data cannot be force
deleted. `terraform destroy` can therefore partially destroy other resources and
then fail. **Do not use destroy to pause a retained deployment.** Retained resources
continue to cost money. Export/verify the desired database and object backups
before intentionally enabling destructive teardown:

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

Confirm state is empty and inspect EC2/EBS, ELB, RDS/backups/Secrets Manager, S3,
IAM, ACM, and VPC resources in the recorded region for the deployment name/tags.
An empty state alone cannot prove the account has no leaks. If deletion times out,
retain the state and rerun the inspected destroy; never delete state to hide a
failed cleanup. Terraform-managed artifact storage is disposable too. Any manual
snapshots/backups and your state backend are outside this destroy and require
separate retention/cleanup decisions.

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

Terraform tests use mocked providers and **plan only**; bootstrap unit tests use
local fixtures/stubs. They do not establish AWS IAM, RDS, networking, certificate
import, ALB readiness, clean-account provisioning, or dependable AWS cleanup.
