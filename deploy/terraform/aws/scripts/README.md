# AWS Smoke Adapter

## Overview

`smoke.sh` connects a deployed AWS reference stack to an external acceptance runner. It reads the Terraform `deployment` output, merges it with local inputs into a runner `config.json`, and then runs the runner command that you pass. The script itself does not provision, apply, or call the service. Its header comment says: "the acceptance runner owns scenarios and result evidence" [smoke.sh].

## Interface

- Usage: `smoke.sh <local-inputs.json> <new-results-dir> <runner> [runner-args...]` [smoke.sh:usage].
- Environment: `OPENSTORAGE_AWS_SMOKE=1` is required [smoke.sh:OPENSTORAGE_AWS_SMOKE].
- `$1`: local inputs JSON. Its keys must be exactly `candidate_commit`, `binary`, `binary_sha256`, `private_key_file` [smoke.sh:candidate_commit].
- `$2`: results directory. The script passes it to the runner as `--output` [smoke.sh:--output].
- Remaining arguments: the runner command. The script runs it as `"$@" --config <work>/config.json --output <results-dir>` [smoke.sh:--config].
- Terraform input: `terraform -chdir=<deploy/terraform/aws> output -json deployment`. The script reads `schema_version`, `topology`, `tls_ca_certificate_pem`, `service_url`, `org`, `region`, `bucket`, `account_id` [smoke.sh:output -json deployment] [smoke.sh:tls_ca_certificate_pem].
- Generated runner config: the inputs, plus `schema_version=1`, `topology="pg-s3-single-node"`, `replica_urls=[]`, `endpoint`, `org`, `ca_file`, and `aws.region`, `aws.bucket`, `aws.expected_bucket_owner` [smoke.sh:replica_urls].
- The Terraform root is the parent of this directory, `deploy/terraform/aws` [smoke.sh:BASH_SOURCE]. See [../README.md](../README.md).

## Invariants

- The script runs with `set -euo pipefail` [smoke.sh:set -euo pipefail].
- It exits with status 2 unless `OPENSTORAGE_AWS_SMOKE` is exactly `1` [smoke.sh:OPENSTORAGE_AWS_SMOKE].
- It exits with status 2 when it gets fewer than 3 arguments [smoke.sh:usage].
- It exits when the inputs key set is not exactly the four required keys [smoke.sh:candidate_commit].
- It exits unless the deployment has `schema_version == 1` and `topology == "postgres-s3-single-node"` [smoke.sh:postgres-s3-single-node].
- It exits when Python's `ssl.get_default_verify_paths()` finds no CA bundle file [smoke.sh:ssl.get_default_verify_paths].
- The runner CA file contains the OS CA bundle and the deployment's private ALB CA. The OS bundle stays because presigned S3 transfers use public AWS trust [smoke.sh:ca.pem].
- `binary` and `private_key_file` are resolved to absolute paths [smoke.sh:resolve].
- `aws.expected_bucket_owner` is the deployment `account_id` [smoke.sh:expected_bucket_owner].
- The script does not validate `candidate_commit` or `binary_sha256`. It does not check that the results directory is new [smoke.sh:realpath].

## Side Effects

- Sets `umask 077` and creates a temporary directory with `mktemp -d`. A `trap` deletes it on exit [smoke.sh:umask 077] [smoke.sh:mktemp -d] [smoke.sh:trap].
- Writes `deployment.json`, `ca.pem`, and `config.json` in that temporary directory [smoke.sh:$work/deployment.json] [smoke.sh:ca.write_text] [smoke.sh:json.dumps].
- Runs `terraform output`, which reads state only and performs no apply [smoke.sh:output -json deployment].
- Prints usage and warning messages to stderr [smoke.sh:echo].
- Runs the external runner command. Network access and writes to the results directory come from that runner, not from this script [smoke.sh:--config].

## Verification

Run from the repository root. CI runs both commands in `.github/workflows/ci.yml`.

```sh
bash -n deploy/terraform/aws/scripts/smoke.sh
python3 -m unittest discover -s deploy/terraform/aws/tests -p 'test_*.py' -v
```

The second command runs `deploy/terraform/aws/tests/test_smoke.py`. Usage against a live stack is in [Deploy on AWS with Terraform](../../../../docs/how-to/deploy-aws-terraform.md).
