# AWS Smoke Adapter

## Overview

`smoke.sh` connects a deployed AWS reference stack to an external acceptance runner. It reads the Terraform `deployment` output, merges it with local inputs into a runner `config.json`, and then runs the runner command that you pass. The script itself does not provision, apply, or call the service. The comment on line 2 says: "the acceptance runner owns scenarios and result evidence" [smoke.sh:2].

## Interface

- Usage: `smoke.sh <local-inputs.json> <new-results-dir> <runner> [runner-args...]` [smoke.sh:8].
- Environment: `OPENSTORAGE_AWS_SMOKE=1` is required [smoke.sh:4].
- `$1`: local inputs JSON. Its keys must be exactly `candidate_commit`, `binary`, `binary_sha256`, `private_key_file` [smoke.sh:27].
- `$2`: results directory. The script passes it to the runner as `--output` [smoke.sh:10] [smoke.sh:46].
- Remaining arguments: the runner command. The script runs it as `"$@" --config <work>/config.json --output <results-dir>` [smoke.sh:46].
- Terraform input: `terraform -chdir=<deploy/terraform/aws> output -json deployment`. The script reads `schema_version`, `topology`, `tls_ca_certificate_pem`, `service_url`, `org`, `region`, `bucket`, `account_id` [smoke.sh:17] [smoke.sh:29-42].
- Generated runner config: the inputs, plus `schema_version=1`, `topology="pg-s3-single-node"`, `replica_urls=[]`, `endpoint`, `org`, `ca_file`, and `aws.region`, `aws.bucket`, `aws.expected_bucket_owner` [smoke.sh:39-42].
- The Terraform root is the parent of this directory, `deploy/terraform/aws` [smoke.sh:12]. See [../README.md](../README.md).

## Invariants

- The script runs with `set -euo pipefail` [smoke.sh:3].
- It exits with status 2 unless `OPENSTORAGE_AWS_SMOKE` is exactly `1` [smoke.sh:4-7].
- It exits with status 2 when it gets fewer than 3 arguments [smoke.sh:8].
- It exits when the inputs key set is not exactly the four required keys [smoke.sh:27-28].
- It exits unless the deployment has `schema_version == 1` and `topology == "postgres-s3-single-node"` [smoke.sh:29-30].
- It exits when Python's `ssl.get_default_verify_paths()` finds no CA bundle file [smoke.sh:31-33].
- The runner CA file contains the OS CA bundle and the deployment's private ALB CA. The OS bundle stays because presigned S3 transfers use public AWS trust [smoke.sh:34-36].
- `binary` and `private_key_file` are resolved to absolute paths [smoke.sh:37-38].
- `aws.expected_bucket_owner` is the deployment `account_id` [smoke.sh:42].
- The script does not validate `candidate_commit` or `binary_sha256`. It does not check that the results directory is new [smoke.sh:9-10].

## Side Effects

- Sets `umask 077` and creates a temporary directory with `mktemp -d`. A `trap` deletes it on exit [smoke.sh:13-15].
- Writes `deployment.json`, `ca.pem`, and `config.json` in that temporary directory [smoke.sh:17] [smoke.sh:36] [smoke.sh:43].
- Runs `terraform output`, which reads state only and performs no apply [smoke.sh:16-17].
- Prints usage and warning messages to stderr [smoke.sh:5] [smoke.sh:8] [smoke.sh:45].
- Runs the external runner command. Network access and writes to the results directory come from that runner, not from this script [smoke.sh:46].

## Verification

Run from the repository root. CI runs both commands in `.github/workflows/ci.yml`.

```sh
bash -n deploy/terraform/aws/scripts/smoke.sh
python3 -m unittest discover -s deploy/terraform/aws/tests -p 'test_*.py' -v
```

The second command runs `deploy/terraform/aws/tests/test_smoke.py`. Usage against a live stack is in [Deploy on AWS with Terraform](../../../../docs/how-to/deploy-aws-terraform.md).
