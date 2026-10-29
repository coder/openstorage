# AWS Reference Deployment (Terraform)

## Overview

This Terraform root deploys a single-node open.storage evaluation stack in one AWS commercial region. It creates the following:

1. A VPC with two public and two database subnets.
2. An internet-facing Application Load Balancer (ALB) that terminates HTTPS with a private CA that Terraform generates.
3. One Amazon Linux 2023 EC2 instance that runs `openstorage serve` on port 8080.
4. An RDS PostgreSQL 17 instance.
5. Two S3 buckets: `data` for repository objects and `artifact` for the pinned release archive.

The instance installs itself at first boot. Terraform builds a cloud-init document from `local.cloud_config`. That document writes `config.json`, the organization public key, and the files from [bootstrap/](./bootstrap/README.md) to `/etc/openstorage/`, and then runs `bootstrap.py` [compute.tf:local.cloud_config].

Human guide: [Deploy on AWS with Terraform](../../../docs/how-to/deploy-aws-terraform.md).

## Interface

### Child contracts

- [bootstrap/README.md](./bootstrap/README.md): the instance installer (`bootstrap.py`), the credential refresher (`runtime.py`), and the systemd units that cloud-init copies to the instance.
- [scripts/README.md](./scripts/README.md): `smoke.sh`, the adapter that turns the `deployment` output into an acceptance-runner config.

### Tool and provider pins

- Terraform `= 1.17.1`, `hashicorp/aws = 6.71.0`, `hashicorp/tls = 4.4.2` [versions.tf:terraform]. `.terraform.lock.hcl` locks the provider hashes.
- The `aws` provider uses `region = var.region` and adds the default tags `Project = "openstorage-reference"` and `Deployment = var.name` [versions.tf:provider.aws].

### Input variables

| Variable | Type | Default | Validation |
|---|---|---|---|
| `region` | `string` | none | matches `^[a-z]{2}-[a-z]+-[0-9]+$` [variables.tf:region] |
| `name` | `string` | none | 3 to 20 characters, `^[a-z][a-z0-9-]{2,19}$`, no trailing `-` [variables.tf:name] |
| `org` | `string` | none | `^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$` (JWT issuer) [variables.tf:org] |
| `org_public_key_pem` | `string` | none | one PKIX `PUBLIC KEY` PEM block [variables.tf:org_public_key_pem] |
| `release_tag` | `string` | none | exact `vX.Y.Z` or prerelease tag [variables.tf:release_tag] |
| `release_archive` | `string` | none | none. Local path to the `linux_amd64` archive [variables.tf:release_archive] |
| `release_sha256` | `string` | none | lowercase 64-character hex [variables.tf:release_sha256] |
| `allowed_client_cidrs` | `set(string)` | none | at least one IPv4 CIDR, none with netmask `0.0.0.0` [variables.tf:allowed_client_cidrs] |
| `allow_destroy_persistent_data` | `bool` | `false` | none [variables.tf:allow_destroy_persistent_data] |
| `ami_id` | `string` | `null` | `null` or `^ami-[a-f0-9]{8,17}$` [variables.tf:ami_id] |

### Output `deployment`

A non-sensitive handoff object [outputs.tf:deployment] with these keys: `schema_version = 1`, `topology = "postgres-s3-single-node"`, `service_url`, `health_url`, `alb_dns_name`, `tls_ca_certificate_pem`, `org`, `region`, `account_id`, `instance_ids`, `ami_id`, `bucket`, `database_id`, `load_balancer_arn`, `target_group_arn`, `release { tag, sha256 }`. `scripts/smoke.sh` reads `schema_version`, `topology`, `tls_ca_certificate_pem`, `service_url`, `org`, `region`, `bucket`, and `account_id`.

### Resources by file

- `versions.tf`: data sources `aws_caller_identity.current` [versions.tf:data.aws_caller_identity.current], `aws_partition.current` [versions.tf:data.aws_partition.current], `aws_availability_zones.available` [versions.tf:data.aws_availability_zones.available], and `aws_ssm_parameter.al2023` (the current AL2023 x86_64 AMI) [versions.tf:data.aws_ssm_parameter.al2023].
- `compute.tf`: `local.service_url` (`https://<ALB DNS name>`), `local.runtime_config` (the instance `config.json`), `local.cloud_config`, and `aws_instance.server` (`t3.small`, 50 GiB encrypted `gp3` root volume) [compute.tf:aws_instance.server].
- `iam.tf`: `aws_iam_role.server`, `aws_iam_role_policy.server`, `aws_iam_instance_profile.server` [iam.tf].
- `load-balancer.tf`: `aws_lb.this`, the CA and server keys and certificates (`tls_private_key.ca`, `tls_self_signed_cert.ca`, `tls_private_key.server`, `tls_cert_request.server`, `tls_locally_signed_cert.server`), `aws_acm_certificate.server`, `aws_lb_target_group.this` (HTTP 8080, health check `/healthz` expecting `200`), `aws_lb_listener.https` (443), `aws_lb_target_group_attachment.server` [load-balancer.tf].
- `network.tf`: `aws_vpc.this` (`10.42.0.0/16`), `aws_internet_gateway.this`, `aws_subnet.public` and `aws_subnet.database` (2 each), the public route table and default route, the security groups `alb`, `server`, `database`, and their ingress and egress rules [network.tf].
- `storage.tf`: `aws_s3_bucket.data`, `aws_s3_bucket.artifact`, per-bucket public access block, encryption, ownership, and TLS policy, the `data` lifecycle rule, `aws_s3_object.release`, `aws_db_subnet_group.this`, and `aws_db_instance.this` (PostgreSQL 17, `db.t4g.micro`, database and user `openstorage`) [storage.tf].

## Invariants

- `local.runtime_config` must keep exactly the keys that `runtime.py validate_config` accepts: `region`, `account_id`, `org`, `service_url`, `artifact_bucket`, `artifact_key`, `release_sha256`, `release_tag`, `postgres_host`, `postgres_port`, `postgres_database`, `postgres_secret_arn`, `data_bucket`, `trusted_proxy_cidrs`. The instance refuses to start with any other key set [compute.tf:local.runtime_config] [bootstrap/runtime.py:validate_config].
- `postgres_port` is the literal `5432`, and `trusted_proxy_cidrs` is the list of public subnet CIDRs [compute.tf:local.runtime_config].
- The keys of the `deployment` output that `smoke.sh` reads must stay present, with `schema_version = 1` and `topology = "postgres-s3-single-node"` [outputs.tf:deployment] [scripts/smoke.sh:postgres-s3-single-node].
- The VPC precondition requires the `aws` partition and at least two available AZs [network.tf:aws_vpc.this].
- The compressed user data must be at most 21844 base64 characters (the AWS 16 KiB limit) [compute.tf:aws_instance.server.precondition].
- The `aws_s3_object.release` precondition requires `filesha256(var.release_archive) == var.release_sha256`. A missing or changed archive fails the plan [storage.tf:aws_s3_object.release].
- `allowed_client_cidrs` rejects an empty set and any `/0` network [variables.tf:allowed_client_cidrs].
- The instance requires IMDSv2 (`http_tokens = "required"`) with a hop limit of 1 [compute.tf:aws_instance.server.metadata_options].
- Network flow is closed by security group: clients reach the ALB on TCP 443 only from `allowed_client_cidrs`. The ALB reaches the server on TCP 8080. The server reaches the database on TCP 5432 and the internet on TCP 443 [network.tf:aws_vpc_security_group_ingress_rule] [network.tf:aws_vpc_security_group_egress_rule].
- The database has `publicly_accessible = false` and `storage_encrypted = true` [storage.tf:aws_db_instance.this].
- Both buckets block all public access, enforce bucket-owner object ownership, use AES256 encryption, and deny every request with `aws:SecureTransport = false` [storage.tf:aws_s3_bucket_public_access_block.this] [storage.tf:aws_s3_bucket_ownership_controls.this] [storage.tf:aws_s3_bucket_server_side_encryption_configuration.this] [storage.tf:aws_s3_bucket_policy.tls].
- The instance role can list and version-check the `data` bucket and read, write, and delete its objects. It can read only the one release object and the one RDS master-user secret. The S3 statements require `aws:ResourceAccount` to equal the caller account [iam.tf:aws_iam_role_policy.server].
- The HTTPS listener uses `ELBSecurityPolicy-TLS13-1-2-2021-06`, and the ALB drops invalid header fields [load-balancer.tf:aws_lb_listener.https] [load-balancer.tf:aws_lb.this].
- Persistent data is protected by default. RDS `deletion_protection = !var.allow_destroy_persistent_data`, and the `data` bucket has `force_destroy = var.allow_destroy_persistent_data`. You must apply `allow_destroy_persistent_data = true` before `terraform destroy` [storage.tf:aws_db_instance.this] [storage.tf:aws_s3_bucket.data].
- The JWT signing private key must never enter Terraform. Only the public key goes in `org_public_key_pem` [variables.tf:org_public_key_pem] [load-balancer.tf:tls_private_key.ca].

## Side Effects

- `terraform apply` creates billable AWS resources: the ALB, the EC2 instance, the RDS instance, the two buckets, the VPC network, the IAM role and instance profile, and an imported ACM certificate [compute.tf] [load-balancer.tf] [network.tf] [storage.tf] [iam.tf].
- `aws_s3_object.release` uploads the local `release_archive` to `releases/<release_tag>/<release_sha256>.tar.gz` in the `artifact` bucket [storage.tf:aws_s3_object.release].
- RDS creates and manages the master-user secret in Secrets Manager (`manage_master_user_password = true`) [storage.tf:aws_db_instance.this].
- CAUTION: Terraform state holds the ALB CA private key and the server private key [load-balancer.tf:tls_private_key.ca] [load-balancer.tf:tls_private_key.server]. Protect the state files. `.gitignore` keeps `*.tfstate*`, `*.tfplan`, and `*.tfvars` out of git.
- Any change to `local.cloud_config` replaces the EC2 instance, because `user_data_replace_on_change = true` [compute.tf:aws_instance.server]. The new instance runs the bootstrap again.
- When `ami_id` is `null`, the plan reads the current AL2023 AMI from SSM. A newer AMI changes the instance AMI [versions.tf:data.aws_ssm_parameter.al2023] [compute.tf:aws_instance.server].
- CAUTION: `terraform destroy` always empties and deletes the `artifact` bucket (`force_destroy = true`) [storage.tf:aws_s3_bucket.artifact]. With `allow_destroy_persistent_data = true`, it also deletes all repository data and the database, with no final snapshot and no automated backups kept (`skip_final_snapshot = true`, `delete_automated_backups = true`) [storage.tf:aws_db_instance.this].
- The `data` bucket aborts incomplete multipart uploads after 1 day [storage.tf:aws_s3_bucket_lifecycle_configuration.data].

## Verification

These are the offline gates. They need no AWS credentials and never apply. Run them from the repository root. CI runs the same commands in the `terraform` job of `.github/workflows/ci.yml`. `mise.toml` pins Terraform `1.17.1`.

```sh
terraform -chdir=deploy/terraform/aws fmt -check -recursive
terraform -chdir=deploy/terraform/aws init -backend=false -lockfile=readonly -input=false
terraform -chdir=deploy/terraform/aws validate
terraform -chdir=deploy/terraform/aws test
python3 -m unittest discover -s deploy/terraform/aws/tests -p 'test_*.py' -v
```

- `terraform test` runs `tests/reference.tftest.hcl`. It uses `mock_provider "aws"` and `mock_provider "tls"`, so it makes no AWS calls.
- The Python command runs `tests/test_smoke.py`, which tests `scripts/smoke.sh`.
- The bootstrap and smoke-script checks are listed in the child READMEs.
