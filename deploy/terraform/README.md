# deploy/terraform

## Overview

This directory groups the Terraform roots for open.storage reference deployments. It contains no Terraform files of its own. `aws` is its only subdirectory.

## Subdirectories & Boundaries

- aws: [./aws/README.md](./aws/README.md) - A single-node evaluation stack in one AWS region: an HTTPS Application Load Balancer, one Amazon Linux 2023 EC2 instance running `openstorage serve`, RDS PostgreSQL 17, and S3 `data` and `artifact` buckets. Cloud-init runs `bootstrap/bootstrap.py` to install the pinned release. The `deployment` output feeds `scripts/smoke.sh`. Persistent data is protected unless `allow_destroy_persistent_data = true`.
  - [aws/bootstrap/README.md](./aws/bootstrap/README.md) - The instance installer, the credential refresher `runtime.py`, and the systemd units.
  - [aws/scripts/README.md](./aws/scripts/README.md) - The `smoke.sh` acceptance-runner adapter.
