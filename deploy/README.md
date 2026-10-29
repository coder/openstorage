# deploy

## Overview

This directory holds the deployment code for open.storage. It does not contain the Go server. Today it contains one group: Terraform reference deployments. The human deployment guides are under `docs/how-to/`, for example [Deploy on AWS with Terraform](../docs/how-to/deploy-aws-terraform.md).

## Subdirectories & Boundaries

- terraform: [./terraform/README.md](./terraform/README.md) - Terraform roots for reference deployments. It holds only [terraform/aws](./terraform/aws/README.md), a single-node AWS evaluation stack (ALB, EC2, RDS PostgreSQL, S3). That stack installs a pinned, checksum-verified release binary with cloud-init and exports a `deployment` output for smoke tests.
