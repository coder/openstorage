# Every run is a plan using mocked providers. No AWS credentials or resources.
mock_provider "aws" {
  override_during = plan
  mock_data "aws_caller_identity" { defaults = { account_id = "123456789012" } }
  mock_data "aws_partition" { defaults = { partition = "aws" } }
  mock_data "aws_availability_zones" { defaults = { names = ["us-east-1a", "us-east-1b"] } }
  mock_data "aws_ssm_parameter" { defaults = { value = "ami-0123456789abcdef0" } }
  override_resource {
    target = aws_security_group.alb
    values = { id = "sg-00000000000000001" }
  }
  override_resource {
    target = aws_security_group.server
    values = { id = "sg-00000000000000002" }
  }
  override_resource {
    target = aws_security_group.database
    values = { id = "sg-00000000000000003" }
  }
  mock_resource "aws_instance" { defaults = { key_name = "" } }
  mock_resource "aws_db_instance" {
    defaults = {
      address            = "database.example.us-east-1.rds.amazonaws.com"
      master_user_secret = [{ secret_arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:rds-test-abcdef", kms_key_id = "test", secret_status = "active" }]
    }
  }
  mock_resource "aws_lb" {
    defaults = { dns_name = "reference-123.us-east-1.elb.amazonaws.com", arn = "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/reference/123" }
  }
  mock_resource "aws_lb_target_group" {
    defaults = { arn = "arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/reference/123" }
  }
  mock_resource "aws_s3_bucket" {
    defaults = { arn = "arn:aws:s3:::reference-test", id = "reference-test" }
  }
  mock_resource "aws_s3_object" {
    defaults = { arn = "arn:aws:s3:::reference-test/release.tar.gz" }
  }
  mock_resource "aws_acm_certificate" {
    defaults = { arn = "arn:aws:acm:us-east-1:123456789012:certificate/00000000-0000-0000-0000-000000000000" }
  }
}
mock_provider "tls" { override_during = plan }

variables {
  region               = "us-east-1"
  name                 = "reference-test"
  org                  = "example"
  org_public_key_pem   = "-----BEGIN PUBLIC KEY-----\nYWJj\n-----END PUBLIC KEY-----\n"
  release_tag          = "v0.15.0"
  release_archive      = "tests/fixtures/archive.txt"
  release_sha256       = filesha256("tests/fixtures/archive.txt")
  allowed_client_cidrs = ["192.0.2.1/32"]
}

run "secure_default" {
  command = plan
  assert {
    condition     = aws_db_instance.this.storage_encrypted && !aws_db_instance.this.publicly_accessible && aws_db_instance.this.manage_master_user_password && aws_db_instance.this.password == null
    error_message = "Database must be encrypted/private and use an AWS-managed password, not state-stored credentials."
  }
  assert {
    condition     = aws_db_instance.this.allocated_storage == 20 && aws_db_instance.this.max_allocated_storage == null
    error_message = "The evaluation database must remain fixed at 20 GiB with storage autoscaling disabled."
  }
  assert {
    condition     = aws_db_instance.this.deletion_protection && !aws_s3_bucket.data.force_destroy && aws_instance.server.root_block_device[0].encrypted
    error_message = "Default persistent data destruction must be blocked and local disk encrypted."
  }
  assert {
    condition = alltrue([for block in aws_s3_bucket_public_access_block.this :
      block.block_public_acls && block.block_public_policy && block.ignore_public_acls && block.restrict_public_buckets
    ]) && alltrue([for encryption in aws_s3_bucket_server_side_encryption_configuration.this : one(encryption.rule).apply_server_side_encryption_by_default[0].sse_algorithm == "AES256"])
    error_message = "Every bucket must block public access and enable encryption."
  }
  assert {
    condition     = aws_lb_listener.https.protocol == "HTTPS" && aws_lb_listener.https.port == 443 && aws_lb_listener.https.ssl_policy == "ELBSecurityPolicy-TLS13-1-2-2021-06"
    error_message = "Only modern TLS may carry client bearer credentials."
  }
  assert {
    condition     = aws_vpc_security_group_ingress_rule.server.referenced_security_group_id == aws_security_group.alb.id && aws_vpc_security_group_ingress_rule.server.cidr_ipv4 == null && aws_vpc_security_group_ingress_rule.database.referenced_security_group_id == aws_security_group.server.id
    error_message = "Only ALB may reach the service, and only the service may reach PostgreSQL."
  }
  assert {
    condition     = aws_instance.server.metadata_options[0].http_tokens == "required" && aws_instance.server.metadata_options[0].http_put_response_hop_limit == 1 && aws_instance.server.key_name == ""
    error_message = "Require IMDSv2 with no SSH key or extra metadata hop."
  }
  assert {
    condition     = jsondecode(aws_iam_role_policy.server.policy).Statement[0].Condition.StringEquals["aws:ResourceAccount"] == "123456789012" && !strcontains(aws_iam_role_policy.server.policy, "s3:CreateBucket") && alltrue([for statement in jsondecode(aws_iam_role_policy.server.policy).Statement : !contains(statement.Resource, "*")])
    error_message = "Scope runtime IAM to account-owned buckets and the specific database secret."
  }
  assert {
    condition     = output.deployment.schema_version == 1 && output.deployment.service_url == "https://reference-123.us-east-1.elb.amazonaws.com" && output.deployment.org == var.org && output.deployment.release.sha256 == var.release_sha256
    error_message = "User and release smoke handoff must use the same endpoint/auth/artifact contract."
  }
  assert {
    condition     = local.runtime_config.service_url == output.deployment.service_url && local.runtime_config.account_id == output.deployment.account_id && local.runtime_config.postgres_secret_arn == aws_db_instance.this.master_user_secret[0].secret_arn
    error_message = "Bootstrap must receive external URLs and secret identifiers, never secret values."
  }
  assert {
    condition     = !strcontains(jsonencode(local.cloud_config), "PRIVATE KEY") && !contains(keys(output.deployment), "password") && !contains(keys(output.deployment), "private_key") && length(local.cloud_config.write_files) == 7
    error_message = "Do not expose private keys/passwords in cloud-init or outputs."
  }
}

run "disposable_opt_in" {
  command = plan
  variables { allow_destroy_persistent_data = true }
  assert {
    condition     = !aws_db_instance.this.deletion_protection && aws_db_instance.this.skip_final_snapshot && aws_s3_bucket.data.force_destroy
    error_message = "Explicit disposable mode must delete its persistent data without leaving a final snapshot."
  }
}

run "reject_private_signing_key" {
  command = plan
  variables { org_public_key_pem = "-----BEGIN PRIVATE KEY-----\nYWJj\n-----END PRIVATE KEY-----" }
  expect_failures = [var.org_public_key_pem]
}
run "reject_unpinned_release" {
  command = plan
  variables { release_tag = "latest" }
  expect_failures = [var.release_tag]
}
run "reject_world_ingress" {
  command = plan
  variables { allowed_client_cidrs = ["0.0.0.0/0"] }
  expect_failures = [var.allowed_client_cidrs]
}
run "reject_noncanonical_world_ingress" {
  command = plan
  variables { allowed_client_cidrs = ["192.0.2.1/0"] }
  expect_failures = [var.allowed_client_cidrs]
}
run "reject_empty_ingress" {
  command = plan
  variables { allowed_client_cidrs = [] }
  expect_failures = [var.allowed_client_cidrs]
}
run "reject_ipv6_ingress" {
  command = plan
  variables { allowed_client_cidrs = ["::/0"] }
  expect_failures = [var.allowed_client_cidrs]
}
run "reject_tampered_archive" {
  command = plan
  variables { release_sha256 = "0000000000000000000000000000000000000000000000000000000000000000" }
  expect_failures = [aws_s3_object.release]
}
run "reject_missing_archive" {
  command = plan
  variables { release_archive = "tests/fixtures/nonexistent.tar.gz" }
  expect_failures = [aws_s3_object.release]
}
