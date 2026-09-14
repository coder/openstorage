locals {
  service_url = "https://${aws_lb.this.dns_name}"
  runtime_config = {
    region              = var.region
    account_id          = data.aws_caller_identity.current.account_id
    org                 = var.org
    service_url         = local.service_url
    artifact_bucket     = aws_s3_bucket.artifact.id
    artifact_key        = aws_s3_object.release.key
    release_sha256      = var.release_sha256
    release_tag         = var.release_tag
    postgres_host       = aws_db_instance.this.address
    postgres_port       = 5432
    postgres_database   = aws_db_instance.this.db_name
    postgres_secret_arn = aws_db_instance.this.master_user_secret[0].secret_arn
    data_bucket         = aws_s3_bucket.data.id
    trusted_proxy_cidrs = aws_subnet.public[*].cidr_block
  }
  cloud_config = {
    write_files = concat([
      { path = "/etc/openstorage/config.json", permissions = "0600", encoding = "b64", content = base64encode(jsonencode(local.runtime_config)) },
      { path = "/etc/openstorage/org.pub.pem", permissions = "0644", encoding = "b64", content = base64encode(var.org_public_key_pem) }
      ], [for name in ["bootstrap.py", "runtime.py", "openstorage.service", "openstorage-refresh.service", "openstorage-refresh.timer"] :
      { path = "/etc/openstorage/${name}", permissions = "0600", encoding = "b64", content = filebase64("${path.module}/bootstrap/${name}") }
    ])
    runcmd = [["/usr/bin/python3", "/etc/openstorage/bootstrap.py"]]
  }
}

resource "aws_instance" "server" {
  ami                         = coalesce(var.ami_id, data.aws_ssm_parameter.al2023.value)
  instance_type               = "t3.small"
  subnet_id                   = aws_subnet.public[0].id
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.server.id]
  iam_instance_profile        = aws_iam_instance_profile.server.name
  user_data_base64            = base64gzip("#cloud-config\n${yamlencode(local.cloud_config)}")
  user_data_replace_on_change = true
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }
  root_block_device {
    volume_type           = "gp3"
    volume_size           = 50
    encrypted             = true
    delete_on_termination = true
  }
  lifecycle {
    precondition {
      condition     = length(base64gzip("#cloud-config\n${yamlencode(local.cloud_config)}")) <= 21844
      error_message = "Compressed EC2 user data must fit AWS's 16 KiB limit."
    }
  }
  depends_on = [aws_iam_role_policy.server, aws_route.internet, aws_route_table_association.public, aws_s3_object.release]
}
