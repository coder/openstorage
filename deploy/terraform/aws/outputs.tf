output "deployment" {
  description = "Versioned non-secret handoff for users and #222's acceptance runner. Save with terraform output -json deployment."
  value = {
    schema_version         = 1
    topology               = "postgres-s3-single-node"
    service_url            = local.service_url
    health_url             = "${local.service_url}/healthz"
    alb_dns_name           = aws_lb.this.dns_name
    tls_ca_certificate_pem = tls_self_signed_cert.ca.cert_pem
    org                    = var.org
    region                 = var.region
    account_id             = data.aws_caller_identity.current.account_id
    instance_ids           = [aws_instance.server.id]
    ami_id                 = nonsensitive(aws_instance.server.ami)
    bucket                 = aws_s3_bucket.data.id
    database_id            = aws_db_instance.this.identifier
    load_balancer_arn      = aws_lb.this.arn
    target_group_arn       = aws_lb_target_group.this.arn
    release                = { tag = var.release_tag, sha256 = var.release_sha256 }
  }
}
