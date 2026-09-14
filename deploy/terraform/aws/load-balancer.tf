resource "aws_lb" "this" {
  name                       = var.name
  load_balancer_type         = "application"
  internal                   = false
  subnets                    = aws_subnet.public[*].id
  security_groups            = [aws_security_group.alb.id]
  drop_invalid_header_fields = true
  idle_timeout               = 300
  depends_on                 = [aws_route_table_association.public, aws_route.internet]
}

# Transport keys only. Both private keys are sensitive but still stored in Terraform state.
# The organization JWT signing key is unrelated and must remain outside Terraform.
resource "tls_private_key" "ca" {
  algorithm = "RSA"
  rsa_bits  = 2048
}
resource "tls_self_signed_cert" "ca" {
  private_key_pem       = tls_private_key.ca.private_key_pem
  is_ca_certificate     = true
  validity_period_hours = 87600
  early_renewal_hours   = 720
  allowed_uses          = ["cert_signing", "crl_signing"]
  subject { common_name = "${var.name} evaluation CA" }
}
resource "tls_private_key" "server" {
  algorithm = "RSA"
  rsa_bits  = 2048
}
resource "tls_cert_request" "server" {
  private_key_pem = tls_private_key.server.private_key_pem
  dns_names       = [aws_lb.this.dns_name]
  subject { common_name = var.name }
}
resource "tls_locally_signed_cert" "server" {
  cert_request_pem      = tls_cert_request.server.cert_request_pem
  ca_private_key_pem    = tls_private_key.ca.private_key_pem
  ca_cert_pem           = tls_self_signed_cert.ca.cert_pem
  validity_period_hours = 8760
  early_renewal_hours   = 720
  allowed_uses          = ["digital_signature", "key_encipherment", "server_auth"]
}
resource "aws_acm_certificate" "server" {
  private_key       = tls_private_key.server.private_key_pem
  certificate_body  = tls_locally_signed_cert.server.cert_pem
  certificate_chain = tls_self_signed_cert.ca.cert_pem
  lifecycle { create_before_destroy = true }
}
resource "aws_lb_target_group" "this" {
  name                 = var.name
  port                 = 8080
  protocol             = "HTTP"
  vpc_id               = aws_vpc.this.id
  deregistration_delay = 30
  health_check {
    path                = "/healthz"
    matcher             = "200"
    interval            = 15
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 2
  }
}
resource "aws_lb_listener" "https" {
  load_balancer_arn = aws_lb.this.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = aws_acm_certificate.server.arn
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.this.arn
  }
}
resource "aws_lb_target_group_attachment" "server" {
  target_group_arn = aws_lb_target_group.this.arn
  target_id        = aws_instance.server.id
  port             = 8080
}
