resource "aws_vpc" "this" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  lifecycle {
    precondition {
      condition     = length(data.aws_availability_zones.available.names) >= 2 && data.aws_partition.current.partition == "aws"
      error_message = "This reference requires a commercial AWS region with at least two available AZs."
    }
  }
}

resource "aws_internet_gateway" "this" { vpc_id = aws_vpc.this.id }

resource "aws_subnet" "public" {
  count             = 2
  vpc_id            = aws_vpc.this.id
  availability_zone = data.aws_availability_zones.available.names[count.index]
  cidr_block        = cidrsubnet(aws_vpc.this.cidr_block, 8, count.index)
}

resource "aws_subnet" "database" {
  count             = 2
  vpc_id            = aws_vpc.this.id
  availability_zone = data.aws_availability_zones.available.names[count.index]
  cidr_block        = cidrsubnet(aws_vpc.this.cidr_block, 8, count.index + 10)
}

resource "aws_route_table" "public" { vpc_id = aws_vpc.this.id }
resource "aws_route" "internet" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.this.id
}
resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "alb" {
  name_prefix = "${var.name}-alb-"
  vpc_id      = aws_vpc.this.id
}
resource "aws_security_group" "server" {
  name_prefix = "${var.name}-server-"
  vpc_id      = aws_vpc.this.id
}
resource "aws_security_group" "database" {
  name_prefix = "${var.name}-db-"
  vpc_id      = aws_vpc.this.id
}

resource "aws_vpc_security_group_ingress_rule" "https" {
  for_each          = var.allowed_client_cidrs
  security_group_id = aws_security_group.alb.id
  cidr_ipv4         = each.value
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}
resource "aws_vpc_security_group_egress_rule" "alb" {
  security_group_id            = aws_security_group.alb.id
  referenced_security_group_id = aws_security_group.server.id
  ip_protocol                  = "tcp"
  from_port                    = 8080
  to_port                      = 8080
}
resource "aws_vpc_security_group_ingress_rule" "server" {
  security_group_id            = aws_security_group.server.id
  referenced_security_group_id = aws_security_group.alb.id
  ip_protocol                  = "tcp"
  from_port                    = 8080
  to_port                      = 8080
}
resource "aws_vpc_security_group_ingress_rule" "database" {
  security_group_id            = aws_security_group.database.id
  referenced_security_group_id = aws_security_group.server.id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
}
# Outbound HTTPS is needed for AL2023 packages, AWS APIs and client presigned S3 access.
resource "aws_vpc_security_group_egress_rule" "https" {
  security_group_id = aws_security_group.server.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}
resource "aws_vpc_security_group_egress_rule" "database" {
  security_group_id            = aws_security_group.server.id
  referenced_security_group_id = aws_security_group.database.id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
}
