terraform {
  required_version = "= 1.16.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "= 6.69.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "= 4.4.1"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { Project = "openstorage-reference", Deployment = var.name }
  }
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
data "aws_availability_zones" "available" { state = "available" }
data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}
