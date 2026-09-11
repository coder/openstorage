resource "aws_s3_bucket" "data" {
  bucket_prefix = "${var.name}-data-"
  force_destroy = var.allow_destroy_persistent_data
}
resource "aws_s3_bucket" "artifact" {
  bucket_prefix = "${var.name}-artifact-"
  force_destroy = true
}
locals {
  buckets = { data = aws_s3_bucket.data, artifact = aws_s3_bucket.artifact }
}
resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = local.buckets
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = local.buckets
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}
resource "aws_s3_bucket_ownership_controls" "this" {
  for_each = local.buckets
  bucket   = each.value.id
  rule { object_ownership = "BucketOwnerEnforced" }
}
resource "aws_s3_bucket_policy" "tls" {
  for_each = local.buckets
  bucket   = each.value.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport", Effect = "Deny", Principal = "*", Action = "s3:*"
      Resource  = [each.value.arn, "${each.value.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}
resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload { days_after_initiation = 1 }
  }
}
resource "aws_s3_object" "release" {
  bucket                 = aws_s3_bucket.artifact.id
  key                    = "releases/${var.release_tag}/${var.release_sha256}.tar.gz"
  source                 = var.release_archive
  source_hash            = var.release_sha256
  server_side_encryption = "AES256"
  lifecycle {
    precondition {
      condition     = try(filesha256(var.release_archive) == var.release_sha256, false)
      error_message = "The local release archive must exist and match the pinned SHA-256."
    }
  }
  depends_on = [aws_s3_bucket_public_access_block.this, aws_s3_bucket_policy.tls]
}

resource "aws_db_subnet_group" "this" {
  name       = var.name
  subnet_ids = aws_subnet.database[*].id
}
resource "aws_db_instance" "this" {
  identifier                   = var.name
  engine                       = "postgres"
  engine_version               = "17"
  instance_class               = "db.t4g.micro"
  allocated_storage            = 20
  max_allocated_storage        = 20
  storage_type                 = "gp3"
  storage_encrypted            = true
  db_name                      = "openstorage"
  username                     = "openstorage"
  manage_master_user_password  = true
  db_subnet_group_name         = aws_db_subnet_group.this.name
  vpc_security_group_ids       = [aws_security_group.database.id]
  publicly_accessible          = false
  multi_az                     = false
  backup_retention_period      = 7
  auto_minor_version_upgrade   = true
  deletion_protection          = !var.allow_destroy_persistent_data
  skip_final_snapshot          = true
  delete_automated_backups     = true
  copy_tags_to_snapshot        = true
  performance_insights_enabled = false
  timeouts {
    create = "40m"
    update = "40m"
    delete = "40m"
  }
}
