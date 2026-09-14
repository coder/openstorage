resource "aws_iam_role" "server" {
  name_prefix = "${var.name}-"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "ec2.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}
resource "aws_iam_role_policy" "server" {
  role = aws_iam_role.server.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "RepositoryBucket", Effect = "Allow"
        Action    = ["s3:ListBucket", "s3:GetBucketVersioning"]
        Resource  = [aws_s3_bucket.data.arn]
        Condition = { StringEquals = { "aws:ResourceAccount" = data.aws_caller_identity.current.account_id } }
      },
      {
        Sid       = "RepositoryObjects", Effect = "Allow"
        Action    = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource  = ["${aws_s3_bucket.data.arn}/*"]
        Condition = { StringEquals = { "aws:ResourceAccount" = data.aws_caller_identity.current.account_id } }
      },
      {
        Sid       = "PinnedRelease", Effect = "Allow", Action = ["s3:GetObject"]
        Resource  = [aws_s3_object.release.arn]
        Condition = { StringEquals = { "aws:ResourceAccount" = data.aws_caller_identity.current.account_id } }
      },
      {
        Sid      = "DatabaseCredential", Effect = "Allow", Action = ["secretsmanager:GetSecretValue"]
        Resource = [aws_db_instance.this.master_user_secret[0].secret_arn]
      }
    ]
  })
}
resource "aws_iam_instance_profile" "server" { role = aws_iam_role.server.name }
