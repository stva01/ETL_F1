provider "aws" {
  region = var.aws_region
}

# Raw data bucket — stores Kaggle CSVs and API responses
resource "aws_s3_bucket" "raw" {
  bucket = "${var.project_name}-raw-layer-034381339055"

  tags = {
    Project = var.project_name
    Stage   = "raw"
  }
}

# Block all public access
resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Processed data bucket — stores Parquet after Glue transforms
resource "aws_s3_bucket" "processed" {
  bucket = "${var.project_name}-processed-layer-034381339055"

  tags = {
    Project = var.project_name
    Stage   = "processed"
  }
}

resource "aws_s3_bucket_public_access_block" "processed" {
  bucket                  = aws_s3_bucket.processed.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# IAM user for Python ingestion scripts
resource "aws_iam_user" "pipeline_user" {
  name = "${var.project_name}-ingest-user"
}

resource "aws_iam_user_policy" "pipeline_user_policy" {
  name = "s3-pipeline-access"
  user = aws_iam_user.pipeline_user.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.raw.arn,
          "${aws_s3_bucket.raw.arn}/*",
          aws_s3_bucket.processed.arn,
          "${aws_s3_bucket.processed.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_access_key" "pipeline_user" {
  user = aws_iam_user.pipeline_user.name
}
