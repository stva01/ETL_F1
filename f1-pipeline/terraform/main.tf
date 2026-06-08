provider "aws" {
  region = var.aws_region
}

# Raw data bucket
resource "aws_s3_bucket" "raw" {
  bucket = "${var.project_name}-raw-layer-034381339055"
  tags   = { Project = var.project_name, Stage = "raw" } # Changed ; to ,
} # Removed trailing ;

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Processed data bucket (Where your Glue Parquet files live)
resource "aws_s3_bucket" "processed" {
  bucket = "${var.project_name}-processed-layer-034381339055"
  tags   = { Project = var.project_name, Stage = "processed" } # Changed ; to ,
}

resource "aws_s3_bucket_public_access_block" "processed" {
  bucket                  = aws_s3_bucket.processed.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}