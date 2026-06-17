# ============================================================================
# GLUE IAM ROLE (Trust + Permissions)
# ============================================================================

data "aws_iam_policy_document" "glue_trust_doc" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_job_role" {
  name               = "${var.project_name}-glue-job-role"
  assume_role_policy = data.aws_iam_policy_document.glue_trust_doc.json
}

# S3 permissions for Glue
resource "aws_iam_role_policy" "glue_s3_policy" {
  name   = "glue-s3-access"
  role   = aws_iam_role.glue_job_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
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

# CloudWatch logs permission for Glue
resource "aws_iam_role_policy" "glue_logs_policy" {
  name   = "glue-logs-access"
  role   = aws_iam_role.glue_job_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# ============================================================================
# GLUE JOBS (Jolpica, OpenF1, Kaggle)
# ============================================================================

resource "aws_glue_job" "jolpica_etl" {
  name              = "${var.project_name}-jolpica-etl"
  role_arn          = aws_iam_role.glue_job_role.arn
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.raw.id}/glue-scripts/02_glue_jolpica_final.py"
  }
  default_arguments = {
    "--job-bookmark-option" = "job-bookmark-enable"
  }
  max_retries = 1
  timeout     = 30
  glue_version = "4.0"

  tags = { Project = var.project_name, Source = "jolpica" }
}

resource "aws_glue_job" "openf1_etl" {
  name              = "${var.project_name}-openf1-etl"
  role_arn          = aws_iam_role.glue_job_role.arn
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.raw.id}/glue-scripts/03_glue_openf1_final.py"
  }
  default_arguments = {
    "--job-bookmark-option" = "job-bookmark-enable"
  }
  max_retries = 1
  timeout     = 30
  glue_version = "4.0"

  tags = { Project = var.project_name, Source = "openf1" }
}

resource "aws_glue_job" "kaggle_etl" {
  name              = "${var.project_name}-kaggle-etl"
  role_arn          = aws_iam_role.glue_job_role.arn
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.raw.id}/glue-scripts/01_glue_kaggle_final.py"
  }
  default_arguments = {
    "--job-bookmark-option" = "job-bookmark-enable"
  }
  max_retries = 1
  timeout     = 30
  glue_version = "4.0"

  tags = { Project = var.project_name, Source = "kaggle" }
}