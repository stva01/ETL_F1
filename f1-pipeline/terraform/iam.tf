
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


#redshift 

data "aws_iam_policy_document" "redshift_trust_doc" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "redshift_s3_readonly_role" {
  name               = "${var.project_name}-redshift-s3-role"
  assume_role_policy = data.aws_iam_policy_document.redshift_trust_doc.json
}

resource "aws_iam_role_policy_attachment" "s3_readonly_attach" {
  role       = aws_iam_role.redshift_s3_readonly_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}