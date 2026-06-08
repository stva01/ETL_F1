# ==========================================
# KEEPING YOUR EXISTING OUTPUTS
# ==========================================
output "raw_bucket_name" {
  value = aws_s3_bucket.raw.bucket
}

output "processed_bucket_name" {
  value = aws_s3_bucket.processed.bucket
}

output "ingest_user_access_key" {
  value     = aws_iam_access_key.pipeline_user.id
  sensitive = true
}

output "ingest_user_secret_key" {
  value     = aws_iam_access_key.pipeline_user.secret
  sensitive = true
}

# ==========================================
# ADDING YOUR NEW REDSHIFT OUTPUTS
# ==========================================
output "redshift_role_arn" {
  value       = aws_iam_role.redshift_s3_readonly_role.arn
  description = "Use this ARN string inside your raw SQL COPY INTO query statements"
}

output "redshift_endpoint" {
  value       = aws_redshiftserverless_workgroup.f1_workgroup.endpoint[0].address
  description = "Host endpoint domain URL address for client UI tooling connections"
}