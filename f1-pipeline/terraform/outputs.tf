
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
