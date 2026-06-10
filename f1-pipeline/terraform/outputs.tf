
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

# Glue Jobs
output "glue_jolpica_job_name" {
  value = aws_glue_job.jolpica_etl.name
}

output "glue_openf1_job_name" {
  value = aws_glue_job.openf1_etl.name
}

output "glue_kaggle_job_name" {
  value = aws_glue_job.kaggle_etl.name
}

output "glue_job_role_arn" {
  value = aws_iam_role.glue_job_role.arn
}