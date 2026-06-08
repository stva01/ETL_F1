# 1. Redshift Serverless Namespace (The Storage & DB Configuration Layer)
resource "aws_redshiftserverless_namespace" "f1_namespace" {
  namespace_name      = "${var.project_name}-namespace"
  db_name             = "f1_db"
  admin_username      = "awsadmin"
  admin_user_password = "SecurePassword123!" # Change this!

  # Binds our read-only S3 role straight into the compute instance cluster
  iam_roles = [aws_iam_role.redshift_s3_readonly_role.arn]
}

# 2. Redshift Serverless Workgroup (The Compute & Networking Engine Layer)
resource "aws_redshiftserverless_workgroup" "f1_workgroup" {
  workgroup_name = "${var.project_name}-workgroup"
  namespace_name = aws_redshiftserverless_namespace.f1_namespace.namespace_name
  base_capacity  = 8 # Scaled down to minimize base running costs
  
  # Crucial for testing: allows your local machine/dbt to hit the endpoint directly
  publicly_accessible = true 
}