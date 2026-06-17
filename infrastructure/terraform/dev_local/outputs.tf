output "redshift_endpoint_address" {
  value       = aws_redshiftserverless_workgroup.main.endpoint[0].address
  description = "The public connection endpoint address for Redshift Serverless"
}

output "redshift_port" {
  value       = aws_redshiftserverless_workgroup.main.endpoint[0].port
  description = "The Redshift connection port"
}

output "redshift_database" {
  value       = aws_redshiftserverless_namespace.main.db_name
  description = "The name of the database created"
}

output "redshift_admin_username" {
  value       = aws_redshiftserverless_namespace.main.admin_username
  sensitive   = true
  description = "The database admin username (sensitive)"
}

output "redshift_admin_password" {
  value       = var.redshift_admin_password
  sensitive   = true
  description = "The database admin password (sensitive)"
}

output "raw_bucket_name" {
  value       = aws_s3_bucket.raw.bucket
  description = "The S3 raw data lake bucket name"
}

output "processed_bucket_name" {
  value       = aws_s3_bucket.processed.bucket
  description = "The S3 processed data lake bucket name"
}

output "model_artifacts_bucket_name" {
  value       = aws_s3_bucket.model_artifacts.bucket
  description = "The S3 model artifacts bucket name"
}
