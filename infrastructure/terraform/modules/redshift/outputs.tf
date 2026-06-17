output "endpoint" {
  value       = aws_redshiftserverless_workgroup.main.endpoint[0].address
  description = "The Redshift Serverless endpoint connection address"
}

output "port" {
  value       = aws_redshiftserverless_workgroup.main.endpoint[0].port
  description = "The Redshift Serverless port"
}

output "database_name" {
  value       = aws_redshiftserverless_namespace.main.db_name
  description = "The name of the Redshift database created"
}

output "admin_username" {
  value       = aws_redshiftserverless_namespace.main.admin_username
  sensitive   = true
  description = "The database admin username (sensitive)"
}

output "admin_password" {
  value       = random_password.redshift_admin.result
  sensitive   = true
  description = "The database admin password (sensitive)"
}
