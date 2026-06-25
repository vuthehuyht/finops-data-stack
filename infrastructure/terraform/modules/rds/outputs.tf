output "rds_instance_endpoint" {
  value       = aws_db_instance.dagster_meta.endpoint
  description = "The connection endpoint for the RDS instance"
}

output "rds_instance_address" {
  value       = aws_db_instance.dagster_meta.address
  description = "The address of the RDS instance"
}

output "rds_instance_id" {
  value       = aws_db_instance.dagster_meta.id
  description = "The ID of the RDS database instance"
}

output "rds_security_group_id" {
  value       = aws_security_group.rds.id
  description = "The Security Group ID of the RDS instance"
}

output "rds_username" {
  value       = aws_db_instance.dagster_meta.username
  sensitive   = true
  description = "The username of the RDS database instance"
}

output "rds_password" {
  value       = random_password.rds_password.result
  sensitive   = true
  description = "The password of the RDS database instance"
}

output "rds_dbname" {
  value       = aws_db_instance.dagster_meta.db_name
  description = "The database name of the RDS database instance"
}
