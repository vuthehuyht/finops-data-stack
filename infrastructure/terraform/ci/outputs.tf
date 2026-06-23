output "redshift_endpoint" {
  value       = aws_redshiftserverless_workgroup.main.endpoint[0].address
  description = "Redshift Serverless endpoint address"
}

output "redshift_port" {
  value       = aws_redshiftserverless_workgroup.main.endpoint[0].port
  description = "Redshift Serverless endpoint port"
}

output "redshift_database" {
  value       = aws_redshiftserverless_namespace.main.db_name
  description = "Redshift Serverless database name"
}

output "redshift_jdbc_url" {
  value       = "jdbc:redshift://${aws_redshiftserverless_workgroup.main.endpoint[0].address}:${aws_redshiftserverless_workgroup.main.endpoint[0].port}/${aws_redshiftserverless_namespace.main.db_name}"
  description = "Redshift Serverless JDBC Connection URL"
}
