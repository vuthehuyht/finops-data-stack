output "db_credentials_secret_arn" {
  value       = aws_secretsmanager_secret.credentials.arn
  description = "The ARN of the consolidated credentials secret (DB + API tokens)"
}
