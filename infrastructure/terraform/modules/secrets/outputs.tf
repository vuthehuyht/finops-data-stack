output "db_credentials_secret_arn" {
  value       = aws_secretsmanager_secret.db_credentials.arn
  description = "The ARN of the DB credentials secret"
}

output "api_tokens_secret_arn" {
  value       = aws_secretsmanager_secret.api_tokens.arn
  description = "The ARN of the API tokens secret"
}
