output "repository_url" {
  value       = aws_ecr_repository.dagster_app.repository_url
  description = "The URL of the ECR repository for pushing Docker images"
}

output "repository_arn" {
  value       = aws_ecr_repository.dagster_app.arn
  description = "The ARN of the ECR repository"
}
