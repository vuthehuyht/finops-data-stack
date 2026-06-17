output "execution_role_arn" {
  value       = aws_iam_role.sagemaker_execution.arn
  description = "The ARN of the SageMaker execution IAM role"
}
