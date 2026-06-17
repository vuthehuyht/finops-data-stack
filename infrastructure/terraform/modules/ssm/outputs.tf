output "active_version_arn" {
  value       = aws_ssm_parameter.model_active_version.arn
  description = "The ARN of the active model version parameter"
}

output "endpoint_name_arn" {
  value       = aws_ssm_parameter.model_endpoint_name.arn
  description = "The ARN of the active endpoint name parameter"
}

output "evaluation_threshold_arn" {
  value       = aws_ssm_parameter.model_evaluation_threshold.arn
  description = "The ARN of the evaluation threshold parameter"
}
