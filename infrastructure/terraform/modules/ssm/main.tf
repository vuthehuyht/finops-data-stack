resource "aws_ssm_parameter" "model_active_version" {
  name        = "/${var.project_name}/model/active_version"
  type        = "String"
  value       = "v1"
  description = "The active deployed version of the ML model"

  tags = {
    Environment = var.environment
  }
}

resource "aws_ssm_parameter" "model_endpoint_name" {
  name        = "/${var.project_name}/model/endpoint_name"
  type        = "String"
  value       = "pending"
  description = "The active SageMaker Serverless Inference Endpoint name"

  tags = {
    Environment = var.environment
  }
}

resource "aws_ssm_parameter" "model_evaluation_threshold" {
  name        = "/${var.project_name}/model/evaluation_threshold"
  type        = "String"
  value       = "0.75"
  description = "The minimum accuracy evaluation threshold for retraining"

  tags = {
    Environment = var.environment
  }
}
