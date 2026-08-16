variable "project_name" {
  type        = string
  description = "Project name prefix for resources"
}

variable "environment" {
  type        = string
  description = "Deployment environment"
}

variable "model_artifacts_bucket_arn" {
  type        = string
  description = "The ARN of the S3 bucket storing model artifacts"
}

variable "processed_bucket_arn" {
  type        = string
  description = "The ARN of the S3 bucket storing processed data (ML training data)"
}
