variable "aws_region" {
  type        = string
  default     = "ap-southeast-1"
  description = "AWS region to deploy resources"
}

variable "project_name" {
  type        = string
  default     = "finops"
  description = "Project name prefix for resources"
}

variable "environment" {
  type        = string
  default     = "production"
  description = "Deployment environment (e.g. dev, prod, staging)"
}
