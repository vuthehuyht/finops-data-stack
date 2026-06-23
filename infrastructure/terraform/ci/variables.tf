variable "aws_region" {
  type        = string
  default     = "ap-southeast-1"
  description = "AWS region to deploy CI resources"
}

variable "project_name" {
  type        = string
  default     = "finops-ci"
  description = "Project name prefix for CI resources"
}

variable "environment" {
  type        = string
  default     = "ci"
  description = "Target deployment environment"
}

variable "redshift_admin_username" {
  type        = string
  default     = "admin"
  description = "Admin username for Redshift Serverless CI"
}

variable "redshift_admin_password" {
  type        = string
  sensitive   = true
  description = "Admin password for Redshift Serverless CI. Must be 8-64 characters, containing at least 1 uppercase, 1 lowercase, and 1 number. Do NOT use '/', '@', '\"', ' ', '\\', or single quote."
}
