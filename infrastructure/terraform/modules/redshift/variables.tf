variable "project_name" {
  type        = string
  description = "Project name prefix for resources"
}

variable "environment" {
  type        = string
  description = "Deployment environment"
}

variable "private_db_subnet_ids" {
  type        = list(string)
  description = "List of private subnet IDs for Redshift Serverless workgroup"
}

variable "redshift_sg_id" {
  type        = string
  description = "Security group ID for Redshift Serverless"
}

variable "monthly_cost_cap_usd" {
  type        = number
  description = "Monthly compute cost cap in USD for Redshift Serverless"
  default     = 10
}
