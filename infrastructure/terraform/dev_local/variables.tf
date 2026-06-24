variable "aws_region" {
  type        = string
  default     = "ap-southeast-1"
  description = "AWS region to deploy resources"
}

variable "project_name" {
  type        = string
  default     = "finops-dev"
  description = "Project name prefix for resources"
}

variable "environment" {
  type        = string
  default     = "dev"
  description = "Target deployment environment"
}

variable "vpc_cidr" {
  type        = string
  default     = "10.99.0.0/16"
  description = "VPC CIDR block for dev environment"
}

variable "availability_zones" {
  type        = list(string)
  default     = ["ap-southeast-1a", "ap-southeast-1b"]
  description = "List of availability zones"
}

variable "allowed_ips" {
  type        = list(string)
  description = "List of public CIDRs/IPs allowed to access Redshift Serverless (e.g. ['115.79.1.2/32'])"
}

variable "redshift_admin_username" {
  type        = string
  description = "Admin username for Redshift Serverless (can be set via TF_VAR_redshift_admin_username env)"
}

variable "redshift_admin_password" {
  type        = string
  sensitive   = true
  description = "Admin password for Redshift Serverless (can be set via TF_VAR_redshift_admin_password env)"
}

variable "monthly_cost_cap_usd" {
  type        = number
  description = "Monthly compute cost cap in USD for Redshift Serverless"
  default     = 10
}
