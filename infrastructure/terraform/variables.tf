variable "aws_region" {
  type        = string
  default     = "ap-southeast-1"
  description = "AWS region to deploy resources"
}

variable "aws_profile" {
  type        = string
  default     = "default"
  description = "AWS CLI profile to use"
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

variable "fireant_email" {
  type        = string
  sensitive   = true
  description = "FireAnt account email, for analyst reports ingestion"
}

variable "fireant_password" {
  type        = string
  sensitive   = true
  description = "FireAnt account password, for analyst reports ingestion"
}

variable "fireant_proxy_url" {
  type        = string
  sensitive   = true
  description = "FireAnt proxy URL to bypass WAF"
}

variable "cluster_admin_principal_arns" {
  type        = list(string)
  default     = []
  description = "Additional IAM principal ARNs (e.g. human operators running kubectl/helm manually) granted EKS cluster-admin access via Access Entries"
}

variable "slack_api_token" {
  type        = string
  sensitive   = true
  description = "Slack API token for Dagster alerts"
}

variable "slack_channel_id" {
  type        = string
  description = "Slack Channel ID for alerts"
}
