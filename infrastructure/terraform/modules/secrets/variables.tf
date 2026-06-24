variable "project_name" {
  type        = string
  description = "Project name prefix for resources"
}

variable "environment" {
  type        = string
  description = "Deployment environment"
}

variable "rds_host" {
  type        = string
  description = "RDS PostgreSQL host address"
}

variable "rds_port" {
  type        = number
  default     = 5432
  description = "RDS PostgreSQL port"
}

variable "rds_username" {
  type        = string
  sensitive   = true
  description = "RDS PostgreSQL username"
}

variable "rds_password" {
  type        = string
  sensitive   = true
  description = "RDS PostgreSQL password"
}

variable "rds_dbname" {
  type        = string
  description = "RDS PostgreSQL database name"
}

variable "redshift_host" {
  type        = string
  description = "Redshift Serverless host address"
}

variable "redshift_port" {
  type        = number
  default     = 5439
  description = "Redshift Serverless port"
}

variable "redshift_username" {
  type        = string
  sensitive   = true
  description = "Redshift Serverless admin username"
}

variable "redshift_password" {
  type        = string
  sensitive   = true
  description = "Redshift Serverless admin password"
}

variable "redshift_dbname" {
  type        = string
  description = "Redshift Serverless database name"
}
