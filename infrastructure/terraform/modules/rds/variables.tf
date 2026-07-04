variable "project_name" {
  type        = string
  description = "Project name prefix for resources"
}

variable "environment" {
  type        = string
  description = "Deployment environment"
}

variable "vpc_id" {
  type        = string
  description = "The ID of the VPC"
}

variable "private_db_subnet_ids" {
  type        = list(string)
  description = "List of private database subnet IDs where RDS should be deployed"
}

variable "instance_class" {
  type        = string
  default     = "db.t4g.micro"
  description = "The instance class of the RDS PostgreSQL database"
}

variable "allocated_storage" {
  type        = number
  default     = 20
  description = "Allocated storage in GB"
}
