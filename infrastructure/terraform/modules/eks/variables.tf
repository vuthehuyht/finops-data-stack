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
  description = "The VPC ID where EKS is deployed"
}

variable "private_app_subnet_ids" {
  type        = list(string)
  description = "List of private subnet IDs for EKS node groups"
}

variable "eks_node_sg_id" {
  type        = string
  description = "Security group ID for EKS worker nodes"
}

variable "model_artifacts_bucket_arn" {
  type        = string
  description = "ARN of model artifacts S3 bucket for IRSA permissions"
}

variable "raw_bucket_arn" {
  type        = string
  description = "ARN of raw data lake S3 bucket for IRSA permissions"
}

variable "processed_bucket_arn" {
  type        = string
  description = "ARN of processed data lake S3 bucket for IRSA permissions"
}

variable "db_credentials_secret_arn" {
  type        = string
  description = "ARN of the consolidated AWS Secrets Manager secret (finops-db-credentials) for External Secrets Operator IRSA permissions"
}

variable "cluster_admin_principal_arns" {
  type        = list(string)
  default     = []
  description = "Additional IAM principal ARNs (e.g. human operators running kubectl/helm manually) granted EKS cluster-admin access via Access Entries. bootstrap_cluster_creator_admin_permissions is intentionally not set on the cluster (ForceNew risk), so the cluster creator does not get automatic access -- add their ARN here instead."
}
