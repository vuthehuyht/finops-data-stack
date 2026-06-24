output "vpc_id" {
  value       = module.vpc.vpc_id
  description = "The ID of the VPC created"
}

output "ecr_repository_url" {
  value       = module.ecr.repository_url
  description = "The URL of the ECR repository"
}

output "raw_bucket_id" {
  value       = module.s3.raw_bucket_id
  description = "The Name of the Raw S3 bucket"
}

output "processed_bucket_id" {
  value       = module.s3.processed_bucket_id
  description = "The Name of the Processed S3 bucket"
}

output "model_artifacts_bucket_id" {
  value       = module.s3.model_artifacts_bucket_id
  description = "The Name of the Model Artifacts S3 bucket"
}

output "redshift_endpoint" {
  value       = module.redshift.endpoint
  description = "The Redshift Serverless endpoint address"
}

output "eks_cluster_name" {
  value       = module.eks.cluster_name
  description = "The name of the EKS Cluster"
}

output "eks_cluster_endpoint" {
  value       = module.eks.cluster_endpoint
  description = "The endpoint of the EKS Cluster"
}

output "dagster_sa_role_arn" {
  value       = module.eks.dagster_sa_role_arn
  description = "The ARN of the IAM role for the Dagster Service Account (IRSA)"
}

output "sagemaker_execution_role_arn" {
  value       = module.sagemaker.execution_role_arn
  description = "The ARN of the SageMaker execution IAM role"
}

# RDS PostgreSQL for Dagster Metadata Outputs
output "rds_endpoint" {
  value       = module.rds.rds_instance_endpoint
  description = "The connection endpoint for the RDS instance (Dagster metadata)"
}

output "rds_address" {
  value       = module.rds.rds_instance_address
  description = "The address of the RDS instance (Dagster metadata)"
}

output "db_credentials_secret_arn" {
  value       = module.secrets.db_credentials_secret_arn
  description = "The ARN of the AWS Secrets Manager secret containing consolidated database credentials"
}

output "rds_secret_arn" {
  value       = module.secrets.db_credentials_secret_arn
  description = "The ARN of the AWS Secrets Manager secret containing RDS credentials (deprecated, use db_credentials_secret_arn)"
}

