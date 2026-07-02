output "cluster_name" {
  value       = aws_eks_cluster.main.name
  description = "The name of the EKS Cluster"
}

output "cluster_endpoint" {
  value       = aws_eks_cluster.main.endpoint
  description = "The endpoint for the EKS Cluster Control Plane"
}

output "cluster_certificate_authority_data" {
  value       = aws_eks_cluster.main.certificate_authority[0].data
  description = "Base64 encoded certificate data required to communicate with the cluster"
}

output "dagster_sa_role_arn" {
  value       = aws_iam_role.dagster_service_account.arn
  description = "The ARN of the IAM role for the Dagster Service Account (IRSA)"
}

output "external_secrets_sa_role_arn" {
  value       = aws_iam_role.external_secrets_sa.arn
  description = "The ARN of the IAM role for the External Secrets Operator ServiceAccount (IRSA)"
}
