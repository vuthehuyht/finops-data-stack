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

output "github_actions_deploy_role_arn" {
  value       = aws_iam_role.github_actions_deploy.arn
  description = "The ARN of the IAM role assumed by the GitHub Actions CI/CD deploy pipeline"
}

output "cluster_security_group_id" {
  value       = aws_eks_cluster.main.vpc_config[0].cluster_security_group_id
  description = "The ID of the security group EKS auto-creates and attaches to managed node group instances (distinct from var.eks_node_sg_id, which is only used for the cluster's vpc_config.security_group_ids and is NOT what gets attached to node EC2 instances by default -- verified against the running node's actual attached security groups)"
}

output "cluster_autoscaler_role_arn" {
  value       = aws_iam_role.cluster_autoscaler.arn
  description = "The ARN of the IAM role for the Cluster Autoscaler ServiceAccount (IRSA)"
}
