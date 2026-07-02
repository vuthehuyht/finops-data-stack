# 1. IAM Role cho EKS Cluster Control Plane
resource "aws_iam_role" "eks_cluster" {
  name = "${var.project_name}-eks-cluster-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "eks.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Environment = var.environment
  }
}

resource "aws_iam_role_policy_attachment" "eks_cluster_policy" {
  role       = aws_iam_role.eks_cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

# 2. Amazon EKS Cluster
resource "aws_eks_cluster" "main" {
  name     = "${var.project_name}-eks-cluster"
  role_arn = aws_iam_role.eks_cluster.arn
  version  = "1.36"

  vpc_config {
    subnet_ids              = var.private_app_subnet_ids
    security_group_ids      = [var.eks_node_sg_id]
    endpoint_private_access = true
    endpoint_public_access  = true
  }

  # Required for aws_eks_access_entry / aws_eks_access_policy_association
  # (added below) to have any effect -- without this block, Terraform/AWS
  # default new clusters to authentication_mode = CONFIG_MAP, which ignores
  # access entries entirely.
  access_config {
    authentication_mode                         = "API_AND_CONFIG_MAP"
    bootstrap_cluster_creator_admin_permissions = true
  }

  depends_on = [
    aws_iam_role_policy_attachment.eks_cluster_policy
  ]

  tags = {
    Environment = var.environment
  }
}

# 3. IAM Role cho EKS Node Groups (Worker Nodes)
resource "aws_iam_role" "eks_nodes" {
  name = "${var.project_name}-eks-node-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Environment = var.environment
  }
}

resource "aws_iam_role_policy_attachment" "eks_worker_node_policy" {
  role       = aws_iam_role.eks_nodes.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
}

resource "aws_iam_role_policy_attachment" "eks_cni_policy" {
  role       = aws_iam_role.eks_nodes.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
}

resource "aws_iam_role_policy_attachment" "eks_registry_policy" {
  role       = aws_iam_role.eks_nodes.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

# 4. Hybrid EKS Node Groups
# 4.1. Core System Node Group (On-Demand)
resource "aws_eks_node_group" "core_system" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "${var.project_name}-core-system-ng"
  node_role_arn   = aws_iam_role.eks_nodes.arn
  subnet_ids      = var.private_app_subnet_ids

  capacity_type  = "ON_DEMAND"
  instance_types = ["t3a.medium"] # Optimization: use cheaper AMD instance type

  # Used by K8s nodeSelector (infrastructure/helm/values.yaml) to pin the
  # always-on webserver/daemon/user-code pods to this node group.
  labels = {
    "node-group" = "core"
  }

  scaling_config {
    desired_size = 1
    min_size     = 1
    max_size     = 1 # Lock to 1 node to prevent accidental scale-up cost
  }

  update_config {
    max_unavailable = 1
  }

  depends_on = [
    aws_iam_role_policy_attachment.eks_worker_node_policy,
    aws_iam_role_policy_attachment.eks_cni_policy,
    aws_iam_role_policy_attachment.eks_registry_policy
  ]

  tags = {
    Name        = "${var.project_name}-core-system-node"
    Environment = var.environment
  }
}

# 4.2. Worker Workload Node Group (Spot - Cost savings, auto-scaling)
resource "aws_eks_node_group" "worker_workload" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "${var.project_name}-worker-workload-ng"
  node_role_arn   = aws_iam_role.eks_nodes.arn
  subnet_ids      = var.private_app_subnet_ids

  capacity_type  = "SPOT"
  instance_types = ["t3a.medium", "t3a.small"] # Cheap AMD instances

  # Used by K8s nodeSelector (infrastructure/helm/values.yaml) to pin run
  # pods to this node group; paired with the spotWorker taint below.
  labels = {
    "node-group" = "worker"
  }

  scaling_config {
    desired_size = 0 # Start at 0 to save idle costs
    min_size     = 0
    max_size     = 3 # Cap at 3 to prevent runaway scaling costs
  }

  # Configure taints to prevent system pods from running on Spot Nodes
  taint {
    key    = "spotWorker"
    value  = "true"
    effect = "NO_SCHEDULE"
  }

  update_config {
    max_unavailable = 1
  }

  depends_on = [
    aws_iam_role_policy_attachment.eks_worker_node_policy,
    aws_iam_role_policy_attachment.eks_cni_policy,
    aws_iam_role_policy_attachment.eks_registry_policy
  ]

  tags = {
    Name        = "${var.project_name}-worker-workload-node"
    Environment = var.environment
  }
}

# 5. IAM OIDC Provider cho IRSA (IAM Roles for Service Accounts)
data "tls_certificate" "eks" {
  url = aws_eks_cluster.main.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "eks" {
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.eks.certificates[0].sha1_fingerprint]
  url             = aws_eks_cluster.main.identity[0].oidc[0].issuer
}

# 6. IRSA: IAM Role for Dagster Service Account running on EKS
resource "aws_iam_role" "dagster_service_account" {
  name = "${var.project_name}-dagster-sa-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = aws_iam_openid_connect_provider.eks.arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            # Restrict to Service Account named "dagster-sa" in "default" or "dagster" namespace
            "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:sub" = [
              "system:serviceaccount:default:dagster-sa",
              "system:serviceaccount:dagster:dagster-sa"
            ]
          }
        }
      }
    ]
  })

  tags = {
    Environment = var.environment
  }
}

# 6.1. IAM Policy assigned to Dagster Service Account (S3, SSM, SageMaker)
resource "aws_iam_policy" "dagster_sa_permissions" {
  name        = "${var.project_name}-dagster-sa-policy"
  description = "Permissions for Dagster SA to access S3 Data Lake, SSM Parameters and SageMaker ML jobs"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Read/write permissions for S3 Data Lake & Model Artifacts
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Resource = [
          var.raw_bucket_arn,
          "${var.raw_bucket_arn}/*",
          var.processed_bucket_arn,
          "${var.processed_bucket_arn}/*",
          var.model_artifacts_bucket_arn,
          "${var.model_artifacts_bucket_arn}/*"
        ]
      },
      # Read/write permissions for SSM Parameter Store related to Model
      {
        Effect = "Allow"
        Action = [
          "ssm:GetParameter",
          "ssm:GetParameters",
          "ssm:PutParameter"
        ]
        Resource = [
          "arn:aws:ssm:*:*:parameter/${var.project_name}/model/*"
        ]
      },
      # SageMaker orchestration permissions (Training, Serverless Endpoint, Invoke)
      {
        Effect = "Allow"
        Action = [
          "sagemaker:CreateTrainingJob",
          "sagemaker:DescribeTrainingJob",
          "sagemaker:StopTrainingJob",
          "sagemaker:CreateModel",
          "sagemaker:DeleteModel",
          "sagemaker:DescribeModel",
          "sagemaker:CreateEndpointConfig",
          "sagemaker:DeleteEndpointConfig",
          "sagemaker:DescribeEndpointConfig",
          "sagemaker:CreateEndpoint",
          "sagemaker:DeleteEndpoint",
          "sagemaker:DescribeEndpoint",
          "sagemaker:UpdateEndpoint",
          "sagemaker:InvokeEndpoint",
          "iam:PassRole" # Required PassRole for SageMaker execution role
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "dagster_sa_permissions_attach" {
  role       = aws_iam_role.dagster_service_account.name
  policy_arn = aws_iam_policy.dagster_sa_permissions.arn
}

# 7. IRSA: IAM Role for External Secrets Operator, to sync AWS Secrets Manager
# secrets into k8s Secrets (e.g. dagster-pg-credentials for the Dagster Helm chart).
resource "aws_iam_role" "external_secrets_sa" {
  name = "${var.project_name}-external-secrets-sa-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = aws_iam_openid_connect_provider.eks.arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            # Restrict to Service Account named "external-secrets-sa" in the "external-secrets" namespace
            "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:sub" = "system:serviceaccount:external-secrets:external-secrets-sa"
          }
        }
      }
    ]
  })

  tags = {
    Environment = var.environment
  }
}

# 7.1. Least-privilege policy: read-only access to exactly the consolidated credentials secret
resource "aws_iam_policy" "external_secrets_read_credentials" {
  name        = "${var.project_name}-external-secrets-read-policy"
  description = "Read-only access to the consolidated AWS Secrets Manager secret, for External Secrets Operator"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = var.db_credentials_secret_arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "external_secrets_read_credentials_attach" {
  role       = aws_iam_role.external_secrets_sa.name
  policy_arn = aws_iam_policy.external_secrets_read_credentials.arn
}

# 8. GitHub OIDC provider, for GitHub Actions to assume an AWS role without
# long-lived credentials (used by the CI/CD deploy pipeline).
resource "aws_iam_openid_connect_provider" "github_actions" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  # AWS no longer validates this value for GitHub's OIDC provider (validates via
  # CA chain instead, per GitHub's own docs) -- Terraform's resource still
  # requires a non-empty value structurally, so this is GitHub's well-known
  # intermediate CA thumbprint, kept for compatibility, not functionally checked.
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

# 8.1. IAM Role assumed by the GitHub Actions CI/CD deploy workflow. Trust
# policy restricts to this exact repo AND only the main branch -- feature
# branches cannot assume this role.
resource "aws_iam_role" "github_actions_deploy" {
  name = "${var.project_name}-github-actions-deploy-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = aws_iam_openid_connect_provider.github_actions.arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
            "token.actions.githubusercontent.com:sub" = "repo:vuthehuyht/finops-data-stack:ref:refs/heads/main"
          }
        }
      }
    ]
  })

  tags = {
    Environment = var.environment
  }
}

# 8.2. Policy covering every AWS resource type Terraform manages in this
# project (EC2/VPC, EKS, RDS, Redshift Serverless, IAM, S3, ECR, Secrets
# Manager, SSM) plus ECR push and EKS describe. This is intentionally broad
# -- the role runs `terraform apply` against everything Terraform manages --
# not an oversight.
resource "aws_iam_policy" "github_actions_deploy_permissions" {
  name        = "${var.project_name}-github-actions-deploy-policy"
  description = "Permissions for the GitHub Actions CI/CD deploy pipeline (terraform apply + ECR push + EKS access)"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ec2:*",
          "eks:*",
          "rds:*",
          "redshift-serverless:*",
          "iam:*",
          "s3:*",
          "ecr:*",
          "secretsmanager:*",
          "ssm:*",
          "sagemaker:*",
          "logs:*"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "github_actions_deploy_permissions_attach" {
  role       = aws_iam_role.github_actions_deploy.name
  policy_arn = aws_iam_policy.github_actions_deploy_permissions.arn
}

# 8.3. Grant the GitHub Actions role admin access to the EKS cluster's
# Kubernetes API (via EKS Access Entries, not the legacy aws-auth ConfigMap),
# so `kubectl`/`helm` in the deploy workflow can operate on the cluster.
resource "aws_eks_access_entry" "github_actions_deploy" {
  cluster_name  = aws_eks_cluster.main.name
  principal_arn = aws_iam_role.github_actions_deploy.arn
}

resource "aws_eks_access_policy_association" "github_actions_deploy_admin" {
  cluster_name  = aws_eks_cluster.main.name
  principal_arn = aws_iam_role.github_actions_deploy.arn
  policy_arn    = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"

  access_scope {
    type = "cluster"
  }
}
