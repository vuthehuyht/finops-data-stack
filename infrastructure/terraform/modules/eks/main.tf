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
