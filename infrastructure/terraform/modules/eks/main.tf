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
  # bootstrap_cluster_creator_admin_permissions intentionally omitted: it is
  # ForceNew (verified in hashicorp/terraform-provider-aws internal/service/eks/cluster.go)
  # -- setting it explicitly on an existing cluster risks Terraform planning a
  # destroy+recreate of this cluster if the computed prior state ever
  # disagrees. It is not needed here anyway: this project grants cluster
  # access via its own explicit aws_eks_access_entry resources (see below),
  # not via the bootstrap cluster-creator entry.
  access_config {
    authentication_mode = "API_AND_CONFIG_MAP"
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
    max_size     = 2 # Allows the node group to scale out once (was locked to 1)
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
      # SageMaker orchestration permissions (Training, Batch Transform)
      {
        Effect = "Allow"
        Action = [
          "sagemaker:CreateTrainingJob",
          "sagemaker:DescribeTrainingJob",
          "sagemaker:StopTrainingJob",
          "sagemaker:CreateModel",
          "sagemaker:DeleteModel",
          "sagemaker:DescribeModel",
          "sagemaker:CreateTransformJob",
          "sagemaker:DescribeTransformJob",
          "sagemaker:StopTransformJob",
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

# 8.4. Optional human-operator cluster-admin access (see variable description
# for why this isn't automatic via bootstrap_cluster_creator_admin_permissions).
resource "aws_eks_access_entry" "cluster_admins" {
  for_each      = toset(var.cluster_admin_principal_arns)
  cluster_name  = aws_eks_cluster.main.name
  principal_arn = each.value
}

resource "aws_eks_access_policy_association" "cluster_admins" {
  for_each      = toset(var.cluster_admin_principal_arns)
  cluster_name  = aws_eks_cluster.main.name
  principal_arn = each.value
  policy_arn    = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"

  access_scope {
    type = "cluster"
  }
}

# 9. IRSA: IAM Role for Karpenter Controller
resource "aws_iam_role" "karpenter_controller" {
  name = "${var.project_name}-karpenter-controller-role"

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
            "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:sub" = "system:serviceaccount:kube-system:karpenter"
          }
        }
      }
    ]
  })

  tags = {
    Environment = var.environment
  }
}

# 9.1. IAM Role assumed by EC2 instances that Karpenter launches. Karpenter
# v1.x creates/manages the instance profile itself from this role's name
# (EC2NodeClass.spec.role in src/k8s/manifest/karpenter/nodepool.yaml) --
# no aws_iam_instance_profile resource needed here.
resource "aws_iam_role" "karpenter_node" {
  name = "${var.project_name}-karpenter-node-role"

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

resource "aws_iam_role_policy_attachment" "karpenter_node_worker_policy" {
  role       = aws_iam_role.karpenter_node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
}

resource "aws_iam_role_policy_attachment" "karpenter_node_cni_policy" {
  role       = aws_iam_role.karpenter_node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
}

resource "aws_iam_role_policy_attachment" "karpenter_node_registry_policy" {
  role       = aws_iam_role.karpenter_node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

resource "aws_iam_role_policy_attachment" "karpenter_node_ssm_policy" {
  role       = aws_iam_role.karpenter_node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# 9.2. EKS Access Entry so kubelets on Karpenter-launched nodes can join the
# cluster -- required because authentication_mode = API_AND_CONFIG_MAP and
# this project has no aws-auth ConfigMap managing node role mappings.
resource "aws_eks_access_entry" "karpenter_node" {
  cluster_name  = aws_eks_cluster.main.name
  principal_arn = aws_iam_role.karpenter_node.arn
  type          = "EC2_LINUX"
}

# 9.3. SQS queue + EventBridge rules for Spot interruption handling --
# Karpenter drains a node proactively (~2 min warning) instead of reacting
# only after the pod is killed.
resource "aws_sqs_queue" "karpenter_interruption" {
  name                      = "${var.project_name}-karpenter-interruption-queue"
  message_retention_seconds = 300
  sqs_managed_sse_enabled   = true

  tags = {
    Environment = var.environment
  }
}

resource "aws_sqs_queue_policy" "karpenter_interruption" {
  queue_url = aws_sqs_queue.karpenter_interruption.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = ["events.amazonaws.com", "sqs.amazonaws.com"]
        }
        Action   = "sqs:SendMessage"
        Resource = aws_sqs_queue.karpenter_interruption.arn
      },
      {
        Sid       = "DenyInsecureTransport"
        Effect    = "Deny"
        Principal = "*"
        Action    = "sqs:*"
        Resource  = aws_sqs_queue.karpenter_interruption.arn
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "karpenter_spot_interruption" {
  name = "${var.project_name}-karpenter-spot-interruption"
  event_pattern = jsonencode({
    source      = ["aws.ec2"]
    detail-type = ["EC2 Spot Instance Interruption Warning"]
  })
}

resource "aws_cloudwatch_event_rule" "karpenter_instance_state_change" {
  name = "${var.project_name}-karpenter-instance-state-change"
  event_pattern = jsonencode({
    source      = ["aws.ec2"]
    detail-type = ["EC2 Instance State-change Notification"]
  })
}

resource "aws_cloudwatch_event_rule" "karpenter_rebalance_recommendation" {
  name = "${var.project_name}-karpenter-rebalance-recommendation"
  event_pattern = jsonencode({
    source      = ["aws.ec2"]
    detail-type = ["EC2 Instance Rebalance Recommendation"]
  })
}

resource "aws_cloudwatch_event_rule" "karpenter_scheduled_change" {
  name = "${var.project_name}-karpenter-scheduled-change"
  event_pattern = jsonencode({
    source      = ["aws.health"]
    detail-type = ["AWS Health Event"]
  })
}

resource "aws_cloudwatch_event_target" "karpenter_spot_interruption" {
  rule = aws_cloudwatch_event_rule.karpenter_spot_interruption.name
  arn  = aws_sqs_queue.karpenter_interruption.arn
}

resource "aws_cloudwatch_event_target" "karpenter_instance_state_change" {
  rule = aws_cloudwatch_event_rule.karpenter_instance_state_change.name
  arn  = aws_sqs_queue.karpenter_interruption.arn
}

resource "aws_cloudwatch_event_target" "karpenter_rebalance_recommendation" {
  rule = aws_cloudwatch_event_rule.karpenter_rebalance_recommendation.name
  arn  = aws_sqs_queue.karpenter_interruption.arn
}

resource "aws_cloudwatch_event_target" "karpenter_scheduled_change" {
  rule = aws_cloudwatch_event_rule.karpenter_scheduled_change.name
  arn  = aws_sqs_queue.karpenter_interruption.arn
}

# 9.4. IAM Policy for the Karpenter controller -- scoped EC2 provisioning,
# instance-profile management (Karpenter v1.x creates/manages the instance
# profile itself from karpenter_node's role name), PassRole restricted to
# the node role, read-only Describe/pricing/ssm, and interruption queue access.
# Needed to scope the instance-profile ARNs below to this account (IAM ARNs
# require an explicit account ID, unlike the EC2 ARNs elsewhere in this file
# which use "*" for account/region).
data "aws_caller_identity" "current" {}

resource "aws_iam_policy" "karpenter_controller_permissions" {
  name        = "${var.project_name}-karpenter-controller-policy"
  description = "Permissions for the Karpenter controller to provision/terminate EC2 nodes for the EKS cluster"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowScopedEC2InstanceAccessActions"
        Effect = "Allow"
        Resource = [
          "arn:aws:ec2:*::image/*",
          "arn:aws:ec2:*::snapshot/*",
          "arn:aws:ec2:*:*:security-group/*",
          "arn:aws:ec2:*:*:subnet/*"
        ]
        Action = [
          "ec2:RunInstances",
          "ec2:CreateFleet"
        ]
      },
      {
        Sid      = "AllowScopedEC2LaunchTemplateAccessActions"
        Effect   = "Allow"
        Resource = "arn:aws:ec2:*:*:launch-template/*"
        Action = [
          "ec2:RunInstances",
          "ec2:CreateFleet"
        ]
        Condition = {
          StringEquals = {
            "aws:ResourceTag/kubernetes.io/cluster/${aws_eks_cluster.main.name}" = "owned"
          }
          StringLike = {
            "aws:ResourceTag/karpenter.sh/nodepool" = "*"
          }
        }
      },
      {
        Sid    = "AllowScopedEC2InstanceActionsWithTags"
        Effect = "Allow"
        Resource = [
          "arn:aws:ec2:*:*:fleet/*",
          "arn:aws:ec2:*:*:instance/*",
          "arn:aws:ec2:*:*:volume/*",
          "arn:aws:ec2:*:*:network-interface/*",
          "arn:aws:ec2:*:*:launch-template/*",
          "arn:aws:ec2:*:*:spot-instances-request/*"
        ]
        Action = [
          "ec2:RunInstances",
          "ec2:CreateFleet",
          "ec2:CreateLaunchTemplate"
        ]
        Condition = {
          StringEquals = {
            "aws:RequestTag/kubernetes.io/cluster/${aws_eks_cluster.main.name}" = "owned"
          }
          StringLike = {
            "aws:RequestTag/karpenter.sh/nodepool" = "*"
          }
        }
      },
      {
        Sid    = "AllowScopedResourceCreationTagging"
        Effect = "Allow"
        Resource = [
          "arn:aws:ec2:*:*:fleet/*",
          "arn:aws:ec2:*:*:instance/*",
          "arn:aws:ec2:*:*:volume/*",
          "arn:aws:ec2:*:*:network-interface/*",
          "arn:aws:ec2:*:*:launch-template/*",
          "arn:aws:ec2:*:*:spot-instances-request/*"
        ]
        Action = "ec2:CreateTags"
        Condition = {
          StringEquals = {
            "aws:RequestTag/kubernetes.io/cluster/${aws_eks_cluster.main.name}" = "owned"
            "ec2:CreateAction" = ["RunInstances", "CreateFleet", "CreateLaunchTemplate"]
          }
          StringLike = {
            "aws:RequestTag/karpenter.sh/nodepool" = "*"
          }
        }
      },
      {
        Sid      = "AllowScopedResourceTagging"
        Effect   = "Allow"
        Resource = "arn:aws:ec2:*:*:instance/*"
        Action    = "ec2:CreateTags"
        Condition = {
          StringEquals = {
            "aws:ResourceTag/kubernetes.io/cluster/${aws_eks_cluster.main.name}" = "owned"
          }
          StringLike = {
            "aws:ResourceTag/karpenter.sh/nodepool" = "*"
          }
          "ForAllValues:StringEquals" = {
            "aws:TagKeys" = ["eks:eks-cluster-name", "karpenter.sh/nodeclaim", "Name"]
          }
        }
      },
      {
        Sid    = "AllowScopedDeletion"
        Effect = "Allow"
        Resource = [
          "arn:aws:ec2:*:*:instance/*",
          "arn:aws:ec2:*:*:launch-template/*"
        ]
        Action = ["ec2:TerminateInstances", "ec2:DeleteLaunchTemplate"]
        Condition = {
          StringEquals = {
            "aws:ResourceTag/kubernetes.io/cluster/${aws_eks_cluster.main.name}" = "owned"
          }
          StringLike = {
            "aws:ResourceTag/karpenter.sh/nodepool" = "*"
          }
        }
      },
      {
        Sid      = "AllowScopedInstanceProfileCreationActions"
        Effect   = "Allow"
        Resource = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:instance-profile/*"
        Action   = ["iam:CreateInstanceProfile"]
        Condition = {
          StringEquals = {
            "aws:RequestTag/kubernetes.io/cluster/${aws_eks_cluster.main.name}" = "owned"
          }
          StringLike = {
            "aws:RequestTag/karpenter.k8s.aws/ec2nodeclass" = "*"
          }
        }
      },
      {
        Sid      = "AllowScopedInstanceProfileTagActions"
        Effect   = "Allow"
        Resource = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:instance-profile/*"
        Action   = ["iam:TagInstanceProfile"]
        Condition = {
          StringEquals = {
            "aws:ResourceTag/kubernetes.io/cluster/${aws_eks_cluster.main.name}" = "owned"
            "aws:RequestTag/kubernetes.io/cluster/${aws_eks_cluster.main.name}"  = "owned"
          }
          StringLike = {
            "aws:ResourceTag/karpenter.k8s.aws/ec2nodeclass" = "*"
            "aws:RequestTag/karpenter.k8s.aws/ec2nodeclass"  = "*"
          }
        }
      },
      {
        Sid      = "AllowScopedInstanceProfileActions"
        Effect   = "Allow"
        Resource = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:instance-profile/*"
        Action = [
          "iam:AddRoleToInstanceProfile",
          "iam:RemoveRoleFromInstanceProfile",
          "iam:DeleteInstanceProfile"
        ]
        Condition = {
          StringEquals = {
            "aws:ResourceTag/kubernetes.io/cluster/${aws_eks_cluster.main.name}" = "owned"
          }
          StringLike = {
            "aws:ResourceTag/karpenter.k8s.aws/ec2nodeclass" = "*"
          }
        }
      },
      {
        Sid      = "AllowPassingNodeRole"
        Effect   = "Allow"
        Resource = aws_iam_role.karpenter_node.arn
        Action   = "iam:PassRole"
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "ec2.amazonaws.com"
          }
        }
      },
      {
        Sid      = "AllowReadActions"
        Effect   = "Allow"
        Resource = "*"
        Action = [
          "ec2:DescribeImages",
          "ec2:DescribeInstances",
          "ec2:DescribeInstanceTypes",
          "ec2:DescribeInstanceTypeOfferings",
          "ec2:DescribeLaunchTemplates",
          "ec2:DescribeSubnets",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeSpotPriceHistory",
          "ec2:DescribeAvailabilityZones",
          "eks:DescribeCluster",
          "iam:GetInstanceProfile",
          "pricing:GetProducts",
          "ssm:GetParameter"
        ]
      },
      {
        Sid      = "AllowInterruptionQueueActions"
        Effect   = "Allow"
        Resource = aws_sqs_queue.karpenter_interruption.arn
        Action   = ["sqs:DeleteMessage", "sqs:GetQueueUrl", "sqs:ReceiveMessage"]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "karpenter_controller_permissions_attach" {
  role       = aws_iam_role.karpenter_controller.name
  policy_arn = aws_iam_policy.karpenter_controller_permissions.arn
}

# 9.5. Tag subnets + the EKS-auto-created cluster security group for
# Karpenter's subnetSelectorTerms/securityGroupSelectorTerms discovery
# (EC2NodeClass, src/k8s/manifest/karpenter/nodepool.yaml). Deliberately NOT
# var.eks_node_sg_id -- that SG is only used for the cluster's
# vpc_config.security_group_ids and is NOT what gets attached to node EC2
# instances (see the cluster_security_group_id output comment in
# ../../outputs.tf, verified empirically against a running node).
resource "aws_ec2_tag" "karpenter_subnet_discovery" {
  for_each    = toset(var.private_app_subnet_ids)
  resource_id = each.value
  key         = "karpenter.sh/discovery"
  value       = aws_eks_cluster.main.name
}

resource "aws_ec2_tag" "karpenter_sg_discovery" {
  resource_id = aws_eks_cluster.main.vpc_config[0].cluster_security_group_id
  key         = "karpenter.sh/discovery"
  value       = aws_eks_cluster.main.name
}

