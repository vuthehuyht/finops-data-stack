# Call VPC Module (Network, Subnets, Route Tables, NAT, Security Groups, S3 Endpoint)
module "vpc" {
  source = "./modules/vpc"

  project_name = var.project_name
  environment  = var.environment
}

# Call Module ECR (Container Image Registry)
module "ecr" {
  source = "./modules/ecr"

  project_name = var.project_name
  environment  = var.environment
}

# Call Module S3 (Data Lake buckets: raw, processed, model-artifacts)
module "s3" {
  source = "./modules/s3"

  project_name = var.project_name
  environment  = var.environment
}

# Call Module Secrets Manager (Database credentials, API tokens)
module "secrets" {
  source = "./modules/secrets"

  project_name = var.project_name
  environment  = var.environment

  rds_host          = module.rds.rds_instance_address
  rds_port          = 5432
  rds_username      = module.rds.rds_username
  rds_password      = module.rds.rds_password
  rds_dbname        = module.rds.rds_dbname
  redshift_host     = module.redshift.endpoint
  redshift_port     = module.redshift.port
  redshift_username = module.redshift.admin_username
  redshift_password = module.redshift.admin_password
  redshift_dbname   = module.redshift.database_name

  fireant_email    = var.fireant_email
  fireant_password = var.fireant_password
}

# Call Module SSM Parameter Store (Model metadata, endpoint name, thresholds)
module "ssm" {
  source = "./modules/ssm"

  project_name = var.project_name
  environment  = var.environment
}

# Call Module SageMaker (Execution role, policy)
module "sagemaker" {
  source = "./modules/sagemaker"

  project_name               = var.project_name
  environment                = var.environment
  model_artifacts_bucket_arn = "arn:aws:s3:::${module.s3.model_artifacts_bucket_id}"
}

# Call Module Redshift Serverless (Namespace, Workgroup, Spectrum IAM role)
module "redshift" {
  source = "./modules/redshift"

  project_name          = var.project_name
  environment           = var.environment
  private_db_subnet_ids = module.vpc.private_db_subnet_ids
  redshift_sg_id        = module.vpc.redshift_sg_id
}

# Call Module EKS (EKS Cluster, Node Groups On-Demand + Spot, OIDC, IRSA Role)
module "eks" {
  source = "./modules/eks"

  project_name                 = var.project_name
  environment                  = var.environment
  vpc_id                       = module.vpc.vpc_id
  private_app_subnet_ids       = module.vpc.private_app_subnet_ids
  eks_node_sg_id               = module.vpc.eks_node_sg_id
  raw_bucket_arn               = "arn:aws:s3:::${module.s3.raw_bucket_id}"
  processed_bucket_arn         = "arn:aws:s3:::${module.s3.processed_bucket_id}"
  model_artifacts_bucket_arn   = "arn:aws:s3:::${module.s3.model_artifacts_bucket_id}"
  db_credentials_secret_arn    = module.secrets.db_credentials_secret_arn
  cluster_admin_principal_arns = var.cluster_admin_principal_arns
}

# Call Module RDS PostgreSQL for Dagster Metadata
module "rds" {
  source = "./modules/rds"

  project_name          = var.project_name
  environment           = var.environment
  vpc_id                = module.vpc.vpc_id
  private_db_subnet_ids = module.vpc.private_db_subnet_ids
}

# All ingress for the RDS and Redshift security groups is managed here as
# standalone aws_security_group_rule resources -- deliberately NOT as inline
# ingress{} blocks on the aws_security_group resources in modules/rds and
# modules/vpc (mixing the two approaches for the same SG causes Terraform to
# fight itself: each apply reverts whichever rule the other approach doesn't
# know about). Two source SGs are needed per destination:
#   - module.vpc.eks_node_sg_id: the custom "finops-eks-nodes-sg", used for
#     the EKS cluster's vpc_config.security_group_ids (control plane ENIs).
#   - module.eks.cluster_security_group_id: the security group EKS
#     auto-creates and actually attaches to managed node group EC2 instances
#     (verified empirically via `aws ec2 describe-instances` on a running
#     node -- it showed only "eks-cluster-sg-<cluster-name>-<suffix>", not
#     finops-eks-nodes-sg). Without this second rule, pods cannot reach
#     RDS/Redshift at all (Dagster's check-db-ready init container hangs
#     forever retrying "no response").
resource "aws_security_group_rule" "rds_allow_eks_node_sg" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = module.rds.rds_security_group_id
  source_security_group_id = module.vpc.eks_node_sg_id
  description              = "Allow PostgreSQL from EKS nodes"
}

resource "aws_security_group_rule" "rds_allow_eks_cluster_sg" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = module.rds.rds_security_group_id
  source_security_group_id = module.eks.cluster_security_group_id
  description              = "Allow PostgreSQL from EKS-managed node group instances (auto-created cluster SG)"
}

resource "aws_security_group_rule" "redshift_allow_eks_node_sg" {
  type                     = "ingress"
  from_port                = 5439
  to_port                  = 5439
  protocol                 = "tcp"
  security_group_id        = module.vpc.redshift_sg_id
  source_security_group_id = module.vpc.eks_node_sg_id
  description              = "Allow Redshift port 5439 from EKS nodes"
}

resource "aws_security_group_rule" "redshift_allow_eks_cluster_sg" {
  type                     = "ingress"
  from_port                = 5439
  to_port                  = 5439
  protocol                 = "tcp"
  security_group_id        = module.vpc.redshift_sg_id
  source_security_group_id = module.eks.cluster_security_group_id
  description              = "Allow Redshift from EKS-managed node group instances (auto-created cluster SG)"
}

