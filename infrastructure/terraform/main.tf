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

  ssi_token       = var.ssi_token
  investing_token = var.investing_token
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

  project_name               = var.project_name
  environment                = var.environment
  vpc_id                     = module.vpc.vpc_id
  private_app_subnet_ids     = module.vpc.private_app_subnet_ids
  eks_node_sg_id             = module.vpc.eks_node_sg_id
  raw_bucket_arn             = "arn:aws:s3:::${module.s3.raw_bucket_id}"
  processed_bucket_arn       = "arn:aws:s3:::${module.s3.processed_bucket_id}"
  model_artifacts_bucket_arn = "arn:aws:s3:::${module.s3.model_artifacts_bucket_id}"
}

# Call Module RDS PostgreSQL for Dagster Metadata
module "rds" {
  source = "./modules/rds"

  project_name          = var.project_name
  environment           = var.environment
  vpc_id                = module.vpc.vpc_id
  private_db_subnet_ids = module.vpc.private_db_subnet_ids
  eks_node_sg_id        = module.vpc.eks_node_sg_id
}

