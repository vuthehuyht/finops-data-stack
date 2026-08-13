# 1. Deploy AWS Lambda Connector cho Redshift thông qua Serverless Application Repository
resource "aws_serverlessapplicationrepository_cloudformation_stack" "athena_redshift_connector" {
  name             = "${var.project_name}-${var.environment}-athena-redshift"
  application_id   = "arn:aws:serverlessrepo:us-east-1:292517598671:applications/AthenaRedshiftConnector"
  
  capabilities     = ["CAPABILITY_IAM", "CAPABILITY_RESOURCE_POLICY", "CAPABILITY_AUTO_EXPAND"]

  parameters = {
    SpillBucket             = module.s3.model_artifacts_bucket_id
    SecretNamePrefix        = aws_secretsmanager_secret.athena_redshift.name
    DefaultConnectionString = "redshift://jdbc:redshift://${module.redshift.endpoint}:${module.redshift.port}/${module.redshift.database_name}?$${${aws_secretsmanager_secret.athena_redshift.name}}"
    SecurityGroupIds        = module.vpc.eks_node_sg_id
    SubnetIds               = join(",", module.vpc.private_app_subnet_ids)
    LambdaFunctionName      = "${var.project_name}-${var.environment}-redshift-connector"
  }
}

# 2. Đăng ký cái Lambda bên trên thành một Data Catalog mới trong giao diện Athena
resource "aws_athena_data_catalog" "redshift_catalog" {
  name        = "redshift_catalog"
  description = "Athena to Redshift Serverless Connector"
  type        = "LAMBDA"
  
  parameters = {
    "function" = "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.project_name}-${var.environment}-redshift-connector"
  }

  depends_on = [
    aws_serverlessapplicationrepository_cloudformation_stack.athena_redshift_connector
  ]
}

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

# 3. Create Secret specifically for Athena Redshift Connector
resource "aws_secretsmanager_secret" "athena_redshift" {
  name                    = "${var.project_name}-${var.environment}-athena-redshift-creds"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "athena_redshift_val" {
  secret_id     = aws_secretsmanager_secret.athena_redshift.id
  secret_string = jsonencode({
    username = module.redshift.admin_username
    password = module.redshift.admin_password
  })
}

# 4. Configure Athena Workgroup to store query results
resource "aws_athena_workgroup" "finops" {
  name = "${var.project_name}_${var.environment}"

  configuration {
    result_configuration {
      output_location = "s3://${module.s3.model_artifacts_bucket_id}/athena-results/"
    }
  }
}

