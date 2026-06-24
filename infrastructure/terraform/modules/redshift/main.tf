# Random username và password for Redshift admin user
resource "random_string" "redshift_username" {
  length  = 8
  special = false
  numeric = false
  upper   = false
}

resource "random_password" "redshift_admin" {
  length  = 16
  special = false
}

# 1. IAM Role for Redshift Serverless to access S3 (Spectrum)
resource "aws_iam_role" "redshift_s3" {
  name = "${var.project_name}-redshift-s3-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "redshift.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Environment = var.environment
  }
}

resource "aws_iam_role_policy_attachment" "redshift_s3_readonly" {
  role       = aws_iam_role.redshift_s3.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

resource "aws_iam_role_policy_attachment" "redshift_glue_access" {
  role       = aws_iam_role.redshift_s3.name
  policy_arn = "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess" # For Glue Data Catalog / External Tables
}

# 2. Redshift Serverless Namespace
resource "aws_redshiftserverless_namespace" "main" {
  namespace_name      = "${var.project_name}-redshift-namespace"
  db_name             = "${var.project_name}_db"
  admin_username      = "rsadmin_${random_string.redshift_username.result}"
  admin_user_password = random_password.redshift_admin.result
  iam_roles           = [aws_iam_role.redshift_s3.arn]

  tags = {
    Environment = var.environment
  }
}

# 3. Redshift Serverless Workgroup
resource "aws_redshiftserverless_workgroup" "main" {
  workgroup_name = "${var.project_name}-redshift-workgroup"
  namespace_name = aws_redshiftserverless_namespace.main.namespace_name

  subnet_ids         = var.private_db_subnet_ids
  security_group_ids = [var.redshift_sg_id]

  # Minimum RPU to reduce cost
  base_capacity = 4
  max_capacity  = 4

  # Internal VPC access only
  publicly_accessible = false

  tags = {
    Environment = var.environment
  }
}

# 4. Usage limit - hard cap on monthly compute cost
resource "aws_redshiftserverless_usage_limit" "monthly_cost_cap" {
  resource_arn  = aws_redshiftserverless_workgroup.main.arn
  usage_type    = "serverless-compute"
  amount        = var.monthly_cost_cap_usd
  period        = "monthly"
  breach_action = "deactivate"
}

