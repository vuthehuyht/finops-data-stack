# ==============================================================================
# 1. DEFAULT NETWORK DATA SOURCES
# ==============================================================================

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_region" "current" {}

# ==============================================================================
# 2. SECURITY GROUP (Public Ingress for CI Runner)
# ==============================================================================

resource "aws_security_group" "redshift_ci" {
  name        = "${var.project_name}-redshift-sg"
  description = "Security group for Redshift Serverless CI inside Default VPC"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "Allow Redshift port 5439 from anywhere for CI integration"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = {
    Name        = "${var.project_name}-redshift-sg"
    Environment = var.environment
  }
}

# ==============================================================================
# 3. IAM ROLE FOR REDSHIFT ACCESS TO S3 & GLUE
# ==============================================================================

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
  policy_arn = "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"
}

# ==============================================================================
# 4. REDSHIFT SERVERLESS
# ==============================================================================

resource "aws_redshiftserverless_namespace" "main" {
  namespace_name      = "${var.project_name}-namespace"
  db_name             = "finops_ci_db"
  admin_username      = var.redshift_admin_username
  admin_user_password = var.redshift_admin_password
  iam_roles           = [aws_iam_role.redshift_s3.arn]

  tags = {
    Environment = var.environment
  }
}

resource "aws_redshiftserverless_workgroup" "main" {
  workgroup_name = "${var.project_name}-workgroup"
  namespace_name = aws_redshiftserverless_namespace.main.namespace_name

  subnet_ids         = data.aws_subnets.default.ids
  security_group_ids = [aws_security_group.redshift_ci.id]

  base_capacity = 4 # Minimum RPU to reduce cost in CI
  max_capacity  = 4

  publicly_accessible = true

  tags = {
    Environment = var.environment
  }
}

# Usage limit - hard cap on monthly compute cost
resource "aws_redshiftserverless_usage_limit" "monthly_cost_cap" {
  resource_arn  = aws_redshiftserverless_workgroup.main.arn
  usage_type    = "serverless-compute"
  amount        = var.monthly_cost_cap_usd
  period        = "monthly"
  breach_action = "deactivate"
}
