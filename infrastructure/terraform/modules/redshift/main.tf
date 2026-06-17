# Random password cho admin user của Redshift
resource "random_password" "redshift_admin" {
  length  = 16
  special = false
}

# 1. IAM Role cho Redshift Serverless truy cập S3 (Spectrum)
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
  policy_arn = "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess" # Dành cho Glue Data Catalog / External Tables
}

# 2. Redshift Serverless Namespace
resource "aws_redshiftserverless_namespace" "main" {
  namespace_name      = "${var.project_name}-redshift-namespace"
  db_name             = "${var.project_name}_db"
  admin_username      = "awsadmin"
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

  # Cấu hình RPU tối thiểu để tiết kiệm chi phí
  base_capacity = 8

  # Chỉ cho phép kết nối nội bộ VPC để đảm bảo an toàn
  publicly_accessible = false

  tags = {
    Environment = var.environment
  }
}
