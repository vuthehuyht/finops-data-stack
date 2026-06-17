# ==============================================================================
# 1. NETWORK & SECURITY (VPC Dev Tối Giản Không NAT)
# ==============================================================================

# VPC
resource "aws_vpc" "dev" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "${var.project_name}-vpc"
  }
}

# Internet Gateway
resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.dev.id

  tags = {
    Name = "${var.project_name}-igw"
  }
}

# 2 Public Subnets
resource "aws_subnet" "public" {
  count                   = 2
  vpc_id                  = aws_vpc.dev.id
  cidr_block              = "10.99.${count.index + 1}.0/24"
  availability_zone       = var.availability_zones[count.index]
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.project_name}-public-subnet-${count.index + 1}"
  }
}

# Public Route Table
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.dev.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }

  tags = {
    Name = "${var.project_name}-public-rt"
  }
}

# Route Table Associations
resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# S3 Gateway Endpoint
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.dev.id
  service_name      = "com.amazonaws.${data.aws_region.current.name}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id]

  tags = {
    Name = "${var.project_name}-s3-endpoint"
  }
}

data "aws_region" "current" {}

# Security Group cho Redshift (Chỉ cho phép IP được cấu hình whitelist)
resource "aws_security_group" "redshift_dev" {
  name        = "${var.project_name}-redshift-sg"
  description = "Security group for Redshift Serverless Dev Local"
  vpc_id      = aws_vpc.dev.id

  # Inbound port 5439 từ dải allowed_ips (IP của nhà phát triển)
  ingress {
    description = "Allow Redshift port 5439 from Dev IP"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = var.allowed_ips
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = {
    Name = "${var.project_name}-redshift-sg"
  }
}

# ==============================================================================
# 2. AMAZON S3 DEV BUCKETS (Bật force_destroy phục vụ testing)
# ==============================================================================

# Bucket Raw (Bronze Dev)
resource "aws_s3_bucket" "raw" {
  bucket        = "${var.project_name}-data-lake-raw"
  force_destroy = true # Cho phép xóa nhanh dữ liệu khi destroy

  tags = {
    Name        = "${var.project_name} Raw Data Lake Bucket"
    Environment = var.environment
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw_crypto" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw_public_block" {
  bucket = aws_s3_bucket.raw.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Bucket Processed (Log Dev)
resource "aws_s3_bucket" "processed" {
  bucket        = "${var.project_name}-data-lake-processed"
  force_destroy = true

  tags = {
    Name        = "${var.project_name} Processed Data Lake Bucket"
    Environment = var.environment
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed_crypto" {
  bucket = aws_s3_bucket.processed.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "processed_public_block" {
  bucket = aws_s3_bucket.processed.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Bucket Model Artifacts (ML Models Dev)
resource "aws_s3_bucket" "model_artifacts" {
  bucket        = "${var.project_name}-model-artifacts"
  force_destroy = true

  tags = {
    Name        = "${var.project_name} Model Artifacts Bucket"
    Environment = var.environment
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "model_artifacts_crypto" {
  bucket = aws_s3_bucket.model_artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "model_artifacts_public_block" {
  bucket = aws_s3_bucket.model_artifacts.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ==============================================================================
# 3. REDSHIFT SERVERLESS (Publicly Accessible)
# ==============================================================================

# IAM Role cho Spectrum
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

# Namespace
resource "aws_redshiftserverless_namespace" "main" {
  namespace_name      = "${var.project_name}-redshift-namespace"
  db_name             = "finops_dev_db"
  admin_username      = var.redshift_admin_username
  admin_user_password = var.redshift_admin_password
  iam_roles           = [aws_iam_role.redshift_s3.arn]

  tags = {
    Environment = var.environment
  }
}

# Workgroup (Publicly accessible)
resource "aws_redshiftserverless_workgroup" "main" {
  workgroup_name = "${var.project_name}-redshift-workgroup"
  namespace_name = aws_redshiftserverless_namespace.main.namespace_name

  subnet_ids         = aws_subnet.public[*].id # Đặt trong Public Subnets
  security_group_ids = [aws_security_group.redshift_dev.id]

  base_capacity = 8 # RPU tối thiểu để tiết kiệm chi phí

  # BẬT Publicly Accessible để AWS cấp DNS Public và IP Public truy cập qua Internet
  publicly_accessible = true

  tags = {
    Environment = var.environment
  }
}
