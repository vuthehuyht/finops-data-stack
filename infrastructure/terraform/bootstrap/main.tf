provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  type        = string
  default     = "ap-southeast-1"
  description = "AWS region to deploy resources"
}

variable "project_name" {
  type        = string
  default     = "finops"
  description = "Project name prefix for resources"
}

# Tạo một chuỗi ngẫu nhiên để tránh trùng tên S3 bucket toàn cầu
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# S3 Bucket lưu trữ Terraform State
resource "aws_s3_bucket" "tf_state" {
  bucket        = "${var.project_name}-tfstate-${lower(random_id.bucket_suffix.hex)}"
  force_destroy = true

  tags = {
    Name        = "${var.project_name} Terraform State Bucket"
    Environment = "production"
    Project     = var.project_name
  }
}

# Kích hoạt Versioning cho S3 Bucket để theo dõi lịch sử và khôi phục state khi cần
resource "aws_s3_bucket_versioning" "tf_state_versioning" {
  bucket = aws_s3_bucket.tf_state.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Mã hóa tĩnh S3 bucket bằng SSE-S3 (Amazon S3 managed keys) - Miễn phí
resource "aws_s3_bucket_server_side_encryption_configuration" "tf_state_crypto" {
  bucket = aws_s3_bucket.tf_state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Chặn hoàn toàn truy cập public vào S3 bucket
resource "aws_s3_bucket_public_access_block" "tf_state_public_block" {
  bucket = aws_s3_bucket.tf_state.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# DynamoDB Table phục vụ cho việc khóa State (State Locking)
resource "aws_dynamodb_table" "tf_locks" {
  name         = "${var.project_name}-tfstate-locks"
  billing_mode = "PAY_PER_REQUEST" # On-Demand billing để tối ưu chi phí
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  tags = {
    Name        = "${var.project_name} Terraform State Lock Table"
    Environment = "production"
    Project     = var.project_name
  }
}

# Output các thông tin cần thiết để điền vào cấu hình backend của module chính
output "state_bucket_name" {
  value       = aws_s3_bucket.tf_state.bucket
  description = "Use this value for 'bucket' in your backend configuration"
}

output "dynamodb_table_name" {
  value       = aws_dynamodb_table.tf_locks.name
  description = "Use this value for 'dynamodb_table' in your backend configuration"
}

output "aws_region" {
  value       = var.aws_region
  description = "AWS Region where state resources are created"
}
