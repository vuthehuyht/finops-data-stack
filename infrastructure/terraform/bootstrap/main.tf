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

# Create a random suffix to ensure a globally unique S3 bucket name
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# S3 Bucket to store Terraform State
resource "aws_s3_bucket" "tf_state" {
  bucket        = "${var.project_name}-tfstate-${lower(random_id.bucket_suffix.hex)}"
  force_destroy = true

  tags = {
    Name        = "${var.project_name} Terraform State Bucket"
    Environment = "production"
    Project     = var.project_name
  }
}

# Enable Versioning for the S3 Bucket to track history and recover state if needed
resource "aws_s3_bucket_versioning" "tf_state_versioning" {
  bucket = aws_s3_bucket.tf_state.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Server-side encryption for the S3 bucket using SSE-S3 (Amazon S3 managed keys) - Free tier
resource "aws_s3_bucket_server_side_encryption_configuration" "tf_state_crypto" {
  bucket = aws_s3_bucket.tf_state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Block all public access to the S3 bucket
resource "aws_s3_bucket_public_access_block" "tf_state_public_block" {
  bucket = aws_s3_bucket.tf_state.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# DynamoDB Table for Terraform State Locking
resource "aws_dynamodb_table" "tf_locks" {
  name         = "${var.project_name}-tfstate-locks"
  billing_mode = "PAY_PER_REQUEST" # On-Demand billing to optimize cost
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

# Outputs required to configure backend settings in the main module
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
