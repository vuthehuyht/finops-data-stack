# 1. Bucket Raw (Bronze Data Lake)
resource "aws_s3_bucket" "raw" {
  bucket        = "${var.project_name}-data-lake-raw"
  force_destroy = true

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

resource "aws_s3_bucket_lifecycle_configuration" "raw_lifecycle" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "archive-old-data"
    status = "Enabled"

    filter {} # Apply to all objects in the bucket

    transition {
      days          = 90
      storage_class = "GLACIER_IR" # Glacier Instant Retrieval for fast query when needed
    }
  }
}

# 2. Bucket Processed (Log/Intermediate Data)
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

# 3. Bucket Model Artifacts (ML Models)
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
