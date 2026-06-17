terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }

  # Điền tên bucket cụ thể nhận được từ output của bootstrap trước khi init
  backend "s3" {
    bucket         = "PLACEHOLDER_BUCKET_NAME"
    key            = "state/terraform.tfstate"
    region         = "ap-southeast-1"
    dynamodb_table = "finops-tfstate-locks"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}
