resource "aws_ecr_repository" "dagster_app" {
  name                 = "${var.project_name}-dagster-app"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Name        = "${var.project_name} Dagster App Repository"
    Environment = var.environment
  }
}

# Lifecycle policy to automatically clean up old/unused images
resource "aws_ecr_lifecycle_policy" "cleanup_policy" {
  repository = aws_ecr_repository.dagster_app.name

  policy = <<EOF
{
    "rules": [
        {
            "rulePriority": 1,
            "description": "Remove untagged images older than 7 days",
            "selection": {
                "tagStatus": "untagged",
                "countType": "sinceImagePushed",
                "countUnit": "days",
                "countNumber": 7
            },
            "action": {
                "type": "expire"
            }
        },
        {
            "rulePriority": 2,
            "description": "Keep only last 10 tagged images",
            "selection": {
                "tagStatus": "any",
                "countType": "imageCountType",
                "countNumber": 10
            },
            "action": {
                "type": "expire"
            }
        }
    ]
}
EOF
}
