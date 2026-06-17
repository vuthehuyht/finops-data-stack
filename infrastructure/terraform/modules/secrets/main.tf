# 1. Database Credentials Secret
resource "aws_secretsmanager_secret" "db_credentials" {
  name                    = "${var.project_name}-db-credentials"
  description             = "Redshift Database connection credentials for Dagster/dbt"
  recovery_window_in_days = 0 # Hủy thời gian giữ để xóa ngay lập tức khi destroy trong môi trường thử nghiệm

  tags = {
    Environment = var.environment
  }
}

# 2. External API Tokens Secret
resource "aws_secretsmanager_secret" "api_tokens" {
  name                    = "${var.project_name}-api-tokens"
  description             = "External API tokens (SSI, Investing, etc.) for data crawlers"
  recovery_window_in_days = 0

  tags = {
    Environment = var.environment
  }
}
