# 1. Database Credentials Secret
resource "aws_secretsmanager_secret" "db_credentials" {
  name                    = "${var.project_name}-db-credentials"
  description             = "Consolidated database connection credentials for RDS (Dagster metadata) and Redshift"
  recovery_window_in_days = 0 # Set recovery window to 0 for immediate deletion when destroyed in test environments

  tags = {
    Environment = var.environment
  }
}

resource "aws_secretsmanager_secret_version" "db_credentials_val" {
  secret_id = aws_secretsmanager_secret.db_credentials.id
  secret_string = jsonencode({
    rds_host          = var.rds_host
    rds_port          = var.rds_port
    rds_username      = var.rds_username
    rds_password      = var.rds_password
    rds_dbname        = var.rds_dbname
    redshift_host     = var.redshift_host
    redshift_port     = var.redshift_port
    redshift_username = var.redshift_username
    redshift_password = var.redshift_password
    redshift_dbname   = var.redshift_dbname
  })
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
