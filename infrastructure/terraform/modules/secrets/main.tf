# 1. Database Credentials Secret
resource "aws_secretsmanager_secret" "credentials" {
  name                    = "${var.project_name}-db-credentials"
  description             = "Consolidated database connection credentials for RDS (Dagster metadata) and Redshift"
  recovery_window_in_days = 0 # Set recovery window to 0 for immediate deletion when destroyed in test environments

  tags = {
    Environment = var.environment
  }
}

resource "aws_secretsmanager_secret_version" "credentials_val" {
  secret_id = aws_secretsmanager_secret.credentials.id
  secret_string = jsonencode({
    rds_host          = var.rds_host
    rds_username      = var.rds_username
    rds_password      = var.rds_password
    rds_dbname        = var.rds_dbname
    redshift_host     = var.redshift_host
    redshift_username = var.redshift_username
    redshift_password = var.redshift_password
    redshift_dbname   = var.redshift_dbname
    fireant_email     = var.fireant_email
    fireant_password  = var.fireant_password
    slack_api_token   = var.slack_api_token
  })
}
