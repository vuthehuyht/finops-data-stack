# 1. Random username and password for RDS master user
resource "random_string" "rds_username" {
  length  = 8
  special = false
  numeric = false
  upper   = false
}

resource "random_password" "rds_password" {
  length  = 16
  special = false
}

# 2. Security Group for RDS PostgreSQL
resource "aws_security_group" "rds" {
  name        = "${var.project_name}-rds-sg"
  description = "Security group for RDS PostgreSQL (Dagster metadata)"
  vpc_id      = var.vpc_id

  # Only accept connections on port 5432 from EKS nodes
  ingress {
    description     = "Allow PostgreSQL from EKS nodes"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [var.eks_node_sg_id]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = {
    Name        = "${var.project_name}-rds-sg"
    Environment = var.environment
  }
}

# 3. DB Subnet Group for RDS
resource "aws_db_subnet_group" "rds" {
  name        = "${var.project_name}-rds-subnet-group"
  subnet_ids  = var.private_db_subnet_ids
  description = "Subnet group for RDS PostgreSQL"

  tags = {
    Name        = "${var.project_name}-rds-subnet-group"
    Environment = var.environment
  }
}

# 4. RDS DB Instance (PostgreSQL)
resource "aws_db_instance" "dagster_meta" {
  identifier            = "${var.project_name}-dagster-metadata"
  engine                = "postgres"
  engine_version        = "16"
  instance_class        = var.instance_class
  allocated_storage     = var.allocated_storage
  max_allocated_storage = 100
  storage_type          = "gp3"
  storage_encrypted     = true

  db_name  = "dagster_metadata"
  username = "rds_${random_string.rds_username.result}"
  password = random_password.rds_password.result

  db_subnet_group_name   = aws_db_subnet_group.rds.name
  vpc_security_group_ids = [aws_security_group.rds.id]

  publicly_accessible = false
  skip_final_snapshot = true

  # Avoid errors when deleting/recreating in test environments
  deletion_protection = false

  tags = {
    Name        = "${var.project_name}-dagster-metadata"
    Environment = var.environment
  }
}
