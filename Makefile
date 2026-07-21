# Makefile to setup and run dev_local environment for FinOps Data Stack

.PHONY: help setup dev lint format test clean dev_local dev_local_up dev_local_down up down lint-sql format-sql ci ci_up ci_down infra infra_plan infra_up infra_down bootstrap

# Default target when running `make`
help:
	@echo "Available commands:"
	@echo "  make setup          - Install dependencies and initialize environment variables (.env, terraform.tfvars)"
	@echo "  make dev            - Start Dagster development server local"
	@echo "  make dev_local up   - Spin up dev local resources using Terraform"
	@echo "  make dev_local down - Destroy dev local resources using Terraform"
	@echo "  make ci up          - Spin up CI resources (Redshift in Default VPC) using Terraform"
	@echo "  make ci down        - Destroy CI resources using Terraform"
	@echo "  make infra plan     - Preview changes to the main infrastructure stack"
	@echo "  make infra up       - Apply the main infrastructure stack (asks for confirmation)"
	@echo "  make infra down     - Destroy the main infrastructure stack (asks for confirmation)"
	@echo "  make bootstrap      - One-time: create the S3 bucket + DynamoDB table used as the main stack's remote state backend"
	@echo "  make lint           - Check code style and formatting issues with ruff"
	@echo "  make format         - Automatically format and fix lint issues with ruff"
	@echo "  make test           - Run all unit tests with pytest"
	@echo "  make clean          - Clean up temporary files, logs, and caches"

# Initialize dev_local environment
setup:
	@echo "Installing dependencies with uv..."
	uv sync
	@echo "Installing pre-commit hooks..."
	uv run pip install pre-commit
	uv run pre-commit install
	@echo "Dependencies installed successfully."
	@uv run python scripts/setup_dev.py

# Run Dagster development server
dev:
	@echo "Starting Dagster dev server..."
	uv run dagster dev

# Run code linting check
lint:
	@echo "Running ruff check..."
	uv run ruff check src/ tests/
	@echo "Running ruff format check..."
	uv run ruff format --check src/ tests/

# Automatically format code
format:
	@echo "Formatting code with ruff..."
	uv run ruff format src/ tests/
	uv run ruff check --fix src/ tests/

# Run SQLFluff linter
lint-sql:
	@echo "Running sqlfluff lint..."
	cd src/transform/dbt && uv run sqlfluff lint models

# Automatically format SQL code
format-sql:
	@echo "Formatting SQL with sqlfluff..."
	cd src/transform/dbt && uv run sqlfluff format models -f

# Run unit tests
test:
	@echo "Running tests with pytest..."
	uv run pytest tests/

# Spin up local development resources (Terraform)
dev_local_up:
	@echo "Initializing local development resources..."
	terraform -chdir=infrastructure/terraform/dev_local init
	terraform -chdir=infrastructure/terraform/dev_local apply -auto-approve

# Destroy local development resources (Terraform)
dev_local_down:
	@echo "Destroying local development resources..."
	terraform -chdir=infrastructure/terraform/dev_local destroy -auto-approve

# Allow command syntax: make dev_local up or make dev_local down
dev_local:
	@$(MAKE) dev_local_$(filter-out $@,$(MAKECMDGOALS))

# Spin up CI resources (Terraform)
ci_up:
	@echo "Initializing CI resources in Default VPC..."
	terraform -chdir=infrastructure/terraform/ci init
	terraform -chdir=infrastructure/terraform/ci apply -auto-approve

# Destroy CI resources (Terraform)
ci_down:
	@echo "Destroying CI resources..."
	terraform -chdir=infrastructure/terraform/ci destroy -auto-approve

# Allow command syntax: make ci up or make ci down
ci:
	@$(MAKE) ci_$(filter-out $@,$(MAKECMDGOALS))

# Preview changes to the main infrastructure stack (EKS, SageMaker, IAM, ...)
infra_plan:
	@echo "Planning main infrastructure stack..."
	terraform -chdir=infrastructure/terraform init
	terraform -chdir=infrastructure/terraform plan

# Apply the main infrastructure stack. No -auto-approve: this provisions
# real shared AWS resources (EKS, SageMaker, IAM), so Terraform's own
# interactive confirmation prompt is kept as a safety check.
infra_up:
	@echo "Applying main infrastructure stack..."
	terraform -chdir=infrastructure/terraform init
	terraform -chdir=infrastructure/terraform apply

# Destroy the main infrastructure stack. No -auto-approve, same reason as
# infra_up -- destroying this stack is hard to reverse.
infra_down:
	@echo "Destroying main infrastructure stack..."
	terraform -chdir=infrastructure/terraform destroy

# Allow command syntax: make infra plan / make infra up / make infra down
infra:
	@$(MAKE) infra_$(filter-out $@,$(MAKECMDGOALS))

# One-time setup: create the S3 bucket + DynamoDB table backing the main
# stack's remote state (infrastructure/terraform/provider.tf). Run this
# ONCE per AWS account, before the first `make infra up`. No `bootstrap down`
# target on purpose: destroying this stack deletes Terraform state history
# for every other stack -- do it manually with
# `terraform -chdir=infrastructure/terraform/bootstrap destroy` if you really mean to.
bootstrap:
	@echo "Bootstrapping Terraform remote state backend (S3 + DynamoDB)..."
	terraform -chdir=infrastructure/terraform/bootstrap init
	terraform -chdir=infrastructure/terraform/bootstrap apply

# Dummy target to prevent make from complaining about 'up', 'down', or 'plan' arguments
up down plan:
	@:

# Clean cache and temporary files (Cross-platform support)
clean:
	@uv run python scripts/clean.py
