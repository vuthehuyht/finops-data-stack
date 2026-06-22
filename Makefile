# Makefile to setup and run dev_local environment for FinOps Data Stack

.PHONY: help setup dev lint format test clean dev_local dev_local_up dev_local_down up down lint-sql format-sql

# Default target when running `make`
help:
	@echo "Available commands:"
	@echo "  make setup          - Install dependencies and initialize environment variables (.env, terraform.tfvars)"
	@echo "  make dev            - Start Dagster development server local"
	@echo "  make dev_local up   - Spin up dev local resources using Terraform"
	@echo "  make dev_local down - Destroy dev local resources using Terraform"
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

# Dummy target to prevent make from complaining about 'up' or 'down' arguments
up down:
	@:

# Clean cache and temporary files (Cross-platform support)
clean:
	@uv run python scripts/clean.py
