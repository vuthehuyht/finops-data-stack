#!/usr/bin/env python
"""
Script to initialize dev_local environment.

Creates placeholder .env, terraform.tfvars, and dbt profiles.yml.
"""

import io
import os
import sys

# Setup UTF-8 encoding for stdout/stderr on Windows to avoid potential encoding issues
if sys.platform.startswith("win"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")


def setup_dev_local():
    # 1. Create template .env file if it doesn't exist
    env_path = ".env"
    if not os.path.exists(env_path):
        print("Creating template .env file...")
        try:
            with open(env_path, "w", encoding="utf-8") as f:
                f.write("# Local dev environment configuration\n")
                f.write("AWS_ACCESS_KEY_ID=mock_key\n")
                f.write("AWS_SECRET_ACCESS_KEY=mock_secret\n")
                f.write("AWS_DEFAULT_REGION=ap-southeast-1\n\n")
                f.write("# Redshift (DW) configuration\n")
                f.write("REDSHIFT_HOST=localhost\n")
                f.write("REDSHIFT_PORT=5439\n")
                f.write("REDSHIFT_DATABASE=dev\n")
                f.write("REDSHIFT_USER=awsuser\n")
                f.write("REDSHIFT_PASSWORD=\n")
            print(
                "Successfully created template .env file. "
                "Please update configurations as needed."
            )
        except Exception as e:
            print(f"Error creating .env file: {e}")
    else:
        print(".env file already exists. Skipping creation.")

    # 2. Create template terraform.tfvars file if it doesn't exist
    tfvars_dir = os.path.join("infrastructure", "terraform", "dev_local")
    tfvars_path = os.path.join(tfvars_dir, "terraform.tfvars")

    if not os.path.exists(tfvars_path):
        print("Creating template terraform.tfvars file for dev_local...")
        try:
            os.makedirs(tfvars_dir, exist_ok=True)
            with open(tfvars_path, "w", encoding="utf-8") as f:
                f.write("# Terraform variables configuration for dev_local\n")
                f.write(
                    'allowed_ips             = ["0.0.0.0/0"] '
                    "# Change to your public IP for better security\n"
                )
                f.write('redshift_admin_username = "awsuser"\n')
                f.write('redshift_admin_password = "SecurePassword123!"\n')
            print("Successfully created terraform.tfvars file.")
        except Exception as e:
            print(f"Error creating terraform.tfvars file: {e}")
    else:
        print("terraform.tfvars file already exists. Skipping creation.")

    # 3. Create dbt profiles.yml dynamic configuration template if it doesn't exist
    dbt_dir = os.path.join("src", "transform")
    profiles_path = os.path.join(dbt_dir, "profiles.yml")

    if not os.path.exists(profiles_path):
        print("Creating template dbt profiles.yml in src/transform/...")
        try:
            os.makedirs(dbt_dir, exist_ok=True)
            with open(profiles_path, "w", encoding="utf-8") as f:
                f.write("finops:\n")
                f.write("  outputs:\n")
                f.write("    dev:\n")
                f.write("      type: redshift\n")
                f.write("      host: \"{{ env_var('REDSHIFT_HOST') }}\"\n")
                f.write("      port: 5439\n")
                f.write("      user: \"{{ env_var('REDSHIFT_USER') }}\"\n")
                f.write("      password: \"{{ env_var('REDSHIFT_PASSWORD') }}\"\n")
                f.write("      dbname: \"{{ env_var('REDSHIFT_DATABASE') }}\"\n")
                f.write("      schema: dev_mart\n")
                f.write("      threads: 4\n")
                f.write("      keepalives_idle: 240\n")
                f.write("  target: dev\n")
            print("Successfully created dbt profiles.yml file.")
        except Exception as e:
            print(f"Error creating dbt profiles.yml file: {e}")
    else:
        print("dbt profiles.yml file already exists. Skipping creation.")


if __name__ == "__main__":
    setup_dev_local()
