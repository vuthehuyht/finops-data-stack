#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script to initialize dev_local environment (creates placeholder .env and terraform.tfvars).
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
            print("Successfully created template .env file. Please update configurations as needed.")
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
                f.write('allowed_ips             = ["0.0.0.0/0"] # Change to your public IP for better security\n')
                f.write('redshift_admin_username = "awsuser"\n')
                f.write('redshift_admin_password = "SecurePassword123!"\n')
            print("Successfully created terraform.tfvars file.")
        except Exception as e:
            print(f"Error creating terraform.tfvars file: {e}")
    else:
        print("terraform.tfvars file already exists. Skipping creation.")


if __name__ == "__main__":
    setup_dev_local()
