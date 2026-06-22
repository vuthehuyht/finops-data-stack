import os

import dagster
import dagster_aws.s3  # noqa: F401

# Set mock environment variables for dbt during testing
os.environ.setdefault("REDSHIFT_HOST", "localhost")
os.environ.setdefault("REDSHIFT_USER", "awsuser")
os.environ.setdefault("REDSHIFT_PASSWORD", "mock_password")
os.environ.setdefault("REDSHIFT_DATABASE", "dev")

print("DAGSTER FILE IS:", dagster.__file__)
