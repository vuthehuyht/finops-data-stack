"""Utility module for connection and querying with AWS Redshift Serverless."""

import json
import logging
import os
import time
from typing import Any

import boto3
import psycopg2
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def inject_secrets_from_aws() -> None:
    """Retrieve database credentials from AWS Secrets Manager on production.

    Updates env variables with the retrieved database configurations.
    """
    secret_name = os.getenv("FINOPS_REDSHIFT_SECRET_NAME", "prod/finops/redshift")
    region_name = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-1")

    # Only run in production and when the password has not been loaded yet
    if os.getenv("FINOPS_ENVIRONMENT", "dev").lower() != "prod":
        return

    if os.getenv("REDSHIFT_PASSWORD"):
        return

    logger.info(
        "Retrieving database credentials from AWS Secrets Manager: %s",
        secret_name,
    )

    try:
        client = boto3.client("secretsmanager", region_name=region_name)
        response = client.get_secret_value(SecretId=secret_name)

        if "SecretString" not in response:
            raise ValueError("No SecretString found in AWS Secret response.")

        secret_dict = json.loads(response["SecretString"])
        key_mapping = {
            "password": "REDSHIFT_PASSWORD",
            "username": "REDSHIFT_USER",
            "user": "REDSHIFT_USER",
            "host": "REDSHIFT_HOST",
            "database": "REDSHIFT_DATABASE",
            "dbname": "REDSHIFT_DATABASE",
        }

        for key, val in secret_dict.items():
            env_key = f"REDSHIFT_{key.upper()}"
            os.environ[env_key] = str(val)

            mapped_key = key_mapping.get(key.lower())
            if mapped_key:
                os.environ[mapped_key] = str(val)

        logger.info("Loaded Redshift configs from Secrets Manager.")

    except ClientError as e:
        logger.error("Error accessing AWS Secrets Manager: %s", e)
        raise e
    except Exception as e:
        logger.error("Unexpected error loading database credentials: %s", e)
        raise e


def get_redshift_connection() -> Any:
    """Establish and return a connection to Amazon Redshift."""
    if os.getenv("FINOPS_ENVIRONMENT", "dev").lower() == "prod":
        inject_secrets_from_aws()

    return psycopg2.connect(
        host=os.getenv("REDSHIFT_HOST", "localhost"),
        port=int(os.getenv("REDSHIFT_PORT", "5439")),
        database=os.getenv("REDSHIFT_DATABASE", "dev"),
        user=os.getenv("REDSHIFT_USER", "awsuser"),
        password=os.getenv("REDSHIFT_PASSWORD", ""),
    )


def execute_query(
    cursor: Any,
    query: str,
    params: tuple[Any, ...] | dict[str, Any] | None = None,
) -> None:
    """Execute SQL query with elapsed time logging.

    Args:
        cursor: psycopg2 cursor object.
        query: SQL query string to execute.
        params: Parameters to pass to the query.
    """
    query_start_ns = time.time_ns()
    try:
        cursor.execute(query, params or ())
    finally:
        query_end_ns = time.time_ns()
        elapsed_millis = (query_end_ns - query_start_ns) // 1_000_000
        logger.debug(
            "Elapsed %s ms: query=%s",
            f"{elapsed_millis:,}",
            cursor.query,
        )
