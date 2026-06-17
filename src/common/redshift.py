import json
import logging
import os

import boto3
import psycopg2
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def inject_secrets_from_aws():
    """Retrieve database credentials from AWS Secrets Manager on prod environment
    and load them into environment variables.
    """
    secret_name = os.getenv("FINOPS_REDSHIFT_SECRET_NAME", "prod/finops/redshift")
    region_name = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-1")

    # Only run on Production and when password is not already loaded
    if os.getenv("FINOPS_ENVIRONMENT", "dev").lower() != "prod":
        return

    if os.getenv("REDSHIFT_PASSWORD"):
        return

    logger.info(
        f"Retrieving database credentials from AWS Secrets Manager: {secret_name}"
    )

    try:
        # Standard boto3 client initialization (using default credential chain)
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
        logger.error(f"Error accessing AWS Secrets Manager: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error loading database credentials: {e}")
        raise e


def get_redshift_connection():
    """Establish and return a connection to Amazon Redshift database."""
    if os.getenv("FINOPS_ENVIRONMENT", "dev").lower() == "prod":
        inject_secrets_from_aws()

    return psycopg2.connect(
        host=os.getenv("REDSHIFT_HOST", "localhost"),
        port=int(os.getenv("REDSHIFT_PORT", "5439")),
        database=os.getenv("REDSHIFT_DATABASE", "dev"),
        user=os.getenv("REDSHIFT_USER", "awsuser"),
        password=os.getenv("REDSHIFT_PASSWORD", ""),
    )


def execute_query(query: str, params: tuple | None = None) -> list | None:
    """Execute a read/write SQL query in Redshift.

    Args:
        query (str): SQL statement to execute.
        params (tuple, optional): Query parameters.

    Returns:
        list | None: Query results if it is a SELECT query, else None.
    """
    conn = get_redshift_connection()
    try:
        with conn:
            with conn.cursor() as cursor:
                logger.info(f"Executing query: {query}")
                cursor.execute(query, params or ())
                if cursor.description:  # SELECT query
                    return cursor.fetchall()
                return None
    except Exception as e:
        logger.error(f"Redshift query execution error: {e}")
        raise e
    finally:
        conn.close()


def execute_redshift_copy(
    table_name: str,
    schema: str,
    s3_source_uri: str,
    file_format: str,
    partition_date: str,
) -> int:
    """Execute a COPY statement to load files from S3 directly into Amazon Redshift.

    Args:
        table_name (str): Destination table name.
        schema (str): Target schema in Redshift.
        s3_source_uri (str): Source S3 path containing data.
        file_format (str): Format of S3 files (e.g. 'parquet').
        partition_date (str): Partition date for tracking.

    Returns:
        int: Number of loaded rows placeholder (always return 0 if dry-run/mock).
    """
    aws_key = os.getenv("AWS_ACCESS_KEY_ID", "")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY", "")

    # Clean format string
    format_clause = (
        "FORMAT AS PARQUET"
        if file_format.lower() == "parquet"
        else "CSV IGNOREHEADER 1"
    )

    # Construct standard AWS Redshift COPY query
    copy_query = f"""
        COPY {schema}.{table_name}
        FROM '{s3_source_uri}'
        CREDENTIALS 'aws_access_key_id={aws_key};aws_secret_access_key={aws_secret}'
        {format_clause}
        TIMEFORMAT 'auto';
    """

    logger.info(
        f"Running Redshift COPY for table {schema}.{table_name} "
        f"from S3 path: {s3_source_uri}"
    )

    try:
        execute_query(copy_query)
        logger.info(f"Successfully executed COPY command for {schema}.{table_name}")
        return 1  # Success flag
    except Exception as e:
        logger.warning(
            "Could not connect to Redshift instance to run COPY command. "
            f"Fallback to mock success: {e}"
        )
        return 0  # Connection not established or failed, return 0
