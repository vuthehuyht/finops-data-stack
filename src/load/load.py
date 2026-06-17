"""Logic to load data from S3 to AWS Redshift."""

import logging
import re
import time
from typing import Any

from src.common.redshift_util import execute_query

logger = logging.getLogger(__name__)

# Regex to validate database identifiers (Redshift)
_RE_VALID_REDSHIFT_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z_0-9$]*$")


def _is_valid_identifier(identifier: str) -> bool:
    """Verify if a table/schema name is a valid identifier to prevent SQL injection."""
    return _RE_VALID_REDSHIFT_IDENTIFIER.match(identifier) is not None


def _build_copy_query(
    temp_table: str,
    s3_url: str,
    file_format: str,
    iam_role_arn: str,
) -> str:
    """Dynamically construct a Redshift COPY query based on the file format.

    Args:
        temp_table: Name of the temporary staging table.
        s3_url: S3 prefix or file path containing the data.
        file_format: The file format ('parquet', 'json', 'csv').
        iam_role_arn: IAM Role ARN with S3 read access.

    Returns:
        A complete SQL COPY query.
    """
    if not s3_url.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_url}")

    fmt = file_format.lower()
    if fmt == "parquet":
        format_clause = "FORMAT AS PARQUET"
    elif fmt == "json":
        format_clause = "FORMAT AS JSON 'auto'\nTIMEFORMAT 'auto'"
    elif fmt == "csv":
        format_clause = "CSV IGNOREHEADER 1\nTIMEFORMAT 'auto'\nDATEFORMAT 'auto'"
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

    # Semicolon is required at the end of COPY statements in Redshift
    return f"""
        COPY {temp_table}
        FROM '{s3_url}'
        IAM_ROLE '{iam_role_arn}'
        {format_clause};
    """


def load_s3_to_redshift(
    cursor: Any,
    s3_url: str,
    table_name: str,
    schema: str,
    file_format: str,
    iam_role_arn: str,
) -> None:
    """Load data from S3 to Redshift target table using a temporary staging table.

    Args:
        cursor: psycopg2 cursor object connected to Redshift.
        s3_url: S3 path containing data (file or folder).
        table_name: Destination table name.
        schema: Target schema name.
        file_format: File format ('parquet', 'json', 'csv').
        iam_role_arn: IAM Role ARN associated with Redshift cluster to access S3.
    """
    if not _is_valid_identifier(table_name):
        raise ValueError(f"Invalid table name: {table_name}")
    if not _is_valid_identifier(schema):
        raise ValueError(f"Invalid schema name: {schema}")

    target_table = f"{schema}.{table_name}"
    unix_timestamp = int(time.time())
    # Unique temporary table name to prevent naming conflict during parallel execution
    temp_table = f"temp_{table_name}_{unix_timestamp}"

    logger.info(
        "Loading data from S3 path: %s into table: %s via temporary table: %s",
        s3_url,
        target_table,
        temp_table,
    )

    try:
        # Start a transaction
        execute_query(cursor, "BEGIN;")

        # 1. Create a temporary staging table mimicking the target table structure
        create_temp_query = (
            f"CREATE TEMPORARY TABLE {temp_table} (LIKE {target_table});"
        )
        execute_query(cursor, create_temp_query)

        # 2. Execute COPY command into the staging table
        copy_query = _build_copy_query(
            temp_table=temp_table,
            s3_url=s3_url,
            file_format=file_format,
            iam_role_arn=iam_role_arn,
        )
        execute_query(cursor, copy_query)

        # 3. Append all data from the temporary table to the target table
        insert_query = f"INSERT INTO {target_table} SELECT * FROM {temp_table};"
        execute_query(cursor, insert_query)

        # Commit transaction
        execute_query(cursor, "COMMIT;")
        logger.info("Successfully loaded data into table: %s", target_table)

    except Exception as e:
        logger.error(
            "Error loading data into table: %s, rolling back transaction. Error: %s",
            target_table,
            e,
        )
        try:
            execute_query(cursor, "ROLLBACK;")
        except Exception as rollback_err:
            logger.error("Failed to rollback transaction: %s", rollback_err)
        raise e
