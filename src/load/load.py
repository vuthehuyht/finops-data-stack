"""Logic to load data from S3 to AWS Redshift."""

import io
import re
import time
from typing import Any
from urllib.parse import urlparse

import boto3
import pyarrow.parquet as pq
from dagster import get_dagster_logger

from src.common.redshift_util import execute_query

logger = get_dagster_logger()

# Regex to validate database identifiers (Redshift)
_RE_VALID_REDSHIFT_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z_0-9$]*$")


def _validate_parquet_schema_count(
    s3_url: str, expected_count: int, table_name: str, base_columns: list[str]
) -> None:
    """Validate that the parquet file on S3 has the expected number of columns."""
    parsed_url = urlparse(s3_url)
    bucket = parsed_url.netloc
    prefix = parsed_url.path.lstrip("/")

    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    parquet_keys = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    if not parquet_keys:
        raise ValueError(f"No parquet files found in {s3_url}")

    obj_response = s3.get_object(Bucket=bucket, Key=parquet_keys[0])
    parquet_file = pq.ParquetFile(io.BytesIO(obj_response["Body"].read()))
    parquet_cols = parquet_file.schema.names

    missing_cols = set(base_columns) - set(parquet_cols)

    if missing_cols:
        raise ValueError(
            f"Schema mismatch for {table_name}: Parquet file is missing "
            f"columns {missing_cols}. Parquet has {len(parquet_cols)} columns "
            f"({parquet_cols}), but Redshift needs {len(base_columns)} base columns "
            f"({base_columns})."
        )


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


def load_s3_to_redshift(  # noqa: C901
    cursor: Any,
    s3_url: str,
    table_name: str,
    schema: str,
    file_format: str,
    iam_role_arn: str,
    batch_date: str | None = None,
) -> int:
    """Load data from S3 to Redshift target table using a temporary staging table.

    Args:
        cursor: psycopg2 cursor object connected to Redshift.
        s3_url: S3 path containing data (file or folder).
        table_name: Destination table name.
        schema: Target schema name.
        file_format: File format ('parquet', 'json', 'csv').
        iam_role_arn: IAM Role ARN associated with Redshift cluster to access S3.
        batch_date: Optional batch date for metadata injection.
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

    # Quote the target table to prevent reserved keyword conflicts (e.g. 'raw')
    target_table_quoted = f'"{schema}"."{table_name}"'

    # Get column names to handle schema evolution / missing columns
    get_cols_query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name = '{table_name.lower()}'
        ORDER BY ordinal_position;
    """
    execute_query(cursor, get_cols_query)
    all_columns = [row[0].lower() for row in cursor.fetchall()]

    if not all_columns:
        raise ValueError(f"No columns found for {target_table}. Ensure table exists.")

    base_columns = [
        c for c in all_columns if not c.startswith("_conata_") and c != "batch_date"
    ]
    base_cols_str = ", ".join(f'"{c}"' for c in base_columns)

    # 1. Create a temporary staging table with EXACTLY the base columns.
    create_temp_query = f"""
        CREATE TEMPORARY TABLE {temp_table} AS
        SELECT {base_cols_str}
        FROM {target_table_quoted}
        WHERE 1=0;
    """
    execute_query(cursor, create_temp_query)

    # 2. Add validation to ensure source data column count matches target base columns
    if file_format.lower() == "parquet":
        _validate_parquet_schema_count(
            s3_url, len(base_columns), table_name, base_columns
        )

    if file_format.lower() == "parquet":
        temp_table_with_cols = temp_table
    else:
        temp_table_with_cols = f"{temp_table} ({base_cols_str})"

    copy_query = _build_copy_query(
        temp_table=temp_table_with_cols,
        s3_url=s3_url,
        file_format=file_format,
        iam_role_arn=iam_role_arn,
    )
    execute_query(cursor, copy_query)

    # 3. Append data from the temporary table to the target table, injecting metadata
    select_items = []
    for col in all_columns:
        if col == "batch_date":
            val = f"'{batch_date}'" if batch_date else "NULL"
            select_items.append(f"{val} AS batch_date")
        elif col == "_conata_source":
            select_items.append(f"'{s3_url}' AS _conata_source")
        elif col == "_conata_source_row_number":
            select_items.append(
                "ROW_NUMBER() OVER(ORDER BY 1) AS _conata_source_row_number"
            )
        elif col == "_conata_partition_key":
            val = f"'{batch_date}'" if batch_date else "NULL"
            select_items.append(f"{val} AS _conata_partition_key")
        elif col == "_conata_loaded_at":
            select_items.append("SYSDATE AS _conata_loaded_at")
        else:
            select_items.append(f'"{col}"')

    select_clause = ", ".join(select_items)

    # Delete existing records for this partition to ensure idempotency
    if batch_date:
        delete_query = (
            f"DELETE FROM {target_table_quoted} "
            f"WHERE _CONATA_PARTITION_KEY = '{batch_date}';"
        )
        execute_query(cursor, delete_query)
    else:
        # Truncate the table if no batch_date is provided (Full Load)
        truncate_query = f"TRUNCATE TABLE {target_table_quoted};"
        execute_query(cursor, truncate_query)

    insert_query = (
        f"INSERT INTO {target_table_quoted} SELECT {select_clause} FROM {temp_table};"
    )
    execute_query(cursor, insert_query)

    rows_inserted = cursor.rowcount
    return rows_inserted
