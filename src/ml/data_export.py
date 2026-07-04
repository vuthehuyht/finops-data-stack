"""Export FACT_ML_FEATURE_SET from Redshift to S3 as Parquet.

Produces the SageMaker Training Job input for the quarterly re-training
pipeline. Mirrors the thin-wrapper pattern in `src/load/load.py`.
"""

from typing import Any

from src.common.redshift_util import execute_query

_SOURCE_TABLE = "MART.FACT_ML_FEATURE_SET"
_DEFAULT_LOOKBACK_MONTHS = 24


def _date_filter_clause(lookback_months: int) -> str:
    """Build the WHERE clause for filtering by lookback months.

    Args:
        lookback_months: Number of months of history to include.

    Returns:
        A WHERE clause string.
    """
    return f"WHERE TRADING_DATE >= DATEADD(month, -{lookback_months}, CURRENT_DATE)"


def build_unload_query(
    s3_url: str, iam_role_arn: str, lookback_months: int = _DEFAULT_LOOKBACK_MONTHS
) -> str:
    """Build a Redshift UNLOAD query exporting recent ML training data to S3.

    Args:
        s3_url: Destination S3 prefix (must start with "s3://").
        iam_role_arn: IAM Role ARN authorizing Redshift to write to S3.
        lookback_months: Number of months of history to include.

    Returns:
        A complete UNLOAD SQL statement.

    Raises:
        ValueError: If `s3_url` is not an S3 URL.
    """
    if not s3_url.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_url}")

    select_query = (
        f"SELECT * FROM {_SOURCE_TABLE} {_date_filter_clause(lookback_months)}"
    )
    return (
        f"UNLOAD ('{select_query}')\n"
        f"TO '{s3_url}'\n"
        f"IAM_ROLE '{iam_role_arn}'\n"
        "FORMAT AS PARQUET\n"
        "ALLOWOVERWRITE"
    )


def unload_training_dataset(
    cursor: Any,
    s3_url: str,
    iam_role_arn: str,
    lookback_months: int = _DEFAULT_LOOKBACK_MONTHS,
) -> int:
    """Run the UNLOAD query and return the number of rows exported.

    Args:
        cursor: An open psycopg2 cursor (see `RedshiftResource.get_connection`).
        s3_url: Destination S3 prefix.
        iam_role_arn: IAM Role ARN authorizing Redshift to write to S3.
        lookback_months: Number of months of history to include.

    Returns:
        The number of rows exported.

    Raises:
        RuntimeError: If the UNLOAD produced zero rows.
    """
    query = build_unload_query(s3_url, iam_role_arn, lookback_months)
    execute_query(cursor, query)

    count_query = (
        f"SELECT COUNT(*) FROM {_SOURCE_TABLE} {_date_filter_clause(lookback_months)}"
    )
    execute_query(cursor, count_query)
    row_count = cursor.fetchone()[0]
    if row_count == 0:
        raise RuntimeError(
            f"UNLOAD produced zero rows for {_SOURCE_TABLE} "
            f"(lookback_months={lookback_months})."
        )
    return row_count
