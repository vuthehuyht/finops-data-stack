"""Publish SageMaker Batch Transform forecast output to Redshift.

Mirrors the COPY-into-staging-table pattern in src/load/load.py, adapted so
the follow-up INSERT carries literal trading_date/model_version/audit
columns that aren't present in the raw Batch Transform output file.
"""

import logging
import time
from typing import Any

from src.common.redshift_util import execute_query

logger = logging.getLogger(__name__)

_TARGET_TABLE = "MART.FCT_ML_FORECAST_RESULTS"
_PROGRAM_NAME = "publish_forecast_results"


def _build_copy_query(staging_table: str, s3_url: str, iam_role_arn: str) -> str:
    """Build the COPY query loading raw Batch Transform output into staging.

    Args:
        staging_table: Name of the temporary staging table.
        s3_url: Exact S3 URI of the Batch Transform output file
            (`{"ticker": ..., "predicted_return": ...}` per line).
        iam_role_arn: IAM Role ARN authorizing Redshift to read from S3.

    Returns:
        A complete SQL COPY query.

    Raises:
        ValueError: If `s3_url` is not an S3 URL.
    """
    if not s3_url.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_url}")
    return (
        f"COPY {staging_table} (TICKER, PREDICTED_RETURN)\n"
        f"FROM '{s3_url}'\n"
        f"IAM_ROLE '{iam_role_arn}'\n"
        "FORMAT AS JSON 'auto';"
    )


def publish_forecast_results(
    cursor: Any,
    s3_url: str,
    iam_role_arn: str,
    trading_date: str,
    model_version: str,
) -> None:
    """COPY Batch Transform output into staging, then publish to Redshift Gold.

    Deletes any existing rows for `trading_date` before inserting, keeping
    re-runs idempotent — same semantics as the row-by-row INSERT this
    replaces.

    Args:
        cursor: An open psycopg2 cursor (see `RedshiftResource.get_connection`).
        s3_url: Exact S3 URI of the Batch Transform output file.
        iam_role_arn: IAM Role ARN authorizing Redshift to read from S3.
        trading_date: ISO date the forecast applies to (bound parameter, not
            interpolated — caller-controlled but validated upstream via
            `_validate_iso_date` in `src/dagster/inference_job.py`).
        model_version: SageMaker model version string that produced the
            forecast.

    Raises:
        ValueError: If `s3_url` is not an S3 URL.
    """
    staging_table = f"temp_ml_forecast_{int(time.time())}"

    try:
        execute_query(cursor, "BEGIN;")
        execute_query(
            cursor,
            f"CREATE TEMPORARY TABLE {staging_table} "
            "(TICKER VARCHAR(256), PREDICTED_RETURN NUMERIC(18, 6));",
        )
        execute_query(cursor, _build_copy_query(staging_table, s3_url, iam_role_arn))
        execute_query(
            cursor,
            f"DELETE FROM {_TARGET_TABLE} WHERE TRADING_DATE = %s;",
            (trading_date,),
        )
        execute_query(
            cursor,
            f"""
            INSERT INTO {_TARGET_TABLE}
                (TICKER, TRADING_DATE, PREDICTED_RETURN, MODEL_VERSION,
                 DATACORE_CREATE_DATETIME, DATACORE_CREATE_PROGRAM,
                 DATACORE_CREATE_BY, DATACORE_UPDATE_DATETIME,
                 DATACORE_UPDATE_PROGRAM, DATACORE_UPDATE_BY, BATCH_DATE)
            SELECT TICKER, %s, PREDICTED_RETURN, %s, CURRENT_TIMESTAMP, %s,
                   CURRENT_USER, CURRENT_TIMESTAMP, %s, CURRENT_USER, %s
            FROM {staging_table};
            """,
            (
                trading_date,
                model_version,
                _PROGRAM_NAME,
                _PROGRAM_NAME,
                trading_date,
            ),
        )
        execute_query(cursor, "COMMIT;")
        logger.info(
            "Successfully published forecast results for trading_date: %s", trading_date
        )

    except Exception as e:
        logger.error(
            "Error publishing forecast for %s, rolling back: %s", trading_date, e
        )
        try:
            execute_query(cursor, "ROLLBACK;")
        except Exception as rollback_err:
            logger.error("Failed to rollback transaction: %s", rollback_err)
        raise e
