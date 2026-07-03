"""Utility module for connection and querying with AWS Redshift Serverless."""

import logging
import os
import time
from typing import Any

import psycopg2

logger = logging.getLogger(__name__)


def get_redshift_connection() -> Any:
    """Establish and return a connection to Amazon Redshift.

    Credentials come from REDSHIFT_* env vars. In production these are
    injected by Kubernetes via secretKeyRef from the "dagster-pg-credentials"
    Secret (kept in sync from AWS Secrets Manager by External Secrets
    Operator — see infrastructure/helm/values.yaml), not fetched here.
    """
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
