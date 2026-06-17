"""
Custom decorators for Dagster assets and operations.
"""

import functools
from collections.abc import Callable
from typing import Any

import dagster


def finops_asset(
    schema: str,
    table_name: str,
    s3_prefix: str | None = None,
    concurrency_limit: int = 1,
    **dagster_kwargs: Any,
) -> Callable[[Callable[..., Any]], dagster.AssetsDefinition]:
    """Custom decorator wrapping Dagster @asset to standardize tags,
    metadata, and concurrency limit.

    Args:
        schema (str): Target Redshift schema name (e.g. 'raw_batch').
        table_name (str): Target Redshift table name.
        s3_prefix (str, optional): Target S3 path directory prefix.
        concurrency_limit (int): Maximum concurrent runs for this asset (default 1).
        dagster_kwargs: Standard kwargs forwarded to dagster.asset.

    Returns:
        Callable: The wrapped assets definition decorator.
    """

    def decorator(fn: Callable[..., Any]) -> dagster.AssetsDefinition:
        # 1. Standardize tags for concurrent run limit and tracking
        tags = dagster_kwargs.setdefault("tags", {})
        tags["schema"] = schema
        tags["table_name"] = table_name
        # Instructs Dagster Daemon queue coordinator to throttle concurrent executions
        tags[f"limit_concurrent_job_runs_to_{concurrency_limit}"] = "true"

        # 2. Standardize metadata dictionary for display on the Dagster UI
        metadata = dagster_kwargs.setdefault("metadata", {})
        metadata["schema"] = schema
        metadata["table"] = table_name
        if s3_prefix:
            metadata["s3_prefix"] = s3_prefix

        # 3. Compile standard Dagster asset grouped under the schema keyprefix
        @dagster.asset(name=table_name, key_prefix=[schema], **dagster_kwargs)
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        return wrapper

    return decorator
