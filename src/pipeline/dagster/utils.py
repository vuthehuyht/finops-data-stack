"""Dagster API helper utilities."""

import os
from collections.abc import Generator

import dagster

# Import K8s infrastructure utilities from k8s.py for backward compatibility
# These will be removed from this module in Task 8 when __init__.py is updated
from src.pipeline.dagster.k8s import (
    _io_manager_bucket_name,
    io_manager,
    kubernetes_cluster_name,
    on_k8s,
)

__all__ = [
    "asset_key",
    "fetch_materializations",
    "kubernetes_cluster_name",
    "on_k8s",
    "_io_manager_bucket_name",
    "io_manager",
]


def asset_key(path: list[str]) -> dagster.AssetKey:
    """Create an asset key with an optional prefix based on the environment variable."""
    if not path:
        raise ValueError("Path must not be empty")
    key = dagster.AssetKey(path)
    prefix = os.getenv("DAGSTER_ASSET_PREFIX")
    if prefix and path[0] != prefix:
        # For assets in a workspace, the asset key should be the path.
        return key.with_prefix(prefix)
    return key


def fetch_materializations(
    context: dagster.MultiAssetSensorEvaluationContext,
    fetch_limit_for_each_asset: int = 10,
) -> Generator[
    tuple[
        dagster.AssetKey,
        dagster.EventLogRecord,
        dagster.AssetMaterialization,
    ],
    None,
    None,
]:
    """Fetches asset materialization event records, with the earliest event first.

    Only fetches events after the latest consumed event ID.
    """
    for key, has_asset_event in context.latest_materialization_records_by_key().items():
        if has_asset_event:
            # fetch all materialization, not only the latest one
            for asset_event in context.materialization_records_for_key(
                key,
                fetch_limit_for_each_asset,
            ):
                materialization = asset_event.asset_materialization
                if materialization is not None:
                    yield key, asset_event, materialization
