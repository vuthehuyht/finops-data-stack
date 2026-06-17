"""Module initializer."""

# Import k8s to trigger the K8s StepHandler monkey-patch when the package is loaded.
# The patch lives in k8s.py wrapped in try/except ImportError so it is safe
# when dagster-k8s is absent (local dev / CI).
from . import k8s  # noqa: F401

from src.pipeline.dagster.decorators import (
    asset,
    asset_check,
    dbt_assets,
    job,
    multi_asset,
    multi_asset_check,
    op,
)
from src.pipeline.dagster.define_asset_jobs import define_asset_job
from src.pipeline.dagster.definitions import (
    definitions,
)
from src.pipeline.dagster.k8s import (
    kubernetes_cluster_name,
    on_k8s,
)
from src.pipeline.dagster.testing import (
    validate_definitions_and_run_configs,
)
from src.pipeline.dagster.utils import (
    asset_key,
    fetch_materializations,
)

__all__ = [
    "asset",
    "asset_check",
    "asset_key",
    "dbt_assets",
    "define_asset_job",
    "definitions",
    "fetch_materializations",
    "job",
    "kubernetes_cluster_name",
    "multi_asset",
    "multi_asset_check",
    "on_k8s",
    "op",
    "validate_definitions_and_run_configs",
]
