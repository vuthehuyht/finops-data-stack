"""dbt assets configuration for Redshift."""

from __future__ import annotations

import functools
import os
import pathlib
import re
from collections.abc import Iterator, Mapping
from typing import Any

import dagster
import dagster_dbt

import src.pipeline.dagster as dagster_lib
from src.dagster import resources

_PROJECT_ROOT = pathlib.Path(__file__).parent.parent.parent
DBT_PROJECT_DIR = _PROJECT_ROOT / "src" / "transform"
DBT_MANIFEST_FILE_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"


class FwCustomDagsterDbtTranslator(dagster_dbt.DagsterDbtTranslator):
    """Custom dbt model translator mapping asset keys to uppercase."""

    def get_asset_key(
        self,
        dbt_resource_props: Mapping[str, Any],
    ) -> dagster.AssetKey:
        """Get the Dagster asset key with uppercase parts."""
        asset_key = super().get_asset_key(dbt_resource_props)
        uppercased_path = [part.upper() for part in asset_key.path]
        asset_key = dagster.AssetKey(uppercased_path)
        if prefix := os.getenv("DAGSTER_ASSET_PREFIX"):
            return asset_key.with_prefix(prefix)
        return asset_key

    def get_metadata(self, dbt_resource_props: Mapping[str, Any]) -> Mapping[str, Any]:
        """Filter metadata and drop unnecessary column schema if needed."""
        metadata = super().get_metadata(dbt_resource_props)
        return metadata


@functools.cache
def get_dbt_asset_dependency(
    select: str = "fqn:*",
    exclude: str | None = None,
) -> dagster.AssetsDefinition:
    """Get the asset graph for all dbt assets but dependency only."""
    dbt_project = dagster_dbt.DbtProject(project_dir=DBT_PROJECT_DIR)

    @dagster_dbt.dbt_assets(
        manifest=DBT_MANIFEST_FILE_PATH,
        select=select,
        exclude=exclude,
        project=dbt_project,
        dagster_dbt_translator=FwCustomDagsterDbtTranslator(),
    )
    def _dbt_asset_dependency() -> Any:
        yield from []

    return _dbt_asset_dependency


def get_compiled_code_path(error_message: str) -> str | None:
    """Get the compiled code path from the error message."""
    match = re.search(r"compiled code at (\S+\.sql)", error_message)
    if match:
        return match.group(1)
    return None


type DbtAssetEvent = (
    dagster.Output[Any]
    | dagster.AssetMaterialization
    | dagster.AssetObservation
    | dagster.AssetCheckResult
    | dagster.AssetCheckEvaluation
)


@functools.lru_cache
def get_dbt_project_assets(
    name: str = "dbt_project_assets",
    select: str = "fqn:*",
    exclude: str | None = None,
) -> dagster.AssetsDefinition:
    """Get the dbt project assets."""
    dbt_project = dagster_dbt.DbtProject(project_dir=DBT_PROJECT_DIR)

    @dagster_lib.dbt_assets(
        name=name,
        manifest=DBT_MANIFEST_FILE_PATH,
        project=dbt_project,
        dagster_dbt_translator=FwCustomDagsterDbtTranslator(
            settings=dagster_dbt.DagsterDbtTranslatorSettings(
                enable_code_references=True
            )
        ),
        select=select,
        exclude=exclude,
    )
    def dbt_project_assets(
        context: dagster.AssetExecutionContext,
        dbt: dagster_dbt.DbtCliResource,
        dbt_config: resources.DbtConfigResource,
    ) -> Iterator[DbtAssetEvent]:
        """All dbt project assets execution."""
        dbt_args = ["build"]

        # Select target models
        if select:
            dbt_args += ["--select", select]
        if exclude:
            dbt_args += ["--exclude", exclude]

        if dbt_config.full_refresh:
            dbt_args.append("--full-refresh")

        if dbt_config.variables:
            import json
            dbt_args += ["--vars", json.dumps(dbt_config.variables)]

        try:
            dbt_cli_invocation = dbt.cli(dbt_args, context=context)
            yield from dbt_cli_invocation.stream()
        except Exception as e:
            path = get_compiled_code_path(str(e))
            if path and os.path.exists(path):
                with open(path) as f:
                    context.log.info("Compiled code:\n\n%s", f.read())
            raise

    return dbt_project_assets


# Asset Job executing all dbt assets
dbt_build_job = dagster_lib.define_asset_job(
    name="dbt_build_job",
    selection=dagster.AssetSelection.assets("dbt_project_assets"),
)
