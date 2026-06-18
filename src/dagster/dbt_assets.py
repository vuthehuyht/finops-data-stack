"""dbt assets configuration for Redshift."""

from __future__ import annotations

import datetime
import functools
import json
import logging
import os
import pathlib
import re
from collections.abc import Iterator, Mapping
from typing import Any

import dagster
import dagster_dbt
from dagster_dbt import dagster_dbt_translator
from slack_sdk.web.client import WebClient

import src.dagster.environment as environment
import src.pipeline.dagster as dagster_lib
from src.dagster import resources
from src.pipeline.dagster.k8s import kubernetes_cluster_name

_PROJECT_ROOT = pathlib.Path(__file__).parent.parent.parent
DBT_PROJECT_DIR = _PROJECT_ROOT / "src" / "transform" / "dbt"
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

    def get_description(self, dbt_resource_props: Mapping[str, Any]) -> str:
        """Filter the metadata and drop unnecessary information."""
        return dagster_dbt.asset_utils.default_description_fn(dbt_resource_props, False)


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


def fetch_row_count(
    asset_key: dagster.AssetKey,
    redshift: resources.RedshiftResource,
    logger: logging.Logger,
) -> int:
    """Get count from Redshift for the given table."""
    count_redshift = 0
    schema = asset_key.parts[0].lower()
    table = asset_key.parts[1].lower()
    try:
        with redshift.get_connection() as conn:
            with conn.cursor() as cursor:
                query = f'SELECT COUNT(*) FROM "{schema}"."{table}"'
                cursor.execute(query)
                result = cursor.fetchone()
                if result:
                    count_redshift = int(result[0])
    except Exception as e:
        logger.error(f"Error retrieving row count for {schema}.{table}: {e}")
    return count_redshift


def fetch_update_count_from_entity_status_table(
    table_name: str,
    redshift: resources.RedshiftResource,
    context: dagster.AssetExecutionContext,
) -> int:
    """Fetch materialization count from the ENTITY_STATUS table."""
    count_redshift = 0
    try:
        with redshift.get_connection() as conn:
            with conn.cursor() as cursor:
                # Assuming ENTITY_STATUS resides in operation schema
                query = """
                SELECT count
                FROM operation.entity_status
                WHERE entity_name = %s;
                """
                cursor.execute(query, (table_name,))
                result = cursor.fetchone()
                if result:
                    count_redshift = int(result[0])
                else:
                    context.log.info(f"No count found for entity {table_name}")
    except Exception as e:
        context.log.error(f"Error retrieving operation.entity_status: {e}")
    return count_redshift


def should_materialize(expected_count: int, current_count: int) -> bool:
    """Check if asset should be materialized based on count and current records."""
    return expected_count == 1 or (
        expected_count > 1 and expected_count <= current_count
    )


def get_unique_id(metadata: Mapping[str, Any]) -> str:
    """Get unique_id from metadata safely."""
    try:
        return metadata["dagster_dbt/unique_id"]
    except KeyError as e:
        raise KeyError("Required metadata fields missing. Is this a dbt asset?") from e


def get_dbt_manifest(metadata: Mapping[str, Any]) -> Mapping[str, Any]:
    """Get manifest from metadata safely."""
    try:
        manifest_wrapper = metadata["dagster_dbt/manifest"]
    except KeyError as e:
        raise KeyError("Required metadata fields missing. Is this a dbt asset?") from e
    if not isinstance(manifest_wrapper, dagster_dbt_translator.DbtManifestWrapper):
        raise TypeError(
            "Invalid type for dagster_dbt/manifest in metadata. "
            "Expected DbtManifestWrapper."
        )
    return manifest_wrapper.manifest


def get_expected_update_count_before_materialize(metadata: Mapping[str, Any]) -> int:
    """Get materialize_count from manifest."""
    unique_id = get_unique_id(metadata)
    manifest = get_dbt_manifest(metadata)
    return (
        manifest["nodes"][unique_id]
        .get("meta", {})
        .get("datacore", {})
        .get("materialize_count", 1)
    )


def _compile_dbt(
    context: dagster.AssetExecutionContext,
    dbt: dagster_dbt.DbtCliResource,
    compile_target_path: str,
) -> None:
    """Compile dbt project and generate artifacts."""
    try:
        invocation = dbt.cli(
            [
                "compile",
                "--target-path",
                compile_target_path,
            ],
            context=context,
        )
        invocation.wait()
    except Exception as e:
        context.log.error(f"Error compiling dbt: {e}")


def _build_dbt_args(
    context: dagster.AssetExecutionContext,
    dbt: dagster_dbt.DbtCliResource,
    dbt_config: resources.DbtConfigResource,
) -> list[str]:
    """Build dbt arguments based on context and dbt_config."""
    if not context.has_partition_key:
        raise ValueError("The dbt_project_assets requires a partition key.")

    partition_key = context.partition_key
    job_name = context.job_name

    # Vietnamese / Ho Chi Minh timezone shift
    # In Vietnam, we shift by +1 day for realtime if partition key is standard date.
    today_for_realtime = datetime.datetime.strptime(
        partition_key, "%Y-%m-%d"
    ) + datetime.timedelta(days=1)
    today_for_realtime_iso = today_for_realtime.isoformat()

    variables: dict[str, Any] = {
        "partition_key": partition_key,
        "batch_date": partition_key,
        "dagster_job_name": job_name,
        "today_for_realtime": today_for_realtime_iso,
    }

    if dbt_config.days_offset_for_output_diff is not None:
        variables["days_offset_for_output_diff"] = (
            dbt_config.days_offset_for_output_diff
        )
    if dbt_config.variables:
        variables.update(dbt_config.variables)

    dbt_args = [
        "build",
        "--no-use-colors",
        "--no-use-colors-file",
        "--vars",
        json.dumps(variables),
    ]

    dbt_args += ["--exclude-resource-type", "unit_test"]

    if dbt_target := os.getenv("DAGSTER_DBT_TARGET"):
        dbt_args += ["--target", dbt_target]
    if dbt_config.no_data_test:
        dbt_args += ["--exclude-resource-type", "data_test"]
    if dbt_config.full_refresh:
        dbt_args.append("--full-refresh")
    if dbt_config.empty:
        dbt_args.append("--empty")

    if dbt_config.use_prod_upstream and not environment.is_prod():
        prod_compile_path = (
            f"{os.environ.get('DBT_TARGET_PATH', 'target')}/prod_compile_artifacts"
        )
        context.log.info(
            "Compiling dbt projects for prod to refer prod tables for upstream dependencies."
        )
        _compile_dbt(context, dbt, prod_compile_path)
        dbt_args += [
            "--defer",
            "--state",
            prod_compile_path,
            "--favor-state",
            "--target-path",
            f"{os.environ.get('DBT_TARGET_PATH', 'target')}/non_prod_target",
        ]

    return dbt_args


type DbtAssetEvent = (
    dagster.Output[Any]
    | dagster.AssetMaterialization
    | dagster.AssetObservation
    | dagster.AssetCheckResult
    | dagster.AssetCheckEvaluation
)


def send_full_refresh_notification(
    asset_key: dagster.AssetKey,
    run: dagster.DagsterRun | None = None,
    logger: logging.Logger | None = None,
) -> None:
    """Send a notification for full refresh."""
    try:
        cluster = kubernetes_cluster_name()
        if cluster is None:
            return
        slack_token = os.getenv("DAGSTER_SLACK_API_TOKEN")
        if not slack_token:
            if logger:
                logger.warning(
                    "DAGSTER_SLACK_API_TOKEN not set, skipping Slack notification."
                )
            return
        slack = WebClient(slack_token)
        plain_text = f"{asset_key.parts[-1]} has been fully refreshed."
        if run:
            run_url = f"/runs/{run.run_id}"
            text = f"<{run_url}|{plain_text}>"
        else:
            text = plain_text
        slack.chat_postMessage(
            channel=f"#dagster-{cluster}",
            text=text,
        )
    except Exception as e:
        if logger:
            logger.error(f"Failed to send full refresh notification: {e}")


def _process_dbt_event(
    dagster_event: DbtAssetEvent,
    context: dagster.AssetExecutionContext,
    dbt_config: resources.DbtConfigResource,
    redshift: resources.RedshiftResource,
) -> Iterator[DbtAssetEvent]:
    """Process each dbt event and yield appropriate Dagster events."""
    if dbt_config.full_refresh:
        if isinstance(dagster_event, dagster.AssetMaterialization):
            send_full_refresh_notification(
                dagster_event.asset_key, run=context.run, logger=context.log
            )
        elif isinstance(dagster_event, dagster.Output):
            send_full_refresh_notification(
                dagster.AssetKey(dagster_event.output_name.split("__")),
                run=context.run,
                logger=context.log,
            )

    if dbt_config.no_materialization:
        if isinstance(
            dagster_event, dagster.AssetCheckResult | dagster.AssetObservation
        ):
            yield dagster_event
        elif isinstance(dagster_event, dagster.AssetMaterialization):
            context.log.info(
                f"Skipping AssetMaterialization event: {dagster_event.asset_key.to_user_string()}."
            )
        elif isinstance(dagster_event, dagster.Output):
            context.log.info(
                f"Skipping Output event: {dagster_event.output_name.replace('__', '/')}."
            )
    elif isinstance(dagster_event, dagster.Output):
        is_prod_env = os.getenv("DAGSTER_WORKSPACE_ENVIRONMENT") == "prod"
        asset_key = dagster.AssetKey(
            dagster_event.output_name.split("__", 2 if is_prod_env else 3)
        )

        metadata = {}
        try:
            metadata = get_dbt_asset_dependency().specs_by_key[asset_key].metadata
        except Exception:
            pass

        is_view = metadata.get("dagster-dbt/materialization_type", "") == "view"
        row_count = (
            None if is_view else fetch_row_count(asset_key, redshift, context.log)
        )

        context.add_output_metadata(
            metadata={
                "full_refresh": dbt_config.full_refresh,
                "empty": dbt_config.empty,
                "no_data_test": dbt_config.no_data_test,
                "variables": dbt_config.variables,
                "dagster/row_count": row_count if row_count is not None else 0,
            },
            output_name=dagster_event.output_name,
        )

        table_name = metadata.get("dagster/table_name", asset_key.path[-1])
        expected_updates_per_day = (
            get_expected_update_count_before_materialize(metadata) if metadata else 1
        )
        update_count = fetch_update_count_from_entity_status_table(
            table_name, redshift, context
        )

        if should_materialize(expected_updates_per_day, update_count):
            context.log.info(
                f"should_materialize({expected_updates_per_day}, {update_count}) == True"
            )
            yield dagster_event
        else:
            context.log.info(
                f"Skipped because should_materialize({expected_updates_per_day}, {update_count}) == False"
            )
    else:
        yield dagster_event


def get_k8s_config(required_memory_gib: int = 3) -> dict[str, Any]:
    """Generate minimal K8s resources requirements config."""
    return {
        "container_config": {
            "resources": {
                "requests": {"memory": f"{required_memory_gib}Gi", "cpu": "500m"},
                "limits": {"memory": f"{required_memory_gib}Gi", "cpu": "1000m"},
            }
        }
    }


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
        k8s_config=get_k8s_config(3),
        select=select,
        exclude=exclude,
    )
    def dbt_project_assets(
        context: dagster.AssetExecutionContext,
        dbt: dagster_dbt.DbtCliResource,
        dbt_config: resources.DbtConfigResource,
        redshift: resources.RedshiftResource,
    ) -> Iterator[DbtAssetEvent]:
        """All dbt project assets execution."""
        dbt_args = _build_dbt_args(context, dbt, dbt_config)

        buffered_dbt_events: list[
            dagster.Output[Any] | dagster.AssetMaterialization
        ] = []

        try:
            dbt_cli_invocation = dbt.cli(dbt_args, context=context)
            for dagster_event in dbt_cli_invocation.stream():
                for event in _process_dbt_event(
                    dagster_event, context, dbt_config, redshift
                ):
                    match event:
                        case dagster.Output() | dagster.AssetMaterialization():
                            buffered_dbt_events.append(event)
                        case _:
                            yield event
        except Exception as e:
            path = get_compiled_code_path(str(e))
            if path and os.path.exists(path):
                with open(path) as f:
                    context.log.info(f"Compiled code:\n\n{f.read()}")
            raise

        yield from buffered_dbt_events

    return dbt_project_assets


# Asset Job executing all dbt assets
dbt_build_job = dagster_lib.define_asset_job(
    name="dbt_build_job",
    selection=dagster.AssetSelection.assets("dbt_project_assets"),
)
