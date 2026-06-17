"""Dagster jobs for dbt Silver layer transformations (Bronze → Silver)."""

import csv
import enum
import functools
import json
import os
from collections.abc import Iterator
from dataclasses import dataclass, field

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    JobDefinition,
    MultiAssetSensorEvaluationContext,
    Output,
    RunConfig,
    RunRequest,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    SensorDefinition,
    SkipReason,
    multi_asset_sensor,
    schedule,
)
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)
from dagster_dbt import DbtCliResource

import src.pipeline.dagster as dagster_lib
from src.dagster.resources import DbtConfigResource

_TIMEZONE = "Asia/Ho_Chi_Minh"
_FETCH_LIMIT = 30
_SILVER_JOB_DEFINITION_FILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "transform_job_defs.csv")
)


@enum.unique
class TriggerType(enum.Enum):
    """Trigger types for transform jobs."""

    Sensor = "SENSOR"
    Schedule = "SCHEDULE"


@dataclass
class TransformJobParameter:
    """Parameters for a single Silver dbt transform job."""

    schema_suffix: str
    table_name: str
    trigger_type: TriggerType
    trigger_parameter: str


@dataclass
class SilverJobBundle:
    """Return value of define_silver_jobs() — consumed by workspace.py."""

    assets: list[AssetsDefinition] = field(default_factory=list)
    jobs: list[JobDefinition | UnresolvedAssetJobDefinition] = field(
        default_factory=list
    )
    schedules: list[ScheduleDefinition] = field(default_factory=list)
    sensors: list[SensorDefinition] = field(default_factory=list)


def read_transform_job_parameter(csv_file: str) -> Iterator[TransformJobParameter]:
    """Parse a transform job CSV into TransformJobParameter objects."""
    with open(csv_file, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            yield TransformJobParameter(
                schema_suffix=row["schema_suffix"].upper(),
                table_name=row["table_name"].lower(),
                trigger_type=TriggerType((row.get("trigger_type") or "SENSOR").upper()),
                trigger_parameter=row.get("trigger_parameter") or "",
            )


def _get_upstream_bronze_key(silver_table_name: str) -> AssetKey:
    """Derive the BRONZE asset key from a Silver staging model name.

    Convention: stg_<entity> -> BRONZE/RAW_<ENTITY>
    """
    entity = silver_table_name.removeprefix("stg_")
    return dagster_lib.asset_key(["BRONZE", f"raw_{entity}".upper()])


def _create_dbt_model_asset(
    parameter: TransformJobParameter,
    upstream_key: AssetKey,
) -> AssetsDefinition:
    """Create a Dagster asset that runs one dbt model (Bronze → Silver)."""
    asset_key = dagster_lib.asset_key(
        [parameter.schema_suffix, parameter.table_name.upper()]
    )

    @dagster_lib.asset(
        key=asset_key,
        deps=[upstream_key],
        group_name=parameter.schema_suffix,
        kinds={"dbt", "redshift"},
        description=f"dbt Silver model {parameter.table_name}.",
    )
    def dbt_model(
        context: AssetExecutionContext,
        dbt: DbtCliResource,
        dbt_config: DbtConfigResource,
    ) -> Output[None]:
        dbt_args = ["run", "--select", parameter.table_name, "--no-use-colors"]
        if dbt_config.variables:
            dbt_args += ["--vars", json.dumps(dbt_config.variables)]
        if dbt_config.full_refresh:
            dbt_args.append("--full-refresh")
        context.log.info("Running: dbt %s", " ".join(dbt_args))
        dbt.cli(dbt_args).wait()
        return Output(
            value=None,
            metadata={"dbt_model": parameter.table_name},
        )

    return dbt_model


def _make_transform_schedule(
    job: JobDefinition | UnresolvedAssetJobDefinition,
    job_name: str,
    cron: str,
    asset_key: AssetKey,
) -> ScheduleDefinition:
    """Create a schedule that runs a transform job at the given cron expression."""

    @schedule(
        job=job,
        cron_schedule=cron,
        execution_timezone=_TIMEZONE,
        name=f"{job_name}_schedule",
    )
    def _schedule(
        context: ScheduleEvaluationContext,
        _asset_key: AssetKey = asset_key,
    ) -> RunRequest:
        partition_key = context.scheduled_execution_time.date().isoformat()
        return RunRequest(
            run_config=RunConfig(
                resources={
                    "dbt_config": DbtConfigResource(
                        variables={"partition_key": partition_key}
                    )
                }
            )
        )

    return _schedule


def _create_sensor_for_jobs(
    sensor_name: str,
    all_upstream_keys: list[AssetKey],
    sensor_jobs: list[JobDefinition | UnresolvedAssetJobDefinition],
    asset_to_upstream: dict[AssetKey, AssetKey],
) -> SensorDefinition:
    """Create a multi-asset sensor to trigger transform jobs when Bronze is ready."""

    @multi_asset_sensor(
        name=sensor_name,
        description="Trigger Silver dbt jobs when Bronze assets are materialized.",
        monitored_assets=all_upstream_keys,
        jobs=sensor_jobs,
        minimum_interval_seconds=60,
    )
    def _sensor(
        context: MultiAssetSensorEvaluationContext,
    ) -> Iterator[RunRequest | SkipReason]:
        job_by_upstream: dict[
            AssetKey, JobDefinition | UnresolvedAssetJobDefinition
        ] = {}
        for asset_key, upstream_key in asset_to_upstream.items():
            for job in sensor_jobs:
                if asset_key.to_python_identifier() in job.name:
                    job_by_upstream[upstream_key] = job
                    break

        for key, asset_event, materialization in dagster_lib.fetch_materializations(
            context, fetch_limit_for_each_asset=_FETCH_LIMIT
        ):
            context.advance_cursor({key: asset_event})
            conata_key = materialization.metadata.get("conata_partition_key")
            partition_key = str(conata_key.value) if conata_key is not None else None
            if key in job_by_upstream:
                job = job_by_upstream[key]
                run_key = (
                    f"{key.to_python_identifier()}_{partition_key}"
                    if partition_key
                    else key.to_python_identifier()
                )
                dbt_vars = {"partition_key": partition_key} if partition_key else {}
                yield RunRequest(
                    job_name=job.name,
                    run_key=run_key,
                    run_config=RunConfig(
                        resources={"dbt_config": DbtConfigResource(variables=dbt_vars)}
                    ),
                )

    return _sensor


def _define_jobs_and_triggers(
    definition_file: str,
    sensor_name: str,
) -> SilverJobBundle:
    """Build Silver job bundle: assets, jobs, schedules, sensor."""
    bundle = SilverJobBundle()
    sensor_jobs: list[JobDefinition | UnresolvedAssetJobDefinition] = []
    asset_to_upstream: dict[AssetKey, AssetKey] = {}
    all_upstream_keys: list[AssetKey] = []

    for param in read_transform_job_parameter(definition_file):
        upstream_key = _get_upstream_bronze_key(param.table_name)
        asset = _create_dbt_model_asset(param, upstream_key)
        bundle.assets.append(asset)

        job_name = f"transform_{asset.key.to_python_identifier()}_job"
        job = dagster_lib.define_asset_job(
            job_name,
            selection=[asset],
            tags={
                "limit_concurrent_job_runs_to_1": job_name,
                "type": "transform",
            },
        )
        bundle.jobs.append(job)
        asset_to_upstream[asset.key] = upstream_key

        match param.trigger_type:
            case TriggerType.Sensor:
                sensor_jobs.append(job)
                if upstream_key not in all_upstream_keys:
                    all_upstream_keys.append(upstream_key)
            case TriggerType.Schedule:
                bundle.schedules.append(
                    _make_transform_schedule(
                        job, job_name, param.trigger_parameter, asset.key
                    )
                )

    if sensor_jobs:
        bundle.sensors.append(
            _create_sensor_for_jobs(
                sensor_name, all_upstream_keys, sensor_jobs, asset_to_upstream
            )
        )

    return bundle


@functools.cache
def define_silver_jobs() -> SilverJobBundle:
    """Define dbt Silver staging jobs and sensor."""
    return _define_jobs_and_triggers(
        _SILVER_JOB_DEFINITION_FILE,
        sensor_name="silver_job_sensor",
    )
