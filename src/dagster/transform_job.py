"""Dagster jobs for dbt Silver layer transformations using dbt assets."""

import csv
import enum
import functools
import os
import time
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field

import dagster
from dagster import (
    AssetKey,
    JobDefinition,
    MultiAssetSensorEvaluationContext,
    RunConfig,
    RunRequest,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    SensorDefinition,
    SkipReason,
)
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

import src.pipeline.dagster as dagster_lib
from src.dagster import dbt_assets
from src.dagster.resources import DbtConfigResource

_TIMEZONE = "Asia/Ho_Chi_Minh"
_FETCH_LIMIT = 30
_STAGING_JOB_DEFINITION_FILE = os.path.abspath(
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
    """Derive the RAW asset key from a Silver staging model name."""
    entity = silver_table_name.removeprefix("stg_")
    return dagster_lib.asset_key(["RAW", f"raw_{entity}".upper()])


def _make_transform_schedule(
    job: JobDefinition | UnresolvedAssetJobDefinition,
    job_name: str,
    cron: str,
) -> ScheduleDefinition:
    """Create a schedule that runs a transform job at the given cron expression."""

    @dagster.schedule(
        job=job,
        cron_schedule=cron,
        execution_timezone=_TIMEZONE,
        name=f"{job_name}_schedule",
    )
    def _schedule(
        context: ScheduleEvaluationContext,
    ) -> RunRequest:
        partition_key = context.scheduled_execution_time.date().isoformat()
        return RunRequest(
            run_config=RunConfig(
                resources={
                    "dbt_config": DbtConfigResource(
                        variables={
                            "partition_key": partition_key,
                            "batch_date": partition_key,
                        }
                    )
                }
            )
        )

    return _schedule


def _create_sensor_for_jobs(  # noqa: C901
    sensor_name: str,
    all_upstream_keys: list[AssetKey],
    sensor_jobs: list[JobDefinition | UnresolvedAssetJobDefinition],
    asset_to_upstream: dict[AssetKey, AssetKey],
) -> SensorDefinition:
    """Create a multi-asset sensor to trigger transform jobs when Bronze is ready."""

    @dagster.multi_asset_sensor(
        name=sensor_name,
        description="Trigger Silver dbt jobs when Bronze assets are materialized.",
        monitored_assets=all_upstream_keys,
        jobs=sensor_jobs,
        minimum_interval_seconds=60,
    )
    def _sensor(  # noqa: C901
        context: MultiAssetSensorEvaluationContext,
    ) -> Iterator[RunRequest | SkipReason]:
        # 1. Invert mapping: Upstream -> set of Jobs
        upstream_to_jobs: dict[
            AssetKey, list[JobDefinition | UnresolvedAssetJobDefinition]
        ] = defaultdict(list)
        for asset_key, upstream_key in asset_to_upstream.items():
            for job in sensor_jobs:
                if asset_key.to_python_identifier() in job.name:
                    if job not in upstream_to_jobs[upstream_key]:
                        upstream_to_jobs[upstream_key].append(job)

        # 2. Group new materializations by partition
        partition_to_materialized_assets: dict[str, set[AssetKey]] = defaultdict(set)
        for key, asset_event, materialization in dagster_lib.fetch_materializations(
            context, fetch_limit_for_each_asset=_FETCH_LIMIT
        ):
            context.advance_cursor({key: asset_event})

            # Extract Partition Key from Bronze
            partition_key = materialization.partition
            if not partition_key:
                conata_key = materialization.metadata.get("conata_partition_key")
                dbt_vars = materialization.metadata.get("variables")
                if conata_key is not None:
                    partition_key = str(conata_key.value)
                elif dbt_vars is not None and isinstance(dbt_vars.value, dict):
                    partition_key = dbt_vars.value.get("partition_key")

            if partition_key:
                partition_to_materialized_assets[partition_key].add(key)

        # 3. Process each partition independently
        for (
            partition_key,
            materialized_assets,
        ) in partition_to_materialized_assets.items():
            # Identify jobs to trigger
            # (Bronze->STG is 1-to-1, no need to cross-check upstreams)
            possible_jobs: list[JobDefinition | UnresolvedAssetJobDefinition] = []
            for asset in materialized_assets:
                if asset in upstream_to_jobs:
                    for job in upstream_to_jobs[asset]:
                        if job not in possible_jobs:
                            possible_jobs.append(job)

            # 4. Yield RunRequest for each job
            for job in possible_jobs:
                kwargs = {
                    "job_name": job.name,
                    "run_key": f"{job.name}_{partition_key}_{int(time.time())}",
                    "run_config": RunConfig(
                        resources={
                            "dbt_config": DbtConfigResource(
                                variables={
                                    "partition_key": partition_key,
                                    "batch_date": partition_key,
                                }
                            )
                        }
                    ),
                }
                kwargs["partition_key"] = partition_key
                yield RunRequest(**kwargs)

    return _sensor


@functools.cache
def define_silver_jobs() -> SilverJobBundle:
    """Define dbt Silver staging jobs and sensor based on dbt assets."""
    bundle = SilverJobBundle()
    sensor_jobs: list[JobDefinition | UnresolvedAssetJobDefinition] = []
    asset_to_upstream: dict[AssetKey, AssetKey] = {}
    all_upstream_keys: list[AssetKey] = []

    dbt_deps = dbt_assets.get_dbt_asset_dependency()

    for param in read_transform_job_parameter(_STAGING_JOB_DEFINITION_FILE):
        # Match AssetKey format: SILVER/STG_STOCK_PRICE_EOD
        asset_key = AssetKey([param.schema_suffix, param.table_name.upper()])

        # Verify if asset exists in dbt manifest dependencies
        if asset_key not in dbt_deps.specs_by_key:
            continue

        job_name = f"transform_{asset_key.to_python_identifier()}_job"

        # Define job selecting this specific asset key from dbt assets graph
        job = dagster_lib.define_asset_job(
            job_name,
            selection=[asset_key],
            tags={
                "limit_concurrent_job_runs_to_1": job_name,
                "type": "transform",
            },
        )
        bundle.jobs.append(job)

        # Map to upstream Bronze raw data
        upstream_key = _get_upstream_bronze_key(param.table_name)
        asset_to_upstream[asset_key] = upstream_key

        match param.trigger_type:
            case TriggerType.Sensor:
                sensor_jobs.append(job)
                if upstream_key not in all_upstream_keys:
                    all_upstream_keys.append(upstream_key)
            case TriggerType.Schedule:
                bundle.schedules.append(
                    _make_transform_schedule(job, job_name, param.trigger_parameter)
                )

    if sensor_jobs:
        bundle.sensors.append(
            _create_sensor_for_jobs(
                "silver_job_sensor", all_upstream_keys, sensor_jobs, asset_to_upstream
            )
        )

    return bundle


# ==============================================================================
# Mart (Gold) Layer Job Definitions
# ==============================================================================

_MART_JOB_DEFINITION_FILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "mart_job_defs.csv")
)


@dataclass
class MartJobParameter:
    """Parameters for a single Mart dbt job."""

    schema_suffix: str
    table_name: str
    trigger_type: TriggerType
    trigger_parameter: str


@dataclass
class MartJobBundle:
    """Return value of define_mart_jobs() — consumed by workspace.py."""

    jobs: list[JobDefinition | UnresolvedAssetJobDefinition] = field(
        default_factory=list
    )
    schedules: list[ScheduleDefinition] = field(default_factory=list)
    sensors: list[SensorDefinition] = field(default_factory=list)


def read_mart_job_parameter(csv_file: str) -> Iterator[MartJobParameter]:
    """Parse a mart job CSV into MartJobParameter objects."""
    with open(csv_file, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            yield MartJobParameter(
                schema_suffix=row["schema_suffix"].upper(),
                table_name=row["table_name"].lower(),
                trigger_type=TriggerType((row.get("trigger_type") or "SENSOR").upper()),
                trigger_parameter=row.get("trigger_parameter") or "",
            )


def _make_mart_schedule(
    job: JobDefinition | UnresolvedAssetJobDefinition,
    job_name: str,
    cron: str,
) -> ScheduleDefinition:
    """Create a schedule that runs a mart job at the given cron expression."""

    @dagster.schedule(
        job=job,
        cron_schedule=cron,
        execution_timezone=_TIMEZONE,
        name=f"{job_name}_schedule",
    )
    def _schedule(
        context: ScheduleEvaluationContext,
    ) -> RunRequest:
        partition_key = context.scheduled_execution_time.date().isoformat()
        return RunRequest(
            run_config=RunConfig(
                resources={
                    "dbt_config": DbtConfigResource(
                        variables={
                            "partition_key": partition_key,
                            "batch_date": partition_key,
                        }
                    )
                }
            )
        )

    return _schedule


def _create_sensor_for_mart_jobs(  # noqa: C901
    sensor_name: str,
    all_upstream_keys: list[AssetKey],
    sensor_jobs: list[JobDefinition | UnresolvedAssetJobDefinition],
    asset_to_upstream: dict[AssetKey, list[AssetKey]],
) -> SensorDefinition:
    """Create a multi-asset sensor to trigger mart jobs when Silver is ready."""

    @dagster.multi_asset_sensor(
        name=sensor_name,
        description="Trigger Gold dbt jobs when Silver assets are materialized.",
        monitored_assets=all_upstream_keys,
        jobs=sensor_jobs,
        minimum_interval_seconds=60,
    )
    def _sensor(  # noqa: C901  # noqa: C901
        context: MultiAssetSensorEvaluationContext,
    ) -> Iterator[RunRequest | SkipReason]:
        # 1. Invert mapping: Upstream -> set of Jobs
        upstream_to_jobs: dict[
            AssetKey, list[JobDefinition | UnresolvedAssetJobDefinition]
        ] = defaultdict(list)
        for asset_key, upstream_keys in asset_to_upstream.items():
            for job in sensor_jobs:
                if asset_key.to_python_identifier() in job.name:
                    for up_key in upstream_keys:
                        if job not in upstream_to_jobs[up_key]:
                            upstream_to_jobs[up_key].append(job)

        # 2. Group new materializations by partition
        partition_to_materialized_assets: dict[str, set[AssetKey]] = defaultdict(set)
        for key, asset_event, materialization in dagster_lib.fetch_materializations(
            context, fetch_limit_for_each_asset=_FETCH_LIMIT
        ):
            context.advance_cursor({key: asset_event})

            # Extract Native Partition from Dagster
            partition_key = materialization.partition
            if not partition_key:
                # Fallback for older data missing native partition
                conata_key = materialization.metadata.get("conata_partition_key")
                dbt_vars = materialization.metadata.get("variables")
                if conata_key is not None:
                    partition_key = str(conata_key.value)
                elif dbt_vars is not None and isinstance(dbt_vars.value, dict):
                    partition_key = dbt_vars.value.get("partition_key")

            if partition_key:
                partition_to_materialized_assets[partition_key].add(key)

        # 3. Process each partition independently
        for (
            partition_key,
            materialized_assets,
        ) in partition_to_materialized_assets.items():
            # Determine which jobs COULD be triggered
            possible_jobs: list[JobDefinition | UnresolvedAssetJobDefinition] = []
            for asset in materialized_assets:
                if asset in upstream_to_jobs:
                    for job in upstream_to_jobs[asset]:
                        if job not in possible_jobs:
                            possible_jobs.append(job)

            # Memoization for upstream partition checks
            materialization_status = dict.fromkeys(materialized_assets, True)

            # 4. Check dependencies for possible jobs
            for job in possible_jobs:
                # Find all required upstream keys for this job
                # from asset_to_upstream mapping
                required_upstreams = set()
                for asset_key, up_keys in asset_to_upstream.items():
                    if asset_key.to_python_identifier() in job.name:
                        required_upstreams.update(up_keys)

                all_ready = True
                for up_key in required_upstreams:
                    if up_key not in materialization_status:
                        is_ready = False
                        # Check if the latest materialization matches the partition_key
                        latest_event = (
                            context.instance.get_latest_materialization_event(up_key)
                        )
                        if latest_event and latest_event.asset_materialization:
                            mat = latest_event.asset_materialization
                            up_partition_key = mat.partition
                            if not up_partition_key:
                                conata_key = mat.metadata.get("conata_partition_key")
                                dbt_vars = mat.metadata.get("variables")
                                if conata_key is not None:
                                    up_partition_key = str(conata_key.value)
                                elif dbt_vars is not None and isinstance(
                                    dbt_vars.value, dict
                                ):
                                    up_partition_key = dbt_vars.value.get(
                                        "partition_key"
                                    )
                            if up_partition_key == partition_key:
                                is_ready = True
                        materialization_status[up_key] = is_ready

                    if not materialization_status[up_key]:
                        all_ready = False
                        break

                if not all_ready:
                    context.log.info(
                        f"Job {job.name} skipped: "
                        f"waiting for upstreams for partition {partition_key}"
                    )
                    continue

                # 5. Yield RunRequest if all upstreams are materialized
                kwargs = {
                    "job_name": job.name,
                    "run_key": f"{job.name}_{partition_key}_{int(time.time())}",
                    "run_config": RunConfig(
                        resources={
                            "dbt_config": DbtConfigResource(
                                variables={
                                    "partition_key": partition_key,
                                    "batch_date": partition_key,
                                }
                            )
                        }
                    ),
                }
                kwargs["partition_key"] = partition_key
                yield RunRequest(**kwargs)

    return _sensor


@functools.cache
def define_mart_jobs() -> MartJobBundle:
    """Define dbt Mart staging jobs and sensor based on dbt assets."""
    bundle = MartJobBundle()
    sensor_jobs: list[JobDefinition | UnresolvedAssetJobDefinition] = []
    asset_to_upstream: dict[AssetKey, list[AssetKey]] = {}
    all_upstream_keys: list[AssetKey] = []

    dbt_deps = dbt_assets.get_dbt_asset_dependency()

    for param in read_mart_job_parameter(_MART_JOB_DEFINITION_FILE):
        # Match AssetKey format: MART/MART_STOCK_MARKET_MOMENTUM
        asset_key = AssetKey([param.schema_suffix, param.table_name.upper()])

        # Verify if asset exists in dbt manifest dependencies
        if asset_key not in dbt_deps.specs_by_key:
            continue

        job_name = f"transform_{asset_key.to_python_identifier()}_job"

        # Define job selecting this specific asset key from dbt assets graph
        job = dagster_lib.define_asset_job(
            job_name,
            selection=[asset_key],
            tags={
                "limit_concurrent_job_runs_to_1": job_name,
                "type": "mart",
            },
        )
        bundle.jobs.append(job)

        # Get upstream keys from dbt specs (mostly Silver tables)
        spec = dbt_deps.specs_by_key[asset_key]
        upstream_keys = [dep.asset_key for dep in spec.deps]
        asset_to_upstream[asset_key] = upstream_keys

        match param.trigger_type:
            case TriggerType.Sensor:
                sensor_jobs.append(job)
                for up_key in upstream_keys:
                    if up_key not in all_upstream_keys:
                        all_upstream_keys.append(up_key)
            case TriggerType.Schedule:
                bundle.schedules.append(
                    _make_mart_schedule(job, job_name, param.trigger_parameter)
                )

    if sensor_jobs:
        bundle.sensors.append(
            _create_sensor_for_mart_jobs(
                "mart_job_sensor", all_upstream_keys, sensor_jobs, asset_to_upstream
            )
        )

    return bundle
