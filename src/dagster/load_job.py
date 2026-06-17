"""Dagster jobs for loading raw data from S3 into Redshift Bronze layer."""

import csv
import enum
import functools
import os
from collections.abc import Iterator
from dataclasses import dataclass, field

import dagster
import pydantic
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

import src.pipeline.dagster as dagster_lib
from src.dagster.resources import LoadJobConfigResource, RedshiftResource
from src.load.load import load_s3_to_redshift

_TIMEZONE = "Asia/Ho_Chi_Minh"
_FETCH_LIMIT = 30
_JOB_DEFINITION_FILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "load_job_defs.csv")
)


@enum.unique
class TriggerType(enum.Enum):
    """Trigger types for load jobs."""

    Sensor = "SENSOR"
    Schedule = "SCHEDULE"


@dataclass
class LoadJobParameter:
    """Parameters for a single load job, parsed from load_job_defs.csv."""

    asset_key: dagster.AssetKey
    table_name: str
    schema: str
    file_format: str
    trigger_type: TriggerType
    trigger_parameter: str


class RawDataAssetConfig(dagster.Config):
    """Runtime config injected per asset run by schedule or sensor."""

    s3_url: str = pydantic.Field(description="S3 prefix or file path for COPY.")
    batch_date: str = pydantic.Field(description="Partition date in YYYY-MM-DD format.")


@dataclass
class LoadJobBundle:
    """Return value of define_load_jobs() — consumed by workspace.py."""

    assets: list[dagster.AssetsDefinition] = field(default_factory=list)
    jobs: list[dagster.JobDefinition | UnresolvedAssetJobDefinition] = field(
        default_factory=list
    )
    schedules: list[dagster.ScheduleDefinition] = field(default_factory=list)
    sensors: list[dagster.SensorDefinition] = field(default_factory=list)


def _get_asset_key(table_name: str) -> dagster.AssetKey:
    return dagster_lib.asset_key(["BRONZE", table_name])


def _read_load_job_parameter(csv_file: str) -> Iterator[LoadJobParameter]:
    """Parse load_job_defs.csv into LoadJobParameter objects."""
    with open(csv_file, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            table_name = row["table_name"].upper()
            yield LoadJobParameter(
                asset_key=_get_asset_key(table_name),
                table_name=table_name,
                schema=row["schema"].lower(),
                file_format=row["file_format"].lower(),
                trigger_type=TriggerType((row.get("trigger_type") or "SENSOR").upper()),
                trigger_parameter=row.get("trigger_parameter") or "",
            )


def _create_raw_data_asset(parameter: LoadJobParameter) -> dagster.AssetsDefinition:
    """Create a Dagster asset that COPYs one Bronze table from S3 to Redshift."""

    @dagster_lib.asset(
        key=parameter.asset_key,
        group_name="BRONZE",
        kinds={"python", "s3", "redshift"},
        description=(
            f"Load {parameter.table_name} ({parameter.file_format}) "
            f"from S3 into Redshift schema {parameter.schema}."
        ),
    )
    def raw_data(
        context: dagster.AssetExecutionContext,
        config: RawDataAssetConfig,
        redshift: RedshiftResource,
        load_config: LoadJobConfigResource,
    ) -> dagster.Output[None]:
        context.log.info(
            "Loading %s from %s (batch_date=%s)",
            parameter.table_name,
            config.s3_url,
            config.batch_date,
        )
        with redshift.get_connection() as conn:
            with conn.cursor() as cursor:
                load_s3_to_redshift(
                    cursor=cursor,
                    s3_url=config.s3_url,
                    table_name=parameter.table_name,
                    schema=parameter.schema,
                    file_format=parameter.file_format,
                    iam_role_arn=load_config.iam_role_arn,
                )
            conn.commit()
        return dagster.Output(
            value=None,
            metadata={
                "s3_url": config.s3_url,
                "batch_date": config.batch_date,
                "conata_partition_key": config.batch_date,
            },
        )

    return raw_data


def _make_load_schedule(
    job: dagster.JobDefinition | UnresolvedAssetJobDefinition,
    job_name: str,
    cron: str,
    asset_py_id: str,
    table_name: str,
) -> dagster.ScheduleDefinition:
    """Create a schedule that runs a load job at the given cron expression."""

    @dagster.schedule(
        job=job,
        cron_schedule=cron,
        execution_timezone=_TIMEZONE,
        name=f"{job_name}_schedule",
        description=f"VN market schedule for {table_name}.",
    )
    def _schedule(
        context: dagster.ScheduleEvaluationContext,
    ) -> dagster.RunRequest:
        batch_date = context.scheduled_execution_time.date().isoformat()
        raw_bucket = os.getenv("FINOPS_RAW_BUCKET", "finops-raw-dev")
        return dagster.RunRequest(
            run_config=dagster.RunConfig(
                ops={
                    asset_py_id: RawDataAssetConfig(
                        s3_url=(
                            f"s3://{raw_bucket}/data_storage"
                            f"/{table_name.lower()}/{batch_date}/"
                        ),
                        batch_date=batch_date,
                    )
                }
            )
        )

    return _schedule


def _create_load_sensor(
    monitored_asset_keys: list[dagster.AssetKey],
    sensor_jobs: list[dagster.JobDefinition | UnresolvedAssetJobDefinition],
    input_to_job: dict[
        dagster.AssetKey,
        dagster.JobDefinition | UnresolvedAssetJobDefinition,
    ],
    input_to_asset: dict[dagster.AssetKey, dagster.AssetsDefinition],
) -> dagster.SensorDefinition:
    """Create a sensor that triggers load jobs when INPUT assets are materialized."""

    @dagster.multi_asset_sensor(
        name="load_job_sensor",
        description="Trigger load jobs when ingestion layer materializes INPUT assets.",
        monitored_assets=monitored_asset_keys,
        jobs=sensor_jobs,
        minimum_interval_seconds=60,
    )
    def load_job_sensor(
        context: dagster.MultiAssetSensorEvaluationContext,
    ) -> Iterator[dagster.RunRequest | dagster.SkipReason]:
        for key, asset_event, materialization in dagster_lib.fetch_materializations(
            context, fetch_limit_for_each_asset=_FETCH_LIMIT
        ):
            context.advance_cursor({key: asset_event})
            s3_meta = materialization.metadata.get("s3_url")
            if s3_meta is None:
                continue
            s3_url = str(s3_meta.value)
            batch_date = s3_url.rstrip("/").rsplit("/", 1)[-1]
            job = input_to_job[key]
            asset = input_to_asset[key]
            yield dagster.RunRequest(
                job_name=job.name,
                run_key=s3_url,
                run_config=dagster.RunConfig(
                    ops={
                        asset.key.to_python_identifier(): RawDataAssetConfig(
                            s3_url=s3_url,
                            batch_date=batch_date,
                        )
                    }
                ),
            )

    return load_job_sensor


@functools.cache
def define_load_jobs() -> LoadJobBundle:
    """Build Dagster assets, jobs, schedules, and sensors for all Bronze tables."""
    bundle = LoadJobBundle()
    sensor_jobs: list[dagster.JobDefinition | UnresolvedAssetJobDefinition] = []
    sensor_keys: list[dagster.AssetKey] = []
    input_to_job: dict[
        dagster.AssetKey,
        dagster.JobDefinition | UnresolvedAssetJobDefinition,
    ] = {}
    input_to_asset: dict[dagster.AssetKey, dagster.AssetsDefinition] = {}

    for param in _read_load_job_parameter(_JOB_DEFINITION_FILE):
        asset = _create_raw_data_asset(param)
        bundle.assets.append(asset)

        job_name = f"load_{asset.key.to_python_identifier()}_job"
        job = dagster_lib.define_asset_job(
            job_name,
            selection=[asset],
            tags={
                "limit_concurrent_job_runs_to_1": job_name,
                "type": "load",
            },
        )
        bundle.jobs.append(job)

        input_key = dagster_lib.asset_key(["INPUT", param.table_name])
        input_to_job[input_key] = job
        input_to_asset[input_key] = asset

        match param.trigger_type:
            case TriggerType.Schedule:
                bundle.schedules.append(
                    _make_load_schedule(
                        job,
                        job_name,
                        param.trigger_parameter,
                        asset.key.to_python_identifier(),
                        param.table_name,
                    )
                )
            case TriggerType.Sensor:
                sensor_jobs.append(job)
                sensor_keys.append(input_key)

    if sensor_keys:
        bundle.sensors.append(
            _create_load_sensor(sensor_keys, sensor_jobs, input_to_job, input_to_asset)
        )

    return bundle
