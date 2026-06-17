"""Dagster workspace — single code location for FinOps Data Stack."""

from typing import Any

import dagster

import src.pipeline.dagster as dagster_lib
from src.dagster import load_job, resources, transform_job


def _get_resources() -> dict[str, Any]:
    return {
        "s3": resources.s3,
        "s3bucket": resources.s3bucket,
        "dbt": resources.dbt,
        "redshift": resources.redshift,
        "load_config": resources.load_config,
        "dbt_config": resources.dbt_config,
    }


def _create_definitions() -> dagster.RepositoryDefinition:
    load = load_job.define_load_jobs()
    silver = transform_job.define_silver_jobs()
    return dagster_lib.definitions(
        code_location_name="finops",
        assets=[*load.assets, *silver.assets],
        jobs=[*load.jobs, *silver.jobs],
        schedules=[*load.schedules, *silver.schedules],
        sensors=[*load.sensors, *silver.sensors],
        resources=_get_resources(),
    )


defs = _create_definitions()
