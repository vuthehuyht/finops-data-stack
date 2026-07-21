"""Dagster workspace — single code location for FinOps Data Stack."""

from typing import Any

import dagster

import src.pipeline.dagster as dagster_lib
from src.dagster import (
    dbt_assets,
    ddl_job,
    inference_job,
    ingest_job,
    load_job,
    ml_job,
    resources,
    transform_job,
)


def _get_resources() -> dict[str, Any]:
    """Get the resource definitions for the workspace."""
    return {
        "s3": resources.s3,
        "s3bucket": resources.s3bucket,
        "dbt": resources.dbt,
        "redshift": resources.redshift,
        "load_config": resources.load_config,
        "dbt_config": resources.dbt_config,
        "sagemaker": resources.sagemaker_config,
        "ssm": resources.ssm,
    }


def _create_definitions() -> dagster.Definitions:
    """Create the Dagster definitions for the workspace."""
    ingest = ingest_job.define_ingest_jobs()
    load = load_job.define_load_jobs()
    silver = transform_job.define_silver_jobs()
    mart = transform_job.define_mart_jobs()
    dbt = dbt_assets.get_dbt_project_assets()
    ml = ml_job.define_ml_jobs()
    inference = inference_job.define_inference_jobs()

    return dagster_lib.definitions(
        code_location_name="finops",
        assets=[*ingest.assets, *load.assets, dbt, *ml.assets, *inference.assets],
        jobs=[
            *ingest.jobs,
            *load.jobs,
            *silver.jobs,
            *mart.jobs,
            dbt_assets.dbt_build_job,
            ddl_job.execute_ddl_job,
            *ml.jobs,
            *inference.jobs,
        ],
        schedules=[
            *ingest.schedules,
            *ml.schedules,
        ],
        sensors=[*load.sensors, *silver.sensors, *mart.sensors, *inference.sensors],
        resources=_get_resources(),
    )


defs = _create_definitions()
