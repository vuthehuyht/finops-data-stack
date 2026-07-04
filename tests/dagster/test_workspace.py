import os
import unittest.mock
from importlib import reload
from unittest.mock import MagicMock, patch

import pytest
from dagster import AssetKey, AssetSpec, multi_asset

from src.dagster.transform_job import (
    _MART_JOB_DEFINITION_FILE,
    _STAGING_JOB_DEFINITION_FILE,
    read_mart_job_parameter,
    read_transform_job_parameter,
)


@pytest.fixture(autouse=True)
def mock_dbt_assets_and_dependencies():
    # Load parameters to simulate dbt objects
    silver_params = list(read_transform_job_parameter(_STAGING_JOB_DEFINITION_FILE))
    mart_params = list(read_mart_job_parameter(_MART_JOB_DEFINITION_FILE))

    # 1. Mock dependencies for define_silver_jobs and define_mart_jobs
    specs = {}
    for param in silver_params:
        key = AssetKey([param.schema_suffix, param.table_name.upper()])
        mock_spec = MagicMock()
        mock_spec.deps = []
        specs[key] = mock_spec

    for param in mart_params:
        key = AssetKey([param.schema_suffix, param.table_name.upper()])
        mock_spec = MagicMock()
        mock_spec.deps = []
        specs[key] = mock_spec

    mock_dbt_deps = MagicMock()
    mock_dbt_deps.specs_by_key = specs

    # 2. Mock project assets for workspace
    silver_keys = [
        AssetKey([p.schema_suffix, p.table_name.upper()]) for p in silver_params
    ]
    mart_keys = [AssetKey([p.schema_suffix, p.table_name.upper()]) for p in mart_params]
    all_keys = silver_keys + mart_keys

    @multi_asset(specs=[AssetSpec(key=k) for k in all_keys], name="dbt_project_assets")
    def mock_dbt_project_assets():
        pass

    with (
        patch(
            "src.dagster.dbt_assets.get_dbt_asset_dependency",
            return_value=mock_dbt_deps,
        ),
        patch(
            "src.dagster.dbt_assets.get_dbt_project_assets",
            return_value=mock_dbt_project_assets,
        ),
    ):
        yield


def test_workspace_defs_created() -> None:
    """workspace.py must import without error and expose defs."""
    with unittest.mock.patch.dict(
        os.environ,
        {
            "FINOPS_RAW_BUCKET": "test-bucket",
            "REDSHIFT_IAM_ROLE_ARN": "arn:aws:iam::123:role/test",
            "REDSHIFT_HOST": "localhost",
            "REDSHIFT_PASSWORD": "test",
        },
    ):
        import src.dagster.workspace

        reload(src.dagster.workspace)
        assert src.dagster.workspace.defs is not None


def test_workspace_has_17_load_assets() -> None:
    """workspace.py must include all 17 Bronze load assets."""
    with unittest.mock.patch.dict(
        os.environ,
        {
            "FINOPS_RAW_BUCKET": "test-bucket",
            "REDSHIFT_IAM_ROLE_ARN": "arn:aws:iam::123:role/test",
            "REDSHIFT_HOST": "localhost",
            "REDSHIFT_PASSWORD": "test",
        },
    ):
        import src.dagster.workspace

        reload(src.dagster.workspace)
        defs = src.dagster.workspace.defs
        bronze_keys = [k for k in defs.assets_defs_by_key.keys() if k.path[0] == "RAW"]
        assert len(bronze_keys) == 17


def test_workspace_has_17_silver_assets() -> None:
    """workspace.py must include all 17 Silver transform assets."""
    with unittest.mock.patch.dict(
        os.environ,
        {
            "FINOPS_RAW_BUCKET": "test-bucket",
            "REDSHIFT_IAM_ROLE_ARN": "arn:aws:iam::123:role/test",
            "REDSHIFT_HOST": "localhost",
            "REDSHIFT_PASSWORD": "test",
        },
    ):
        import src.dagster.workspace

        reload(src.dagster.workspace)
        defs = src.dagster.workspace.defs
        silver_keys = [
            k for k in defs.assets_defs_by_key.keys() if k.path[0] == "STAGING"
        ]
        assert len(silver_keys) == 17


def test_workspace_has_sensor_for_load_jobs() -> None:
    """All 17 Bronze tables are SENSOR-type — must have 1 load_job_sensor."""
    with unittest.mock.patch.dict(
        os.environ,
        {
            "FINOPS_RAW_BUCKET": "test-bucket",
            "REDSHIFT_IAM_ROLE_ARN": "arn:aws:iam::123:role/test",
            "REDSHIFT_HOST": "localhost",
            "REDSHIFT_PASSWORD": "test",
        },
    ):
        import src.dagster.workspace

        reload(src.dagster.workspace)
        defs = src.dagster.workspace.defs
        assert len(defs.schedule_defs) == 17  # 1 schedule per ingest asset
        sensor_names = {s.name for s in defs.sensor_defs}
        assert "load_job_sensor" in sensor_names


def test_workspace_has_transform_sensors() -> None:
    """Silver, Mart, Load, and ML daily inference are SENSOR-type.

    Must have exactly 4 sensors.
    """
    with unittest.mock.patch.dict(
        os.environ,
        {
            "FINOPS_RAW_BUCKET": "test-bucket",
            "REDSHIFT_IAM_ROLE_ARN": "arn:aws:iam::123:role/test",
            "REDSHIFT_HOST": "localhost",
            "REDSHIFT_PASSWORD": "test",
        },
    ):
        import src.dagster.workspace

        reload(src.dagster.workspace)
        defs = src.dagster.workspace.defs
        assert len(defs.sensor_defs) == 4
        sensor_names = {s.name for s in defs.sensor_defs}
        assert "load_job_sensor" in sensor_names
        assert "silver_job_sensor" in sensor_names
        assert "mart_job_sensor" in sensor_names
        assert "ml_daily_inference_sensor" in sensor_names


def test_workspace_definitions_include_ml_jobs() -> None:
    """workspace.py must register ML quarterly re-training and daily inference jobs."""
    with unittest.mock.patch.dict(
        os.environ,
        {
            "FINOPS_RAW_BUCKET": "test-bucket",
            "REDSHIFT_IAM_ROLE_ARN": "arn:aws:iam::123:role/test",
            "REDSHIFT_HOST": "localhost",
            "REDSHIFT_PASSWORD": "test",
        },
    ):
        import src.dagster.workspace

        reload(src.dagster.workspace)
        defs = src.dagster.workspace.defs
        assert "ml_quarterly_retrain_job" in defs.job_names
        assert "ml_daily_inference_job" in defs.job_names


def test_workspace_definitions_include_ml_assets() -> None:
    """workspace.py must register all ML assets (training and inference)."""
    with unittest.mock.patch.dict(
        os.environ,
        {
            "FINOPS_RAW_BUCKET": "test-bucket",
            "REDSHIFT_IAM_ROLE_ARN": "arn:aws:iam::123:role/test",
            "REDSHIFT_HOST": "localhost",
            "REDSHIFT_PASSWORD": "test",
        },
    ):
        import src.dagster.workspace

        reload(src.dagster.workspace)
        defs = src.dagster.workspace.defs
        asset_keys = set(defs.assets_defs_by_key.keys())
        assert AssetKey(["ML", "GOLD_ML_TRAINING_DATASET"]) in asset_keys
        assert AssetKey(["ML", "ML_TRAINING_JOB"]) in asset_keys
        assert AssetKey(["ML", "ML_MODEL_EVALUATION"]) in asset_keys
        assert AssetKey(["ML", "ML_DATA_QUALITY_GATE"]) in asset_keys
        assert AssetKey(["ML", "ML_DAILY_FORECAST"]) in asset_keys
        assert AssetKey(["ML", "ML_PUBLISH_FORECAST_RESULTS"]) in asset_keys


def test_workspace_resources_include_sagemaker_and_ssm() -> None:
    from src.dagster.workspace import _get_resources

    resource_keys = _get_resources().keys()
    assert "sagemaker" in resource_keys
    assert "ssm" in resource_keys
