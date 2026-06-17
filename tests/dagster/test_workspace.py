"""Tests for workspace.py."""

import os
import unittest.mock
from importlib import reload


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
        bronze_keys = [
            k for k in defs.assets_defs_by_key.keys() if k.path[0] == "BRONZE"
        ]
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
            k for k in defs.assets_defs_by_key.keys() if k.path[0] == "SILVER"
        ]
        assert len(silver_keys) == 17


def test_workspace_has_schedules_for_all_load_jobs() -> None:
    """All 17 Bronze tables are SCHEDULE-type — must have 17 schedules."""
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
        assert len(defs.schedule_defs) == 17


def test_workspace_has_one_silver_sensor() -> None:
    """Silver tables are SENSOR-type — must have exactly one multi_asset_sensor."""
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
        assert len(defs.sensor_defs) == 1
        assert defs.sensor_defs[0].name == "silver_job_sensor"
