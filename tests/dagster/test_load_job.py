"""Tests for load_job.py."""

import dagster


def test_trigger_type_values() -> None:
    from src.dagster.load_job import TriggerType

    assert TriggerType.Sensor.value == "SENSOR"
    assert TriggerType.Schedule.value == "SCHEDULE"


def test_load_job_parameter_fields() -> None:
    from src.dagster.load_job import LoadJobParameter, TriggerType

    p = LoadJobParameter(
        asset_key=dagster.AssetKey(["RAW", "RAW_STOCK_PRICE_EOD"]),
        table_name="RAW_STOCK_PRICE_EOD",
        schema="bronze",
        file_format="parquet",
        trigger_type=TriggerType.Schedule,
        trigger_parameter="0 18 * * 1-5",
    )
    assert p.table_name == "RAW_STOCK_PRICE_EOD"
    assert p.schema == "bronze"
    assert p.file_format == "parquet"


def test_raw_data_asset_config() -> None:
    from src.dagster.load_job import RawDataAssetConfig

    config = RawDataAssetConfig(s3_url="s3://bucket/path/", batch_date="2026-06-17")
    assert config.s3_url == "s3://bucket/path/"
    assert config.batch_date == "2026-06-17"


def test_get_asset_key() -> None:
    from src.dagster.load_job import _get_asset_key

    key = _get_asset_key("RAW_STOCK_PRICE_EOD")
    assert key.path == ["RAW", "RAW_STOCK_PRICE_EOD"]


def test_read_load_job_parameter_count() -> None:
    from src.dagster.load_job import _JOB_DEFINITION_FILE, _read_load_job_parameter

    params = list(_read_load_job_parameter(_JOB_DEFINITION_FILE))
    assert len(params) == 17


def test_read_load_job_parameter_first_row() -> None:
    from src.dagster.load_job import (
        _JOB_DEFINITION_FILE,
        TriggerType,
        _read_load_job_parameter,
    )

    params = list(_read_load_job_parameter(_JOB_DEFINITION_FILE))
    first = params[0]
    assert first.table_name == "RAW_STOCK_PRICE_EOD"
    assert first.schema == "bronze"
    assert first.file_format == "parquet"
    assert first.trigger_type == TriggerType.Sensor
    assert first.trigger_parameter == ""
    assert first.asset_key == dagster.AssetKey(["RAW", "RAW_STOCK_PRICE_EOD"])


def test_read_load_job_parameter_all_sensor() -> None:
    from src.dagster.load_job import (
        _JOB_DEFINITION_FILE,
        TriggerType,
        _read_load_job_parameter,
    )

    params = list(_read_load_job_parameter(_JOB_DEFINITION_FILE))
    assert all(p.trigger_type == TriggerType.Sensor for p in params)


def test_create_raw_data_asset_key() -> None:
    from src.dagster.load_job import (
        LoadJobParameter,
        TriggerType,
        _create_raw_data_asset,
    )

    param = LoadJobParameter(
        asset_key=dagster.AssetKey(["RAW", "RAW_STOCK_PRICE_EOD"]),
        table_name="RAW_STOCK_PRICE_EOD",
        schema="bronze",
        file_format="parquet",
        trigger_type=TriggerType.Schedule,
        trigger_parameter="0 18 * * 1-5",
    )
    asset = _create_raw_data_asset(param)
    assert isinstance(asset, dagster.AssetsDefinition)
    assert asset.key == dagster.AssetKey(["RAW", "RAW_STOCK_PRICE_EOD"])


def test_define_load_jobs_returns_bundle() -> None:
    from src.dagster.load_job import LoadJobBundle, define_load_jobs

    bundle = define_load_jobs()
    assert isinstance(bundle, LoadJobBundle)
    assert len(bundle.assets) == 17
    assert len(bundle.jobs) == 17
    assert len(bundle.schedules) == 0  # all SENSOR, no SCHEDULE rows
    assert len(bundle.sensors) == 1  # one sensor monitoring all 17 INPUT assets


def test_define_load_jobs_asset_keys() -> None:
    from src.dagster.load_job import define_load_jobs

    bundle = define_load_jobs()
    keys = {a.key for a in bundle.assets}
    assert dagster.AssetKey(["RAW", "RAW_STOCK_PRICE_EOD"]) in keys
    assert dagster.AssetKey(["RAW", "RAW_BALANCE_SHEET"]) in keys
    assert dagster.AssetKey(["RAW", "RAW_ANALYST_REPORTS"]) in keys


def test_define_load_jobs_sensor_names() -> None:
    from src.dagster.load_job import define_load_jobs

    bundle = define_load_jobs()
    names = {s.name for s in bundle.sensors}
    assert any("load_job_sensor" in name.lower() for name in names)
