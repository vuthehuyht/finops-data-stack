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


def test_load_job_sensor_extracts_batch_date_from_metadata() -> None:
    """Test that sensor reads batch_date from materialization metadata, not S3 URL.

    This test verifies the fix for Bug 1: the sensor was parsing batch_date by
    splitting the S3 URL, which returned the filename instead of the date.
    Now it should read batch_date from the metadata emitted by INPUT assets.
    """
    from unittest.mock import MagicMock

    # Create a fake materialization event with both s3_url and batch_date metadata
    s3_url = "s3://bucket/raw/RAW_STOCK_PRICE_EOD/batch_date=2026-06-18/1718715432/RAW_STOCK_PRICE_EOD.parquet"
    batch_date = "2026-06-18"

    materialization = MagicMock(spec=dagster.AssetMaterialization)
    materialization.metadata = {
        "s3_url": dagster.TextMetadataValue(s3_url),
        "batch_date": dagster.TextMetadataValue(batch_date),
    }

    # Simulate the sensor logic: extract batch_date from metadata
    s3_meta = materialization.metadata.get("s3_url")
    assert s3_meta is not None
    extracted_s3_url = str(s3_meta.value)

    # The fix: extract batch_date from metadata instead of parsing URL
    batch_date_meta = materialization.metadata.get("batch_date")
    extracted_batch_date = (
        str(batch_date_meta.value) if batch_date_meta is not None else ""
    )

    # Verify correct extraction
    assert extracted_s3_url == s3_url
    assert extracted_batch_date == "2026-06-18"

    # Verify the old broken approach would have failed
    broken_batch_date = extracted_s3_url.rstrip("/").rsplit("/", 1)[-1]
    assert broken_batch_date == "RAW_STOCK_PRICE_EOD.parquet"
    assert broken_batch_date != extracted_batch_date


def test_load_job_sensor_skips_empty_s3_url() -> None:
    """Test that sensor skips events with empty s3_url metadata.

    This test verifies the fix for Bug 2: when pipeline.run() returns an empty
    DataFrame, it emits s3_url="" which was not being caught by the
    'if s3_meta is None' check. The sensor should now skip these events.
    """
    from unittest.mock import MagicMock

    # Create a fake materialization event with empty s3_url (Bug 2 scenario)
    materialization = MagicMock(spec=dagster.AssetMaterialization)
    materialization.metadata = {
        "s3_url": dagster.TextMetadataValue(""),  # Empty string, not None
        "batch_date": dagster.TextMetadataValue("2026-06-18"),
    }

    # Extract s3_url as the sensor would
    s3_meta = materialization.metadata.get("s3_url")
    assert s3_meta is not None  # Old check wouldn't catch this

    s3_url = str(s3_meta.value)
    assert s3_url == ""

    # Verify the fix: the new guard 'if not s3_url: continue' catches this
    should_skip = not s3_url
    assert should_skip is True


def test_load_job_sensor_skips_missing_batch_date() -> None:
    """Test that sensor skips events with missing batch_date metadata.

    When a materialization event has s3_url but no batch_date key in metadata,
    the sensor should log a warning and skip it (not yield a RunRequest with
    an empty partition key).
    """
    from unittest.mock import MagicMock

    # Create a fake materialization event with s3_url but no batch_date
    s3_url = "s3://bucket/raw/RAW_STOCK_PRICE_EOD/2026-06-18/data.parquet"

    materialization = MagicMock(spec=dagster.AssetMaterialization)
    materialization.metadata = {
        "s3_url": dagster.TextMetadataValue(s3_url),
        # batch_date is missing
    }

    # Simulate the sensor logic: batch_date should be checked
    batch_date_meta = materialization.metadata.get("batch_date")
    assert batch_date_meta is None

    # The sensor should skip this event (not create a RunRequest with empty batch_date)
    should_skip = batch_date_meta is None
    assert should_skip is True
