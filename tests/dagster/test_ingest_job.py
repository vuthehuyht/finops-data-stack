"""Tests for ingest_job.py."""

import datetime

import dagster


def test_ingest_job_bundle_default_fields() -> None:
    from src.dagster.ingest_job import IngestJobBundle

    bundle = IngestJobBundle()
    assert bundle.assets == []
    assert bundle.jobs == []
    assert bundle.schedules == []


def test_ingest_asset_config_batch_date_defaults_to_today() -> None:
    from src.dagster.ingest_job import IngestAssetConfig

    config = IngestAssetConfig()
    assert config.batch_date == datetime.date.today().isoformat()


def test_ingest_asset_config_batch_date_accepts_explicit_value() -> None:
    from src.dagster.ingest_job import IngestAssetConfig

    config = IngestAssetConfig(batch_date="2026-06-18")
    assert config.batch_date == "2026-06-18"


def test_ingest_asset_config_symbols_defaults_to_empty_list() -> None:
    from src.dagster.ingest_job import IngestAssetConfig

    config = IngestAssetConfig()
    assert config.symbols == []


def test_ingest_cron_and_timezone_constants() -> None:
    from src.dagster.ingest_job import _INGEST_CRON, _TIMEZONE

    assert _TIMEZONE == "Asia/Ho_Chi_Minh"
    assert _INGEST_CRON == "30 15 * * 1-5"


def test_define_ingest_jobs_returns_bundle() -> None:
    from src.dagster.ingest_job import IngestJobBundle, define_ingest_jobs

    bundle = define_ingest_jobs()
    assert isinstance(bundle, IngestJobBundle)
    assert len(bundle.assets) == 15
    assert len(bundle.jobs) == 15
    assert len(bundle.schedules) == 15


def test_define_ingest_jobs_asset_keys() -> None:
    from src.dagster.ingest_job import define_ingest_jobs

    bundle = define_ingest_jobs()
    keys = {a.key for a in bundle.assets}
    assert dagster.AssetKey(["INPUT", "RAW_STOCK_PRICE_EOD"]) in keys
    assert dagster.AssetKey(["INPUT", "RAW_EXCHANGE_RATES"]) in keys
    assert dagster.AssetKey(["INPUT", "RAW_ANALYST_REPORTS"]) in keys
    assert dagster.AssetKey(["INPUT", "RAW_COMMODITIES_PRICE"]) in keys


def test_define_ingest_jobs_job_naming_convention() -> None:
    from src.dagster.ingest_job import define_ingest_jobs

    bundle = define_ingest_jobs()
    job_names = {j.name for j in bundle.jobs}
    assert "ingest_INPUT__RAW_STOCK_PRICE_EOD_job" in job_names
    assert "ingest_INPUT__RAW_EXCHANGE_RATES_job" in job_names
    assert "ingest_INPUT__RAW_COMMODITIES_PRICE_job" in job_names


def test_define_ingest_jobs_schedule_naming_convention() -> None:
    from src.dagster.ingest_job import define_ingest_jobs

    bundle = define_ingest_jobs()
    schedule_names = {s.name for s in bundle.schedules}
    assert "ingest_INPUT__RAW_STOCK_PRICE_EOD_job_schedule" in schedule_names
    assert "ingest_INPUT__RAW_EXCHANGE_RATES_job_schedule" in schedule_names


def test_define_ingest_jobs_schedule_cron_and_timezone() -> None:
    from src.dagster.ingest_job import _INGEST_CRON, _TIMEZONE, define_ingest_jobs

    bundle = define_ingest_jobs()
    for schedule in bundle.schedules:
        assert schedule.cron_schedule == _INGEST_CRON
        assert schedule.execution_timezone == _TIMEZONE


def test_define_ingest_jobs_returns_cached_instance() -> None:
    """define_ingest_jobs() called twice must return the same cached object."""
    from src.dagster.ingest_job import define_ingest_jobs

    first = define_ingest_jobs()
    second = define_ingest_jobs()
    assert first is second


def test_all_ingest_assets_have_input_group() -> None:
    from src.dagster.ingest_job import _ALL_INGEST_ASSETS

    assert len(_ALL_INGEST_ASSETS) == 15
    for asset in _ALL_INGEST_ASSETS:
        assert asset.group_names_by_key[asset.key] == "INPUT"
