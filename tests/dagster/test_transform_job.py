import datetime
from unittest.mock import MagicMock, patch

import pytest
from dagster import (
    AssetKey,
    AssetMaterialization,
    AssetsDefinition,
    EventLogRecord,
    MetadataValue,
    build_multi_asset_sensor_context,
    build_schedule_context,
)

import src.pipeline.dagster as dagster_lib
from src.dagster.transform_job import (
    _STAGING_JOB_DEFINITION_FILE,
    SilverJobBundle,
    TransformJobParameter,
    TriggerType,
    _create_sensor_for_jobs,
    _get_upstream_bronze_key,
    _make_transform_schedule,
    define_silver_jobs,
    read_transform_job_parameter,
)


@pytest.fixture(autouse=True)
def mock_dbt_dependency():
    silver_params = list(read_transform_job_parameter(_STAGING_JOB_DEFINITION_FILE))
    specs = {}
    for param in silver_params:
        key = AssetKey([param.schema_suffix, param.table_name.upper()])
        mock_spec = MagicMock()
        mock_spec.deps = []
        specs[key] = mock_spec

    mock_dbt_deps = MagicMock()
    mock_dbt_deps.specs_by_key = specs

    with patch(
        "src.dagster.dbt_assets.get_dbt_asset_dependency", return_value=mock_dbt_deps
    ):
        yield mock_dbt_deps


def test_trigger_type_values() -> None:
    assert TriggerType.Sensor.value == "SENSOR"
    assert TriggerType.Schedule.value == "SCHEDULE"


def test_get_upstream_bronze_key() -> None:
    key = _get_upstream_bronze_key("stg_stock_price_eod")
    assert key.path == ["RAW", "RAW_STOCK_PRICE_EOD"]


def test_get_upstream_bronze_key_all_models() -> None:
    params = list(read_transform_job_parameter(_STAGING_JOB_DEFINITION_FILE))
    for p in params:
        key = _get_upstream_bronze_key(p.table_name)
        assert key.path[0] == "RAW"
        assert key.path[1].startswith("RAW_")


def test_read_transform_job_parameter_count() -> None:
    params = list(read_transform_job_parameter(_STAGING_JOB_DEFINITION_FILE))
    assert len(params) == 17


def test_read_transform_job_parameter_first_row() -> None:
    params = list(read_transform_job_parameter(_STAGING_JOB_DEFINITION_FILE))
    first = params[0]
    assert first.schema_suffix == "STAGING"
    assert first.table_name == "stg_stock_price_eod"
    assert first.trigger_type == TriggerType.Sensor
    assert first.trigger_parameter == ""


def test_read_transform_job_parameter_all_sensor() -> None:
    params = list(read_transform_job_parameter(_STAGING_JOB_DEFINITION_FILE))
    assert all(p.trigger_type == TriggerType.Sensor for p in params)


def test_define_silver_jobs_returns_bundle() -> None:
    bundle = define_silver_jobs()
    assert isinstance(bundle, SilverJobBundle)
    assert len(bundle.jobs) == 17
    assert len(bundle.schedules) == 0  # all SENSOR, no schedules
    assert len(bundle.sensors) == 1  # one multi_asset_sensor


def test_define_silver_jobs_asset_keys() -> None:
    bundle = define_silver_jobs()
    job_names = {j.name for j in bundle.jobs}
    assert "transform_STAGING__STG_STOCK_PRICE_EOD_job" in job_names
    assert "transform_STAGING__STG_BALANCE_SHEET_job" in job_names
    assert "transform_STAGING__STG_ANALYST_REPORTS_job" in job_names


def test_define_silver_jobs_sensor_name() -> None:
    bundle = define_silver_jobs()
    assert bundle.sensors[0].name == "silver_job_sensor"


def test_transform_schedule_evaluates() -> None:
    @dagster_lib.asset(key=AssetKey(["STAGING", "TEST"]))
    def dummy_asset() -> None:
        pass

    mock_job = dagster_lib.define_asset_job("test_job", selection=[dummy_asset])
    schedule_def = _make_transform_schedule(mock_job, "test_job", "0 0 * * *")
    context = build_schedule_context(
        scheduled_execution_time=datetime.datetime(2026, 6, 17, 12, 0, 0)
    )
    res = schedule_def.evaluate_tick(context)
    assert res is not None
    assert len(res.run_requests) == 1
    assert res.run_requests[0].run_config is not None


def test_transform_sensor_evaluates() -> None:
    @dagster_lib.asset(key=AssetKey(["STAGING", "STG_TEST"]))
    def dummy_asset() -> None:
        pass

    mock_job = dagster_lib.define_asset_job(
        "transform_STAGING__STG_TEST_job", selection=[dummy_asset]
    )
    sensor_jobs = [mock_job]
    all_upstream_keys = [AssetKey(["RAW", "RAW_TEST"])]
    asset_to_upstream = {
        AssetKey(["STAGING", "STG_TEST"]): AssetKey(["RAW", "RAW_TEST"])
    }

    sensor_def = _create_sensor_for_jobs(
        "test_sensor", all_upstream_keys, sensor_jobs, asset_to_upstream
    )

    # Mock fetch_materializations
    mock_key = AssetKey(["RAW", "RAW_TEST"])
    mock_event = MagicMock(spec=EventLogRecord)
    mock_materialization = AssetMaterialization(
        asset_key=mock_key,
        metadata={"conata_partition_key": MetadataValue.text("2026-06-17")},
    )

    mock_fetch = MagicMock(return_value=[(mock_key, mock_event, mock_materialization)])

    from dagster import DagsterInstance, Definitions

    defs = Definitions(
        assets=[dummy_asset],
        jobs=[mock_job],
    )
    context = build_multi_asset_sensor_context(
        monitored_assets=all_upstream_keys,
        definitions=defs,
        instance=DagsterInstance.ephemeral(),
    )

    with patch("src.pipeline.dagster.fetch_materializations", mock_fetch):
        generator = sensor_def._evaluation_fn(context)
        results = list(generator)

    assert len(results) == 1
    run_request = results[0]
    assert run_request.job_name == "transform_STAGING__STG_TEST_job"
    assert run_request.run_key == "RAW__RAW_TEST_2026-06-17"
