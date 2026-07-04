"""Tests for inference_job.py."""

import json
import unittest.mock

import dagster
import pandas as pd
import pytest

import src.pipeline.dagster as dagster_lib


def test_ml_inference_gate_config_default_threshold() -> None:
    from src.dagster.inference_job import MlInferenceGateConfig

    config = MlInferenceGateConfig()
    assert config.null_rate_threshold == 0.2


def test_define_inference_jobs_returns_bundle_with_three_assets() -> None:
    from src.dagster.inference_job import InferenceJobBundle, define_inference_jobs

    bundle = define_inference_jobs()
    assert isinstance(bundle, InferenceJobBundle)
    assert len(bundle.assets) == 3
    assert len(bundle.jobs) == 1
    assert len(bundle.sensors) == 1


def test_ml_daily_inference_sensor_monitors_fact_ml_feature_set() -> None:
    from src.dagster.inference_job import define_inference_jobs

    bundle = define_inference_jobs()
    sensor = bundle.sensors[0]
    assert sensor.name == "ml_daily_inference_sensor"
    # In the installed Dagster version (1.13.10), multi_asset_sensor's public
    # `asset_selection` reflects `request_assets` (unset here), not
    # `monitored_assets` — it is always None unless request_assets is passed
    # explicitly. `_monitored_assets` is the only attribute that reflects what
    # was passed to `monitored_assets=`, so we check it directly here instead
    # of the brief's originally-proposed `str(sensor.asset_selection)` check.
    assert dagster.AssetKey(["MART", "FACT_ML_FEATURE_SET"]) in sensor._monitored_assets


def test_ml_daily_inference_sensor_evaluates() -> None:
    """Behavioral test mirroring test_transform_job.py's sensor-evaluation test.

    Verifies the sensor's `_evaluation_fn` actually produces a RunRequest with the
    expected `run_key`/`job_name` when FACT_ML_FEATURE_SET materializes, rather than
    only checking static sensor config.
    """
    from src.dagster.inference_job import (
        _FACT_ML_FEATURE_SET_KEY,
        define_inference_jobs,
    )

    bundle = define_inference_jobs()
    sensor_def = bundle.sensors[0]

    mock_key = _FACT_ML_FEATURE_SET_KEY
    mock_event = unittest.mock.MagicMock(spec=dagster.EventLogRecord)
    mock_event.storage_id = 42
    mock_materialization = dagster.AssetMaterialization(
        asset_key=mock_key,
        metadata={"trading_date": dagster.MetadataValue.text("2026-07-03")},
    )

    mock_fetch = unittest.mock.MagicMock(
        return_value=[(mock_key, mock_event, mock_materialization)]
    )

    # Real bundle assets/job require AWS/Redshift resources; stub with a dummy
    # asset job sharing the real job's name so the sensor's job association
    # resolves without pulling in those resources.
    @dagster_lib.asset(key=dagster.AssetKey(["ML", "ML_DATA_QUALITY_GATE"]))
    def dummy_asset() -> None:
        pass

    dummy_job = dagster_lib.define_asset_job(
        "ml_daily_inference_job", selection=[dummy_asset]
    )
    defs = dagster.Definitions(assets=[dummy_asset], jobs=[dummy_job])
    context = dagster.build_multi_asset_sensor_context(
        monitored_assets=[mock_key],
        definitions=defs,
        instance=dagster.DagsterInstance.ephemeral(),
    )

    # Unlike test_transform_job.py's sensor (which sets `job_name=` explicitly
    # in each RunRequest), ml_daily_inference_sensor is bound to a single
    # `job=` at decoration time, so Dagster resolves the target job via
    # `sensor_def.job_name` rather than via `RunRequest.job_name` (which stays
    # None for single-job sensors).
    with unittest.mock.patch(
        "src.dagster.inference_job.dagster_lib.fetch_materializations", mock_fetch
    ):
        result = sensor_def.evaluate_tick(context)

    assert sensor_def.job_name == "ml_daily_inference_job"
    assert len(result.run_requests) == 1
    assert result.run_requests[0].run_key == "ml_daily_inference_42"  # gitleaks:allow


def test_define_inference_jobs_asset_keys() -> None:
    from src.dagster.inference_job import define_inference_jobs

    bundle = define_inference_jobs()
    keys = {a.key for a in bundle.assets}
    assert dagster.AssetKey(["ML", "ML_DATA_QUALITY_GATE"]) in keys
    assert dagster.AssetKey(["ML", "ML_DAILY_FORECAST"]) in keys
    assert dagster.AssetKey(["ML", "ML_PUBLISH_FORECAST_RESULTS"]) in keys


def test_ml_data_quality_gate_depends_on_fact_ml_feature_set() -> None:
    from src.dagster.inference_job import define_inference_jobs

    bundle = define_inference_jobs()
    gate_key = dagster.AssetKey(["ML", "ML_DATA_QUALITY_GATE"])
    gate_asset = next(a for a in bundle.assets if a.key == gate_key)
    assert (
        dagster.AssetKey(["MART", "FACT_ML_FEATURE_SET"]) in gate_asset.dependency_keys
    )


def test_ml_daily_forecast_depends_on_gate() -> None:
    from src.dagster.inference_job import define_inference_jobs

    bundle = define_inference_jobs()
    forecast_key = dagster.AssetKey(["ML", "ML_DAILY_FORECAST"])
    forecast_asset = next(a for a in bundle.assets if a.key == forecast_key)
    gate_key = dagster.AssetKey(["ML", "ML_DATA_QUALITY_GATE"])
    assert gate_key in forecast_asset.dependency_keys


def test_ml_publish_depends_on_forecast() -> None:
    from src.dagster.inference_job import define_inference_jobs

    bundle = define_inference_jobs()
    publish_key = dagster.AssetKey(["ML", "ML_PUBLISH_FORECAST_RESULTS"])
    publish_asset = next(a for a in bundle.assets if a.key == publish_key)
    forecast_key = dagster.AssetKey(["ML", "ML_DAILY_FORECAST"])
    assert forecast_key in publish_asset.dependency_keys


def test_ml_data_quality_gate_raises_on_null_rate_breach() -> None:
    from src.dagster.inference_job import MlInferenceGateConfig, ml_data_quality_gate
    from src.ml.config import SEQUENCE_FEATURE_COLUMNS, TABULAR_FEATURE_COLUMNS

    row = {"TRADING_DATE": "2026-07-03", "TICKER": "AAA"}
    for column in SEQUENCE_FEATURE_COLUMNS + TABULAR_FEATURE_COLUMNS:
        row[column] = None
    df = pd.DataFrame([row])
    mock_redshift = unittest.mock.MagicMock()
    mock_redshift.get_connection.return_value.__enter__.return_value = (
        unittest.mock.MagicMock()
    )
    context = dagster.build_asset_context()

    with unittest.mock.patch("src.dagster.inference_job.pd.read_sql", return_value=df):
        with pytest.raises(ValueError, match="Data quality gate failed"):
            ml_data_quality_gate(context, MlInferenceGateConfig(), mock_redshift)


def test_ml_data_quality_gate_passes_and_returns_trading_date() -> None:
    from src.dagster.inference_job import MlInferenceGateConfig, ml_data_quality_gate
    from src.ml.config import SEQUENCE_FEATURE_COLUMNS, TABULAR_FEATURE_COLUMNS

    row = {"TRADING_DATE": "2026-07-03", "TICKER": "AAA"}
    for column in SEQUENCE_FEATURE_COLUMNS + TABULAR_FEATURE_COLUMNS:
        row[column] = 1.0
    df = pd.DataFrame([row])
    mock_redshift = unittest.mock.MagicMock()
    mock_redshift.get_connection.return_value.__enter__.return_value = (
        unittest.mock.MagicMock()
    )
    context = dagster.build_asset_context()

    with unittest.mock.patch("src.dagster.inference_job.pd.read_sql", return_value=df):
        result = ml_data_quality_gate(context, MlInferenceGateConfig(), mock_redshift)

    assert result.value == "2026-07-03"


def _build_ticker_block(ticker: str, end_date: str) -> pd.DataFrame:
    """Build WINDOW_SIZE dummy feature rows for one ticker, ending on end_date."""
    from src.ml.config import (
        SEQUENCE_FEATURE_COLUMNS,
        TABULAR_FEATURE_COLUMNS,
        WINDOW_SIZE,
    )

    dates = pd.date_range(end=end_date, periods=WINDOW_SIZE)
    rows = []
    for date in dates:
        row = {"TICKER": ticker, "TRADING_DATE": date}
        for column in SEQUENCE_FEATURE_COLUMNS + TABULAR_FEATURE_COLUMNS:
            row[column] = 1.0
        rows.append(row)
    return pd.DataFrame(rows)


def test_ml_daily_forecast_runs_batch_transform_successfully() -> None:
    from src.dagster.inference_job import ml_daily_forecast

    df = pd.concat(
        [
            _build_ticker_block("AAA", "2026-07-03"),
            _build_ticker_block("BBB", "2026-07-03"),
        ],
        ignore_index=True,
    )

    mock_redshift = unittest.mock.MagicMock()
    mock_redshift.get_connection.return_value.__enter__.return_value = (
        unittest.mock.MagicMock()
    )
    mock_ssm = unittest.mock.MagicMock()
    mock_ssm.get_parameter.side_effect = lambda name: {
        "/finops/model/active_version": "v1",
    }.get(name)

    mock_sagemaker = unittest.mock.MagicMock()
    mock_sagemaker.model_artifacts_bucket = "finops-model-artifacts-dev"

    mock_s3bucket = unittest.mock.MagicMock()
    mock_s3bucket.raw_bucket = "finops-raw-bucket-dev"

    mock_s3 = unittest.mock.MagicMock()
    mock_s3_client = unittest.mock.MagicMock()
    mock_s3.get_client.return_value = mock_s3_client

    # serve.py now echoes ticker alongside predicted_return (Task 2).
    def mock_download(bucket, key, local_path):
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(json.dumps({"ticker": "AAA", "predicted_return": 0.05}) + "\n")
            f.write(json.dumps({"ticker": "BBB", "predicted_return": -0.02}) + "\n")

    mock_s3_client.download_file.side_effect = mock_download

    context = dagster.build_asset_context()

    with unittest.mock.patch("src.dagster.inference_job.pd.read_sql", return_value=df):
        result = ml_daily_forecast(
            context,
            "2026-07-03",  # Friday -> next_trading_day = Monday 2026-07-06
            mock_redshift,
            mock_ssm,
            mock_sagemaker,
            mock_s3bucket,
            mock_s3,
        )

    assert result.value["trading_date"] == "2026-07-06"
    assert result.value["model_version"] == "v1"
    assert len(result.value["results"]) == 2
    assert result.value["results"][0] == {"ticker": "AAA", "predicted_return": 0.05}
    assert result.value["results"][1] == {"ticker": "BBB", "predicted_return": -0.02}
    assert result.value["output_s3_uri"] == (
        "s3://finops-raw-bucket-dev/ml-inference-output/2026-07-03/input.jsonl.out"
    )

    mock_sagemaker.create_model_if_not_exists.assert_called_once_with(
        model_name="v1",
        model_data_s3_uri="s3://finops-model-artifacts-dev/finops-multimodal-regressor/v1/model.tar.gz",
        inference_image=unittest.mock.ANY,
    )
    mock_sagemaker.run_batch_transform_job.assert_called_once()


def test_ml_daily_forecast_raises_when_no_valid_tickers() -> None:
    from src.dagster.inference_job import ml_daily_forecast

    # Ticker rỗng
    df = pd.DataFrame(columns=["TICKER", "TRADING_DATE"])

    mock_redshift = unittest.mock.MagicMock()
    mock_redshift.get_connection.return_value.__enter__.return_value = (
        unittest.mock.MagicMock()
    )
    mock_ssm = unittest.mock.MagicMock()
    mock_ssm.get_parameter.side_effect = lambda name: {
        "/finops/model/active_version": "v1",
    }.get(name)

    mock_sagemaker = unittest.mock.MagicMock()
    mock_sagemaker.model_artifacts_bucket = "finops-model-artifacts-dev"
    mock_s3bucket = unittest.mock.MagicMock()
    mock_s3 = unittest.mock.MagicMock()

    context = dagster.build_asset_context()

    with unittest.mock.patch("src.dagster.inference_job.pd.read_sql", return_value=df):
        with pytest.raises(ValueError, match="No tickers had valid features"):
            ml_daily_forecast(
                context,
                "2026-07-03",
                mock_redshift,
                mock_ssm,
                mock_sagemaker,
                mock_s3bucket,
                mock_s3,
            )


def test_ml_publish_forecast_results_copies_then_publishes() -> None:
    from src.dagster.inference_job import ml_publish_forecast_results

    mock_cursor = unittest.mock.MagicMock()
    mock_conn = unittest.mock.MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_redshift = unittest.mock.MagicMock()
    mock_redshift.get_connection.return_value.__enter__.return_value = mock_conn
    mock_load_config = unittest.mock.MagicMock()
    mock_load_config.iam_role_arn = "arn:aws:iam::role"
    context = dagster.build_asset_context()

    forecast_result = {
        "trading_date": "2026-07-06",
        "model_version": "v1",
        "results": [{"ticker": "AAA", "predicted_return": 0.05}],
        "output_s3_uri": "s3://bucket/ml-inference-output/2026-07-03/input.jsonl.out",
    }

    result = ml_publish_forecast_results(
        context, forecast_result, mock_redshift, mock_load_config
    )

    assert result.value == 1
    queries = [call.args[0] for call in mock_cursor.execute.call_args_list]
    assert any("COPY" in q for q in queries)
    assert any("DELETE FROM MART.FCT_ML_FORECAST_RESULTS" in q for q in queries)
    assert any("INSERT INTO MART.FCT_ML_FORECAST_RESULTS" in q for q in queries)


def test_ml_publish_forecast_results_raises_on_empty_results() -> None:
    from src.dagster.inference_job import ml_publish_forecast_results

    mock_redshift = unittest.mock.MagicMock()
    mock_load_config = unittest.mock.MagicMock()
    context = dagster.build_asset_context()
    forecast_result = {
        "trading_date": "2026-07-06",
        "model_version": "v1",
        "results": [],
        "output_s3_uri": "s3://bucket/ml-inference-output/2026-07-03/input.jsonl.out",
    }

    with pytest.raises(ValueError, match="No forecast results"):
        ml_publish_forecast_results(
            context, forecast_result, mock_redshift, mock_load_config
        )
