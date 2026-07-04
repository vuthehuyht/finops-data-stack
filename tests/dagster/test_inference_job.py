"""Tests for inference_job.py."""

import unittest.mock

import dagster
import pandas as pd
import pytest


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


def test_define_inference_jobs_asset_keys() -> None:
    from src.dagster.inference_job import define_inference_jobs

    bundle = define_inference_jobs()
    keys = {a.key for a in bundle.assets}
    assert dagster.AssetKey(["ML", "ML_DATA_QUALITY_GATE"]) in keys
    assert dagster.AssetKey(["ML", "ML_DAILY_FORECAST"]) in keys
    assert dagster.AssetKey(["ML", "ML_PUBLISH_FORECAST_RESULTS"]) in keys


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


def test_ml_daily_forecast_skips_ticker_on_endpoint_failure_but_continues() -> None:
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
        "/finops/model/endpoint_name": "finops-endpoint",
        "/finops/model/active_version": "v1",
    }.get(name)
    mock_runtime = unittest.mock.MagicMock()
    mock_runtime.invoke_endpoint.side_effect = [
        {"predicted_return": 0.05},
        RuntimeError("boom"),
    ]
    context = dagster.build_asset_context()

    with unittest.mock.patch("src.dagster.inference_job.pd.read_sql", return_value=df):
        result = ml_daily_forecast(
            context, "2026-07-03", mock_redshift, mock_ssm, mock_runtime
        )

    assert result.value["trading_date"] == "2026-07-03"
    assert result.value["model_version"] == "v1"
    assert len(result.value["results"]) == 1
    assert result.value["results"][0]["ticker"] == "AAA"


def test_ml_daily_forecast_raises_when_all_tickers_fail() -> None:
    from src.dagster.inference_job import ml_daily_forecast

    df = _build_ticker_block("AAA", "2026-07-03")

    mock_redshift = unittest.mock.MagicMock()
    mock_redshift.get_connection.return_value.__enter__.return_value = (
        unittest.mock.MagicMock()
    )
    mock_ssm = unittest.mock.MagicMock()
    mock_ssm.get_parameter.side_effect = lambda name: {
        "/finops/model/endpoint_name": "finops-endpoint",
        "/finops/model/active_version": "v1",
    }.get(name)
    mock_runtime = unittest.mock.MagicMock()
    mock_runtime.invoke_endpoint.side_effect = RuntimeError("boom")
    context = dagster.build_asset_context()

    with unittest.mock.patch("src.dagster.inference_job.pd.read_sql", return_value=df):
        with pytest.raises(ValueError, match="failed inference"):
            ml_daily_forecast(
                context, "2026-07-03", mock_redshift, mock_ssm, mock_runtime
            )


def test_ml_publish_forecast_results_deletes_then_inserts() -> None:
    from src.dagster.inference_job import ml_publish_forecast_results

    mock_cursor = unittest.mock.MagicMock()
    mock_conn = unittest.mock.MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_redshift = unittest.mock.MagicMock()
    mock_redshift.get_connection.return_value.__enter__.return_value = mock_conn
    context = dagster.build_asset_context()

    forecast_result = {
        "trading_date": "2026-07-03",
        "model_version": "v1",
        "results": [{"ticker": "AAA", "predicted_return": 0.05}],
    }

    result = ml_publish_forecast_results(context, forecast_result, mock_redshift)

    assert result.value == 1
    delete_call = mock_cursor.execute.call_args_list[0]
    assert "DELETE FROM MART.FCT_ML_FORECAST_RESULTS" in delete_call.args[0]
    assert delete_call.args[1] == ("2026-07-03",)
    insert_call = mock_cursor.execute.call_args_list[1]
    assert "INSERT INTO MART.FCT_ML_FORECAST_RESULTS" in insert_call.args[0]
    assert insert_call.args[1][:4] == ("AAA", "2026-07-03", 0.05, "v1")
    mock_conn.commit.assert_called_once()


def test_ml_publish_forecast_results_raises_on_empty_results() -> None:
    from src.dagster.inference_job import ml_publish_forecast_results

    mock_redshift = unittest.mock.MagicMock()
    context = dagster.build_asset_context()
    forecast_result = {
        "trading_date": "2026-07-03",
        "model_version": "v1",
        "results": [],
    }

    with pytest.raises(ValueError, match="No forecast results"):
        ml_publish_forecast_results(context, forecast_result, mock_redshift)
