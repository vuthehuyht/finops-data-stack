"""Tests for inference_job.py."""

import json
import unittest.mock
from pathlib import Path

import dagster
import pandas as pd
import pytest

import src.pipeline.dagster as dagster_lib


def _read_inference_sql(df):
    def read(query, conn):
        if "STG_INDEX_PRICE_EOD" in query:
            return df[["trading_date"]].drop_duplicates().sort_values("trading_date")
        if "STG_STOCK_PRICE_EOD" in query:
            return df[["ticker"]].drop_duplicates()
        if "SELECT MAX(TRADING_DATE)" in query:
            return df.loc[df["trading_date"] == df["trading_date"].max()].copy()
        return df.copy()

    return read


def _metadata():
    from src.ml.config import (
        FEATURE_SCHEMA_VERSION,
        SECTOR_VOCAB,
        SEQUENCE_FEATURE_COLUMNS,
        TABULAR_FEATURE_COLUMNS,
    )

    return {
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "sector_vocab": SECTOR_VOCAB,
        "feature_columns": {
            "sequence": SEQUENCE_FEATURE_COLUMNS,
            "tabular": TABULAR_FEATURE_COLUMNS,
        },
        "hyperparameters": {"window_size": 30},
    }


def _run_forecast(df, predictions, monkeypatch):
    from src.dagster.inference_job import ml_daily_forecast

    redshift, ssm, sm, bucket, s3 = [unittest.mock.MagicMock() for _ in range(5)]
    ssm.get_parameter.return_value = "v1"
    sm.model_artifacts_bucket = "artifacts"
    bucket.processed_bucket = "processed"
    client = s3.get_client.return_value
    client.get_object.return_value = {
        "Body": unittest.mock.MagicMock(read=lambda: json.dumps(_metadata()).encode())
    }
    uploaded = {}
    client.upload_file.side_effect = lambda path, bucket, key: uploaded.update(
        {key: Path(path).read_text(encoding="utf-8")}
    )
    client.download_file.side_effect = lambda bucket, key, path: Path(path).write_text(
        "\n".join(json.dumps(p) for p in predictions), encoding="utf-8"
    )
    monkeypatch.setattr(
        "src.dagster.inference_job.pd.read_sql", _read_inference_sql(df)
    )
    result = ml_daily_forecast(
        dagster.build_asset_context(), "2026-07-03", redshift, ssm, sm, bucket, s3
    )
    return result, uploaded


def test_forecast_publishes_only_validated_output(monkeypatch):
    df = pd.concat(
        [_build_ticker_block(t, "2026-07-03") for t in ["AAA", "BBB", "CCC", "DDD"]]
    )
    predictions = [
        {"ticker": t, "predicted_return": 0.05} for t in ["AAA", "BBB", "CCC"]
    ]
    result, uploaded = _run_forecast(
        df, predictions + [{"ticker": "DDD", "predicted_return": None}], monkeypatch
    )
    key = result.value["output_s3_uri"].removeprefix("s3://processed/")
    assert [json.loads(line) for line in uploaded[key].splitlines()] == predictions
    assert result.value["trading_date"] == "2026-07-03"
    assert result.value["horizon_sessions"] == 5
    assert result.metadata["skipped_count"].value == 1


def test_forecast_aborts_when_most_predictions_missing(monkeypatch):
    df = pd.concat([_build_ticker_block(t, "2026-07-03") for t in ["AAA", "BBB"]])
    with pytest.raises(ValueError, match="coverage"):
        _run_forecast(df, [{"ticker": "AAA", "predicted_return": 0.05}], monkeypatch)


@pytest.mark.parametrize("extra", ["AAA", "UNKNOWN"])
def test_forecast_rejects_duplicate_or_unexpected_output_ticker(monkeypatch, extra):
    df = _build_ticker_block("AAA", "2026-07-03")
    with pytest.raises(ValueError, match="ticker"):
        _run_forecast(
            df,
            [{"ticker": t, "predicted_return": 0.05} for t in ["AAA", extra]],
            monkeypatch,
        )


def test_gate_rejects_stale_date(monkeypatch):
    from src.dagster.inference_job import MlInferenceGateConfig, ml_data_quality_gate

    df = _build_ticker_block("AAA", "2026-07-03")
    monkeypatch.setattr(
        "src.dagster.inference_job.pd.read_sql", _read_inference_sql(df)
    )
    with pytest.raises(ValueError, match="date|stale"):
        ml_data_quality_gate(
            dagster.build_asset_context(),
            MlInferenceGateConfig(expected_trading_date="2026-07-06"),
            unittest.mock.MagicMock(),
        )


def test_gate_rejects_partial_snapshot_against_source_universe(monkeypatch):
    from src.dagster.inference_job import MlInferenceGateConfig, ml_data_quality_gate

    df = _build_ticker_block("AAA", "2026-07-03")
    read = _read_inference_sql(df)
    monkeypatch.setattr(
        "src.dagster.inference_job.pd.read_sql",
        lambda q, c: (
            pd.DataFrame({"ticker": ["AAA", "BBB"]})
            if "STG_STOCK_PRICE_EOD" in q
            else read(q, c)
        ),
    )
    with pytest.raises(ValueError, match="coverage"):
        ml_data_quality_gate(
            dagster.build_asset_context(),
            MlInferenceGateConfig(expected_trading_date="2026-07-03"),
            unittest.mock.MagicMock(),
        )


def test_forecast_skips_incomplete_ticker_before_sagemaker(monkeypatch):
    from src.ml.config import TABULAR_FEATURE_COLUMNS

    blocks = [
        _build_ticker_block(t, "2026-07-03") for t in ["AAA", "BBB", "CCC", "DDD"]
    ]
    blocks[-1].loc[29, TABULAR_FEATURE_COLUMNS] = None
    predictions = [
        {"ticker": t, "predicted_return": 0.1} for t in ["AAA", "BBB", "CCC"]
    ]
    result, uploaded = _run_forecast(pd.concat(blocks), predictions, monkeypatch)
    inputs = next(v for k, v in uploaded.items() if k.startswith("ml-inference-input/"))
    assert [json.loads(line)["ticker"] for line in inputs.splitlines()] == [
        "AAA",
        "BBB",
        "CCC",
    ]
    assert "DDD" in result.metadata["skipped_tickers"].data


def test_ml_inference_gate_config_default_threshold() -> None:
    from src.dagster.inference_job import MlInferenceGateConfig

    config = MlInferenceGateConfig()
    assert config.null_rate_threshold == 0.3
    assert config.min_ticker_completeness == 0.7
    assert config.max_incomplete_ticker_ratio == 0.3


def test_inference_image_is_gpu_build() -> None:
    from src.dagster.inference_job import _INFERENCE_IMAGE

    # Batch Transform runs on ml.g4dn.xlarge (GPU); the serving image and
    # serve.py device handling must match — see resources.py::run_batch_transform_job.
    assert _INFERENCE_IMAGE.endswith(":2.2-gpu-py310")


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


def test_inference_asset_retry_policies() -> None:
    """Forecast/publish get retries; the data quality gate must never be retried."""
    from src.dagster.inference_job import define_inference_jobs
    from src.dagster.retry_policies import LOAD_RETRY, SAGEMAKER_RETRY

    bundle = define_inference_jobs()
    by_name = {a.key.path[-1]: a.node_def.retry_policy for a in bundle.assets}
    assert by_name["ML_DATA_QUALITY_GATE"] is None
    assert by_name["ML_DAILY_FORECAST"] == SAGEMAKER_RETRY
    assert by_name["ML_PUBLISH_FORECAST_RESULTS"] == LOAD_RETRY


def test_ml_data_quality_gate_raises_on_null_rate_breach() -> None:
    from src.dagster.inference_job import MlInferenceGateConfig, ml_data_quality_gate
    from src.ml.config import SEQUENCE_FEATURE_COLUMNS, TABULAR_FEATURE_COLUMNS

    row = {"trading_date": "2026-07-03", "ticker": "AAA", "sector": "non_financial"}
    for column in SEQUENCE_FEATURE_COLUMNS + TABULAR_FEATURE_COLUMNS:
        row[column] = None
    df = _build_ticker_block("AAA", "2026-07-03")
    for col, value in row.items():
        if col not in ("ticker", "trading_date", "sector"):
            df[col] = value
    mock_redshift = unittest.mock.MagicMock()
    mock_redshift.get_connection.return_value.__enter__.return_value = (
        unittest.mock.MagicMock()
    )
    context = dagster.build_asset_context()

    with unittest.mock.patch(
        "src.dagster.inference_job.pd.read_sql", side_effect=_read_inference_sql(df)
    ):
        with pytest.raises(ValueError, match="sequence feature"):
            ml_data_quality_gate(
                context,
                MlInferenceGateConfig(expected_trading_date="2026-07-03"),
                mock_redshift,
            )


def test_ml_data_quality_gate_passes_and_returns_trading_date() -> None:
    from src.dagster.inference_job import MlInferenceGateConfig, ml_data_quality_gate
    from src.ml.config import SEQUENCE_FEATURE_COLUMNS, TABULAR_FEATURE_COLUMNS

    row = {"trading_date": "2026-07-03", "ticker": "AAA", "sector": "non_financial"}
    for column in SEQUENCE_FEATURE_COLUMNS + TABULAR_FEATURE_COLUMNS:
        row[column] = 1.0
    df = _build_ticker_block("AAA", "2026-07-03")
    for col, value in row.items():
        if col not in ("ticker", "trading_date", "sector"):
            df[col] = value
    mock_redshift = unittest.mock.MagicMock()
    mock_redshift.get_connection.return_value.__enter__.return_value = (
        unittest.mock.MagicMock()
    )
    context = dagster.build_asset_context()

    with unittest.mock.patch(
        "src.dagster.inference_job.pd.read_sql", side_effect=_read_inference_sql(df)
    ):
        result = ml_data_quality_gate(
            context,
            MlInferenceGateConfig(expected_trading_date="2026-07-03"),
            mock_redshift,
        )

    assert result.value == "2026-07-03"
    assert result.metadata["sector_breakdown"].data == {
        "non_financial": {"tickers": 1, "mean_null_rate": 0.0}
    }


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
        row = {"ticker": ticker, "trading_date": date, "sector": "non_financial"}
        for column in SEQUENCE_FEATURE_COLUMNS + TABULAR_FEATURE_COLUMNS:
            row[column] = 1.0
        rows.append(row)
    return pd.DataFrame(rows)


def test_ml_daily_forecast_runs_batch_transform_successfully(monkeypatch):
    df = pd.concat([_build_ticker_block(t, "2026-07-03") for t in ["AAA", "BBB"]])
    predictions = [
        {"ticker": "AAA", "predicted_return": 0.05},
        {"ticker": "BBB", "predicted_return": -0.02},
    ]
    result, uploaded = _run_forecast(df, predictions, monkeypatch)
    assert result.value["results"] == predictions
    assert result.value["model_version"] == "v1"
    input_key = next(k for k in uploaded if k.startswith("ml-inference-input/"))
    payload = json.loads(uploaded[input_key].splitlines()[0])
    assert set(payload) == {"ticker", "sequence", "tabular", "sector"}
    assert len(payload["sequence"]) == 30
    assert isinstance(payload["tabular"], dict)


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
