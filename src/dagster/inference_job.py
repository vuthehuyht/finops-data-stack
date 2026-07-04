"""Dagster assets, job, and sensor for the ML daily inference pipeline."""

import datetime
import json
import os
import tempfile
import time
from collections.abc import Iterator
from dataclasses import dataclass, field

import dagster
import pandas as pd
import pydantic
from dagster_aws.s3 import S3Resource

import src.pipeline.dagster as dagster_lib
from src.dagster.resources import (
    LoadJobConfigResource,
    RedshiftResource,
    S3BucketResource,
    SageMakerResource,
    SsmParameterResource,
)
from src.ml.config import (
    SEQUENCE_FEATURE_COLUMNS,
    TABULAR_FEATURE_COLUMNS,
    WINDOW_SIZE,
)
from src.ml.evaluation import model_version_prefix
from src.ml.forecast_publish import publish_forecast_results
from src.ml.inference import (
    build_latest_window,
    check_feature_null_rate,
    next_trading_day,
)

_FEATURE_TABLE = "MART.FACT_ML_FEATURE_SET"
_ACTIVE_VERSION_PARAM = "/finops/model/active_version"
_INFERENCE_IMAGE = (
    "763104351884.dkr.ecr.ap-southeast-1.amazonaws.com/pytorch-inference:2.2-cpu-py310"
)
_LOOKBACK_DAYS = 90  # comfortably covers WINDOW_SIZE=30 trading days

_DATA_QUALITY_GATE_ASSET_KEY = dagster_lib.asset_key(["ML", "ML_DATA_QUALITY_GATE"])
_DAILY_FORECAST_ASSET_KEY = dagster_lib.asset_key(["ML", "ML_DAILY_FORECAST"])
_PUBLISH_FORECAST_RESULTS_ASSET_KEY = dagster_lib.asset_key(
    ["ML", "ML_PUBLISH_FORECAST_RESULTS"]
)
_FACT_ML_FEATURE_SET_KEY = dagster.AssetKey(["MART", "FACT_ML_FEATURE_SET"])


def _validate_iso_date(value: str) -> str:
    """Round-trip through date.fromisoformat to guard against SQL injection."""
    return datetime.date.fromisoformat(str(value)[:10]).isoformat()


@dataclass
class InferenceJobBundle:
    """Return value of define_inference_jobs() — consumed by workspace.py."""

    assets: list[dagster.AssetsDefinition] = field(default_factory=list)
    jobs: list[dagster.JobDefinition] = field(default_factory=list)
    sensors: list[dagster.SensorDefinition] = field(default_factory=list)


class MlInferenceGateConfig(dagster.Config):
    """Runtime config for the data quality gate asset."""

    null_rate_threshold: float = pydantic.Field(
        default=0.2,
        description="Max acceptable null rate per feature column (0.0-1.0).",
    )


@dagster_lib.asset(
    key=_DATA_QUALITY_GATE_ASSET_KEY,
    group_name="ML",
    kinds={"python", "redshift"},
    deps=[_FACT_ML_FEATURE_SET_KEY],
    description=(
        "Gate inference on FACT_ML_FEATURE_SET null rates for the latest trading date."
    ),
)
def ml_data_quality_gate(
    context: dagster.AssetExecutionContext,
    config: MlInferenceGateConfig,
    redshift: RedshiftResource,
) -> dagster.Output[str]:
    """Check the latest trading date's feature null rates; fail fast if unhealthy."""
    with redshift.get_connection() as conn:
        df = pd.read_sql(
            f"""
            SELECT * FROM {_FEATURE_TABLE}
            WHERE TRADING_DATE = (SELECT MAX(TRADING_DATE) FROM {_FEATURE_TABLE})
            """,
            conn,
        )

    if len(df) == 0:
        raise ValueError(f"{_FEATURE_TABLE} has no rows; cannot run inference.")

    trading_date = _validate_iso_date(df["TRADING_DATE"].iloc[0])
    null_rates = check_feature_null_rate(
        df,
        SEQUENCE_FEATURE_COLUMNS + TABULAR_FEATURE_COLUMNS,
        config.null_rate_threshold,
    )
    max_null_rate = max(null_rates.values()) if null_rates else 0.0
    context.log.info(
        "Data quality gate passed for %s (%s tickers).", trading_date, len(df)
    )
    return dagster.Output(
        value=trading_date,
        metadata={
            "trading_date": trading_date,
            "ticker_count": len(df),
            "max_null_rate": max_null_rate,
        },
    )


@dagster_lib.asset(
    key=_DAILY_FORECAST_ASSET_KEY,
    group_name="ML",
    kinds={"python", "redshift", "sagemaker", "s3"},
    ins={"trading_date": dagster.AssetIn(key=_DATA_QUALITY_GATE_ASSET_KEY)},
    description=(
        "Run SageMaker Batch Transform (Serverless Batch) to forecast "
        "LABEL_NEXT_5D_RETURN for each ticker."
    ),
)
def ml_daily_forecast(  # noqa: C901
    context: dagster.AssetExecutionContext,
    trading_date: str,
    redshift: RedshiftResource,
    ssm: SsmParameterResource,
    sagemaker: SageMakerResource,
    s3bucket: S3BucketResource,
    s3: S3Resource,
) -> dagster.Output[dict]:
    """Forecast every ticker on `trading_date` using SageMaker Batch Transform."""
    model_version = ssm.get_parameter(_ACTIVE_VERSION_PARAM)
    if model_version is None:
        raise ValueError(f"SSM parameter {_ACTIVE_VERSION_PARAM} is not set.")

    # 1. Đảm bảo model đã được đăng ký trên SageMaker
    model_data_s3_uri = (
        f"s3://{sagemaker.model_artifacts_bucket}/"
        f"{model_version_prefix(model_version)}model.tar.gz"
    )
    try:
        sagemaker.create_model_if_not_exists(
            model_name=model_version,
            model_data_s3_uri=model_data_s3_uri,
            inference_image=_INFERENCE_IMAGE,
        )
    except Exception as exc:
        raise RuntimeError(f"Failed to ensure SageMaker model exists: {exc}") from exc

    validated_date = _validate_iso_date(trading_date)
    lower_bound = (
        datetime.date.fromisoformat(validated_date)
        - datetime.timedelta(days=_LOOKBACK_DAYS)
    ).isoformat()
    with redshift.get_connection() as conn:
        df = pd.read_sql(
            f"""
            SELECT * FROM {_FEATURE_TABLE}
            WHERE TRADING_DATE <= '{validated_date}'
              AND TRADING_DATE > '{lower_bound}'
            """,
            conn,
        )

    tickers = sorted(
        df.loc[df["TRADING_DATE"] == pd.Timestamp(validated_date), "TICKER"].unique()
    )

    results = []
    with tempfile.TemporaryDirectory() as tmpdir:
        local_input_path = os.path.join(tmpdir, "input.jsonl")

        valid_tickers = []
        with open(local_input_path, "w", encoding="utf-8") as f_in:
            for ticker in tickers:
                try:
                    sequence, tabular = build_latest_window(df, ticker, WINDOW_SIZE)
                    payload = {
                        "ticker": ticker,
                        "sequence": sequence.tolist(),
                        "tabular": tabular.tolist(),
                    }
                    f_in.write(json.dumps(payload) + "\n")
                    valid_tickers.append(ticker)
                except Exception as exc:
                    context.log.warning(
                        "Skip preparing features for ticker %s: %s", ticker, exc
                    )

        if not valid_tickers:
            raise ValueError("No tickers had valid features to forecast.")

        # 3. Upload file JSONL lên S3
        input_key = f"ml-inference-input/{validated_date}/input.jsonl"
        s3_client = s3.get_client()
        s3_client.upload_file(local_input_path, s3bucket.raw_bucket, input_key)

        # 4. Chạy Batch Transform Job
        input_s3_uri = f"s3://{s3bucket.raw_bucket}/{input_key}"
        output_prefix = f"ml-inference-output/{validated_date}/"
        output_s3_uri = f"s3://{s3bucket.raw_bucket}/{output_prefix}"
        job_name = f"finops-forecast-{validated_date}-{int(time.time())}"

        context.log.info("Starting SageMaker Batch Transform Job: %s", job_name)
        try:
            sagemaker.run_batch_transform_job(
                job_name=job_name,
                model_name=model_version,
                input_s3_uri=input_s3_uri,
                output_s3_uri=output_s3_uri,
            )
        except Exception as exc:
            raise RuntimeError(f"Failed to run Batch Transform Job: {exc}") from exc

        # 5. Tải kết quả đầu ra về local
        output_key = f"{output_prefix}input.jsonl.out"
        local_output_path = os.path.join(tmpdir, "output.jsonl.out")
        s3_client.download_file(s3bucket.raw_bucket, output_key, local_output_path)

        # 6. Đọc kết quả — output.jsonl.out is now self-contained
        # ({"ticker": ..., "predicted_return": ...} per line), no position
        # matching against valid_tickers needed.
        with open(local_output_path, encoding="utf-8") as f_out:
            for line_out in f_out:
                if not line_out.strip():
                    continue
                try:
                    prediction = json.loads(line_out)
                    results.append(
                        {
                            "ticker": prediction["ticker"],
                            "predicted_return": prediction["predicted_return"],
                        }
                    )
                except Exception as exc:
                    context.log.warning("Failed to parse a prediction line: %s", exc)

    if not results:
        raise ValueError(f"All {len(tickers)} tickers failed inference; aborting.")

    anchor_date = datetime.date.fromisoformat(validated_date)
    forecast_trading_date = next_trading_day(anchor_date).isoformat()

    context.log.info("Forecasted %s/%s tickers.", len(results), len(tickers))
    return dagster.Output(
        value={
            "trading_date": forecast_trading_date,
            "model_version": model_version,
            "results": results,
            "output_s3_uri": f"{output_s3_uri}input.jsonl.out",
        },
        metadata={
            "trading_date": forecast_trading_date,
            "model_version": model_version,
            "success_count": len(results),
            "ticker_count": len(tickers),
        },
    )


@dagster_lib.asset(
    key=_PUBLISH_FORECAST_RESULTS_ASSET_KEY,
    group_name="ML",
    kinds={"python", "redshift"},
    ins={"forecast_result": dagster.AssetIn(key=_DAILY_FORECAST_ASSET_KEY)},
    description=(
        "COPY Batch Transform forecast output into Redshift Gold "
        "(FCT_ML_FORECAST_RESULTS)."
    ),
)
def ml_publish_forecast_results(
    context: dagster.AssetExecutionContext,
    forecast_result: dict,
    redshift: RedshiftResource,
    load_config: LoadJobConfigResource,
) -> dagster.Output[int]:
    """COPY the forecast output file into Redshift, replacing rows for the date."""
    trading_date = _validate_iso_date(forecast_result["trading_date"])
    model_version = forecast_result["model_version"]
    results = forecast_result["results"]
    output_s3_uri = forecast_result["output_s3_uri"]

    if not results:
        raise ValueError("No forecast results to publish.")

    with redshift.get_connection() as conn:
        with conn.cursor() as cursor:
            publish_forecast_results(
                cursor=cursor,
                s3_url=output_s3_uri,
                iam_role_arn=load_config.iam_role_arn,
                trading_date=trading_date,
                model_version=model_version,
            )
        conn.commit()

    context.log.info("Published %s forecast rows for %s.", len(results), trading_date)
    return dagster.Output(
        value=len(results),
        metadata={"trading_date": trading_date, "row_count": len(results)},
    )


def define_inference_jobs() -> InferenceJobBundle:
    """Define the ML daily inference assets, job, and triggering sensor."""
    assets: list[dagster.AssetsDefinition] = [
        ml_data_quality_gate,
        ml_daily_forecast,
        ml_publish_forecast_results,
    ]
    job = dagster_lib.define_asset_job(
        "ml_daily_inference_job",
        selection=[
            _DATA_QUALITY_GATE_ASSET_KEY,
            _DAILY_FORECAST_ASSET_KEY,
            _PUBLISH_FORECAST_RESULTS_ASSET_KEY,
        ],
        tags={"type": "ml"},
    )

    @dagster.multi_asset_sensor(
        monitored_assets=[_FACT_ML_FEATURE_SET_KEY],
        job=job,
        name="ml_daily_inference_sensor",
        minimum_interval_seconds=60,
        description=(
            "Trigger the ML daily inference job when FACT_ML_FEATURE_SET materializes."
        ),
    )
    def ml_daily_inference_sensor(
        context: dagster.MultiAssetSensorEvaluationContext,
    ) -> Iterator[dagster.RunRequest]:
        for key, asset_event, _materialization in dagster_lib.fetch_materializations(
            context, fetch_limit_for_each_asset=1
        ):
            context.advance_cursor({key: asset_event})
            yield dagster.RunRequest(
                run_key=f"ml_daily_inference_{asset_event.storage_id}"
            )

    return InferenceJobBundle(
        assets=assets, jobs=[job], sensors=[ml_daily_inference_sensor]
    )
