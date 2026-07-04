"""Dagster assets, job, and sensor for the ML daily inference pipeline."""

import datetime
from dataclasses import dataclass, field

import dagster
import pandas as pd
import pydantic

import src.pipeline.dagster as dagster_lib
from src.common.redshift_util import execute_query
from src.dagster.resources import (
    RedshiftResource,
    SageMakerRuntimeResource,
    SsmParameterResource,
)
from src.ml.config import (
    SEQUENCE_FEATURE_COLUMNS,
    TABULAR_FEATURE_COLUMNS,
    WINDOW_SIZE,
)
from src.ml.inference import build_latest_window, check_feature_null_rate

_FEATURE_TABLE = "MART.FACT_ML_FEATURE_SET"
_FORECAST_TABLE = "MART.FCT_ML_FORECAST_RESULTS"
_ENDPOINT_NAME_PARAM = "/finops/model/endpoint_name"
_ACTIVE_VERSION_PARAM = "/finops/model/active_version"
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
    kinds={"python", "redshift", "sagemaker"},
    ins={"trading_date": dagster.AssetIn(key=_DATA_QUALITY_GATE_ASSET_KEY)},
    description=(
        "Call the SageMaker endpoint once per ticker to forecast LABEL_NEXT_5D_RETURN."
    ),
)
def ml_daily_forecast(
    context: dagster.AssetExecutionContext,
    trading_date: str,
    redshift: RedshiftResource,
    ssm: SsmParameterResource,
    sagemaker_runtime: SageMakerRuntimeResource,
) -> dagster.Output[dict]:
    """Forecast every ticker on `trading_date`; tolerate per-ticker failures."""
    endpoint_name = ssm.get_parameter(_ENDPOINT_NAME_PARAM)
    if endpoint_name is None:
        raise ValueError(f"SSM parameter {_ENDPOINT_NAME_PARAM} is not set.")
    model_version = ssm.get_parameter(_ACTIVE_VERSION_PARAM) or "unknown"

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
    results: list[dict] = []
    for ticker in tickers:
        try:
            sequence, tabular = build_latest_window(df, ticker, WINDOW_SIZE)
            payload = {"sequence": sequence.tolist(), "tabular": tabular.tolist()}
            response = sagemaker_runtime.invoke_endpoint(endpoint_name, payload)
            results.append(
                {"ticker": ticker, "predicted_return": response["predicted_return"]}
            )
        except Exception as exc:
            # Per-ticker tolerance is intentional (spec §4.2/§9): one bad
            # ticker must not abort the whole run. Logged, not swallowed.
            context.log.warning("Forecast failed for ticker %s: %s", ticker, exc)

    if not results:
        raise ValueError(f"All {len(tickers)} tickers failed inference; aborting.")

    context.log.info("Forecasted %s/%s tickers.", len(results), len(tickers))
    return dagster.Output(
        value={
            "trading_date": validated_date,
            "model_version": model_version,
            "results": results,
        },
        metadata={
            "trading_date": validated_date,
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
        "Write daily forecast results to Redshift Gold (FCT_ML_FORECAST_RESULTS)."
    ),
)
def ml_publish_forecast_results(
    context: dagster.AssetExecutionContext,
    forecast_result: dict,
    redshift: RedshiftResource,
) -> dagster.Output[int]:
    """Delete any existing rows for the trading date, then insert fresh results."""
    trading_date = _validate_iso_date(forecast_result["trading_date"])
    model_version = forecast_result["model_version"]
    results = forecast_result["results"]

    if not results:
        raise ValueError("No forecast results to publish.")

    with redshift.get_connection() as conn:
        with conn.cursor() as cursor:
            execute_query(
                cursor,
                f"DELETE FROM {_FORECAST_TABLE} WHERE TRADING_DATE = %s",
                (trading_date,),
            )
            for result in results:
                execute_query(
                    cursor,
                    f"""
                    INSERT INTO {_FORECAST_TABLE}
                        (TICKER, TRADING_DATE, PREDICTED_RETURN, MODEL_VERSION,
                         DATACORE_CREATE_DATETIME, DATACORE_CREATE_PROGRAM,
                         DATACORE_CREATE_BY, DATACORE_UPDATE_DATETIME,
                         DATACORE_UPDATE_PROGRAM, DATACORE_UPDATE_BY, BATCH_DATE)
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, %s, CURRENT_USER,
                            CURRENT_TIMESTAMP, %s, CURRENT_USER, %s)
                    """,
                    (
                        result["ticker"],
                        trading_date,
                        result["predicted_return"],
                        model_version,
                        "ml_publish_forecast_results",
                        "ml_publish_forecast_results",
                        trading_date,
                    ),
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

    @dagster.asset_sensor(
        asset_key=_FACT_ML_FEATURE_SET_KEY,
        job=job,
        name="ml_daily_inference_sensor",
        minimum_interval_seconds=60,
    )
    def ml_daily_inference_sensor(
        context: dagster.SensorEvaluationContext,
        asset_event: dagster.EventLogEntry,
    ) -> dagster.RunRequest:
        return dagster.RunRequest(run_key=context.cursor)

    return InferenceJobBundle(
        assets=assets, jobs=[job], sensors=[ml_daily_inference_sensor]
    )
