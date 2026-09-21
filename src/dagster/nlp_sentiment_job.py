"""Dagster assets, job, and sensor for the NLP news sentiment pipeline.

Chain (mirrors src/dagster/inference_job.py's gate -> run -> publish shape):
nlp_sentiment_prep (unscored articles -> JSONL -> S3) ->
nlp_sentiment_batch_transform (SageMaker Batch Transform) ->
nlp_publish_sentiment_scores (COPY into RAW.RAW_NEWS_SENTIMENT).

The publish asset is keyed RAW/NEWS_SENTIMENT and emits `conata_partition_key`
metadata so the existing silver sensor (transform_job.py) builds
STG_NEWS_SENTIMENT the same way it does for every other RAW table.
"""

import datetime
import json
import os
import tempfile
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field

import dagster
import pandas as pd
from dagster_aws.s3 import S3Resource

import src.pipeline.dagster as dagster_lib
from src.dagster.resources import (
    LoadJobConfigResource,
    RedshiftResource,
    S3BucketResource,
    SageMakerResource,
    SsmParameterResource,
)
from src.dagster.retry_policies import INGEST_RETRY, LOAD_RETRY, SAGEMAKER_RETRY
from src.load.load import load_s3_to_redshift
from src.nlp.config import MODEL_ID
from src.nlp.evaluation import ACTIVE_VERSION_PARAM, model_version_prefix

# SageMaker model names allow only alphanumerics and hyphens (max 63 chars),
# unlike S3 keys, so the "/" in the Hub id becomes "-" here.
_MODEL_NAME = f"nlp-sentiment-{MODEL_ID.replace('/', '-')}"[:63]
_MODEL_ARTIFACT_KEY = f"nlp-sentiment/{MODEL_ID.replace('/', '_')}/model.tar.gz"
# Batch inference on a base-size encoder does not need the GPU image the price
# model uses.
_TRANSFORM_IMAGE = (
    "763104351884.dkr.ecr.ap-southeast-1.amazonaws.com/pytorch-inference:2.2-cpu-py310"
)
_TRANSFORM_INSTANCE_TYPE = "ml.m5.xlarge"
_TIMEZONE = "Asia/Ho_Chi_Minh"

_STG_NEWS_ARTICLES_KEY = dagster_lib.asset_key(["STAGING", "STG_NEWS_ARTICLES"])
_NLP_SENTIMENT_PREP_KEY = dagster_lib.asset_key(["NLP", "NLP_SENTIMENT_PREP"])
_NLP_BATCH_TRANSFORM_KEY = dagster_lib.asset_key(
    ["NLP", "NLP_SENTIMENT_BATCH_TRANSFORM"]
)
# Same key transform_job._get_upstream_bronze_key derives for stg_news_sentiment.
_RAW_NEWS_SENTIMENT_KEY = dagster_lib.asset_key(["RAW", "NEWS_SENTIMENT"])


def resolve_active_model(active_version: str | None) -> tuple[str, str]:
    """Return `(model_name, artifact_key)` for the model that should score articles.

    A promoted fine-tuned version (SSM `ACTIVE_VERSION_PARAM`) wins; unset or
    the "none" sentinel means the pretrained Phase A checkpoint.
    """
    if active_version and active_version != "none":
        return (
            f"nlp-{active_version}"[:63].rstrip("-"),
            f"{model_version_prefix(active_version)}model.tar.gz",
        )
    return _MODEL_NAME, _MODEL_ARTIFACT_KEY


def _validate_iso_date(value: str) -> str:
    """Round-trip through date.fromisoformat to guard against SQL injection."""
    return datetime.date.fromisoformat(str(value)[:10]).isoformat()


@dataclass
class NlpSentimentJobBundle:
    """Return value of define_nlp_sentiment_jobs() — consumed by workspace.py."""

    assets: list[dagster.AssetsDefinition] = field(default_factory=list)
    jobs: list[dagster.JobDefinition] = field(default_factory=list)
    sensors: list[dagster.SensorDefinition] = field(default_factory=list)


class NlpSentimentPrepConfig(dagster.Config):
    """Runtime config for the prep asset."""

    batch_date: str = "auto"


def build_sentiment_payloads(df: pd.DataFrame) -> list[dict]:
    """Turn unscored article rows into `{"article_id", "text"}` payloads.

    Title, summary and content are concatenated so the model sees whatever
    text exists; articles with no usable text at all are dropped rather than
    scored as noise.
    """
    payloads = []
    for row in df.itertuples():
        text = " ".join(
            part.strip()
            for part in (row.title, row.summary, row.content)
            if isinstance(part, str) and part.strip()
        )
        if text:
            payloads.append({"article_id": row.article_id, "text": text})
    return payloads


@dagster_lib.asset(
    key=_NLP_SENTIMENT_PREP_KEY,
    group_name="NLP",
    kinds={"python", "redshift", "s3"},
    deps=[_STG_NEWS_ARTICLES_KEY],
    description=(
        "Gather news articles not yet in STG_NEWS_SENTIMENT and stage them as "
        "JSONL on S3."
    ),
    retry_policy=INGEST_RETRY,
)
def nlp_sentiment_prep(
    context: dagster.AssetExecutionContext,
    config: NlpSentimentPrepConfig,
    redshift: RedshiftResource,
    s3bucket: S3BucketResource,
    s3: S3Resource,
) -> dagster.Output[dict]:
    """Pull unscored articles and write them as JSONL to S3."""
    batch_date = (
        pd.Timestamp.now(tz=_TIMEZONE).date().isoformat()
        if config.batch_date == "auto"
        else _validate_iso_date(config.batch_date)
    )

    with redshift.get_connection() as conn:
        df = pd.read_sql(
            """
            SELECT A.ARTICLE_ID, A.TITLE, A.SUMMARY, A.CONTENT
            FROM STG.STG_NEWS_ARTICLES A
            LEFT JOIN STG.STG_NEWS_SENTIMENT S ON A.ARTICLE_ID = S.ARTICLE_ID
            WHERE S.ARTICLE_ID IS NULL
              AND A.ARTICLE_ID IS NOT NULL
            """,
            conn,
        )

    payloads = build_sentiment_payloads(df)
    if not payloads:
        context.log.info("No unscored articles with text; nothing to score.")
        return dagster.Output(value={"article_count": 0}, metadata={"article_count": 0})

    run_id = uuid.uuid4().hex
    input_key = f"nlp-sentiment-input/{batch_date}/{run_id}/input.jsonl"
    with tempfile.TemporaryDirectory() as tmpdir:
        local_input_path = os.path.join(tmpdir, "input.jsonl")
        with open(local_input_path, "w", encoding="utf-8") as f_in:
            for payload in payloads:
                f_in.write(json.dumps(payload, allow_nan=False) + "\n")
        s3.get_client().upload_file(
            local_input_path, s3bucket.processed_bucket, input_key
        )

    context.log.info("Staged %d unscored articles for %s.", len(payloads), batch_date)
    return dagster.Output(
        value={
            "article_count": len(payloads),
            "input_s3_uri": f"s3://{s3bucket.processed_bucket}/{input_key}",
            "batch_date": batch_date,
            "run_id": run_id,
        },
        metadata={
            "article_count": len(payloads),
            "skipped_no_text": len(df) - len(payloads),
            "batch_date": batch_date,
        },
    )


@dagster_lib.asset(
    key=_NLP_BATCH_TRANSFORM_KEY,
    group_name="NLP",
    kinds={"python", "sagemaker", "s3"},
    ins={"prep_result": dagster.AssetIn(key=_NLP_SENTIMENT_PREP_KEY)},
    description="Score staged articles with SageMaker Batch Transform.",
    retry_policy=SAGEMAKER_RETRY,
)
def nlp_sentiment_batch_transform(
    context: dagster.AssetExecutionContext,
    prep_result: dict,
    sagemaker: SageMakerResource,
    ssm: SsmParameterResource,
    s3bucket: S3BucketResource,
    s3: S3Resource,
) -> dagster.Output[dict]:
    """Run Batch Transform over prep_result's input and return the output URI."""
    if prep_result["article_count"] == 0:
        context.log.info("Nothing to score; skipping Batch Transform.")
        return dagster.Output(value={"article_count": 0}, metadata={"article_count": 0})

    model_name, artifact_key = resolve_active_model(
        ssm.get_parameter(ACTIVE_VERSION_PARAM)
    )
    sagemaker.create_model_if_not_exists(
        model_name=model_name,
        model_data_s3_uri=f"s3://{sagemaker.model_artifacts_bucket}/{artifact_key}",
        inference_image=_TRANSFORM_IMAGE,
    )

    batch_date = prep_result["batch_date"]
    run_id = prep_result["run_id"]
    output_prefix = f"nlp-sentiment-output/{batch_date}/{run_id}/"
    job_name = f"finops-nlp-sentiment-{batch_date}-{run_id[:12]}"

    context.log.info("Starting SageMaker Batch Transform Job: %s", job_name)
    sagemaker.run_batch_transform_job(
        job_name=job_name,
        model_name=model_name,
        input_s3_uri=prep_result["input_s3_uri"],
        output_s3_uri=f"s3://{s3bucket.processed_bucket}/{output_prefix}",
        instance_type=_TRANSFORM_INSTANCE_TYPE,
    )

    # Batch Transform names its output after the input file plus ".out".
    output_key = f"{output_prefix}input.jsonl.out"
    s3.get_client().head_object(Bucket=s3bucket.processed_bucket, Key=output_key)

    return dagster.Output(
        value={
            "article_count": prep_result["article_count"],
            "batch_date": batch_date,
            "output_s3_uri": f"s3://{s3bucket.processed_bucket}/{output_key}",
        },
        metadata={
            "article_count": prep_result["article_count"],
            "batch_date": batch_date,
            "job_name": job_name,
        },
    )


@dagster_lib.asset(
    key=_RAW_NEWS_SENTIMENT_KEY,
    group_name="NLP",
    kinds={"python", "redshift"},
    ins={"transform_result": dagster.AssetIn(key=_NLP_BATCH_TRANSFORM_KEY)},
    description="COPY Batch Transform scores into RAW.RAW_NEWS_SENTIMENT.",
    retry_policy=LOAD_RETRY,
)
def nlp_publish_sentiment_scores(
    context: dagster.AssetExecutionContext,
    transform_result: dict,
    redshift: RedshiftResource,
    load_config: LoadJobConfigResource,
) -> dagster.Output[int]:
    """Load scores through the generic loader so `_CONATA_*` columns get written.

    `latest_source()` in STG_NEWS_SENTIMENT dedups on `_CONATA_LOADED_AT` and
    filters incrementally on `_CONATA_PARTITION_KEY`, so a hand-rolled COPY
    would break the dbt layer.
    """
    if transform_result["article_count"] == 0:
        context.log.info("No scores to publish.")
        return dagster.Output(value=0, metadata={"row_count": 0})

    batch_date = _validate_iso_date(transform_result["batch_date"])
    with redshift.get_connection() as conn:
        with conn.cursor() as cursor:
            rows_loaded = load_s3_to_redshift(
                cursor=cursor,
                s3_url=transform_result["output_s3_uri"],
                table_name="RAW_NEWS_SENTIMENT",
                schema="raw",
                file_format="json",
                iam_role_arn=load_config.iam_role_arn,
                batch_date=batch_date,
            )
        conn.commit()

    context.log.info("Published %s sentiment rows for %s.", rows_loaded, batch_date)
    return dagster.Output(
        value=rows_loaded,
        metadata={
            "row_count": rows_loaded,
            "s3_url": transform_result["output_s3_uri"],
            "batch_date": batch_date,
            # Read by the silver sensor to pick the dbt partition.
            "conata_partition_key": batch_date,
        },
    )


def define_nlp_sentiment_jobs() -> NlpSentimentJobBundle:
    """Define the NLP sentiment assets, job, and triggering sensor."""
    assets: list[dagster.AssetsDefinition] = [
        nlp_sentiment_prep,
        nlp_sentiment_batch_transform,
        nlp_publish_sentiment_scores,
    ]
    job = dagster_lib.define_asset_job(
        "nlp_sentiment_job",
        selection=[
            _NLP_SENTIMENT_PREP_KEY,
            _NLP_BATCH_TRANSFORM_KEY,
            _RAW_NEWS_SENTIMENT_KEY,
        ],
        tags={"type": "nlp"},
    )

    @dagster.multi_asset_sensor(
        monitored_assets=[_STG_NEWS_ARTICLES_KEY],
        job=job,
        name="nlp_sentiment_sensor",
        minimum_interval_seconds=60,
        description=(
            "Trigger the NLP sentiment job when STG_NEWS_ARTICLES materializes."
        ),
    )
    def nlp_sentiment_sensor(
        context: dagster.MultiAssetSensorEvaluationContext,
    ) -> Iterator[dagster.RunRequest]:
        for key, asset_event, _materialization in dagster_lib.fetch_materializations(
            context, fetch_limit_for_each_asset=1
        ):
            context.advance_cursor({key: asset_event})
            yield dagster.RunRequest(run_key=f"nlp_sentiment_{asset_event.storage_id}")

    return NlpSentimentJobBundle(
        assets=assets, jobs=[job], sensors=[nlp_sentiment_sensor]
    )
