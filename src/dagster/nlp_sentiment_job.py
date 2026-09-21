"""Dagster assets, job and sensor for news sentiment scoring.

prep (unscored articles -> S3) -> batch transform -> publish (RAW.RAW_NEWS_SENTIMENT).
"""

import datetime
import json
import os
import re
import tempfile
import uuid
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
from src.dagster.retry_policies import INGEST_RETRY, LOAD_RETRY, SAGEMAKER_RETRY
from src.load.load import load_s3_to_redshift
from src.nlp.config import MODEL_ID
from src.nlp.evaluation import ACTIVE_VERSION_PARAM, model_version_prefix

# SageMaker model names allow only alphanumerics and hyphens (max 63).
_MODEL_NAME = f"nlp-sentiment-{MODEL_ID.replace('/', '-')}"[:63]
_MODEL_ARTIFACT_KEY = f"nlp-sentiment/{MODEL_ID.replace('/', '_')}/model.tar.gz"
_TRANSFORM_IMAGE = (
    "763104351884.dkr.ecr.ap-southeast-1.amazonaws.com/pytorch-inference:2.2-cpu-py310"
)
_TRANSFORM_INSTANCE_TYPE = "ml.m5.xlarge"
_TIMEZONE = "Asia/Ho_Chi_Minh"
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_VERSION_PATTERN = re.compile(r"^[A-Za-z0-9._/-]+$")

_STG_NEWS_ARTICLES_KEY = dagster_lib.asset_key(["STAGING", "STG_NEWS_ARTICLES"])
_NLP_SENTIMENT_PREP_KEY = dagster_lib.asset_key(["NLP", "NLP_SENTIMENT_PREP"])
_NLP_BATCH_TRANSFORM_KEY = dagster_lib.asset_key(
    ["NLP", "NLP_SENTIMENT_BATCH_TRANSFORM"]
)
# Key the silver sensor derives for stg_news_sentiment.
_RAW_NEWS_SENTIMENT_KEY = dagster_lib.asset_key(["RAW", "NEWS_SENTIMENT"])


def _validate_iso_date(value: str) -> str:
    """Normalize to YYYY-MM-DD; raises ValueError otherwise."""
    return datetime.date.fromisoformat(str(value)[:10]).isoformat()


def resolve_active_model(active_version: str | None) -> tuple[str, str]:
    """(model_name, artifact_key) of the promoted version, else the pretrained model."""
    if active_version and active_version != "none":
        return (
            f"nlp-{active_version}"[:63].rstrip("-"),
            f"{model_version_prefix(active_version)}model.tar.gz",
        )
    return _MODEL_NAME, _MODEL_ARTIFACT_KEY


def _staging_schema() -> str:
    """Staging schema from the env var dbt uses; must be a plain identifier."""
    schema = os.getenv("REDSHIFT_STAGING_SCHEMA", "staging")
    if not _IDENTIFIER_PATTERN.match(schema):
        raise ValueError(f"Invalid staging schema name: {schema!r}")
    return schema


def _serving_model_label(active_version: str | None) -> str:
    """MODEL_VERSION that rows scored by the serving model carry."""
    if active_version and active_version != "none":
        return active_version
    return MODEL_ID


def build_unscored_articles_query(
    staging_schema: str, max_articles: int, stale_model_label: str | None
) -> str:
    """SQL for articles needing a score, newest first, capped at `max_articles`.

    With `stale_model_label`, articles scored by another model are included too.
    Raises ValueError if an argument is unsafe to embed.
    """
    if not _IDENTIFIER_PATTERN.match(staging_schema):
        raise ValueError(f"Invalid staging schema name: {staging_schema!r}")
    limit = int(max_articles)
    if limit <= 0:
        raise ValueError("max_articles must be positive")

    condition = "S.ARTICLE_ID IS NULL"
    if stale_model_label is not None:
        if not _VERSION_PATTERN.match(stale_model_label):
            raise ValueError(f"Invalid model version: {stale_model_label!r}")
        condition += f" OR S.MODEL_VERSION <> '{stale_model_label}'"

    return f"""
        SELECT A.ARTICLE_ID, A.TITLE, A.SUMMARY, A.CONTENT
        FROM {staging_schema}.STG_NEWS_ARTICLES A
        LEFT JOIN {staging_schema}.STG_NEWS_SENTIMENT S
          ON A.ARTICLE_ID = S.ARTICLE_ID
        WHERE A.ARTICLE_ID IS NOT NULL
          AND ({condition})
        ORDER BY A.PUBLISH_TIME DESC
        LIMIT {limit}
    """


def materialization_partition(materialization) -> str | None:
    """Batch date from the native partition, else conata/dbt metadata."""
    if materialization.partition:
        return str(materialization.partition)
    metadata = materialization.metadata
    conata_key = metadata.get("conata_partition_key")
    if conata_key is not None:
        return str(conata_key.value)
    dbt_vars = metadata.get("variables")
    if dbt_vars is not None and isinstance(dbt_vars.value, dict):
        partition = dbt_vars.value.get("partition_key")
        return str(partition) if partition else None
    return None


def build_run_request(storage_id: int, partition: str | None) -> dagster.RunRequest:
    """RunRequest scoring under `partition`; prep falls back to today when None."""
    run_config = None
    if partition:
        run_config = dagster.RunConfig(
            ops={
                nlp_sentiment_prep.op.name: NlpSentimentPrepConfig(
                    batch_date=_validate_iso_date(partition)
                )
            }
        )
    return dagster.RunRequest(
        run_key=f"nlp_sentiment_{storage_id}", run_config=run_config
    )


@dataclass
class NlpSentimentJobBundle:
    """Assets, jobs and sensors for workspace.py."""

    assets: list[dagster.AssetsDefinition] = field(default_factory=list)
    jobs: list[dagster.JobDefinition] = field(default_factory=list)
    sensors: list[dagster.SensorDefinition] = field(default_factory=list)


class NlpSentimentPrepConfig(dagster.Config):
    """Runtime config for the prep asset."""

    batch_date: str = pydantic.Field(
        default="auto",
        description=(
            "Partition date (YYYY-MM-DD) to publish under; the sensor passes "
            "the triggering STG_NEWS_ARTICLES partition."
        ),
    )
    max_articles: int = pydantic.Field(
        default=5000,
        gt=0,
        description="Max articles scored per run (newest first).",
    )
    rescore_stale: bool = pydantic.Field(
        default=False,
        description="Also re-score articles scored by a different model version.",
    )


def build_sentiment_payloads(df: pd.DataFrame) -> list[dict]:
    """Join title/summary/content into {"article_id", "text"}; drop empty ones."""
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
    description="Stage unscored news articles as JSONL on S3.",
    retry_policy=INGEST_RETRY,
)
def nlp_sentiment_prep(
    context: dagster.AssetExecutionContext,
    config: NlpSentimentPrepConfig,
    redshift: RedshiftResource,
    ssm: SsmParameterResource,
    s3bucket: S3BucketResource,
    s3: S3Resource,
) -> dagster.Output[dict]:
    """Write unscored articles to S3 as JSONL."""
    batch_date = (
        pd.Timestamp.now(tz=_TIMEZONE).date().isoformat()
        if config.batch_date == "auto"
        else _validate_iso_date(config.batch_date)
    )

    stale_model_label = (
        _serving_model_label(ssm.get_parameter(ACTIVE_VERSION_PARAM))
        if config.rescore_stale
        else None
    )
    query = build_unscored_articles_query(
        _staging_schema(), config.max_articles, stale_model_label
    )
    with redshift.get_connection() as conn:
        df = pd.read_sql(query, conn)

    payloads = build_sentiment_payloads(df)
    if not payloads:
        context.log.info("No unscored articles with text; nothing to score.")
        # Keep batch_date: publish still emits the partition marker.
        return dagster.Output(
            value={"article_count": 0, "batch_date": batch_date},
            metadata={"article_count": 0, "batch_date": batch_date},
        )

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
    """Run Batch Transform and return the output URI."""
    if prep_result["article_count"] == 0:
        context.log.info("Nothing to score; skipping Batch Transform.")
        return dagster.Output(
            value={"article_count": 0, "batch_date": prep_result["batch_date"]},
            metadata={"article_count": 0, "batch_date": prep_result["batch_date"]},
        )

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

    # Output is named after the input file plus ".out".
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
    """COPY scores via the generic loader (writes the `_CONATA_*` columns dbt needs)."""
    batch_date = _validate_iso_date(transform_result["batch_date"])
    if transform_result["article_count"] == 0:
        # Emit the partition marker even for an empty day.
        context.log.info("No scores to publish for %s.", batch_date)
        return dagster.Output(
            value=0,
            metadata={
                "row_count": 0,
                "batch_date": batch_date,
                "conata_partition_key": batch_date,
            },
        )
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
            # Read by the silver sensor.
            "conata_partition_key": batch_date,
        },
    )


def define_nlp_sentiment_jobs() -> NlpSentimentJobBundle:
    """Define the sentiment assets, job and sensor."""
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
        for key, asset_event, materialization in dagster_lib.fetch_materializations(
            context, fetch_limit_for_each_asset=1
        ):
            context.advance_cursor({key: asset_event})
            yield build_run_request(
                asset_event.storage_id, materialization_partition(materialization)
            )

    return NlpSentimentJobBundle(
        assets=assets, jobs=[job], sensors=[nlp_sentiment_sensor]
    )
