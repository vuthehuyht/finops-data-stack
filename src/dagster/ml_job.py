"""Dagster assets and job for the ML quarterly re-training pipeline."""

import datetime
import json
from dataclasses import dataclass, field

import dagster
import pydantic
from dagster_aws.s3 import S3Resource

import src.pipeline.dagster as dagster_lib
from src.common.s3_util import split_s3_url
from src.dagster.resources import (
    LoadJobConfigResource,
    RedshiftResource,
    S3BucketResource,
    SageMakerResource,
    SsmParameterResource,
)
from src.ml.data_export import unload_training_dataset
from src.ml.evaluation import (
    compare_and_promote,
    extract_metadata_from_tarball,
    model_version_prefix,
)
from src.ml.training_job import launch_training_job

_ACTIVE_VERSION_PARAM = "/finops/model/active_version"
_EVALUATION_THRESHOLD_PARAM = "/finops/model/evaluation_threshold"

_TRAINING_DATASET_ASSET_KEY = dagster_lib.asset_key(["ML", "GOLD_ML_TRAINING_DATASET"])
_TRAINING_JOB_ASSET_KEY = dagster_lib.asset_key(["ML", "ML_TRAINING_JOB"])
_MODEL_EVALUATION_ASSET_KEY = dagster_lib.asset_key(["ML", "ML_MODEL_EVALUATION"])


@dataclass
class MlJobBundle:
    """Return value of define_ml_jobs() — consumed by workspace.py."""

    assets: list[dagster.AssetsDefinition] = field(default_factory=list)
    jobs: list[dagster.JobDefinition] = field(default_factory=list)


class MlTrainingDatasetConfig(dagster.Config):
    """Runtime config for the training dataset export asset."""

    run_date: str = pydantic.Field(
        default_factory=lambda: datetime.date.today().isoformat(),
        description="Date stamp used to namespace the exported S3 prefix.",
    )


class MlTrainingJobConfig(dagster.Config):
    """Hyperparameters forwarded to the SageMaker training job."""

    # Widened past WINDOW_SIZE=30 trading rows: 90 calendar days is roughly
    # 65 VN trading days (5-day week), comfortably above the minimum window
    # size the dataset needs to produce even one training sample per ticker.
    train_end_date: str = pydantic.Field(
        default_factory=lambda: (
            datetime.date.today() - datetime.timedelta(days=180)
        ).isoformat()
    )
    val_end_date: str = pydantic.Field(
        default_factory=lambda: (
            datetime.date.today() - datetime.timedelta(days=90)
        ).isoformat()
    )
    epochs: int = pydantic.Field(default=20)
    batch_size: int = pydantic.Field(default=64)
    learning_rate: float = pydantic.Field(default=1e-3)


class MlEvaluationConfig(dagster.Config):
    """Runtime config for the model evaluation & promotion asset."""

    evaluation_threshold: float = pydantic.Field(
        default=0.0,
        description=(
            "Fallback promotion threshold used if SSM "
            f"{_EVALUATION_THRESHOLD_PARAM} is not set."
        ),
    )


@dagster_lib.asset(
    key=_TRAINING_DATASET_ASSET_KEY,
    group_name="ML",
    kinds={"python", "redshift", "s3"},
    description=(
        "Export the last 24 months of FACT_ML_FEATURE_SET from Redshift "
        "to S3 as Parquet, for SageMaker Training Job input."
    ),
)
def gold_ml_training_dataset(
    context: dagster.AssetExecutionContext,
    config: MlTrainingDatasetConfig,
    redshift: RedshiftResource,
    s3bucket: S3BucketResource,
    load_config: LoadJobConfigResource,
) -> dagster.Output[str]:
    """Export FACT_ML_FEATURE_SET (last 24 months) to S3 as Parquet."""
    s3_url = f"s3://{s3bucket.raw_bucket}/ml-training-data/{config.run_date}/"
    with redshift.get_connection() as conn:
        with conn.cursor() as cursor:
            row_count = unload_training_dataset(
                cursor=cursor,
                s3_url=s3_url,
                iam_role_arn=load_config.iam_role_arn,
            )
        conn.commit()
    context.log.info("Exported %s rows to %s.", row_count, s3_url)
    return dagster.Output(
        value=s3_url,
        metadata={"s3_url": s3_url, "row_count": row_count},
    )


@dagster_lib.asset(
    key=_TRAINING_JOB_ASSET_KEY,
    group_name="ML",
    kinds={"python", "sagemaker"},
    ins={"training_dataset_s3_url": dagster.AssetIn(key=_TRAINING_DATASET_ASSET_KEY)},
    description="Launch a SageMaker Training Job for the multimodal regressor.",
)
def ml_training_job(
    context: dagster.AssetExecutionContext,
    training_dataset_s3_url: str,
    config: MlTrainingJobConfig,
    sagemaker: SageMakerResource,
) -> dagster.Output[dict[str, str]]:
    """Launch and block on a SageMaker Training Job using the exported dataset."""
    result = launch_training_job(
        role_arn=sagemaker.execution_role_arn,
        input_s3_uri=training_dataset_s3_url,
        hyperparameters={
            "train-end-date": config.train_end_date,
            "val-end-date": config.val_end_date,
            "epochs": str(config.epochs),
            "batch-size": str(config.batch_size),
            "learning-rate": str(config.learning_rate),
        },
    )
    context.log.info(
        "Training job %s finished, model_data=%s.",
        result.job_name,
        result.model_data_s3_uri,
    )
    return dagster.Output(
        value={
            "job_name": result.job_name,
            "model_data_s3_uri": result.model_data_s3_uri,
        },
        metadata={
            "job_name": result.job_name,
            "model_data_s3_uri": result.model_data_s3_uri,
        },
    )


@dagster_lib.asset(
    key=_MODEL_EVALUATION_ASSET_KEY,
    group_name="ML",
    kinds={"python", "s3", "ssm"},
    ins={"training_job_result": dagster.AssetIn(key=_TRAINING_JOB_ASSET_KEY)},
    description=(
        "Version the trained model on S3 and promote it to active if it "
        "beats the current Champion by the configured threshold."
    ),
)
def ml_model_evaluation(
    context: dagster.AssetExecutionContext,
    training_job_result: dict[str, str],
    config: MlEvaluationConfig,
    s3: S3Resource,
    sagemaker: SageMakerResource,
    ssm: SsmParameterResource,
) -> dagster.Output[bool]:
    """Version the model artifact and decide Champion/Challenger promotion."""
    version = training_job_result["job_name"]
    model_data_s3_uri = training_job_result["model_data_s3_uri"]

    s3_client = s3.get_client()
    (source_bucket, source_key) = split_s3_url(model_data_s3_uri)
    tarball_bytes = s3_client.get_object(Bucket=source_bucket, Key=source_key)[
        "Body"
    ].read()
    challenger_metadata = extract_metadata_from_tarball(tarball_bytes)
    challenger_metrics = challenger_metadata["metrics"]

    target_bucket = sagemaker.model_artifacts_bucket
    version_prefix = model_version_prefix(version)
    s3_client.copy_object(
        Bucket=target_bucket,
        Key=f"{version_prefix}model.tar.gz",
        CopySource={"Bucket": source_bucket, "Key": source_key},
    )
    s3_client.put_object(
        Bucket=target_bucket,
        Key=f"{version_prefix}metadata.json",
        Body=json.dumps(challenger_metadata).encode("utf-8"),
    )

    champion_version = ssm.get_parameter(_ACTIVE_VERSION_PARAM)
    champion_metrics = None
    if champion_version is not None:
        champion_key = f"{model_version_prefix(champion_version)}metadata.json"
        champion_body = s3_client.get_object(Bucket=target_bucket, Key=champion_key)[
            "Body"
        ].read()
        champion_metrics = json.loads(champion_body)["metrics"]

    threshold_param = ssm.get_parameter(_EVALUATION_THRESHOLD_PARAM)
    threshold = (
        float(threshold_param)
        if threshold_param is not None
        else config.evaluation_threshold
    )

    promoted = compare_and_promote(challenger_metrics, champion_metrics, threshold)
    if promoted:
        ssm.put_parameter(_ACTIVE_VERSION_PARAM, version)
        context.log.info("Promoted model version %s to active (Champion).", version)
    else:
        context.log.info(
            "Model version %s did not beat Champion %s; not promoted.",
            version,
            champion_version,
        )

    return dagster.Output(
        value=promoted,
        metadata={
            "version": version,
            "promoted": promoted,
            "challenger_rmse": challenger_metrics["rmse"],
            "champion_version": champion_version or "",
        },
    )


def define_ml_jobs() -> MlJobBundle:
    """Define the ML quarterly re-training assets and job."""
    assets: list[dagster.AssetsDefinition] = [
        gold_ml_training_dataset,
        ml_training_job,
        ml_model_evaluation,
    ]
    job = dagster_lib.define_asset_job(
        "ml_quarterly_retrain_job",
        selection=[
            _TRAINING_DATASET_ASSET_KEY,
            _TRAINING_JOB_ASSET_KEY,
            _MODEL_EVALUATION_ASSET_KEY,
        ],
        tags={"type": "ml"},
    )
    return MlJobBundle(assets=assets, jobs=[job])
