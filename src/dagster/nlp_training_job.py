"""Dagster assets and manual job: fine-tune, evaluate, promote."""

import json
from dataclasses import dataclass, field

import botocore.exceptions
import dagster
import pydantic
from dagster_aws.s3 import S3Resource

import src.pipeline.dagster as dagster_lib
from src.common.s3_util import split_s3_url
from src.dagster.resources import SageMakerResource, SsmParameterResource
from src.dagster.retry_policies import SAGEMAKER_RETRY
from src.ml.evaluation import extract_metadata_from_tarball
from src.nlp.evaluation import (
    ACTIVE_VERSION_PARAM,
    model_version_prefix,
    promotion_basis,
    promotion_score,
    should_promote,
)
from src.nlp.training_job import launch_nlp_training_job

_TRAINING_JOB_ASSET_KEY = dagster_lib.asset_key(["NLP", "NLP_TRAINING_JOB"])
_MODEL_EVALUATION_ASSET_KEY = dagster_lib.asset_key(["NLP", "NLP_MODEL_EVALUATION"])


@dataclass
class NlpTrainingJobBundle:
    """Assets and jobs for workspace.py."""

    assets: list[dagster.AssetsDefinition] = field(default_factory=list)
    jobs: list[dagster.JobDefinition] = field(default_factory=list)


class NlpTrainingJobConfig(dagster.Config):
    """Runtime config for the fine-tuning job."""

    input_s3_uri: str = pydantic.Field(
        description="S3 prefix with train.csv, val.csv and optional eval_domain.csv."
    )
    epochs: int = pydantic.Field(default=3)
    batch_size: int = pydantic.Field(default=16)
    learning_rate: float = pydantic.Field(default=2e-5)


class NlpEvaluationConfig(dagster.Config):
    """Runtime config for the promotion decision."""

    baseline_score: float | None = pydantic.Field(
        default=None,
        description=(
            "Pretrained model's macro-F1 on the same eval set; the bar to beat "
            "while no champion exists (unset: first model always promotes)."
        ),
    )


@dagster_lib.asset(
    key=_TRAINING_JOB_ASSET_KEY,
    group_name="NLP",
    kinds={"python", "sagemaker"},
    description="Fine-tune the sentiment model on SageMaker.",
)
def nlp_training_job(
    context: dagster.AssetExecutionContext,
    config: NlpTrainingJobConfig,
    sagemaker: SageMakerResource,
) -> dagster.Output[dict[str, str]]:
    """Run the fine-tuning job."""
    result = launch_nlp_training_job(
        role_arn=sagemaker.execution_role_arn,
        input_s3_uri=config.input_s3_uri,
        hyperparameters={
            "epochs": str(config.epochs),
            "batch-size": str(config.batch_size),
            "learning-rate": str(config.learning_rate),
        },
        model_artifacts_bucket=sagemaker.model_artifacts_bucket,
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
    group_name="NLP",
    kinds={"python", "s3", "ssm"},
    ins={"training_job_result": dagster.AssetIn(key=_TRAINING_JOB_ASSET_KEY)},
    description="Version the model on S3; promote it if it beats the champion.",
)
def nlp_model_evaluation(
    context: dagster.AssetExecutionContext,
    training_job_result: dict[str, str],
    config: NlpEvaluationConfig,
    s3: S3Resource,
    sagemaker: SageMakerResource,
    ssm: SsmParameterResource,
) -> dagster.Output[bool]:
    """Version the artifact and decide promotion."""
    version = training_job_result["job_name"]
    source_bucket, source_key = split_s3_url(training_job_result["model_data_s3_uri"])

    s3_client = s3.get_client()
    tarball_bytes = s3_client.get_object(Bucket=source_bucket, Key=source_key)[
        "Body"
    ].read()
    challenger_metadata = extract_metadata_from_tarball(tarball_bytes)
    if challenger_metadata.get("model_version") != version:
        # Rows carry the artifact's model_version; a mismatch breaks rescoring.
        context.log.warning(
            "Artifact metadata model_version %r differs from job name %r.",
            challenger_metadata.get("model_version"),
            version,
        )

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

    champion_version = ssm.get_parameter(ACTIVE_VERSION_PARAM)
    champion_metadata = None
    if champion_version is not None and champion_version != "none":
        champion_key = f"{model_version_prefix(champion_version)}metadata.json"
        try:
            champion_metadata = json.loads(
                s3_client.get_object(Bucket=target_bucket, Key=champion_key)[
                    "Body"
                ].read()
            )
        except botocore.exceptions.ClientError as exc:
            if exc.response["Error"]["Code"] != "NoSuchKey":
                raise
            context.log.warning(
                "Champion metadata not found for %s; treating as no champion.",
                champion_version,
            )

    challenger_score = promotion_score(challenger_metadata)
    if champion_metadata is not None:
        if promotion_basis(champion_metadata) != promotion_basis(challenger_metadata):
            context.log.warning(
                "Champion scored on '%s' but challenger on '%s'; scores are not "
                "comparable, so the challenger is not promoted.",
                promotion_basis(champion_metadata),
                promotion_basis(challenger_metadata),
            )
            promoted = False
        else:
            promoted = should_promote(
                challenger_score, promotion_score(champion_metadata)
            )
    else:
        # No champion: the pretrained model is serving.
        if config.baseline_score is None:
            context.log.warning(
                "No baseline_score configured; promoting the first fine-tuned "
                "model without comparing it to the pretrained model."
            )
        promoted = should_promote(challenger_score, config.baseline_score)

    if promoted:
        ssm.put_parameter(ACTIVE_VERSION_PARAM, version)
        context.log.info("Promoted NLP model version %s to active.", version)
    else:
        context.log.info(
            "NLP model version %s not promoted (champion=%s).",
            version,
            champion_version,
        )

    return dagster.Output(
        value=promoted,
        metadata={
            "version": version,
            "promoted": promoted,
            "challenger_score": challenger_score,
            "score_basis": promotion_basis(challenger_metadata),
            "champion_version": champion_version or "",
        },
    )


def define_nlp_training_jobs() -> NlpTrainingJobBundle:
    """Define the fine-tuning assets and manual job."""
    job = dagster_lib.define_asset_job(
        "nlp_finetune_job",
        selection=[_TRAINING_JOB_ASSET_KEY, _MODEL_EVALUATION_ASSET_KEY],
        # Training is expensive: retry once at most.
        op_retry_policy=SAGEMAKER_RETRY,
        k8s_config={
            "pod_spec_config": {
                "node_selector": {"karpenter.sh/capacity-type": "on-demand"}
            }
        },
        tags={"type": "nlp"},
    )
    return NlpTrainingJobBundle(
        assets=[nlp_training_job, nlp_model_evaluation], jobs=[job]
    )
